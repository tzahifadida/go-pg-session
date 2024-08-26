package gopgsession

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"sync"
	"time"
)

const (
	lockAttributeKeyPrefix = "distributed_lock_"
	heartbeatMultiplier    = 2.5
)

var (
	// ErrLockAlreadyHeld is returned when attempting to acquire a lock that is already held by another node.
	ErrLockAlreadyHeld = errors.New("lock is already held by another node")
	// ErrLockNotHeld is returned when attempting to perform an operation on a lock that is not held by the current node.
	ErrLockNotHeld = errors.New("lock is not held by this node")
	// ErrLockExpired is returned when attempting to perform an operation on a lock that has expired.
	ErrLockExpired = errors.New("lock has expired")
)

// DistributedLockConfig holds the configuration options for a DistributedLock.
type DistributedLockConfig struct {
	// MaxRetries is the maximum number of attempts to acquire the lock.
	MaxRetries int
	// RetryDelay is the duration to wait between lock acquisition attempts.
	RetryDelay time.Duration
	// HeartbeatInterval is the duration between heartbeats to maintain the lock.
	HeartbeatInterval time.Duration
	// LeaseTime is the duration for which the lock is considered valid.
	LeaseTime time.Duration
}

// DefaultDistributedLockConfig provides default configuration values for DistributedLock.
var DefaultDistributedLockConfig = DistributedLockConfig{
	MaxRetries:        2,
	RetryDelay:        1 * time.Millisecond,
	HeartbeatInterval: 10 * time.Second,
	LeaseTime:         60 * time.Second,
}

// DistributedLock represents a distributed lock implementation.
type DistributedLock struct {
	sm               *SessionManager
	sessionID        uuid.UUID
	nodeID           uuid.UUID
	resource         string
	heartbeatTicker  *time.Ticker
	mutex            sync.Mutex
	config           DistributedLockConfig
	heartbeatCtx     context.Context
	heartbeatCancel  context.CancelFunc
	expirationTimer  *time.Timer
	leaseExpiration  time.Time
	heartbeatStopped bool
}

// LockInfo holds information about a lock.
type LockInfo struct {
	NodeID        uuid.UUID
	Resource      string
	ExpiresAt     time.Time
	LastHeartbeat time.Time
}

// NewDistributedLock creates a new DistributedLock instance.
//
// Parameters:
//   - sessionID: The UUID of the session associated with this lock.
//   - resource: The name of the resource being locked.
//   - config: Optional configuration for the lock. If nil, default configuration is used.
//
// Returns:
//   - A pointer to the newly created DistributedLock.
func (sm *SessionManager) NewDistributedLock(sessionID uuid.UUID, resource string, config *DistributedLockConfig) *DistributedLock {
	cfg := DefaultDistributedLockConfig
	if config != nil {
		if config.MaxRetries > 0 {
			cfg.MaxRetries = config.MaxRetries
		}
		if config.RetryDelay > 0 {
			cfg.RetryDelay = config.RetryDelay
		}
		if config.HeartbeatInterval > 0 {
			cfg.HeartbeatInterval = config.HeartbeatInterval
		}
		if config.LeaseTime > 0 {
			cfg.LeaseTime = config.LeaseTime
		}
	}

	return &DistributedLock{
		sm:               sm,
		sessionID:        sessionID,
		nodeID:           sm.nodeID,
		resource:         resource,
		config:           cfg,
		heartbeatStopped: true,
	}
}

func (dl *DistributedLock) lockAttributeKey() string {
	return lockAttributeKeyPrefix + dl.resource
}

// Lock attempts to acquire the distributed lock.
//
// Parameters:
//   - ctx: The context for the operation.
//
// Returns:
//   - An error if the lock cannot be acquired, nil otherwise.
func (dl *DistributedLock) Lock(ctx context.Context) error {
	dl.mutex.Lock()
	defer dl.mutex.Unlock()

	for attempt := 0; attempt < dl.config.MaxRetries; attempt++ {
		err := dl.attemptLock(ctx, true)
		if err == nil {
			dl.leaseExpiration = dl.sm.clock.Now().Add(dl.config.LeaseTime)
			dl.startHeartbeat(ctx)
			return nil
		}
		if errors.Is(err, ErrLockAlreadyHeld) {
			return err
		}
		if attempt < dl.config.MaxRetries-1 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(dl.config.RetryDelay):
				// Continue to next iteration
			}
		}
	}

	return fmt.Errorf("failed to acquire lock after %d attempts", dl.config.MaxRetries)
}

func (dl *DistributedLock) attemptLock(ctx context.Context, forceRefresh bool) error {
	now := dl.sm.clock.Now()
	expiresAt := now.Add(dl.config.LeaseTime)

	lockInfo := LockInfo{
		NodeID:        dl.nodeID,
		Resource:      dl.resource,
		ExpiresAt:     expiresAt,
		LastHeartbeat: now,
	}

	var session *Session
	var err error
	if forceRefresh {
		session, err = dl.sm.GetSessionWithVersion(ctx, dl.sessionID, 0, WithForceRefresh())
	} else {
		session, err = dl.sm.GetSessionWithVersion(ctx, dl.sessionID, 0)
	}
	if err != nil {
		return fmt.Errorf("failed to get session: %w", err)
	}

	var currentLockInfo LockInfo
	_, err = session.GetAttributeAndRetainUnmarshaled(dl.lockAttributeKey(), &currentLockInfo)
	if err == nil {
		if now.Before(currentLockInfo.ExpiresAt) {
			if now.Sub(currentLockInfo.LastHeartbeat) <= time.Duration(heartbeatMultiplier*float64(dl.config.LeaseTime)) {
				return ErrLockAlreadyHeld
			}
		}
	} else if !errors.Is(err, ErrAttributeNotFound) {
		return fmt.Errorf("failed to get current lock info: %w", err)
	}

	err = session.UpdateAttribute(dl.lockAttributeKey(), lockInfo, WithExpiresAt(expiresAt))
	if err != nil {
		return fmt.Errorf("failed to update lock attribute: %w", err)
	}

	_, err = dl.sm.UpdateSession(ctx, session, WithCheckAttributeVersion(), WithDoNotNotify())
	if err != nil {
		if errors.Is(err, ErrSessionVersionIsOutdated) {
			return err // This will trigger a retry
		}
		return fmt.Errorf("failed to update session: %w", err)
	}

	return nil
}

func (dl *DistributedLock) startHeartbeat(ctx context.Context) {
	dl.stopHeartbeatLocked()

	heartbeatInterval := dl.config.HeartbeatInterval
	if heartbeatInterval >= dl.config.LeaseTime {
		return
	}

	dl.heartbeatCtx, dl.heartbeatCancel = context.WithCancel(ctx)
	dl.heartbeatTicker = time.NewTicker(heartbeatInterval)
	dl.expirationTimer = time.NewTimer(time.Until(dl.leaseExpiration))
	dl.heartbeatStopped = false

	go func() {
		defer dl.stopHeartbeat()
		for {
			select {
			case <-dl.heartbeatTicker.C:
				dl.mutex.Lock()
				if dl.heartbeatStopped {
					dl.mutex.Unlock()
					return
				}
				err := dl.sendHeartbeat(dl.heartbeatCtx)
				dl.mutex.Unlock()
				if err != nil {
					fmt.Printf("Failed to send heartbeat: %v\n", err)
					return
				}
			case <-dl.expirationTimer.C:
				dl.mutex.Lock()
				fmt.Println("Lock lease expired, stopping heartbeat")
				dl.heartbeatStopped = true
				dl.mutex.Unlock()
				return
			case <-dl.heartbeatCtx.Done():
				return
			}
		}
	}()
}

// Unlock releases the distributed lock.
//
// Parameters:
//   - ctx: The context for the operation.
//
// Returns:
//   - An error if the lock cannot be released, nil otherwise.
func (dl *DistributedLock) Unlock(ctx context.Context) error {
	dl.mutex.Lock()
	defer dl.mutex.Unlock()

	dl.stopHeartbeatLocked()

	session, err := dl.sm.GetSessionWithVersion(ctx, dl.sessionID, 0, WithForceRefresh())
	if err != nil {
		return fmt.Errorf("failed to get session: %w", err)
	}

	var currentLockInfo LockInfo
	_, err = session.GetAttributeAndRetainUnmarshaled(dl.lockAttributeKey(), &currentLockInfo)
	if err != nil {
		if errors.Is(err, ErrAttributeNotFound) {
			return ErrLockNotHeld
		}
		return fmt.Errorf("failed to get current lock info: %w", err)
	}

	if currentLockInfo.NodeID != dl.nodeID || currentLockInfo.Resource != dl.resource {
		return ErrLockNotHeld
	}

	err = session.DeleteAttribute(dl.lockAttributeKey())
	if err != nil {
		return fmt.Errorf("failed to unlock when deleteing an attribute in the session: %w", err)
	}

	_, err = dl.sm.UpdateSession(ctx, session, WithCheckAttributeVersion(), WithDoNotNotify())
	if err != nil {
		return fmt.Errorf("failed to update session: %w", err)
	}

	return nil
}

// ExtendLease extends the lease time of the lock.
//
// Parameters:
//   - ctx: The context for the operation.
//   - extension: The duration by which to extend the lease.
//
// Returns:
//   - An error if the lease cannot be extended, nil otherwise.
func (dl *DistributedLock) ExtendLease(ctx context.Context, extension time.Duration) error {
	dl.mutex.Lock()
	defer dl.mutex.Unlock()

	session, err := dl.sm.GetSessionWithVersion(ctx, dl.sessionID, 0, WithForceRefresh())
	if err != nil {
		return fmt.Errorf("failed to get session: %w", err)
	}

	var currentLockInfo LockInfo
	_, err = session.GetAttributeAndRetainUnmarshaled(dl.lockAttributeKey(), &currentLockInfo)
	if err != nil {
		if errors.Is(err, ErrAttributeNotFound) {
			return ErrLockNotHeld
		}
		return fmt.Errorf("failed to get current lock info: %w", err)
	}

	if currentLockInfo.NodeID != dl.nodeID || currentLockInfo.Resource != dl.resource {
		return ErrLockNotHeld
	}

	now := dl.sm.clock.Now()
	if now.After(dl.leaseExpiration) {
		return ErrLockExpired
	}

	newExpiresAt := dl.leaseExpiration.Add(extension)
	currentLockInfo.ExpiresAt = newExpiresAt
	currentLockInfo.LastHeartbeat = now
	dl.config.LeaseTime += extension
	dl.leaseExpiration = newExpiresAt

	err = session.UpdateAttribute(dl.lockAttributeKey(), currentLockInfo, WithExpiresAt(newExpiresAt))
	if err != nil {
		return fmt.Errorf("failed to update lock attribute: %w", err)
	}

	_, err = dl.sm.UpdateSession(ctx, session, WithCheckAttributeVersion(), WithDoNotNotify())
	if err != nil {
		return fmt.Errorf("failed to update session: %w", err)
	}

	// Update the expiration timer
	if dl.expirationTimer != nil {
		if !dl.expirationTimer.Stop() {
			select {
			case <-dl.expirationTimer.C:
			default:
			}
		}
		dl.expirationTimer.Reset(time.Until(newExpiresAt))
	}

	return nil
}

func (dl *DistributedLock) sendHeartbeat(ctx context.Context) error {
	session, err := dl.sm.GetSessionWithVersion(ctx, dl.sessionID, 0, WithForceRefresh())
	if err != nil {
		return fmt.Errorf("failed to get session: %w", err)
	}

	var currentLockInfo LockInfo
	_, err = session.GetAttributeAndRetainUnmarshaled(dl.lockAttributeKey(), &currentLockInfo)
	if err != nil {
		if errors.Is(err, ErrAttributeNotFound) {
			return ErrLockNotHeld
		}
		return fmt.Errorf("failed to get current lock info: %w", err)
	}

	if currentLockInfo.NodeID != dl.nodeID || currentLockInfo.Resource != dl.resource {
		return ErrLockNotHeld
	}

	now := dl.sm.clock.Now()
	currentLockInfo.LastHeartbeat = now

	err = session.UpdateAttribute(dl.lockAttributeKey(), currentLockInfo, WithExpiresAt(dl.leaseExpiration))
	if err != nil {
		return fmt.Errorf("failed to update lock attribute: %w", err)
	}

	_, err = dl.sm.UpdateSession(ctx, session, WithCheckAttributeVersion(), WithDoNotNotify())
	if err != nil {
		return fmt.Errorf("failed to update session: %w", err)
	}

	return nil
}

func (dl *DistributedLock) stopHeartbeat() {
	dl.mutex.Lock()
	defer dl.mutex.Unlock()
	dl.stopHeartbeatLocked()
}

func (dl *DistributedLock) stopHeartbeatLocked() {
	if !dl.heartbeatStopped {
		if dl.heartbeatCancel != nil {
			dl.heartbeatCancel()
		}
		if dl.heartbeatTicker != nil {
			dl.heartbeatTicker.Stop()
		}
		if dl.expirationTimer != nil {
			dl.expirationTimer.Stop()
		}
		dl.heartbeatStopped = true
	}
}
