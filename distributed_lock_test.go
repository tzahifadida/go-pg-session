package gopgsession

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestDistributedLockConcurrent(t *testing.T) {
	// Setup
	ctx := context.Background()
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a configuration with CreateSchemaIfMissing set to true
	config := DefaultConfig()
	config.CreateSchemaIfMissing = true

	sm, err := NewSessionManager(ctx, config, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	t.Run("ConcurrentLockAcquisition", func(t *testing.T) {
		userID := uuid.New()
		session, err := sm.CreateSession(ctx, userID, nil)
		require.NoError(t, err)

		resource := "test-resource"
		numWorkers := 10

		var wg sync.WaitGroup
		successCount := 0
		failureCount := 0
		var counterMutex sync.Mutex

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				lock := sm.NewDistributedLock(session.ID, resource, nil)
				err := lock.Lock(ctx)
				if err == nil {
					counterMutex.Lock()
					successCount++
					counterMutex.Unlock()
					time.Sleep(time.Second * 10)
					err = lock.Unlock(ctx)
					assert.NoError(t, err)
				} else {
					counterMutex.Lock()
					failureCount++
					counterMutex.Unlock()
				}
			}()
		}

		wg.Wait()
		assert.Equal(t, 1, successCount, "Only one worker should have acquired the lock")
	})

	t.Run("LockReleasedAfterExpiration", func(t *testing.T) {
		userID := uuid.New()
		session, err := sm.CreateSession(ctx, userID, nil)
		require.NoError(t, err)

		resource := "expiring-resource"
		config := &DistributedLockConfig{
			LeaseTime:         500 * time.Millisecond,
			HeartbeatInterval: 1 * time.Second, // Longer than lease time to prevent auto-renewal
		}

		lock1 := sm.NewDistributedLock(session.ID, resource, config)
		err = lock1.Lock(ctx)
		require.NoError(t, err)

		// Wait for the lock to expire
		time.Sleep(600 * time.Millisecond)

		// Try to acquire the lock with a different instance
		lock2 := sm.NewDistributedLock(session.ID, resource, config)
		err = lock2.Lock(ctx)
		assert.NoError(t, err, "Second lock should be acquirable after expiration")

		if err == nil {
			err = lock2.Unlock(ctx)
			assert.NoError(t, err)
		}
	})

	t.Run("ConcurrentLockAndExtend", func(t *testing.T) {
		userID := uuid.New()
		session, err := sm.CreateSession(ctx, userID, nil)
		require.NoError(t, err)

		resource := "extend-resource"
		config := &DistributedLockConfig{
			LeaseTime:         2 * time.Second,
			HeartbeatInterval: 100 * time.Millisecond,
		}

		lock := sm.NewDistributedLock(session.ID, resource, config)
		err = lock.Lock(ctx)
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(2)

		// Goroutine to extend the lease
		extendErrors := make(chan error, 5)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				time.Sleep(750 * time.Millisecond)
				err := lock.ExtendLease(ctx, 2*time.Second)
				if err != nil {
					extendErrors <- err
					return
				}
			}
		}()

		//Goroutine to try acquiring the lock
		acquireErrors := make(chan error, 5)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				time.Sleep(1 * time.Second)
				lock2 := sm.NewDistributedLock(session.ID, resource, config)
				err := lock2.Lock(ctx)
				if err == nil {
					acquireErrors <- fmt.Errorf("should not be able to acquire the lock while it's being extended")
					lock2.Unlock(ctx)
					return
				}
				if !errors.Is(err, ErrLockAlreadyHeld) {
					acquireErrors <- fmt.Errorf("unexpected error while trying to acquire lock: %v", err)
					return
				}
			}
		}()

		wg.Wait()
		close(extendErrors)
		close(acquireErrors)

		for err := range extendErrors {
			t.Errorf("Failed to extend lease: %v", err)
		}

		for err := range acquireErrors {
			t.Error(err)
		}

		err = lock.Unlock(ctx)
		if err != nil {
			t.Errorf("Failed to unlock: %v", err)
		}
	})

	t.Run("MultipleResourceLocks", func(t *testing.T) {
		userID := uuid.New()
		session, err := sm.CreateSession(ctx, userID, nil)
		require.NoError(t, err)

		numResources := 5
		numWorkers := 10
		maxRetries := 20
		retryDelay := 100 * time.Millisecond

		var wg sync.WaitGroup
		successCounts := make([]int, numResources)
		var successMutex sync.Mutex

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < numResources; j++ {
					resource := fmt.Sprintf("resource-%d", j)
					for retry := 0; retry < maxRetries; retry++ {
						lock := sm.NewDistributedLock(session.ID, resource, nil)
						err := lock.Lock(ctx)
						if err == nil {
							successMutex.Lock()
							successCounts[j]++
							successMutex.Unlock()
							time.Sleep(50 * time.Millisecond) // Hold the lock for a bit
							err = lock.Unlock(ctx)
							assert.NoError(t, err)
							break // Successfully acquired and released the lock, move to next resource
						} else if errors.Is(err, ErrLockAlreadyHeld) {
							// If the lock is held, wait before retrying
							time.Sleep(retryDelay)
						} else {
							t.Errorf("Unexpected error acquiring lock: %v", err)
							break
						}
					}
				}
			}()
		}

		wg.Wait()

		totalSuccesses := 0
		for i, count := range successCounts {
			assert.Equal(t, numWorkers, count, fmt.Sprintf("Resource %d should have been locked %d times", i, numWorkers))
			totalSuccesses += count
		}
		assert.Equal(t, numResources*numWorkers, totalSuccesses, "Total number of successful locks should equal number of resources times number of workers")
	})

	t.Run("LockReacquisitionAfterUnlock", func(t *testing.T) {
		userID := uuid.New()
		session, err := sm.CreateSession(ctx, userID, nil)
		require.NoError(t, err)

		resource := "reacquire-resource"

		lock1 := sm.NewDistributedLock(session.ID, resource, nil)
		err = lock1.Lock(ctx)
		require.NoError(t, err)

		err = lock1.Unlock(ctx)
		require.NoError(t, err)

		lock2 := sm.NewDistributedLock(session.ID, resource, nil)
		err = lock2.Lock(ctx)
		assert.NoError(t, err, "Should be able to reacquire the lock after unlocking")

		if err == nil {
			err = lock2.Unlock(ctx)
			assert.NoError(t, err)
		}
	})

	t.Run("HeartbeatStopsAfterUnlock", func(t *testing.T) {
		userID := uuid.New()
		session, err := sm.CreateSession(ctx, userID, nil)
		require.NoError(t, err)

		resource := "heartbeat-stop-resource"
		config := &DistributedLockConfig{
			LeaseTime:         5 * time.Second,
			HeartbeatInterval: 100 * time.Millisecond,
		}

		lock := sm.NewDistributedLock(session.ID, resource, config)
		err = lock.Lock(ctx)
		require.NoError(t, err)

		// Allow some heartbeats to occur
		time.Sleep(500 * time.Millisecond)

		err = lock.Unlock(ctx)
		require.NoError(t, err)

		// Wait a bit to ensure no more heartbeats are sent
		time.Sleep(500 * time.Millisecond)

		// Try to acquire the lock again
		lock2 := sm.NewDistributedLock(session.ID, resource, config)
		err = lock2.Lock(ctx)
		assert.NoError(t, err, "Should be able to acquire the lock if heartbeats have stopped")

		if err == nil {
			err = lock2.Unlock(ctx)
			assert.NoError(t, err)
		}
	})
}
