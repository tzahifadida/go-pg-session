// Package gopgsession provides a distributed session management library using PostgreSQL.
//
// It implements an eventually consistent memory caching strategy on each node,
// offering a hybrid solution that leverages the benefits of both cookie-based
// and server-side session management. This package is designed for high-performance,
// scalable applications that require robust session handling across multiple nodes.
package gopgsession

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/jonboulle/clockwork"
	"github.com/tzahifadida/pgln"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	NotificationTypeSessionsRemovalFromCache      = "sessions_removal_from_cache"
	NotificationTypeUserSessionsRemovalFromCache  = "user_sessions_removal_from_cache"
	NotificationTypeClearEntireCache              = "clear_entire_cache"
	NotificationTypeGroupSessionsRemovalFromCache = "group_sessions_removal_from_cache"
)

// Logger defines the interface for logging operations.
type Logger interface {
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Config holds the configuration options for the SessionManager.
type Config struct {
	MaxSessions               int           `json:"maxSessions"`
	MaxAttributeLength        int           `json:"maxAttributeLength"`
	SessionExpiration         time.Duration `json:"sessionExpiration"`
	InactivityDuration        time.Duration `json:"inactivityDuration"`
	CleanupInterval           time.Duration `json:"cleanupInterval"`
	CacheSize                 int           `json:"cacheSize"`
	TablePrefix               string        `json:"tablePrefix"`
	SchemaName                string        `json:"schemaName"`
	CreateSchemaIfMissing     bool          `json:"createSchemaIfMissing"`
	LastAccessUpdateInterval  time.Duration `json:"lastAccessUpdateInterval"`
	LastAccessUpdateBatchSize int           `json:"lastAccessUpdateBatchSize"`
	NotifyOnUpdates           bool
	NotifyOnFailedUpdates     bool
	CustomPGLN                *pgln.PGListenNotify `json:"-"`
	MaxSessionLifetimeInCache time.Duration        `json:"maxSessionLifetimeInCache"`
	Logger                    Logger
}

// DefaultConfig returns a Config struct with default values.
func DefaultConfig() *Config {
	return &Config{
		MaxSessions:               5,
		MaxAttributeLength:        16 * 1024,
		SessionExpiration:         30 * 24 * time.Hour,
		InactivityDuration:        48 * time.Hour,
		CleanupInterval:           1 * time.Hour,
		CacheSize:                 1000,
		TablePrefix:               "",
		SchemaName:                "",
		CreateSchemaIfMissing:     false,
		LastAccessUpdateInterval:  10 * time.Minute,
		LastAccessUpdateBatchSize: 5000,
		NotifyOnUpdates:           true,
		NotifyOnFailedUpdates:     false,
		MaxSessionLifetimeInCache: 48 * time.Hour,
		Logger:                    slog.Default(),
	}
}

type SessionAttributeRecord struct {
	SessionID uuid.UUID  `db:"session_id"`
	Key       string     `db:"key"`
	Value     string     `db:"value"`
	ExpiresAt *time.Time `db:"expires_at"`
	Version   int        `db:"version"`
}

type SessionAttributeValue struct {
	Value     any
	Marshaled bool
	ExpiresAt *time.Time
	Version   int
}

type Session struct {
	ID           uuid.UUID  `db:"id"`
	UserID       uuid.UUID  `db:"user_id"`
	GroupID      *uuid.UUID `db:"group_id"`
	LastAccessed time.Time  `db:"last_accessed"`
	ExpiresAt    time.Time  `db:"expires_at"`
	UpdatedAt    time.Time  `db:"updated_at"`
	Version      int        `db:"version"`
	attributes   map[string]SessionAttributeValue
	changed      map[string]bool
	deleted      map[string]bool
	sm           *SessionManager
	fromCache    bool
	invalidated  bool
	groupId      *uuid.UUID
}

// Invalidate marks the session as invalidated
func (s *Session) Invalidate() {
	s.invalidated = true
}

// IsInvalidated returns true if the session has been invalidated
func (s *Session) IsInvalidated() bool {
	return s.invalidated
}

// IsFromCache returns true if the session was loaded from the cache,
// and false if it was loaded from the database table.
func (s *Session) IsFromCache() bool {
	return s.fromCache
}

// IsModified returns true if the session has been modified (attributes changed or deleted).
func (s *Session) IsModified() bool {
	if s.groupId != nil && s.GroupID != nil && *s.groupId != *s.GroupID {
		return true
	} else if s.groupId != s.GroupID {
		return true
	}
	return len(s.changed) > 0 || len(s.deleted) > 0
}

type cacheItem struct {
	session     *Session
	element     *list.Element
	cachedSince time.Time
}

// sessionNotification represents a notification for session updates.
type sessionNotification struct {
	NodeID  uuid.UUID `json:"nodeID"`
	Type    string    `json:"type"`
	Payload []string  `json:"payload"`
}

// SessionManager manages sessions in a PostgreSQL database with caching capabilities.
type SessionManager struct {
	Config                *Config
	cache                 map[uuid.UUID]*cacheItem
	userSessionsIndex     map[uuid.UUID][]uuid.UUID
	lru                   *list.List
	db                    *sqlx.DB
	pgln                  *pgln.PGListenNotify
	mutex                 sync.RWMutex
	shutdownChan          chan struct{}
	wg                    sync.WaitGroup
	nodeID                uuid.UUID
	outOfSync             bool
	lastAccessUpdates     map[uuid.UUID]time.Time
	lastAccessUpdateMutex sync.Mutex
	clock                 clockwork.Clock
	ctx                   context.Context
}

type GetSessionOptions struct {
	DoNotUpdateSessionLastAccess bool
	ForceRefresh                 bool
}

// NewSessionManager creates a new SessionManager with the given configuration and connection string.
func NewSessionManager(ctx context.Context, cfg *Config, db *sql.DB) (*SessionManager, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	} else {
		defaultCfg := DefaultConfig()
		if cfg.Logger == nil {
			cfg.Logger = defaultCfg.Logger
		}
		if cfg.MaxSessions == 0 {
			cfg.MaxSessions = defaultCfg.MaxSessions
		}
		if cfg.MaxAttributeLength == 0 {
			cfg.MaxAttributeLength = defaultCfg.MaxAttributeLength
		}
		if cfg.SessionExpiration == 0 {
			cfg.SessionExpiration = defaultCfg.SessionExpiration
		}
		if cfg.InactivityDuration == 0 {
			cfg.InactivityDuration = defaultCfg.InactivityDuration
		}
		if cfg.CleanupInterval == 0 {
			cfg.CleanupInterval = defaultCfg.CleanupInterval
		}
		if cfg.CacheSize == 0 {
			cfg.CacheSize = defaultCfg.CacheSize
		}
		if cfg.LastAccessUpdateInterval == 0 {
			cfg.LastAccessUpdateInterval = defaultCfg.LastAccessUpdateInterval
		}
		if cfg.LastAccessUpdateBatchSize == 0 {
			cfg.LastAccessUpdateBatchSize = defaultCfg.LastAccessUpdateBatchSize
		}
	}

	sqlxDB := sqlx.NewDb(db, "pgx")

	nodeID, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate node ID: %w", err)
	}

	sm := &SessionManager{
		Config:            cfg,
		cache:             make(map[uuid.UUID]*cacheItem),
		userSessionsIndex: make(map[uuid.UUID][]uuid.UUID),
		lru:               list.New(),
		db:                sqlxDB,
		shutdownChan:      make(chan struct{}),
		nodeID:            nodeID,
		lastAccessUpdates: make(map[uuid.UUID]time.Time),
		clock:             clockwork.NewRealClock(),
		ctx:               ctx,
	}

	if err := sm.checkTables(); err != nil {
		return nil, err
	}

	// Use the custom PGLN if provided, otherwise create a new one
	if cfg.CustomPGLN != nil {
		sm.pgln = cfg.CustomPGLN
	} else {
		builder := pgln.NewPGListenNotifyBuilder().
			SetContext(ctx).
			SetReconnectInterval(1 * time.Second).
			SetDB(db)

		sm.pgln, err = builder.Build()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize pgln: %w", err)
		}

		err = sm.pgln.Start()
		if err != nil {
			return nil, fmt.Errorf("failed to start pgln: %w", err)
		}
	}

	channelName := sm.getChannelName("session_updates")
	err = sm.pgln.ListenAndWaitForListening(channelName, pgln.ListenOptions{
		NotificationCallback: sm.handleNotification,
		ErrorCallback: func(channel string, err error) {
			sm.Config.Logger.Warn(fmt.Sprintf("Warning PGLN error (this may be fine if your loadbalancer or db connection lifetime is preset) on channel %s", channel), "error", err)
			sm.mutex.Lock()
			sm.outOfSync = true
			sm.mutex.Unlock()
		},
		OutOfSyncBlockingCallback: sm.handleOutOfSync,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to listen on channel: %w", err)
	}

	sm.wg.Add(2)
	go sm.cleanupWorker()
	go sm.lastAccessUpdateWorker()

	return sm, nil
}

func (sm *SessionManager) checkTables() error {
	var count int
	query := fmt.Sprintf(`
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = $1 AND table_name = $2
    `)
	schemaName := sm.Config.SchemaName
	if schemaName == "" {
		schemaName = "public"
	}
	err := sm.db.QueryRow(query, schemaName, sm.Config.TablePrefix+"sessions").Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check if tables exist: %w", err)
	}
	if count == 0 {
		if sm.Config.CreateSchemaIfMissing {
			return sm.createSchemaAndTables()
		}
		return fmt.Errorf("required tables are missing. Schema: %s, Table prefix: %s", schemaName, sm.Config.TablePrefix)
	}
	return nil
}

func (sm *SessionManager) createSchemaAndTables() error {
	queries := []string{}

	if sm.Config.SchemaName != "" {
		queries = append(queries, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s";`, sm.Config.SchemaName))
	}

	queries = append(queries, []string{
		fmt.Sprintf(`
            CREATE TABLE IF NOT EXISTS %s (
                "id" UUID PRIMARY KEY,
                "user_id" UUID NOT NULL,
				"group_id" UUID,
                "last_accessed" TIMESTAMP WITH TIME ZONE NOT NULL,
                "expires_at" TIMESTAMP WITH TIME ZONE NOT NULL,
                "updated_at" TIMESTAMP WITH TIME ZONE NOT NULL,
                "version" INTEGER NOT NULL DEFAULT 1
            );`, sm.getTableName("sessions")),
		fmt.Sprintf(`
            CREATE TABLE IF NOT EXISTS %s (
                "session_id" UUID REFERENCES %s("id") ON DELETE CASCADE,
                "key" TEXT NOT NULL,
                "value" TEXT NOT NULL,
                "expires_at" TIMESTAMP WITH TIME ZONE,
                "version" INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY ("session_id", "key")
            );`, sm.getTableName("session_attributes"), sm.getTableName("sessions")),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%ssessions_user_id_idx" ON %s ("user_id");`,
			sm.Config.TablePrefix, sm.getTableName("sessions")),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%ssessions_expires_at_idx" ON %s ("expires_at");`,
			sm.Config.TablePrefix, sm.getTableName("sessions")),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%ssessions_last_accessed_idx" ON %s ("last_accessed");`,
			sm.Config.TablePrefix, sm.getTableName("sessions")),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%ssessions_updated_at_idx" ON %s ("updated_at");`,
			sm.Config.TablePrefix, sm.getTableName("sessions")),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS "%ssession_attributes_expires_at_session_id_idx" ON %s ("expires_at", "session_id");`,
			sm.Config.TablePrefix, sm.getTableName("session_attributes")),
	}...)

	for _, query := range queries {
		_, err := sm.db.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to execute query: %v\nQuery: %s", err, query)
		}
	}

	return nil
}

// CreateSessionOption is a function type that modifies Session during creation
type CreateSessionOption func(*Session)

// WithGroupID sets the GroupID for the session during creation. Can be an account or tenant id. You can also change it before updateSession.
func WithGroupID(groupID uuid.UUID) CreateSessionOption {
	return func(s *Session) {
		s.GroupID = &groupID
		s.groupId = s.GroupID
	}
}

// ... (existing code)

// CreateSession creates a new session for the given user with the provided attributes and optional group ID.
func (sm *SessionManager) CreateSession(ctx context.Context, userID uuid.UUID, attributes map[string]SessionAttributeValue, opts ...CreateSessionOption) (*Session, error) {
	sessionID, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate UUID: %w", err)
	}

	now := sm.clock.Now()
	expiresAt := now.Add(sm.Config.SessionExpiration)

	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	session := &Session{
		ID:           sessionID,
		UserID:       userID,
		LastAccessed: now,
		ExpiresAt:    expiresAt,
		UpdatedAt:    now,
		Version:      1,
		attributes:   attributes,
		changed:      make(map[string]bool),
		deleted:      make(map[string]bool),
		sm:           sm,
	}

	for _, opt := range opts {
		opt(session)
	}

	query := fmt.Sprintf(`
        INSERT INTO %s ("id", "user_id", "group_id", "last_accessed", "expires_at", "updated_at", "version")
        VALUES (:id, :user_id, :group_id, :last_accessed, :expires_at, :updated_at, :version)
    `, sm.getTableName("sessions"))

	_, err = tx.NamedExecContext(ctx, query, session)
	if err != nil {
		return nil, fmt.Errorf("failed to insert session: %w", err)
	}

	attributeVersions := make(map[string]int)
	for key, attr := range attributes {
		var marshaledValue string
		if attr.Marshaled {
			marshaledValue = attr.Value.(string)
		} else {
			marshaledValue, err = convertToString(attr.Value)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal attribute value on create session: %w", err)
			}
		}
		attributeRecord := SessionAttributeRecord{
			SessionID: sessionID,
			Key:       key,
			Value:     marshaledValue,
			ExpiresAt: attr.ExpiresAt,
			Version:   1, // Initial version for new attributes
		}
		query := fmt.Sprintf(`
            INSERT INTO %s ("session_id", "key", "value", "expires_at", "version")
            VALUES (:session_id, :key, :value, :expires_at, :version)
        `, sm.getTableName("session_attributes"))

		_, err = tx.NamedExecContext(ctx, query, attributeRecord)
		if err != nil {
			return nil, fmt.Errorf("failed to insert session attribute: %w", err)
		}
		attributeVersions[key] = 1
	}

	for key, version := range attributeVersions {
		attr := session.attributes[key]
		attr.Version = version
		session.attributes[key] = attr
	}

	sm.addOrUpdateCache(session)

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	go sm.enforceMaxSessions(sm.ctx, userID)

	return session.deepCopy(), nil
}

// SessionOption is a function type that modifies GetSessionOptions
type SessionOption func(*GetSessionOptions)

// WithDoNotUpdateSessionLastAccess sets the DoNotUpdateSessionLastAccess option
func WithDoNotUpdateSessionLastAccess() SessionOption {
	return func(opts *GetSessionOptions) {
		opts.DoNotUpdateSessionLastAccess = true
	}
}

// WithForceRefresh sets the ForceRefresh option
func WithForceRefresh() SessionOption {
	return func(opts *GetSessionOptions) {
		opts.ForceRefresh = true
	}
}

// GetSession retrieves a session by its ID with optional parameters.
func (sm *SessionManager) GetSession(ctx context.Context, sessionID uuid.UUID, options ...SessionOption) (*Session, error) {
	return sm.GetSessionWithVersion(ctx, sessionID, 0, options...)
}

// GetSessionWithVersion retrieves a session by its ID and version with optional parameters.
func (sm *SessionManager) GetSessionWithVersion(ctx context.Context, sessionID uuid.UUID, version int, options ...SessionOption) (*Session, error) {
	opts := GetSessionOptions{}
	for _, option := range options {
		option(&opts)
	}

	if !opts.ForceRefresh {
		sm.mutex.RLock()
		item, exists := sm.cache[sessionID]
		if !sm.outOfSync && exists && item.session.Version >= version {
			if sm.Config.MaxSessionLifetimeInCache > 0 && sm.clock.Now().Sub(item.cachedSince) > sm.Config.MaxSessionLifetimeInCache {
				sm.mutex.RUnlock()
				opts.ForceRefresh = true
			} else {
				// Existing code continues here
				sessionCopy := item.session.deepCopy()
				sessionCopy.fromCache = true
				sm.mutex.RUnlock()
				if !opts.DoNotUpdateSessionLastAccess {
					sm.mutex.Lock()
					if item, exists = sm.cache[sessionID]; exists && item.session.Version >= version {
						now := sm.clock.Now()
						item.session.LastAccessed = now
						sm.lru.MoveToFront(item.element)
						sessionCopy.LastAccessed = now
					}
					sm.mutex.Unlock()
				}
				if exists {
					go sm.updateSessionAccessAsync(sm.ctx, sessionID)
					return sessionCopy, nil
				}
			}
		} else {
			sm.mutex.RUnlock()
		}
	}

	query := fmt.Sprintf(`
        SELECT "id", "user_id", "group_id", "last_accessed", "expires_at", "updated_at", "version"
            FROM %s
            WHERE "id" = $1
    `, sm.getTableName("sessions"))

	session := &Session{}
	err := sm.db.GetContext(ctx, session, query, sessionID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("session not found")
		}
		return nil, fmt.Errorf("failed to get session: %w", err)
	}

	attributes, err := sm.listAttributes(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("failed to list attributes: %w", err)
	}
	session.attributes = attributes
	session.changed = make(map[string]bool)
	session.deleted = make(map[string]bool)
	session.sm = sm

	if !opts.DoNotUpdateSessionLastAccess {
		now := sm.clock.Now()
		session.LastAccessed = now
	}

	session.fromCache = false
	sm.addOrUpdateCache(session)

	if !opts.DoNotUpdateSessionLastAccess {
		sm.updateSessionAccessAsync(sm.ctx, sessionID)
	}

	return session.deepCopy(), nil
}

var ErrSessionVersionIsOutdated = errors.New("session version is outdated")

// UpdateSessionOption is a function type that modifies UpdateSessionOptions
type UpdateSessionOption func(*UpdateSessionOptions)

// UpdateSessionOptions holds the options for updating a session
type UpdateSessionOptions struct {
	CheckVersion          bool
	CheckAttributeVersion bool
	DoNotNotify           bool
}

// WithCheckVersion sets the CheckVersion option to true
func WithCheckVersion() UpdateSessionOption {
	return func(opts *UpdateSessionOptions) {
		opts.CheckVersion = true
	}
}

// WithCheckAttributeVersion sets the CheckAttributeVersion option
func WithCheckAttributeVersion() UpdateSessionOption {
	return func(opts *UpdateSessionOptions) {
		opts.CheckAttributeVersion = true
	}
}

// WithDoNotNotify sets the DoNotNotify option
func WithDoNotNotify() UpdateSessionOption {
	return func(opts *UpdateSessionOptions) {
		opts.DoNotNotify = true
	}
}

// UpdateSession updates the session in the database with any changes made to its attributes.
func (sm *SessionManager) UpdateSession(ctx context.Context, session *Session, options ...UpdateSessionOption) (*Session, error) {
	session = session.deepCopy()
	opts := UpdateSessionOptions{}
	for _, option := range options {
		option(&opts)
	}

	if session.IsInvalidated() {
		return nil, fmt.Errorf("cannot update an invalidated session")
	}

	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Handle updated and new attributes
	for key, changed := range session.changed {
		if changed {
			attr := session.attributes[key]
			var marshaledValue string
			if attr.Marshaled {
				marshaledValue = attr.Value.(string)
			} else {
				marshaledValue, err = convertToString(attr.Value)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal attribute value on update session: %w", err)
				}
			}

			var query string
			var args []interface{}

			if opts.CheckAttributeVersion {
				if attr.Version < 0 {
					// Insert new attribute
					query = fmt.Sprintf(`
                        INSERT INTO %s ("session_id", "key", "value", "expires_at", "version")
                        VALUES ($1, $2, $3, $4, 1)
                        RETURNING "version"
                    `, sm.getTableName("session_attributes"))
					args = []interface{}{session.ID, key, marshaledValue, attr.ExpiresAt}
				} else {
					// Update existing attribute
					query = fmt.Sprintf(`
                        UPDATE %s
                        SET "value" = $1, "expires_at" = $2, "version" = "version" + 1
                        WHERE "session_id" = $3 AND "key" = $4 AND "version" = $5
                        RETURNING "version"
                    `, sm.getTableName("session_attributes"))
					args = []interface{}{marshaledValue, attr.ExpiresAt, session.ID, key, attr.Version}
				}
			} else {
				// Original upsert logic
				query = fmt.Sprintf(`
                    INSERT INTO %s ("session_id", "key", "value", "expires_at", "version")
                    VALUES ($1, $2, $3, $4, 1)
                    ON CONFLICT ("session_id", "key") DO UPDATE
                    SET "value" = EXCLUDED.value, 
                        "expires_at" = EXCLUDED.expires_at, 
                        "version" = %s.version + 1
                    RETURNING "version"
                `, sm.getTableName("session_attributes"), sm.getTableName("session_attributes"))
				args = []interface{}{session.ID, key, marshaledValue, attr.ExpiresAt}
			}

			var newVersion int
			err = tx.QueryRowContext(ctx, query, args...).Scan(&newVersion)

			if err != nil {
				if err == sql.ErrNoRows && opts.CheckAttributeVersion {
					return nil, fmt.Errorf("attribute %s version mismatch or not found", key)
				}
				return nil, fmt.Errorf("failed to update session attribute: %w", err)
			}

			attr.Version = newVersion
			session.attributes[key] = attr
		}
	}

	// Handle deleted attributes
	if len(session.deleted) > 0 {
		var deletedKeys []string
		for key := range session.deleted {
			deletedKeys = append(deletedKeys, key)
		}
		query := fmt.Sprintf(`
            DELETE FROM %s
            WHERE "session_id" = ? AND "key" IN (?)
        `, sm.getTableName("session_attributes"))

		query, args, err := sqlx.In(query, session.ID, deletedKeys)
		if err != nil {
			return nil, fmt.Errorf("failed to expand IN clause: %w", err)
		}
		query = tx.Rebind(query)

		_, err = tx.ExecContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to delete session attributes: %w", err)
		}
	}

	now := sm.clock.Now()
	updateQueryTemplate := `
        UPDATE %s
        SET "updated_at" = $1, "version" = "version" + 1, "group_id" = $3
        WHERE "id" = $2%s 
        RETURNING "id", "user_id", "group_id", "last_accessed", "expires_at", "updated_at", "version"
    `
	var updateQuery string
	var updateQueryRow *sql.Row
	if opts.CheckVersion {
		updateQuery = fmt.Sprintf(updateQueryTemplate, sm.getTableName("sessions"), ` AND "version" = $4`)
		updateQueryRow = tx.QueryRowContext(ctx, updateQuery, now, session.ID, session.GroupID, session.Version)
	} else {
		updateQuery = fmt.Sprintf(updateQueryTemplate, sm.getTableName("sessions"), "")
		updateQueryRow = tx.QueryRowContext(ctx, updateQuery, now, session.ID, session.GroupID)
	}

	var updatedSession Session
	err = updateQueryRow.Scan(
		&updatedSession.ID,
		&updatedSession.UserID,
		&updatedSession.GroupID,
		&updatedSession.LastAccessed,
		&updatedSession.ExpiresAt,
		&updatedSession.UpdatedAt,
		&updatedSession.Version,
	)
	updatedSession.groupId = updatedSession.GroupID
	if err != nil {
		if err == sql.ErrNoRows {
			// Remove the session from the cache
			sm.mutex.Lock()
			if item, exists := sm.cache[session.ID]; exists {
				sm.lru.Remove(item.element)
				delete(sm.cache, session.ID)
			}
			sm.mutex.Unlock()

			// TX is going to fail so we have to use DB instead.
			// Send notification to remove the session from other caches
			if sm.Config.NotifyOnFailedUpdates && sm.Config.NotifyOnUpdates && !opts.DoNotNotify {
				err = sm.sendNotification(sm.db, NotificationTypeSessionsRemovalFromCache, []string{session.ID.String()})
				if err != nil {
					return nil, fmt.Errorf("failed to send notification: %w", err)
				}
			}

			return nil, ErrSessionVersionIsOutdated
		}
		return nil, fmt.Errorf("failed to update session: %w", err)
	}

	if sm.Config.NotifyOnUpdates && !opts.DoNotNotify {
		err = sm.sendNotificationTx(tx, NotificationTypeSessionsRemovalFromCache, []string{session.ID.String()})
		if err != nil {
			return nil, fmt.Errorf("failed to send notification after update: %w", err)
		}
	}

	updatedSession.attributes = session.attributes
	updatedSession.changed = make(map[string]bool)
	updatedSession.deleted = make(map[string]bool)
	updatedSession.sm = sm

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	if updatedSession.Version == session.Version+1 {
		deepCopy := updatedSession.deepCopy()
		sm.addOrUpdateCache(deepCopy)
	} else {
		sm.mutex.Lock()
		if item, exists := sm.cache[updatedSession.ID]; exists {
			sm.lru.Remove(item.element)
			delete(sm.cache, updatedSession.ID)
		}
		sm.mutex.Unlock()
	}

	return &updatedSession, nil
}

// DeleteSession deletes a session by its ID.
func (sm *SessionManager) DeleteSession(ctx context.Context, sessionID uuid.UUID) error {
	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`
        DELETE FROM %s
        WHERE "id" = $1
    `, sm.getTableName("sessions"))

	_, err = tx.ExecContext(ctx, query, sessionID)
	if err != nil {
		return fmt.Errorf("failed to delete session: %w", err)
	}

	err = sm.sendNotificationTx(tx, NotificationTypeSessionsRemovalFromCache, []string{sessionID.String()})
	if err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	sm.mutex.Lock()
	if item, exists := sm.cache[sessionID]; exists {
		sm.removeFromUserSessionsIndex(item.session.UserID, sessionID)
		sm.lru.Remove(item.element)
		delete(sm.cache, sessionID)
	}
	sm.mutex.Unlock()

	return nil
}

// DeleteAllSessionsByGroupID deletes all sessions for a given group ID.
func (sm *SessionManager) DeleteAllSessionsByGroupID(ctx context.Context, groupID uuid.UUID) error {
	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`
        DELETE FROM %s
        WHERE "group_id" = $1
        RETURNING "id"
    `, sm.getTableName("sessions"))

	rows, err := tx.QueryContext(ctx, query, groupID)
	if err != nil {
		return fmt.Errorf("failed to delete group sessions: %w", err)
	}
	defer rows.Close()

	var deletedSessionIDs []uuid.UUID
	for rows.Next() {
		var sessionID uuid.UUID
		if err := rows.Scan(&sessionID); err != nil {
			return fmt.Errorf("failed to scan deleted session ID: %w", err)
		}
		deletedSessionIDs = append(deletedSessionIDs, sessionID)
	}

	sessionIDStrings := make([]string, len(deletedSessionIDs))
	for i, id := range deletedSessionIDs {
		sessionIDStrings[i] = id.String()
	}

	err = sm.sendNotificationTx(tx, NotificationTypeGroupSessionsRemovalFromCache, []string{groupID.String()})
	if err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	sm.mutex.Lock()
	for _, sessionID := range deletedSessionIDs {
		if item, exists := sm.cache[sessionID]; exists {
			sm.removeFromUserSessionsIndex(item.session.UserID, sessionID)
			sm.lru.Remove(item.element)
			delete(sm.cache, sessionID)
		}
	}
	sm.mutex.Unlock()

	return nil
}

// DeleteAllSessions deletes all sessions from the database and cache.
func (sm *SessionManager) DeleteAllSessions(ctx context.Context) error {
	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`
        DELETE FROM %s
        RETURNING "id"
    `, sm.getTableName("sessions"))

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to delete user sessions: %w", err)
	}
	defer rows.Close()

	var deletedSessionIDs []uuid.UUID
	for rows.Next() {
		var sessionID uuid.UUID
		if err := rows.Scan(&sessionID); err != nil {
			return fmt.Errorf("failed to scan deleted session ID: %w", err)
		}
		deletedSessionIDs = append(deletedSessionIDs, sessionID)
	}

	sessionIDStrings := make([]string, len(deletedSessionIDs))
	for i, id := range deletedSessionIDs {
		sessionIDStrings[i] = id.String()
	}

	err = sm.sendNotificationTx(tx, NotificationTypeSessionsRemovalFromCache, sessionIDStrings)
	if err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	sm.mutex.Lock()
	for _, sessionID := range deletedSessionIDs {
		if item, exists := sm.cache[sessionID]; exists {
			sm.removeFromUserSessionsIndex(item.session.UserID, sessionID)
			sm.lru.Remove(item.element)
			delete(sm.cache, sessionID)
		}
	}
	sm.mutex.Unlock()

	return nil
}

// DeleteAllUserSessions deletes all sessions for a given user.
func (sm *SessionManager) DeleteAllUserSessions(ctx context.Context, userID uuid.UUID) error {
	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`
        DELETE FROM %s
        WHERE "user_id" = $1
        RETURNING "id"
    `, sm.getTableName("sessions"))

	rows, err := tx.QueryContext(ctx, query, userID)
	if err != nil {
		return fmt.Errorf("failed to delete user sessions: %w", err)
	}
	defer rows.Close()

	var deletedSessionIDs []uuid.UUID
	for rows.Next() {
		var sessionID uuid.UUID
		if err := rows.Scan(&sessionID); err != nil {
			return fmt.Errorf("failed to scan deleted session ID: %w", err)
		}
		deletedSessionIDs = append(deletedSessionIDs, sessionID)
	}

	sessionIDStrings := make([]string, len(deletedSessionIDs))
	for i, id := range deletedSessionIDs {
		sessionIDStrings[i] = id.String()
	}

	err = sm.sendNotificationTx(tx, NotificationTypeSessionsRemovalFromCache, sessionIDStrings)
	if err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	sm.mutex.Lock()
	for _, sessionID := range deletedSessionIDs {
		if item, exists := sm.cache[sessionID]; exists {
			sm.removeFromUserSessionsIndex(item.session.UserID, sessionID)
			sm.lru.Remove(item.element)
			delete(sm.cache, sessionID)
		}
	}
	sm.mutex.Unlock()

	return nil
}

func (sm *SessionManager) listAttributes(ctx context.Context, sessionID uuid.UUID) (map[string]SessionAttributeValue, error) {
	query := fmt.Sprintf(`
        SELECT "key", "value", "expires_at", "version"
        FROM %s
        WHERE "session_id" = $1
    `, sm.getTableName("session_attributes"))

	rows, err := sm.db.QueryxContext(ctx, query, sessionID)
	if err != nil {
		return nil, fmt.Errorf("failed to query session attributes: %w", err)
	}
	defer rows.Close()

	attributes := make(map[string]SessionAttributeValue)
	for rows.Next() {
		var key, value string
		var expiresAt *time.Time
		var version int
		err := rows.Scan(&key, &value, &expiresAt, &version)
		if err != nil {
			return nil, fmt.Errorf("failed to scan attribute row: %w", err)
		}
		attributes[key] = SessionAttributeValue{Value: value, ExpiresAt: expiresAt, Marshaled: true, Version: version}
	}

	return attributes, nil
}

func convertToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	default:
		marshaledValue, err := json.Marshal(value)
		if err != nil {
			return "", fmt.Errorf("failed to marshal attribute value: %w", err)
		}
		return string(marshaledValue), nil
	}
}

func (sm *SessionManager) handleNotification(channel string, payload string) {
	var notification sessionNotification
	err := json.Unmarshal([]byte(payload), &notification)
	if err != nil {
		sm.Config.Logger.Error("Error unmarshalling notification", "error", err)
		return
	}

	// Ignore our own messages
	if notification.NodeID == sm.nodeID {
		return
	}

	switch notification.Type {
	case NotificationTypeSessionsRemovalFromCache:
		sessionsToRemove := make([]uuid.UUID, 0, len(notification.Payload))
		sm.mutex.RLock()
		for _, sessionIDStr := range notification.Payload {
			sessionID, err := uuid.Parse(sessionIDStr)
			if err != nil {
				sm.Config.Logger.Error("Error parsing session ID", "error", err)
				continue
			}
			if _, exists := sm.cache[sessionID]; exists {
				sessionsToRemove = append(sessionsToRemove, sessionID)
			}
		}
		sm.mutex.RUnlock()
		sm.mutex.Lock()
		for _, sessionID := range sessionsToRemove {
			if item, exists := sm.cache[sessionID]; exists {
				sm.removeFromUserSessionsIndex(item.session.UserID, sessionID)
				sm.lru.Remove(item.element)
				delete(sm.cache, sessionID)
			}
		}
		sm.mutex.Unlock()
	case NotificationTypeUserSessionsRemovalFromCache:
		userID, err := uuid.Parse(notification.Payload[0])
		if err != nil {
			sm.Config.Logger.Error("Error parsing user ID", "error", err)
			return
		}
		sm.mutex.Lock()
		if sessions, exists := sm.userSessionsIndex[userID]; exists {
			for _, sessionID := range sessions {
				if item, exists := sm.cache[sessionID]; exists {
					sm.lru.Remove(item.element)
					delete(sm.cache, sessionID)
				}
			}
			delete(sm.userSessionsIndex, userID)
		}
		sm.mutex.Unlock()
	case NotificationTypeClearEntireCache:
		sm.mutex.Lock()
		sm.cache = make(map[uuid.UUID]*cacheItem)
		sm.userSessionsIndex = make(map[uuid.UUID][]uuid.UUID)
		sm.lru = list.New()
		sm.mutex.Unlock()
	case NotificationTypeGroupSessionsRemovalFromCache:
		groupID, err := uuid.Parse(notification.Payload[0])
		if err != nil {
			sm.Config.Logger.Error("Error parsing group ID", "error", err)
			return
		}
		sm.mutex.Lock()
		for sessionID, item := range sm.cache {
			if item.session.GroupID != nil && *item.session.GroupID == groupID {
				sm.removeFromUserSessionsIndex(item.session.UserID, sessionID)
				sm.lru.Remove(item.element)
				delete(sm.cache, sessionID)
			}
		}
		sm.mutex.Unlock()
	default:
		sm.Config.Logger.Error(fmt.Sprintf("Unknown notification type: %s", notification.Type))
	}
}

func (sm *SessionManager) handleOutOfSync(channel string) error {
	sm.Config.Logger.Warn(fmt.Sprintf("Out of sync detected on channel %s, refreshing cache", channel))
	return sm.refreshCache(context.Background())
}

func (sm *SessionManager) refreshCache(ctx context.Context) error {
	var cachedIDs []uuid.UUID
	var placeholders []string
	var args []interface{}
	var mostRecentUpdate time.Time

	sm.mutex.Lock()
	sm.outOfSync = true
	sm.mutex.Unlock()

	defer func() {
		sm.mutex.Lock()
		sm.outOfSync = false
		sm.mutex.Unlock()
	}()

	// Collect cached IDs and find most recent update under read lock
	sm.mutex.RLock()
	for id, item := range sm.cache {
		cachedIDs = append(cachedIDs, id)
		placeholders = append(placeholders, fmt.Sprintf("($%d::uuid)", len(args)+1))
		args = append(args, id)
		if item.session.UpdatedAt.After(mostRecentUpdate) {
			mostRecentUpdate = item.session.UpdatedAt
		}
	}
	sm.mutex.RUnlock()

	if len(cachedIDs) == 0 {
		return nil
	}

	// Calculate threshold
	threshold := mostRecentUpdate.Add(-2 * time.Second)
	args = append(args, threshold)

	query := fmt.Sprintf(`
        WITH existing(id) AS (
            VALUES %s
        )
        SELECT existing.id FROM existing
        LEFT JOIN %s s ON existing.id = s.id
        WHERE s.id IS NULL
        UNION
        SELECT s.id FROM %s s
        JOIN existing ON s.id = existing.id
        WHERE s.updated_at > $%d
    `, strings.Join(placeholders, ", "),
		sm.getTableName("sessions"),
		sm.getTableName("sessions"),
		len(args))

	rows, err := sm.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query sessions to refresh: %w", err)
	}
	defer rows.Close()

	var toRemoveIDs []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("failed to scan session id: %w", err)
		}
		toRemoveIDs = append(toRemoveIDs, id)
	}

	// Remove sessions from cache that are either not in the database or have been updated
	sm.mutex.Lock()
	for _, id := range toRemoveIDs {
		if item, exists := sm.cache[id]; exists {
			sm.removeFromUserSessionsIndex(item.session.UserID, id)
			sm.lru.Remove(item.element)
			delete(sm.cache, id)
		}
	}
	sm.mutex.Unlock()

	return nil
}

// UpdateAttributeOption is a function type that modifies UpdateAttributeOptions
type UpdateAttributeOption func(*UpdateAttributeOptions)

// UpdateAttributeOptions holds the options for updating an attribute
type UpdateAttributeOptions struct {
	ExpiresAt *time.Time
}

// WithExpiresAt sets the ExpiresAt option
func WithExpiresAt(expiresAt time.Time) UpdateAttributeOption {
	return func(opts *UpdateAttributeOptions) {
		opts.ExpiresAt = &expiresAt
	}
}

// UpdateAttribute sets or updates an attribute for the session.
func (s *Session) UpdateAttribute(key string, value interface{}, options ...UpdateAttributeOption) error {
	if s.IsInvalidated() {
		return fmt.Errorf("cannot update attribute of an invalidated session")
	}

	opts := UpdateAttributeOptions{}
	for _, option := range options {
		option(&opts)
	}

	valueStr, err := convertToString(value)
	if err != nil {
		return fmt.Errorf("failed to convert attribute to string: %w", err)
	}
	if len(valueStr) > s.sm.Config.MaxAttributeLength {
		return fmt.Errorf("attribute value for key %s exceeds max length of %d", key, s.sm.Config.MaxAttributeLength)
	}

	attr := SessionAttributeValue{Value: value, ExpiresAt: opts.ExpiresAt, Marshaled: false}
	if existing, ok := s.attributes[key]; !ok {
		// insert attribute and createsession starts at 1 so to avoid collisions, new attributes should be -1 to fail on update.
		attr.Version = -1
	} else {
		attr.Version = existing.Version
	}
	s.attributes[key] = attr
	s.changed[key] = true
	return nil
}

// DeleteAttribute removes an attribute from the session.
func (s *Session) DeleteAttribute(key string) error {
	if s.IsInvalidated() {
		return fmt.Errorf("cannot delete attribute of an invalidated session")
	}

	delete(s.attributes, key)
	s.deleted[key] = true

	return nil
}

// GetAttributes returns all attributes of the session.
func (s *Session) GetAttributes() map[string]SessionAttributeValue {
	return s.attributes
}

// GetAttribute retrieves a specific attribute from the session.
func (s *Session) GetAttribute(key string) (SessionAttributeValue, bool) {
	attr, ok := s.attributes[key]
	return attr, ok
}

var ErrAttributeNotFound = errors.New("attribute not found")

// GetAttributeAndRetainUnmarshaled retrieves a specific attribute, unmarshals it if necessary,
// and retains the unmarshaled value in memory for future use.
func (s *Session) GetAttributeAndRetainUnmarshaled(key string, v interface{}) (SessionAttributeValue, error) {
	attr, ok := s.attributes[key]
	if !ok {
		return SessionAttributeValue{}, ErrAttributeNotFound
	}

	if !attr.Marshaled {
		err := setValue(v, attr.Value)
		if err != nil {
			return SessionAttributeValue{}, fmt.Errorf("failed to set value (unmarshaled) to interface: %w", err)
		}
		return attr, nil
	}

	var unmarshaledValue interface{}
	switch v.(type) {
	case *string:
		*v.(*string) = attr.Value.(string)
		unmarshaledValue = *v.(*string)
	default:
		err := json.Unmarshal([]byte(attr.Value.(string)), v)
		if err != nil {
			return SessionAttributeValue{}, fmt.Errorf("failed to unmarshal attribute %s: %w", key, err)
		}
		unmarshaledValue = reflect.ValueOf(v).Elem().Interface()
	}

	unmarshaled := SessionAttributeValue{
		Value:     unmarshaledValue,
		ExpiresAt: attr.ExpiresAt,
		Marshaled: false,
		Version:   attr.Version,
	}

	s.attributes[key] = unmarshaled

	s.sm.mutex.Lock()
	defer s.sm.mutex.Unlock()

	if item, exists := s.sm.cache[s.ID]; exists {
		cachedAttr, ok := item.session.attributes[key]
		if ok && cachedAttr.Marshaled {
			// Only update if the cached value is still marshaled and the values are exactly the same
			if cachedValue, ok := cachedAttr.Value.(string); ok && cachedValue == attr.Value.(string) {
				item.session.attributes[key] = unmarshaled
			}
		}
	}

	return unmarshaled, nil
}

// setValue is a helper function to set the value of v to the given value
func setValue(v interface{}, value interface{}) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("v must be a non-nil pointer")
	}
	rv = rv.Elem()
	if !rv.CanSet() {
		return fmt.Errorf("v must be settable")
	}

	vv := reflect.ValueOf(value)
	if !vv.Type().AssignableTo(rv.Type()) {
		return fmt.Errorf("cannot assign %v to %v", vv.Type(), rv.Type())
	}

	rv.Set(vv)
	return nil
}

func (s *Session) deepCopy() *Session {
	copiedAttributes := make(map[string]SessionAttributeValue, len(s.attributes))
	for k, v := range s.attributes {
		copiedAttributes[k] = v
	}

	copiedChanged := make(map[string]bool, len(s.changed))
	for k, v := range s.changed {
		copiedChanged[k] = v
	}

	copiedDeleted := make(map[string]bool, len(s.deleted))
	for k, v := range s.deleted {
		copiedDeleted[k] = v
	}

	return &Session{
		ID:           s.ID,
		UserID:       s.UserID,
		GroupID:      s.GroupID,
		groupId:      s.groupId,
		LastAccessed: s.LastAccessed,
		ExpiresAt:    s.ExpiresAt,
		UpdatedAt:    s.UpdatedAt,
		Version:      s.Version,
		attributes:   copiedAttributes,
		changed:      copiedChanged,
		deleted:      copiedDeleted,
		sm:           s.sm,
		fromCache:    s.fromCache,
		invalidated:  s.invalidated,
	}
}

func (sm *SessionManager) cleanupWorker() {
	defer sm.wg.Done()
	ticker := sm.clock.NewTicker(sm.Config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.Chan():
			if err := sm.cleanupExpiredSessions(context.Background()); err != nil {
				sm.Config.Logger.Error("Error cleaning up expired sessions", "error", err)
			}
		case <-sm.shutdownChan:
			return
		}
	}
}

func (sm *SessionManager) cleanupExpiredSessions(ctx context.Context) error {
	now := sm.clock.Now()
	query := fmt.Sprintf(`
        WITH expired_sessions AS (
        DELETE FROM %s
        WHERE "expires_at" < $1 OR "last_accessed" < $2
        RETURNING "id"
        ),
        expired_attributes AS (
            DELETE FROM %s
            WHERE "expires_at" < $1 and "expires_at" is not null
            RETURNING "session_id"
        )
        SELECT "id" FROM expired_sessions
        UNION
        SELECT DISTINCT "session_id" FROM expired_attributes
    `, sm.getTableName("sessions"), sm.getTableName("session_attributes"))

	rows, err := sm.db.QueryContext(ctx, query, now, now.Add(-sm.Config.InactivityDuration))
	if err != nil {
		return fmt.Errorf("failed to delete expired sessions and attributes: %w", err)
	}
	defer rows.Close()

	var deletedSessionIDs []uuid.UUID
	for rows.Next() {
		var sessionID uuid.UUID
		if err := rows.Scan(&sessionID); err != nil {
			return fmt.Errorf("failed to scan deleted session ID: %w", err)
		}
		deletedSessionIDs = append(deletedSessionIDs, sessionID)
	}

	sm.mutex.Lock()
	for _, sessionID := range deletedSessionIDs {
		if item, exists := sm.cache[sessionID]; exists {
			sm.removeFromUserSessionsIndex(item.session.UserID, sessionID)
			sm.lru.Remove(item.element)
			delete(sm.cache, sessionID)
		}
	}
	sm.mutex.Unlock()

	if len(deletedSessionIDs) > 0 {
		sessionIDStrings := make([]string, len(deletedSessionIDs))
		for i, id := range deletedSessionIDs {
			sessionIDStrings[i] = id.String()
		}
		err = sm.sendNotification(sm.db, NotificationTypeSessionsRemovalFromCache, sessionIDStrings)
		if err != nil {
			sm.Config.Logger.Error("Failed to send notification for expired sessions", "error", err)
		}
	}

	return nil
}

func (sm *SessionManager) enforceMaxSessions(ctx context.Context, userID uuid.UUID) {
	query := fmt.Sprintf(`
        WITH keep_sessions AS (
            SELECT "id"
            FROM %s
            WHERE "user_id" = $1
            ORDER BY "last_accessed" DESC
            LIMIT $2
        ),
        deleted AS (
            DELETE FROM %s
            WHERE "user_id" = $1
            AND "id" NOT IN (SELECT "id" FROM keep_sessions)
            RETURNING "id"
        )
        SELECT "id" FROM deleted;
    `, sm.getTableName("sessions"), sm.getTableName("sessions"))

	var deletedSessionIDs []uuid.UUID
	err := sm.db.SelectContext(ctx, &deletedSessionIDs, query, userID, sm.Config.MaxSessions)
	if err != nil {
		sm.Config.Logger.Error("Failed to enforce max sessions", "error", err)
		return
	}

	if len(deletedSessionIDs) > 0 {
		sm.mutex.Lock()
		for _, sessionID := range deletedSessionIDs {
			if item, exists := sm.cache[sessionID]; exists {
				sm.removeFromUserSessionsIndex(item.session.UserID, sessionID)
				sm.lru.Remove(item.element)
				delete(sm.cache, sessionID)
			}
		}
		sm.mutex.Unlock()

		sessionIDStrings := make([]string, len(deletedSessionIDs))
		for i, id := range deletedSessionIDs {
			sessionIDStrings[i] = id.String()
		}
		err = sm.sendNotification(sm.db, NotificationTypeSessionsRemovalFromCache, sessionIDStrings)
		if err != nil {
			sm.Config.Logger.Error("Failed to send notification for enforced max sessions", "error", err)
		}
	}
}

func (sm *SessionManager) updateSessionAccessAsync(ctx context.Context, sessionID uuid.UUID) {
	sm.lastAccessUpdateMutex.Lock()
	sm.lastAccessUpdates[sessionID] = sm.clock.Now()
	sm.lastAccessUpdateMutex.Unlock()
}

func (sm *SessionManager) lastAccessUpdateWorker() {
	defer sm.wg.Done()
	ticker := sm.clock.NewTicker(sm.Config.LastAccessUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.Chan():
			sm.processLastAccessUpdates()
		case <-sm.shutdownChan:
			sm.processLastAccessUpdates() // Final update before shutting down
			return
		}
	}
}

func (sm *SessionManager) processLastAccessUpdates() {
	sm.lastAccessUpdateMutex.Lock()
	updates := sm.lastAccessUpdates
	sm.lastAccessUpdates = make(map[uuid.UUID]time.Time)
	sm.lastAccessUpdateMutex.Unlock()

	if len(updates) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(sm.ctx, 10*time.Minute)
	defer cancel()

	var sessionIDs []uuid.UUID
	for sessionID := range updates {
		sessionIDs = append(sessionIDs, sessionID)
	}

	batchSize := sm.Config.LastAccessUpdateBatchSize
	for i := 0; i < len(sessionIDs); i += batchSize {
		end := i + batchSize
		if end > len(sessionIDs) {
			end = len(sessionIDs)
		}
		chunk := sessionIDs[i:end]

		err := sm.processBatch(ctx, chunk, updates)
		if err != nil {
			sm.Config.Logger.Error("Failed to process batch", "error", err)
			// Add unprocessed updates back to lastAccessUpdates
			sm.lastAccessUpdateMutex.Lock()
			for _, sessionID := range chunk {
				if lastAccess, ok := updates[sessionID]; ok {
					sm.lastAccessUpdates[sessionID] = lastAccess
				}
			}
			sm.lastAccessUpdateMutex.Unlock()
		}
	}
}

func (sm *SessionManager) processBatch(ctx context.Context, sessionIDs []uuid.UUID, updates map[uuid.UUID]time.Time) error {
	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	var placeholders []string
	var args []interface{}
	for i, sessionID := range sessionIDs {
		placeholders = append(placeholders, fmt.Sprintf("($%d::uuid, $%d::timestamp with time zone)", i*2+1, i*2+2))
		args = append(args, sessionID, updates[sessionID])
	}

	query := fmt.Sprintf(`
        WITH updates(id, last_accessed) AS (
            VALUES %s
        )
        UPDATE %s AS s SET
            "last_accessed" = u.last_accessed
        FROM updates u
        WHERE s.id = u.id
    `, strings.Join(placeholders, ","), sm.getTableName("sessions"))

	_, err = tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute batch update: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (sm *SessionManager) sendNotificationTx(tx *sqlx.Tx, notificationType string, payload []string) error {
	notification := sessionNotification{
		NodeID:  sm.nodeID,
		Type:    notificationType,
		Payload: payload,
	}

	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	notifyQuery := sm.pgln.NotifyQuery(sm.getChannelName("session_updates"), string(notificationJSON))
	_, err = tx.ExecContext(context.Background(), notifyQuery.Query, notifyQuery.Params...)
	if err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}

	return nil
}

func (sm *SessionManager) sendNotification(db *sqlx.DB, notificationType string, payload []string) error {
	notification := sessionNotification{
		NodeID:  sm.nodeID,
		Type:    notificationType,
		Payload: payload,
	}

	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	notifyQuery := sm.pgln.NotifyQuery(sm.getChannelName("session_updates"), string(notificationJSON))
	_, err = db.ExecContext(context.Background(), notifyQuery.Query, notifyQuery.Params...)
	if err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}

	return nil
}

// ClearEntireCache clears all sessions from the cache and notifies other nodes to do the same
func (sm *SessionManager) ClearEntireCache(ctx context.Context) error {
	sm.mutex.Lock()
	sm.cache = make(map[uuid.UUID]*cacheItem)
	sm.userSessionsIndex = make(map[uuid.UUID][]uuid.UUID)
	sm.lru = list.New()
	sm.mutex.Unlock()

	// Send notification to clear cache on other nodes
	err := sm.sendNotification(sm.db, NotificationTypeClearEntireCache, nil)
	if err != nil {
		return fmt.Errorf("failed to send clear cache notification: %w", err)
	}

	return nil
}

// RemoveAllUserCachedSessionsFromAllNodes removes all cached sessions for a given user from all nodes.
func (sm *SessionManager) RemoveAllUserCachedSessionsFromAllNodes(userID uuid.UUID) error {
	sm.mutex.Lock()
	if sessions, exists := sm.userSessionsIndex[userID]; exists {
		for _, sessionID := range sessions {
			if item, exists := sm.cache[sessionID]; exists {
				sm.lru.Remove(item.element)
				delete(sm.cache, sessionID)
			}
		}
		delete(sm.userSessionsIndex, userID)
	}
	sm.mutex.Unlock()
	return sm.sendNotification(sm.db, NotificationTypeUserSessionsRemovalFromCache, []string{userID.String()})
}

// EncodeSessionIDAndVersion encodes a session ID and version into a single string.
func (sm *SessionManager) EncodeSessionIDAndVersion(sessionID uuid.UUID, version int) string {
	return fmt.Sprintf("%s:%d", sessionID.String(), version)
}

// ParseSessionIDAndVersion parses an encoded session ID and version string.
func (sm *SessionManager) ParseSessionIDAndVersion(encodedData string) (uuid.UUID, int, error) {
	parts := strings.Split(encodedData, ":")
	if len(parts) != 2 {
		return uuid.Nil, 0, fmt.Errorf("invalid session data format")
	}

	sessionID, err := uuid.Parse(parts[0])
	if err != nil {
		return uuid.Nil, 0, fmt.Errorf("invalid session ID: %w", err)
	}

	version, err := strconv.Atoi(parts[1])
	if err != nil {
		return uuid.Nil, 0, fmt.Errorf("invalid version: %w", err)
	}

	return sessionID, version, nil
}

// Shutdown gracefully shuts down the SessionManager.
func (sm *SessionManager) Shutdown(ctx context.Context) error {
	close(sm.shutdownChan)

	done := make(chan struct{})
	go func() {
		sm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Workers finished successfully
	case <-ctx.Done():
		return fmt.Errorf("shutdown timed out: %v", ctx.Err())
	}

	if sm.Config.CustomPGLN == nil {
		sm.pgln.Shutdown()
	}

	// We don't close the db connection here anymore, as it's managed externally

	return nil
}

// Helper functions

func (sm *SessionManager) getTableName(baseName string) string {
	if sm.Config.SchemaName != "" {
		return fmt.Sprintf(`"%s"."%s%s"`, sm.Config.SchemaName, sm.Config.TablePrefix, baseName)
	}
	return fmt.Sprintf(`"%s%s"`, sm.Config.TablePrefix, baseName)
}

func (sm *SessionManager) getChannelName(baseName string) string {
	return sm.Config.TablePrefix + baseName
}

func (sm *SessionManager) addOrUpdateCache(session *Session) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if item, exists := sm.cache[session.ID]; exists {
		if session.Version > item.session.Version || session.UpdatedAt.After(item.session.UpdatedAt) {
			if item.session.UserID != session.UserID {
				sm.removeFromUserSessionsIndex(item.session.UserID, session.ID)
			}
			item.session = session
			item.cachedSince = sm.clock.Now()
			sm.lru.MoveToFront(item.element)
			sm.addToUserSessionsIndex(session.UserID, session.ID)
		}
	} else {
		if len(sm.cache) >= sm.Config.CacheSize {
			sm.evictOldestSession()
		}
		element := sm.lru.PushFront(session.ID)
		sm.cache[session.ID] = &cacheItem{
			session:     session,
			element:     element,
			cachedSince: sm.clock.Now(),
		}
		sm.addToUserSessionsIndex(session.UserID, session.ID)
	}
}

func (sm *SessionManager) addToUserSessionsIndex(userID, sessionID uuid.UUID) {
	if sm.userSessionsIndex == nil {
		sm.userSessionsIndex = make(map[uuid.UUID][]uuid.UUID)
	}
	sm.userSessionsIndex[userID] = append(sm.userSessionsIndex[userID], sessionID)
}

func (sm *SessionManager) removeFromUserSessionsIndex(userID, sessionID uuid.UUID) {
	if sessions, exists := sm.userSessionsIndex[userID]; exists {
		for i, id := range sessions {
			if id == sessionID {
				// Remove the session ID from the slice
				sm.userSessionsIndex[userID] = append(sessions[:i], sessions[i+1:]...)
				break
			}
		}
		if len(sm.userSessionsIndex[userID]) == 0 {
			delete(sm.userSessionsIndex, userID)
		}
	}
}

func (sm *SessionManager) evictOldestSession() {
	if oldest := sm.lru.Back(); oldest != nil {
		sessionID := oldest.Value.(uuid.UUID)
		if item, exists := sm.cache[sessionID]; exists {
			sm.removeFromUserSessionsIndex(item.session.UserID, sessionID)
			delete(sm.cache, sessionID)
		}
		sm.lru.Remove(oldest)
	}
}

// DeleteAttributeFromAllUserSessions deletes a specific attribute from all sessions of a given user.
func (sm *SessionManager) DeleteAttributeFromAllUserSessions(ctx context.Context, userID uuid.UUID, attributeKey string) error {
	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Use a CTE to perform both the deletion and update in a single query
	query := fmt.Sprintf(`
        WITH user_sessions AS (
            SELECT "id"
            FROM %s
            WHERE "user_id" = $1
        ),
        deleted_attributes AS (
            DELETE FROM %s
            WHERE "session_id" IN (SELECT "id" FROM user_sessions)
            AND "key" = $2
            RETURNING "session_id"
        )
        UPDATE %s
        SET "version" = "version" + 1,
            "updated_at" = $3
        WHERE "id" IN (SELECT "session_id" FROM deleted_attributes)
    `, sm.getTableName("sessions"), sm.getTableName("session_attributes"), sm.getTableName("sessions"))

	_, err = tx.ExecContext(ctx, query, userID, attributeKey, sm.clock.Now())
	if err != nil {
		return fmt.Errorf("failed to execute delete and update query: %w", err)
	}

	err = sm.sendNotificationTx(tx, NotificationTypeUserSessionsRemovalFromCache, []string{userID.String()})
	if err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	sm.mutex.Lock()
	if sessions, exists := sm.userSessionsIndex[userID]; exists {
		for _, sessionID := range sessions {
			if item, exists := sm.cache[sessionID]; exists {
				sm.lru.Remove(item.element)
				delete(sm.cache, sessionID)
			}
		}
		delete(sm.userSessionsIndex, userID)
	}
	sm.mutex.Unlock()

	return nil
}

// Test helper functions

// setClock sets the clock used by the SessionManager (for testing purposes)
func (sm *SessionManager) setClock(clock clockwork.Clock) {
	sm.clock = clock
}

// clearCache clears the session cache (for testing purposes)
func (sm *SessionManager) clearCache() {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	sm.cache = make(map[uuid.UUID]*cacheItem)
	sm.userSessionsIndex = make(map[uuid.UUID][]uuid.UUID)
	sm.lru = list.New()
}
