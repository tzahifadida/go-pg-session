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
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	NotificationTypeSessionsRemovalFromCache     = "sessions_removal_from_cache"
	NotificationTypeUserSessionsRemovalFromCache = "user_sessions_removal_from_cache"
)

// Config holds the configuration options for the SessionManager.
type Config struct {
	// MaxSessions is the maximum number of concurrent sessions allowed per user.
	// When this limit is reached, the oldest session will be removed.
	MaxSessions int `json:"maxSessions"`

	// MaxAttributeLength is the maximum length (in bytes) allowed for a single session attribute value.
	MaxAttributeLength int `json:"maxAttributeLength"`

	// SessionExpiration is the duration after which a session expires if not accessed.
	SessionExpiration time.Duration `json:"sessionExpiration"`

	// InactivityDuration is the duration of inactivity after which a session is considered expired.
	InactivityDuration time.Duration `json:"inactivityDuration"`

	// CleanupInterval is the time interval between cleanup operations for expired sessions.
	CleanupInterval time.Duration `json:"cleanupInterval"`

	// CacheSize is the maximum number of sessions to keep in the in-memory cache.
	CacheSize int `json:"cacheSize"`

	// TablePrefix is the prefix to be used for all database tables created by the SessionManager.
	// This allows multiple SessionManager instances to coexist in the same database.
	TablePrefix string `json:"tablePrefix"`

	// SchemaName is the name of the PostgreSQL schema to use for session tables.
	// If empty, the default schema (usually "public") will be used.
	SchemaName string `json:"schemaName"`

	// CreateSchemaIfMissing, if true, will create the specified schema if it doesn't exist.
	CreateSchemaIfMissing bool `json:"createSchemaIfMissing"`

	// LastAccessUpdateInterval is the time interval between batch updates of session last access times.
	LastAccessUpdateInterval time.Duration `json:"lastAccessUpdateInterval"`

	// LastAccessUpdateBatchSize is the maximum number of sessions to update in a single batch operation.
	LastAccessUpdateBatchSize int `json:"lastAccessUpdateBatchSize"`

	// NotifyOnUpdates determines whether to send notifications on session updates.
	// This is a noisier option (true by default) but safer if you do not use additional cookies to note the last version.
	// For dozens of nodes, you may want to turn it off.
	NotifyOnUpdates bool

	// NotifyOnFailedUpdates sends a removal notification when, for example, a version check fails. FALSE by default.
	NotifyOnFailedUpdates bool

	// CustomPGLN is an optional custom PGLN instance. If not supplied, a new one will be created with defaults.
	CustomPGLN *pgln.PGListenNotify `json:"-"`
}

// DefaultConfig returns a Config struct with default values.
func DefaultConfig() *Config {
	return &Config{
		MaxSessions:               5,
		MaxAttributeLength:        16 * 1024,           // 16KB
		SessionExpiration:         30 * 24 * time.Hour, // 30 days
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
	ID     uuid.UUID `db:"id"`
	UserID uuid.UUID `db:"user_id"`

	// These are not updated in the cache often, only the table is the source of truth.
	LastAccessed time.Time `db:"last_accessed"`
	ExpiresAt    time.Time `db:"expires_at"`
	UpdatedAt    time.Time `db:"updated_at"`
	Version      int       `db:"version"`

	attributes map[string]SessionAttributeValue
	changed    map[string]bool
	deleted    map[string]bool
	sm         *SessionManager
	fromCache  bool
}

// IsFromCache returns true if the session was loaded from the cache,
// and false if it was loaded from the database table.
func (s *Session) IsFromCache() bool {
	return s.fromCache
}

type cacheItem struct {
	session *Session
	element *list.Element
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
}

type GetSessionOptions struct {
	DoNotUpdateSessionLastAccess bool
	ForceRefresh                 bool
}

// NewSessionManager creates a new SessionManager with the given configuration and connection string.
//
// Parameters:
//   - cfg: A pointer to a Config struct containing the configuration options for the SessionManager.
//   - db: An pgx (v5) stdlib
//
// Returns:
//   - A pointer to the created SessionManager and an error if any occurred during initialization.
func NewSessionManager(cfg *Config, db *sql.DB) (*SessionManager, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	} else {
		defaultCfg := DefaultConfig()
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
		return nil, fmt.Errorf("failed to generate node ID: %v", err)
	}

	sm := &SessionManager{
		Config:            cfg,
		cache:             make(map[uuid.UUID]*cacheItem),
		lru:               list.New(),
		db:                sqlxDB,
		shutdownChan:      make(chan struct{}),
		nodeID:            nodeID,
		lastAccessUpdates: make(map[uuid.UUID]time.Time),
		clock:             clockwork.NewRealClock(),
	}

	if err := sm.checkTables(); err != nil {
		return nil, err
	}

	// Use the custom PGLN if provided, otherwise create a new one
	if cfg.CustomPGLN != nil {
		sm.pgln = cfg.CustomPGLN
	} else {
		builder := pgln.NewPGListenNotifyBuilder().
			SetContext(context.Background()).
			SetReconnectInterval(1 * time.Second).
			SetDB(db)

		sm.pgln, err = builder.Build()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize pgln: %v", err)
		}

		err = sm.pgln.Start()
		if err != nil {
			return nil, fmt.Errorf("failed to start pgln: %v", err)
		}
	}

	channelName := sm.getChannelName("session_updates")
	err = sm.pgln.ListenAndWaitForListening(channelName, pgln.ListenOptions{
		NotificationCallback: sm.handleNotification,
		ErrorCallback: func(channel string, err error) {
			log.Printf("Warning PGLN error (this may be fine if your loadbalancer or db connection lifetime is preset) on channel %s: %v", channel, err)
			sm.mutex.Lock()
			sm.outOfSync = true
			sm.mutex.Unlock()
		},
		OutOfSyncBlockingCallback: sm.handleOutOfSync,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to listen on channel: %v", err)
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
		return fmt.Errorf("failed to check if tables exist: %v", err)
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

// CreateSession creates a new session for the given user with the provided attributes.
//
// Parameters:
//   - ctx: The context for the operation.
//   - userID: The UUID of the user for whom the session is being created.
//   - attributes: A map of initial attributes for the session.
//
// Returns:
//   - A pointer to the created Session and an error if any occurred during creation.
func (sm *SessionManager) CreateSession(ctx context.Context, userID uuid.UUID, attributes map[string]SessionAttributeValue) (*Session, error) {
	sessionID, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate UUID: %v", err)
	}

	now := sm.clock.Now()
	expiresAt := now.Add(sm.Config.SessionExpiration)

	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
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

	query := fmt.Sprintf(`
		INSERT INTO %s ("id", "user_id", "last_accessed", "expires_at", "updated_at", "version")
		VALUES (:id, :user_id, :last_accessed, :expires_at, :updated_at, :version)
	`, sm.getTableName("sessions"))

	_, err = tx.NamedExecContext(ctx, query, session)
	if err != nil {
		return nil, fmt.Errorf("failed to insert session: %v", err)
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
			return nil, fmt.Errorf("failed to insert session attribute: %v", err)
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
		return nil, fmt.Errorf("failed to commit transaction: %v", err)
	}

	go sm.enforceMaxSessions(ctx, userID)

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
//
// Parameters:
//   - ctx: The context for the operation.
//   - sessionID: The UUID of the session to retrieve.
//   - options: Variadic SessionOption parameters to customize the retrieval behavior.
//
// Returns:
//   - A pointer to the retrieved Session and an error if any occurred during retrieval.
func (sm *SessionManager) GetSession(ctx context.Context, sessionID uuid.UUID, options ...SessionOption) (*Session, error) {
	return sm.GetSessionWithVersion(ctx, sessionID, 0, options...)
}

// GetSessionWithVersion retrieves a session by its ID and version with optional parameters.
//
// Parameters:
//   - ctx: The context for the operation.
//   - sessionID: The UUID of the session to retrieve.
//   - version: The version of the session to retrieve.
//   - options: Variadic SessionOption parameters to customize the retrieval behavior.
//
// Returns:
//   - A pointer to the retrieved Session and an error if any occurred during retrieval.
func (sm *SessionManager) GetSessionWithVersion(ctx context.Context, sessionID uuid.UUID, version int, options ...SessionOption) (*Session, error) {
	opts := GetSessionOptions{}
	for _, option := range options {
		option(&opts)
	}

	if !opts.ForceRefresh {
		sm.mutex.RLock()
		item, exists := sm.cache[sessionID]
		if !sm.outOfSync && exists && item.session.Version >= version {
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
				go sm.updateSessionAccessAsync(ctx, sessionID)
				return sessionCopy, nil
			}
		} else {
			sm.mutex.RUnlock()
		}
	}

	query := fmt.Sprintf(`
		SELECT "id", "user_id", "last_accessed", "expires_at", "updated_at", "version"
            FROM %s
            WHERE "id" = $1
	`, sm.getTableName("sessions"))

	session := &Session{}
	err := sm.db.GetContext(ctx, session, query, sessionID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("session not found")
		}
		return nil, fmt.Errorf("failed to get session: %v", err)
	}

	attributes, err := sm.listAttributes(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("failed to list attributes: %v", err)
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
		sm.updateSessionAccessAsync(ctx, sessionID)
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
//
// Parameters:
//   - ctx: The context for the operation.
//   - session: A pointer to the Session to be updated.
//   - options: Variadic UpdateSessionOption parameters to customize the update behavior.
//
// Returns:
//   - A pointer to the updated Session and an error if any occurred during the update.
func (sm *SessionManager) UpdateSession(ctx context.Context, session *Session, options ...UpdateSessionOption) (*Session, error) {
	session = session.deepCopy()
	opts := UpdateSessionOptions{}
	for _, option := range options {
		option(&opts)
	}

	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
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
				return nil, fmt.Errorf("failed to update session attribute: %v", err)
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
			return nil, fmt.Errorf("failed to expand IN clause: %v", err)
		}
		query = tx.Rebind(query)

		_, err = tx.ExecContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to delete session attributes: %v", err)
		}
	}

	now := sm.clock.Now()
	updateQueryTemplate := `
        UPDATE %s
        SET "updated_at" = $1, "version" = "version" + 1
        WHERE "id" = $2%s 
        RETURNING "id", "user_id", "last_accessed", "expires_at", "updated_at", "version"
    `
	var updateQuery string
	var updateQueryRow *sql.Row
	if opts.CheckVersion {
		updateQuery = fmt.Sprintf(updateQueryTemplate, sm.getTableName("sessions"), ` AND "version" = $3`)
		updateQueryRow = tx.QueryRowContext(ctx, updateQuery, now, session.ID, session.Version)
	} else {
		updateQuery = fmt.Sprintf(updateQueryTemplate, sm.getTableName("sessions"), "")
		updateQueryRow = tx.QueryRowContext(ctx, updateQuery, now, session.ID)
	}

	var updatedSession Session
	err = updateQueryRow.Scan(
		&updatedSession.ID,
		&updatedSession.UserID,
		&updatedSession.LastAccessed,
		&updatedSession.ExpiresAt,
		&updatedSession.UpdatedAt,
		&updatedSession.Version,
	)
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
					return nil, fmt.Errorf("failed to send notification: %v", err)
				}
			}

			return nil, ErrSessionVersionIsOutdated
		}
		return nil, fmt.Errorf("failed to update session: %v", err)
	}

	if sm.Config.NotifyOnUpdates && !opts.DoNotNotify {
		err = sm.sendNotificationTx(tx, NotificationTypeSessionsRemovalFromCache, []string{session.ID.String()})
		if err != nil {
			return nil, fmt.Errorf("failed to send notification after update: %v", err)
		}
	}

	updatedSession.attributes = session.attributes
	updatedSession.changed = make(map[string]bool)
	updatedSession.deleted = make(map[string]bool)
	updatedSession.sm = sm

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %v", err)
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
//
// Parameters:
//   - ctx: The context for the operation.
//   - sessionID: The UUID of the session to delete.
//
// Returns:
//   - An error if any occurred during the deletion.
func (sm *SessionManager) DeleteSession(ctx context.Context, sessionID uuid.UUID) error {
	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`
        DELETE FROM %s
        WHERE "id" = $1
    `, sm.getTableName("sessions"))

	_, err = tx.ExecContext(ctx, query, sessionID)
	if err != nil {
		return fmt.Errorf("failed to delete session: %v", err)
	}

	err = sm.sendNotificationTx(tx, NotificationTypeSessionsRemovalFromCache, []string{sessionID.String()})
	if err != nil {
		return fmt.Errorf("failed to send notification: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	sm.mutex.Lock()
	if item, exists := sm.cache[sessionID]; exists {
		sm.lru.Remove(item.element)
		delete(sm.cache, sessionID)
	}
	sm.mutex.Unlock()

	return nil
}

// DeleteAllSessions deletes all sessions from the database and cache.
//
// Parameters:
//   - ctx: The context for the operation.
//
// Returns:
//   - An error if any occurred during the deletion.
func (sm *SessionManager) DeleteAllSessions(ctx context.Context) error {
	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`
        DELETE FROM %s
        RETURNING "id"
    `, sm.getTableName("sessions"))

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to delete user sessions: %v", err)
	}
	defer rows.Close()

	var deletedSessionIDs []uuid.UUID
	for rows.Next() {
		var sessionID uuid.UUID
		if err := rows.Scan(&sessionID); err != nil {
			return fmt.Errorf("failed to scan deleted session ID: %v", err)
		}
		deletedSessionIDs = append(deletedSessionIDs, sessionID)
	}

	sessionIDStrings := make([]string, len(deletedSessionIDs))
	for i, id := range deletedSessionIDs {
		sessionIDStrings[i] = id.String()
	}

	err = sm.sendNotificationTx(tx, NotificationTypeSessionsRemovalFromCache, sessionIDStrings)
	if err != nil {
		return fmt.Errorf("failed to send notification: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	sm.mutex.Lock()
	for _, sessionID := range deletedSessionIDs {
		if item, exists := sm.cache[sessionID]; exists {
			sm.lru.Remove(item.element)
			delete(sm.cache, sessionID)
		}
	}
	sm.mutex.Unlock()

	return nil
}

// DeleteAllUserSessions deletes all sessions for a given user.
//
// Parameters:
//   - ctx: The context for the operation.
//   - userID: The UUID of the user whose sessions should be deleted.
//
// Returns:
//   - An error if any occurred during the deletion.
func (sm *SessionManager) DeleteAllUserSessions(ctx context.Context, userID uuid.UUID) error {
	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`
        DELETE FROM %s
        WHERE "user_id" = $1
        RETURNING "id"
    `, sm.getTableName("sessions"))

	rows, err := tx.QueryContext(ctx, query, userID)
	if err != nil {
		return fmt.Errorf("failed to delete user sessions: %v", err)
	}
	defer rows.Close()

	var deletedSessionIDs []uuid.UUID
	for rows.Next() {
		var sessionID uuid.UUID
		if err := rows.Scan(&sessionID); err != nil {
			return fmt.Errorf("failed to scan deleted session ID: %v", err)
		}
		deletedSessionIDs = append(deletedSessionIDs, sessionID)
	}

	sessionIDStrings := make([]string, len(deletedSessionIDs))
	for i, id := range deletedSessionIDs {
		sessionIDStrings[i] = id.String()
	}

	err = sm.sendNotificationTx(tx, NotificationTypeSessionsRemovalFromCache, sessionIDStrings)
	if err != nil {
		return fmt.Errorf("failed to send notification: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	sm.mutex.Lock()
	for _, sessionID := range deletedSessionIDs {
		if item, exists := sm.cache[sessionID]; exists {
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
		return nil, fmt.Errorf("failed to query session attributes: %v", err)
	}
	defer rows.Close()

	attributes := make(map[string]SessionAttributeValue)
	for rows.Next() {
		var key, value string
		var expiresAt *time.Time
		var version int
		err := rows.Scan(&key, &value, &expiresAt, &version)
		if err != nil {
			return nil, fmt.Errorf("failed to scan attribute row: %v", err)
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
		log.Printf("Error unmarshalling notification: %v", err)
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
				log.Printf("Error parsing session ID: %v", err)
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
				sm.lru.Remove(item.element)
				delete(sm.cache, sessionID)
			}
		}
		sm.mutex.Unlock()
	case NotificationTypeUserSessionsRemovalFromCache:
		userID, err := uuid.Parse(notification.Payload[0])
		if err != nil {
			log.Printf("Error parsing user ID: %v", err)
			return
		}
		var sessionsToRemove []uuid.UUID
		sm.mutex.RLock()
		for sessionID, item := range sm.cache {
			if item.session.UserID == userID {
				sessionsToRemove = append(sessionsToRemove, sessionID)
			}
		}
		sm.mutex.RUnlock()

		sm.mutex.Lock()
		for _, sessionID := range sessionsToRemove {
			if item, exists := sm.cache[sessionID]; exists {
				sm.lru.Remove(item.element)
				delete(sm.cache, sessionID)
			}
		}
		sm.mutex.Unlock()
	default:
		log.Printf("Unknown notification type: %s", notification.Type)
	}
}

func (sm *SessionManager) handleOutOfSync(channel string) error {
	log.Printf("Out of sync detected on channel %s, refreshing cache", channel)
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
		return fmt.Errorf("failed to query sessions to refresh: %v", err)
	}
	defer rows.Close()

	var toRemoveIDs []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("failed to scan session id: %v", err)
		}
		toRemoveIDs = append(toRemoveIDs, id)
	}

	// Remove sessions from cache that are either not in the database or have been updated
	sm.mutex.Lock()
	for _, id := range toRemoveIDs {
		if item, exists := sm.cache[id]; exists {
			sm.lru.Remove(item.element)
			delete(sm.cache, id)
		}
	}
	sm.mutex.Unlock()

	return nil
}

// UpdateAttribute sets or updates an attribute for the session.
//
// Parameters:
//   - key: The key of the attribute to update.
//   - value: The value to set for the attribute. This will be converted to a string.
//   - expiresAt: An optional pointer to a time.Time value indicating when the attribute should expire.
//     If nil, the attribute will not have an expiration time.
//
// The method will return an error if:
//   - The value cannot be converted to a string.
//   - The resulting string exceeds the maximum allowed length for an attribute value.
//
// Example usage:
//
//	// Set an attribute without expiration
//	err := session.UpdateAttribute("theme", "dark", nil)
//
//	// Set an attribute with expiration
//	expiresAt := time.Now().Add(24 * time.Hour)
//	err := session.UpdateAttribute("temporary_flag", true, &expiresAt)
func (s *Session) UpdateAttribute(key string, value interface{}, expiresAt *time.Time) error {
	valueStr, err := convertToString(value)
	if err != nil {
		return fmt.Errorf("failed to convert attribute to string: %v", err)
	}
	if len(valueStr) > s.sm.Config.MaxAttributeLength {
		return fmt.Errorf("attribute value for key %s exceeds max length of %d", key, s.sm.Config.MaxAttributeLength)
	}

	attr := SessionAttributeValue{Value: value, ExpiresAt: expiresAt, Marshaled: false}
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
//
// Parameters:
//   - key: The key of the attribute to delete.
func (s *Session) DeleteAttribute(key string) {
	delete(s.attributes, key)
	s.deleted[key] = true
}

// GetAttributes returns all attributes of the session.
//
// Returns:
//   - A map of all session attributes.
func (s *Session) GetAttributes() map[string]SessionAttributeValue {
	return s.attributes
}

// GetAttribute retrieves a specific attribute from the session.
//
// Parameters:
//   - key: The key of the attribute to retrieve.
//
// Returns:
//   - The SessionAttributeValue for the given key and a boolean indicating whether the attribute was found.
func (s *Session) GetAttribute(key string) (SessionAttributeValue, bool) {
	attr, ok := s.attributes[key]
	return attr, ok
}

var ErrAttributeNotFound = errors.New("attribute not found")

// GetAttributeAndRetainUnmarshaled retrieves a specific attribute, unmarshals it if necessary,
// and retains the unmarshaled value in memory for future use. This method is optimized to
// prevent repeated unmarshaling of the same attribute as long as the session remains in memory.
//
// It's particularly beneficial for attributes that are frequently accessed and expensive to unmarshal.
// By retaining the unmarshaled value, subsequent calls to this method for the same attribute will
// return the cached unmarshaled value without the need for repeated unmarshaling operations.
//
// The method also ensures thread-safety when updating the shared cache, only doing so if the
// cached value hasn't been modified by another goroutine.
//
// Parameters:
//   - key: The key of the attribute to retrieve and unmarshal.
//   - v: A pointer to the struct where the unmarshaled value will be stored.
//
// Returns:
//   - A copy of the SessionAttributeValue (which may be newly unmarshaled or previously cached)
//     and an error if any occurred during the retrieval or unmarshaling process.
//   - If an attribute is not found it returns ErrAttributeNotFound
//
// Usage:
//
//	var myStruct MyStructType
//	attr, err := session.GetAttributeAndRetainUnmarshaled("myKey", &myStruct)
//	if err != nil {
//	    // Handle error
//	}
//	// Use myStruct and attr as needed
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
		LastAccessed: s.LastAccessed,
		ExpiresAt:    s.ExpiresAt,
		UpdatedAt:    s.UpdatedAt,
		Version:      s.Version,
		attributes:   copiedAttributes,
		changed:      copiedChanged,
		deleted:      copiedDeleted,
		sm:           s.sm,
		fromCache:    s.fromCache,
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
				log.Printf("Error cleaning up expired sessions: %v", err)
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
		return fmt.Errorf("failed to delete expired sessions and attributes: %v", err)
	}
	defer rows.Close()

	var deletedSessionIDs []uuid.UUID
	for rows.Next() {
		var sessionID uuid.UUID
		if err := rows.Scan(&sessionID); err != nil {
			return fmt.Errorf("failed to scan deleted session ID: %v", err)
		}
		deletedSessionIDs = append(deletedSessionIDs, sessionID)
	}

	sm.mutex.Lock()
	for _, sessionID := range deletedSessionIDs {
		if item, exists := sm.cache[sessionID]; exists {
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
			log.Printf("Failed to send notification for expired sessions: %v", err)
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
		log.Printf("Failed to enforce max sessions: %v", err)
		return
	}

	if len(deletedSessionIDs) > 0 {
		sm.mutex.Lock()
		for _, sessionID := range deletedSessionIDs {
			if item, exists := sm.cache[sessionID]; exists {
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
			log.Printf("Failed to send notification for enforced max sessions: %v", err)
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
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
			log.Printf("Failed to process batch: %v", err)
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
		return fmt.Errorf("failed to begin transaction: %v", err)
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
		return fmt.Errorf("failed to execute batch update: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
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
		return fmt.Errorf("failed to marshal notification: %v", err)
	}

	notifyQuery := sm.pgln.NotifyQuery(sm.getChannelName("session_updates"), string(notificationJSON))
	_, err = tx.ExecContext(context.Background(), notifyQuery.Query, notifyQuery.Params...)
	if err != nil {
		return fmt.Errorf("failed to send notification: %v", err)
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
		return fmt.Errorf("failed to marshal notification: %v", err)
	}

	notifyQuery := sm.pgln.NotifyQuery(sm.getChannelName("session_updates"), string(notificationJSON))
	_, err = db.ExecContext(context.Background(), notifyQuery.Query, notifyQuery.Params...)
	if err != nil {
		return fmt.Errorf("failed to send notification: %v", err)
	}

	return nil
}

// RemoveAllUserCachedSessionsFromAllNodes removes all cached sessions for a given user from all nodes.
//
// Parameters:
//   - userID: The UUID of the user whose cached sessions should be removed.
//
// Returns:
//   - An error if any occurred during the removal process.
func (sm *SessionManager) RemoveAllUserCachedSessionsFromAllNodes(userID uuid.UUID) error {
	sm.mutex.Lock()
	for sessionID, item := range sm.cache {
		if item.session.UserID == userID {
			sm.lru.Remove(item.element)
			delete(sm.cache, sessionID)
		}
	}
	sm.mutex.Unlock()
	return sm.sendNotification(sm.db, NotificationTypeUserSessionsRemovalFromCache, []string{userID.String()})
}

// EncodeSessionIDAndVersion encodes a session ID and version into a single string.
//
// Parameters:
//   - sessionID: The UUID of the session.
//   - version: The version of the session.
//
// Returns:
//   - A string containing the encoded session ID and version.
func (sm *SessionManager) EncodeSessionIDAndVersion(sessionID uuid.UUID, version int) string {
	return fmt.Sprintf("%s:%d", sessionID.String(), version)
}

// ParseSessionIDAndVersion parses an encoded session ID and version string.
//
// Parameters:
//   - encodedData: The string containing the encoded session ID and version.
//
// Returns:
//   - The parsed session UUID, version, and an error if any occurred during parsing.
func (sm *SessionManager) ParseSessionIDAndVersion(encodedData string) (uuid.UUID, int, error) {
	parts := strings.Split(encodedData, ":")
	if len(parts) != 2 {
		return uuid.Nil, 0, fmt.Errorf("invalid session data format")
	}

	sessionID, err := uuid.Parse(parts[0])
	if err != nil {
		return uuid.Nil, 0, fmt.Errorf("invalid session ID: %v", err)
	}

	version, err := strconv.Atoi(parts[1])
	if err != nil {
		return uuid.Nil, 0, fmt.Errorf("invalid version: %v", err)
	}

	return sessionID, version, nil
}

// Shutdown gracefully shuts down the SessionManager.
//
// Parameters:
//   - ctx: The context for the shutdown operation.
//
// Returns:
//   - An error if any occurred during the shutdown process.
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
			item.session = session
			sm.lru.MoveToFront(item.element)
		}
	} else {
		if len(sm.cache) >= sm.Config.CacheSize {
			oldest := sm.lru.Back()
			if oldest != nil {
				delete(sm.cache, oldest.Value.(uuid.UUID))
				sm.lru.Remove(oldest)
			}
		}
		element := sm.lru.PushFront(session.ID)
		sm.cache[session.ID] = &cacheItem{
			session: session,
			element: element,
		}
	}
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
	sm.lru = list.New()
}
