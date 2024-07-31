package gopgsession

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/tzahifadida/pgln"
	"log"
	"strings"
	"sync"
	"time"
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
}

// DefaultConfig returns a Config struct with default values.
func DefaultConfig() *Config {
	return &Config{
		MaxSessions:               5,
		MaxAttributeLength:        16 * 1024,           // 16KB
		SessionExpiration:         30 * 24 * time.Hour, // 1 month
		InactivityDuration:        48 * time.Hour,
		CleanupInterval:           1 * time.Hour,
		CacheSize:                 10000,
		TablePrefix:               "",
		SchemaName:                "",
		CreateSchemaIfMissing:     false,
		LastAccessUpdateInterval:  10 * time.Minute,
		LastAccessUpdateBatchSize: 5000,
	}
}

type Session struct {
	ID           uuid.UUID `db:"id"`
	UserID       uuid.UUID `db:"user_id"`
	LastAccessed time.Time `db:"last_accessed"`
	ExpiresAt    time.Time `db:"expires_at"`
	UpdatedAt    time.Time `db:"updated_at"`
	attributes   map[string]interface{}
	changed      map[string]bool
	sm           *SessionManager
}

// GetAttributes returns all attributes of the session.
func (s *Session) GetAttributes() map[string]interface{} {
	return s.attributes
}

// GetAttribute returns the value of a specific attribute.
func (s *Session) GetAttribute(key string) (interface{}, bool) {
	value, ok := s.attributes[key]
	return value, ok
}

// UpdateAttribute updates or adds an attribute to the session.
func (s *Session) UpdateAttribute(key string, value interface{}) error {
	valueStr, err := convertToString(value)
	if err != nil {
		return fmt.Errorf("failed to convert attribute to string: %v", err)
	}
	if len(valueStr) > s.sm.Config.MaxAttributeLength {
		return fmt.Errorf("attribute value for key %s exceeds max length of %d", key, s.sm.Config.MaxAttributeLength)
	}

	s.attributes[key] = value
	s.changed[key] = true
	return nil
}

func (s *Session) deepCopy() *Session {
	copiedAttributes := make(map[string]interface{}, len(s.attributes))
	for k, v := range s.attributes {
		copiedAttributes[k] = v
	}

	return &Session{
		ID:           s.ID,
		UserID:       s.UserID,
		LastAccessed: s.LastAccessed,
		ExpiresAt:    s.ExpiresAt,
		UpdatedAt:    s.UpdatedAt,
		attributes:   copiedAttributes,
		changed:      make(map[string]bool),
		sm:           s.sm,
	}
}

type cacheItem struct {
	session *Session
	element *list.Element
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
	lastAccessUpdates     map[uuid.UUID]time.Time
	lastAccessUpdateMutex sync.Mutex
}

// sessionNotification represents a notification for session updates.
type sessionNotification struct {
	NodeID    uuid.UUID `json:"nodeID"`
	SessionID uuid.UUID `json:"sessionID"`
}

// NewSessionManager creates a new SessionManager with the given configuration and connection string.
func NewSessionManager(cfg *Config, pgxConnectionString string) (*SessionManager, error) {
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

	sqlxDB, err := sqlx.Open("pgx", pgxConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %v", err)
	}

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
	}

	if err := sm.checkTables(); err != nil {
		return nil, err
	}

	builder := pgln.NewPGListenNotifyBuilder().
		SetContext(context.Background()).
		SetReconnectInterval(5000).
		UseConnectionString(pgxConnectionString)

	sm.pgln, err = builder.Build()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize pgln: %v", err)
	}

	err = sm.pgln.Start()
	if err != nil {
		return nil, fmt.Errorf("failed to start pgln: %v", err)
	}

	channelName := sm.getChannelName("session_updates")
	err = sm.pgln.ListenAndWaitForListening(channelName, pgln.ListenOptions{
		NotificationCallback: sm.handleNotification,
		ErrorCallback: func(channel string, err error) {
			log.Printf("PGLN error on channel %s: %v", channel, err)
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
				"updated_at" TIMESTAMP WITH TIME ZONE NOT NULL
			);`, sm.getTableName("sessions")),
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				"session_id" UUID REFERENCES %s("id") ON DELETE CASCADE,
				"key" TEXT NOT NULL,
				"value" TEXT NOT NULL,
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
	}...)

	for _, query := range queries {
		_, err := sm.db.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to execute query: %v\nQuery: %s", err, query)
		}
	}

	return nil
}

func (sm *SessionManager) getTableName(baseName string) string {
	if sm.Config.SchemaName != "" {
		return fmt.Sprintf(`"%s"."%s%s"`, sm.Config.SchemaName, sm.Config.TablePrefix, baseName)
	}
	return fmt.Sprintf(`"%s%s"`, sm.Config.TablePrefix, baseName)
}

func (sm *SessionManager) getChannelName(baseName string) string {
	return sm.Config.TablePrefix + baseName
}

// CreateSession creates a new session for the given user with the provided attributes.
func (sm *SessionManager) CreateSession(ctx context.Context, userID uuid.UUID, attributes map[string]interface{}) (uuid.UUID, error) {
	sessionID, err := uuid.NewRandom()
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to generate UUID: %v", err)
	}

	now := time.Now()
	expiresAt := now.Add(sm.Config.SessionExpiration)

	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	query := fmt.Sprintf(`
		INSERT INTO %s ("id", "user_id", "last_accessed", "expires_at", "updated_at")
		VALUES ($1, $2, $3, $4, $5)
	`, sm.getTableName("sessions"))

	_, err = tx.ExecContext(ctx, query, sessionID, userID, now, expiresAt, now)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to insert session: %v", err)
	}

	for key, value := range attributes {
		valueStr, err := convertToString(value)
		if err != nil {
			return uuid.Nil, fmt.Errorf("failed to convert attribute to string: %v", err)
		}
		if len(valueStr) > sm.Config.MaxAttributeLength {
			return uuid.Nil, fmt.Errorf("attribute value for key %s exceeds max length of %d", key, sm.Config.MaxAttributeLength)
		}

		query := fmt.Sprintf(`
			INSERT INTO %s ("session_id", "key", "value")
			VALUES ($1, $2, $3)
		`, sm.getTableName("session_attributes"))

		_, err = tx.ExecContext(ctx, query, sessionID, key, valueStr)
		if err != nil {
			return uuid.Nil, fmt.Errorf("failed to insert session attribute: %v", err)
		}
	}

	session := &Session{
		ID:           sessionID,
		UserID:       userID,
		LastAccessed: now,
		ExpiresAt:    expiresAt,
		UpdatedAt:    now,
		attributes:   attributes,
		changed:      make(map[string]bool),
		sm:           sm,
	}

	sm.mutex.Lock()
	sm.addOrUpdateCache(session)
	sm.mutex.Unlock()

	notification := sessionNotification{
		NodeID:    sm.nodeID,
		SessionID: sessionID,
	}
	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to marshal notification: %v", err)
	}

	notifyQuery := sm.pgln.NotifyQuery(sm.getChannelName("session_updates"), string(notificationJSON))
	_, err = tx.ExecContext(ctx, notifyQuery.Query, notifyQuery.Params...)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to notify about new session: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to commit transaction: %v", err)
	}

	go sm.enforceMaxSessions(ctx, userID)

	return sessionID, nil
}

// GetSession retrieves a session by its ID and optionally updates its last access time.
func (sm *SessionManager) GetSession(ctx context.Context, sessionID uuid.UUID, updateSessionAccess bool) (*Session, error) {
	sm.mutex.RLock()
	item, exists := sm.cache[sessionID]
	sm.mutex.RUnlock()

	if exists {
		sessionCopy := item.session.deepCopy()
		if updateSessionAccess {
			sm.updateSessionAccessAsync(ctx, sessionID)
		}
		return sessionCopy, nil
	}

	query := fmt.Sprintf(`
		UPDATE %s
		SET "last_accessed" = $1
		WHERE "id" = $2
		RETURNING "id", "user_id", "last_accessed", "expires_at", "updated_at"
	`, sm.getTableName("sessions"))

	session := &Session{}
	err := sm.db.GetContext(ctx, session, query, time.Now(), sessionID)
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
	session.sm = sm

	sm.mutex.Lock()
	sm.addOrUpdateCache(session)
	sm.mutex.Unlock()

	return session.deepCopy(), nil
}

// UpdateSession updates the session in the database with any changes made to its attributes.
func (sm *SessionManager) UpdateSession(ctx context.Context, session *Session) error {
	tx, err := sm.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	for key, changed := range session.changed {
		if changed {
			valueStr, err := convertToString(session.attributes[key])
			if err != nil {
				return fmt.Errorf("failed to convert attribute to string: %v", err)
			}

			query := fmt.Sprintf(`
                INSERT INTO %s ("session_id", "key", "value")
                VALUES ($1, $2, $3)
                ON CONFLICT ("session_id", "key") DO UPDATE
                SET "value" = EXCLUDED."value"
            `, sm.getTableName("session_attributes"))

			_, err = tx.ExecContext(ctx, query, session.ID, key, valueStr)
			if err != nil {
				return fmt.Errorf("failed to update session attribute: %v", err)
			}
		}
	}

	now := time.Now()
	updateQuery := fmt.Sprintf(`
        UPDATE %s
        SET "updated_at" = $1
        WHERE "id" = $2
    `, sm.getTableName("sessions"))

	_, err = tx.ExecContext(ctx, updateQuery, now, session.ID)
	if err != nil {
		return fmt.Errorf("failed to update session updated_at: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	sm.mutex.Lock()
	if item, exists := sm.cache[session.ID]; exists {
		item.session = session.deepCopy()
		item.session.UpdatedAt = now
		sm.lru.MoveToFront(item.element)
	}
	sm.mutex.Unlock()

	notification := sessionNotification{
		NodeID:    sm.nodeID,
		SessionID: session.ID,
	}
	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %v", err)
	}

	notifyQuery := sm.pgln.NotifyQuery(sm.getChannelName("session_updates"), string(notificationJSON))
	_, err = sm.db.ExecContext(ctx, notifyQuery.Query, notifyQuery.Params...)
	if err != nil {
		log.Printf("Failed to send notification for session attributes update: %v", err)
	}

	return nil
}

// DeleteSession deletes a session by its ID.
func (sm *SessionManager) DeleteSession(ctx context.Context, sessionID uuid.UUID) error {
	query := fmt.Sprintf(`
        DELETE FROM %s
        WHERE "id" = $1
    `, sm.getTableName("sessions"))

	_, err := sm.db.ExecContext(ctx, query, sessionID)
	if err != nil {
		return fmt.Errorf("failed to delete session: %v", err)
	}

	sm.mutex.Lock()
	if item, exists := sm.cache[sessionID]; exists {
		sm.lru.Remove(item.element)
		delete(sm.cache, sessionID)
	}
	sm.mutex.Unlock()

	notification := sessionNotification{
		NodeID:    sm.nodeID,
		SessionID: sessionID,
	}
	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %v", err)
	}

	notifyQuery := sm.pgln.NotifyQuery(sm.getChannelName("session_updates"), string(notificationJSON))
	_, err = sm.db.ExecContext(ctx, notifyQuery.Query, notifyQuery.Params...)
	if err != nil {
		log.Printf("Failed to send notification for session deletion: %v", err)
	}

	return nil
}

// DeleteAllUserSessions deletes all sessions for a given user.
func (sm *SessionManager) DeleteAllUserSessions(ctx context.Context, userID uuid.UUID) error {
	query := fmt.Sprintf(`
        DELETE FROM %s
        WHERE "user_id" = $1
        RETURNING "id"
    `, sm.getTableName("sessions"))

	rows, err := sm.db.QueryContext(ctx, query, userID)
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

	sm.mutex.Lock()
	for _, sessionID := range deletedSessionIDs {
		if item, exists := sm.cache[sessionID]; exists {
			sm.lru.Remove(item.element)
			delete(sm.cache, sessionID)
		}
	}
	sm.mutex.Unlock()

	channelName := sm.getChannelName("session_updates")
	for _, sessionID := range deletedSessionIDs {
		notification := sessionNotification{
			NodeID:    sm.nodeID,
			SessionID: sessionID,
		}
		notificationJSON, err := json.Marshal(notification)
		if err != nil {
			log.Printf("Failed to marshal notification: %v", err)
			continue
		}
		notifyQuery := sm.pgln.NotifyQuery(channelName, string(notificationJSON))
		_, err = sm.db.ExecContext(ctx, notifyQuery.Query, notifyQuery.Params...)
		if err != nil {
			log.Printf("Failed to send notification for session deletion: %v", err)
		}
	}

	return nil
}

func (sm *SessionManager) listAttributes(ctx context.Context, sessionID uuid.UUID) (map[string]interface{}, error) {
	query := fmt.Sprintf(`
        SELECT "key", "value"
        FROM %s
        WHERE "session_id" = $1
    `, sm.getTableName("session_attributes"))

	rows, err := sm.db.QueryxContext(ctx, query, sessionID)
	if err != nil {
		return nil, fmt.Errorf("failed to query session attributes: %v", err)
	}
	defer rows.Close()

	attributes := make(map[string]interface{})
	for rows.Next() {
		var key, valueStr string
		err := rows.Scan(&key, &valueStr)
		if err != nil {
			return nil, fmt.Errorf("failed to scan attribute row: %v", err)
		}
		value, err := convertFromString(valueStr)
		if err != nil {
			return nil, fmt.Errorf("failed to convert attribute from string: %v", err)
		}
		attributes[key] = value
	}

	return attributes, nil
}

func convertToString(value interface{}) (string, error) {
	valueStr, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("failed to marshal value: %v", err)
	}
	return string(valueStr), nil
}

func convertFromString(valueStr string) (interface{}, error) {
	var value interface{}
	err := json.Unmarshal([]byte(valueStr), &value)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal value: %v", err)
	}
	return value, nil
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

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if item, exists := sm.cache[notification.SessionID]; exists {
		sm.lru.Remove(item.element)
		delete(sm.cache, notification.SessionID)
	}
	log.Printf("Session %s removed from cache due to update", notification.SessionID)
}

func (sm *SessionManager) handleOutOfSync(channel string) error {
	log.Printf("Out of sync detected on channel %s, refreshing cache", channel)
	return sm.refreshCache(context.Background())
}

func (sm *SessionManager) refreshCache(ctx context.Context) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	var mostRecentUpdate time.Time
	for _, item := range sm.cache {
		if item.session.UpdatedAt.After(mostRecentUpdate) {
			mostRecentUpdate = item.session.UpdatedAt
		}
	}

	threshold := mostRecentUpdate.Add(-2 * time.Second)

	query := fmt.Sprintf(`
        SELECT "id", "user_id", "last_accessed", "expires_at", "updated_at"
        FROM %s
        WHERE "updated_at" > $1
        ORDER BY "updated_at" DESC
        LIMIT $2
    `, sm.getTableName("sessions"))

	rows, err := sm.db.QueryxContext(ctx, query, threshold, sm.Config.CacheSize+1)
	if err != nil {
		return fmt.Errorf("failed to query sessions: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var session Session
		err := rows.StructScan(&session)
		if err != nil {
			return fmt.Errorf("failed to scan session: %v", err)
		}

		attributes, err := sm.listAttributes(ctx, session.ID)
		if err != nil {
			return fmt.Errorf("failed to list attributes: %v", err)
		}
		session.attributes = attributes
		session.changed = make(map[string]bool)
		session.sm = sm

		sm.addOrUpdateCache(&session)
	}

	return nil
}

func (sm *SessionManager) addOrUpdateCache(session *Session) {
	if item, exists := sm.cache[session.ID]; exists {
		if session.UpdatedAt.After(item.session.UpdatedAt) {
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

func (sm *SessionManager) cleanupWorker() {
	defer sm.wg.Done()
	ticker := time.NewTicker(sm.Config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := sm.cleanupExpiredSessions(context.Background()); err != nil {
				log.Printf("Error cleaning up expired sessions: %v", err)
			}
		case <-sm.shutdownChan:
			return
		}
	}
}

func (sm *SessionManager) cleanupExpiredSessions(ctx context.Context) error {
	now := time.Now()
	query := fmt.Sprintf(`
        DELETE FROM %s
        WHERE "expires_at" < $1 OR "last_accessed" < $2
        RETURNING "id"
    `, sm.getTableName("sessions"))

	rows, err := sm.db.QueryContext(ctx, query, now, now.Add(-sm.Config.InactivityDuration))
	if err != nil {
		return fmt.Errorf("failed to delete expired sessions: %v", err)
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

	channelName := sm.getChannelName("session_updates")
	for _, sessionID := range deletedSessionIDs {
		notification := sessionNotification{
			NodeID:    sm.nodeID,
			SessionID: sessionID,
		}
		notificationJSON, err := json.Marshal(notification)
		if err != nil {
			log.Printf("Failed to marshal notification: %v", err)
			continue
		}
		notifyQuery := sm.pgln.NotifyQuery(channelName, string(notificationJSON))
		_, err = sm.db.ExecContext(ctx, notifyQuery.Query, notifyQuery.Params...)
		if err != nil {
			log.Printf("Failed to send notification for session deletion: %v", err)
		}
	}

	return nil
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

	sm.pgln.Shutdown()

	if err := sm.db.Close(); err != nil {
		return fmt.Errorf("failed to close database connection: %v", err)
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

		channelName := sm.getChannelName("session_updates")
		for _, sessionID := range deletedSessionIDs {
			notification := sessionNotification{
				NodeID:    sm.nodeID,
				SessionID: sessionID,
			}
			notificationJSON, err := json.Marshal(notification)
			if err != nil {
				log.Printf("Failed to marshal notification: %v", err)
				continue
			}
			notifyQuery := sm.pgln.NotifyQuery(channelName, string(notificationJSON))
			_, err = sm.db.ExecContext(ctx, notifyQuery.Query, notifyQuery.Params...)
			if err != nil {
				log.Printf("Failed to notify about removed session: %v", err)
			}
		}
	}
}

func (sm *SessionManager) updateSessionAccessAsync(ctx context.Context, sessionID uuid.UUID) {
	sm.lastAccessUpdateMutex.Lock()
	sm.lastAccessUpdates[sessionID] = time.Now()
	sm.lastAccessUpdateMutex.Unlock()
}

func (sm *SessionManager) lastAccessUpdateWorker() {
	defer sm.wg.Done()
	ticker := time.NewTicker(sm.Config.LastAccessUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
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
        RETURNING s.id, s.last_accessed
    `, strings.Join(placeholders, ","), sm.getTableName("sessions"))

	rows, err := tx.QueryxContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to execute batch update: %v", err)
	}
	defer rows.Close()

	sm.mutex.Lock()
	for rows.Next() {
		var sessionID uuid.UUID
		var lastAccessed time.Time
		err := rows.Scan(&sessionID, &lastAccessed)
		if err != nil {
			log.Printf("Failed to scan updated session: %v", err)
			continue
		}
		if item, exists := sm.cache[sessionID]; exists {
			item.session.LastAccessed = lastAccessed
			sm.lru.MoveToFront(item.element)
		}
		delete(updates, sessionID)
	}
	sm.mutex.Unlock()

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}
