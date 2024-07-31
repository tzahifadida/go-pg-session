package gopgsession

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"log"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestReadmeExamples(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, pgConnString, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new SessionManager
	cfg := DefaultConfig()
	cfg.MaxSessions = 10
	cfg.SessionExpiration = 24 * time.Hour // 1 day
	cfg.CreateSchemaIfMissing = true

	sessionManager, err := NewSessionManager(cfg, pgConnString)
	require.NoError(t, err)
	defer sessionManager.Shutdown(context.Background())

	// Example: Creating a Session
	userID := uuid.New()
	attributes := map[string]interface{}{
		"role": "admin",
		"preferences": map[string]string{
			"theme": "dark",
		},
	}

	sessionID, err := sessionManager.CreateSession(context.Background(), userID, attributes)
	require.NoError(t, err)
	log.Printf("Created session with ID: %s", sessionID)

	// Example: Retrieving a Session
	session, err := sessionManager.GetSession(context.Background(), sessionID, true)
	require.NoError(t, err)
	log.Printf("Retrieved session for user ID: %s", session.UserID)

	// Example: Updating a Session Attribute
	err = session.UpdateAttribute("preferences", map[string]string{"theme": "light"})
	require.NoError(t, err)

	err = sessionManager.UpdateSession(context.Background(), session)
	require.NoError(t, err)
	log.Printf("Updated session attribute for session ID: %s", sessionID)

	// Verify the updated attribute
	updatedSession, err := sessionManager.GetSession(context.Background(), sessionID, false)
	require.NoError(t, err)
	require.Equal(t, "light", updatedSession.GetAttributes()["preferences"].(map[string]string)["theme"])

	// Example: Deleting a Session
	err = sessionManager.DeleteSession(context.Background(), sessionID)
	require.NoError(t, err)
	log.Printf("Deleted session with ID: %s", sessionID)

	// Verify the session was deleted
	_, err = sessionManager.GetSession(context.Background(), sessionID, false)
	require.Error(t, err)
}

func TestSessionManager(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, pgConnString, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new SessionManager
	cfg := &Config{
		MaxSessions:               3,
		MaxAttributeLength:        1024,
		SessionExpiration:         24 * time.Hour,
		InactivityDuration:        1 * time.Hour,
		CleanupInterval:           5 * time.Minute,
		CacheSize:                 100,
		TablePrefix:               "test_",
		SchemaName:                "test_schema",
		CreateSchemaIfMissing:     true,
		LastAccessUpdateInterval:  1 * time.Minute,
		LastAccessUpdateBatchSize: 100,
	}

	sm, err := NewSessionManager(cfg, pgConnString)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	// Test CreateSession
	t.Run("CreateSession", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]interface{}{
			"key1": "value1",
			"key2": 42,
		}

		sessionID, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, sessionID)

		// Verify the session was created
		session, err := sm.GetSession(context.Background(), sessionID, false)
		require.NoError(t, err)
		assert.Equal(t, userID, session.UserID)
		assert.Equal(t, attributes["key1"], session.GetAttributes()["key1"])
		assert.Equal(t, attributes["key2"], session.GetAttributes()["key2"])
	})

	// Test GetSession
	t.Run("GetSession", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]interface{}{"key": "value"}

		sessionID, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		session, err := sm.GetSession(context.Background(), sessionID, true)
		require.NoError(t, err)
		assert.Equal(t, userID, session.UserID)
		assert.Equal(t, "value", session.GetAttributes()["key"])
	})

	// Test UpdateSession
	t.Run("UpdateSession", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]interface{}{"key": "value"}

		sessionID, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		session, err := sm.GetSession(context.Background(), sessionID, false)
		require.NoError(t, err)

		err = session.UpdateAttribute("key", "new_value")
		require.NoError(t, err)

		err = sm.UpdateSession(context.Background(), session)
		require.NoError(t, err)

		updatedSession, err := sm.GetSession(context.Background(), sessionID, false)
		require.NoError(t, err)
		assert.Equal(t, "new_value", updatedSession.GetAttributes()["key"])
	})

	// Test DeleteSession
	t.Run("DeleteSession", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]interface{}{"key": "value"}

		sessionID, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		err = sm.DeleteSession(context.Background(), sessionID)
		require.NoError(t, err)

		_, err = sm.GetSession(context.Background(), sessionID, false)
		assert.Error(t, err)
	})

	// Test DeleteAllUserSessions
	t.Run("DeleteAllUserSessions", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]interface{}{"key": "value"}

		sessionID1, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		sessionID2, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		err = sm.DeleteAllUserSessions(context.Background(), userID)
		require.NoError(t, err)

		_, err = sm.GetSession(context.Background(), sessionID1, false)
		assert.Error(t, err)

		_, err = sm.GetSession(context.Background(), sessionID2, false)
		assert.Error(t, err)
	})

	// Test enforceMaxSessions
	t.Run("EnforceMaxSessions", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]interface{}{"key": "value"}

		sessionID1, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		sessionID2, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		sessionID3, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// This should trigger enforceMaxSessions and delete the oldest session
		sessionID4, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		time.Sleep(2 * time.Second)
		_, err = sm.GetSession(context.Background(), sessionID1, false)
		assert.Error(t, err) // This session should have been deleted

		_, err = sm.GetSession(context.Background(), sessionID2, false)
		assert.NoError(t, err)

		_, err = sm.GetSession(context.Background(), sessionID3, false)
		assert.NoError(t, err)

		_, err = sm.GetSession(context.Background(), sessionID4, false)
		assert.NoError(t, err)
	})

	// Test cleanupExpiredSessions
	t.Run("CleanupExpiredSessions", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]interface{}{"key": "value"}

		// Create a session with a short expiration time
		cfg.SessionExpiration = 1 * time.Second
		sessionID, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Wait for the session to expire
		time.Sleep(2 * time.Second)

		// Manually trigger cleanup
		err = sm.cleanupExpiredSessions(context.Background())
		require.NoError(t, err)

		// The expired session should be deleted
		_, err = sm.GetSession(context.Background(), sessionID, false)
		assert.Error(t, err)
	})

	// Test refreshCache
	t.Run("refreshCache", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]interface{}{"key": "value"}

		sessionID, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Simulate cache invalidation
		sm.mutex.Lock()
		delete(sm.cache, sessionID)
		sm.mutex.Unlock()

		// Refresh the cache
		err = sm.refreshCache(context.Background())
		require.NoError(t, err)

		// The session should be back in the cache
		sm.mutex.RLock()
		_, exists := sm.cache[sessionID]
		sm.mutex.RUnlock()
		assert.True(t, exists)
	})

	// Test updateSessionAccessAsync
	t.Run("UpdateSessionAccessAsync", func(t *testing.T) {
		sessionID := uuid.New()
		sm.updateSessionAccessAsync(context.Background(), sessionID)

		sm.lastAccessUpdateMutex.Lock()
		_, exists := sm.lastAccessUpdates[sessionID]
		sm.lastAccessUpdateMutex.Unlock()

		assert.True(t, exists)
	})

	// New test for processLastAccessUpdates
	t.Run("ProcessLastAccessUpdates", func(t *testing.T) {
		// Create a large number of sessions
		sessionCount := 250 // This should be more than LastAccessUpdateBatchSize
		sessionIDs := make([]uuid.UUID, sessionCount)

		for i := 0; i < sessionCount; i++ {
			userID := uuid.New()
			sessionID, err := sm.CreateSession(context.Background(), userID, nil)
			require.NoError(t, err)
			sessionIDs[i] = sessionID
		}

		time.Sleep(15 * time.Second)
		// Update last access time for all sessions
		for _, sessionID := range sessionIDs {
			sm.updateSessionAccessAsync(context.Background(), sessionID)
		}

		// Process the updates
		sm.processLastAccessUpdates()

		// Verify that all sessions have been updated
		for _, sessionID := range sessionIDs {
			session, err := sm.GetSession(context.Background(), sessionID, false)
			require.NoError(t, err)
			assert.WithinDuration(t, time.Now(), session.LastAccessed, 10*time.Second)
		}

		// Verify that lastAccessUpdates map is empty
		sm.lastAccessUpdateMutex.Lock()
		assert.Empty(t, sm.lastAccessUpdates)
		sm.lastAccessUpdateMutex.Unlock()

		// Verify batch processing by checking the database logs
		// This part is tricky and might require additional setup to capture and analyze database logs
		// For now, we'll just check that the function completed without errors
	})
}

func startPostgresContainer(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:13",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test_user",
			"POSTGRES_PASSWORD": "test_password",
			"POSTGRES_DB":       "test_db",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections"),
	}

	postgres, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to start postgres container: %v", err)
	}

	host, err := postgres.Host(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get postgres host: %v", err)
	}

	port, err := postgres.MappedPort(ctx, "5432")
	if err != nil {
		return nil, "", fmt.Errorf("failed to get postgres port: %v", err)
	}

	pgConnString := fmt.Sprintf("host=%s port=%d user=test_user password=test_password dbname=test_db sslmode=disable", host, port.Int())

	// Ping the database to ensure it's ready
	db, err := sqlx.Open("pgx", pgConnString)
	defer db.Close()

	err = pingDatabase(ctx, db)
	if err != nil {
		return nil, "", fmt.Errorf("failed to ping database: %v", err)
	}

	return postgres, pgConnString, nil
}

func pingDatabase(ctx context.Context, db *sqlx.DB) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("database ping timed out")
		default:
			err := db.PingContext(ctx)
			if err == nil {
				return nil
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
