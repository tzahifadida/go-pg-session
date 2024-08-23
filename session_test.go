package gopgsession

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestReadmeExamplesPrev(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new SessionManager
	cfg := DefaultConfig()
	cfg.MaxSessions = 10
	cfg.SessionExpiration = 24 * time.Hour // 1 day
	cfg.CreateSchemaIfMissing = true

	sessionManager, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sessionManager.Shutdown(context.Background())

	// Set up a fake clock for testing
	fakeClock := clockwork.NewFakeClock()
	sessionManager.setClock(fakeClock)

	// Example: Creating a Session
	userID := uuid.New()
	attributes := map[string]SessionAttributeValue{
		"role":        {Value: "admin", Marshaled: false},
		"preferences": {Value: map[string]string{"theme": "dark"}, Marshaled: false},
	}

	session, err := sessionManager.CreateSession(context.Background(), userID, attributes)
	require.NoError(t, err)
	log.Printf("Created session with ID: %s", session.ID)

	// Advance the clock
	fakeClock.Advance(time.Second)

	// Example: Retrieving a Session
	retrievedSession, err := sessionManager.GetSessionWithVersion(context.Background(), session.ID, 1, WithDoNotUpdateSessionLastAccess())
	require.NoError(t, err)
	log.Printf("Retrieved session for user ID: %s", retrievedSession.UserID)

	// Example: Updating a Session Attribute
	newPreferences := map[string]string{"theme": "light"}
	err = retrievedSession.UpdateAttribute("preferences", newPreferences)
	require.NoError(t, err)

	updatedSession, err := sessionManager.UpdateSession(context.Background(), retrievedSession)
	require.NoError(t, err)
	log.Printf("Updated session attribute for session ID: %s", updatedSession.ID)

	// Advance the clock
	fakeClock.Advance(time.Second)

	// Verify the updated attribute
	finalSession, err := sessionManager.GetSessionWithVersion(context.Background(), updatedSession.ID, updatedSession.Version)
	require.NoError(t, err)

	var preferences map[string]string
	attr, err := finalSession.GetAttributeAndRetainUnmarshaled("preferences", &preferences)
	require.NoError(t, err)
	assert.Equal(t, "light", preferences["theme"])
	assert.False(t, attr.Marshaled)

	// Example: Deleting a Session
	err = sessionManager.DeleteSession(context.Background(), finalSession.ID)
	require.NoError(t, err)
	log.Printf("Deleted session with ID: %s", finalSession.ID)

	// Advance the clock
	fakeClock.Advance(time.Second)

	// Verify the session was deleted
	_, err = sessionManager.GetSessionWithVersion(context.Background(), finalSession.ID, finalSession.Version)
	require.Error(t, err)
}

func TestSessionManager(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new SessionManager
	cfg := &Config{
		MaxSessions:               3,
		MaxAttributeLength:        1024,
		SessionExpiration:         24 * time.Hour,
		InactivityDuration:        1 * time.Hour,
		CleanupInterval:           100 * time.Hour,
		CacheSize:                 100,
		TablePrefix:               "test_",
		SchemaName:                "test_schema",
		CreateSchemaIfMissing:     true,
		LastAccessUpdateInterval:  1 * time.Minute,
		LastAccessUpdateBatchSize: 100,
	}

	sm, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	// Set up a fake clock for testing
	fakeClock := clockwork.NewFakeClock()
	sm.setClock(fakeClock)

	// Create HMAC signer pool
	signerPool := NewHMACSHA256SignerPool([]byte("test_secret"), 10)

	// Test CreateSession
	t.Run("CreateSession", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key1": {Value: "value1", Marshaled: false},
			"key2": {Value: "42", Marshaled: false},
		}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, session.ID)

		// Advance the clock
		fakeClock.Advance(time.Second)

		// Verify the session was created
		retrievedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, 1)
		require.NoError(t, err)
		assert.Equal(t, userID, retrievedSession.UserID)

		key1Value, ok := retrievedSession.GetAttributes()["key1"].Value.(string)
		require.True(t, ok, "key1 value is not a string")
		assert.Equal(t, "value1", key1Value)

		key2Value, ok := retrievedSession.GetAttributes()["key2"].Value.(string)
		require.True(t, ok, "key2 value is not a string")
		assert.Equal(t, "42", key2Value)

		assert.Equal(t, 1, retrievedSession.Version)
	})

	// Test GetSessionWithVersion
	t.Run("GetSessionWithVersion", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "value", Marshaled: false},
		}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Advance the clock
		fakeClock.Advance(time.Second)

		retrievedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, 1, WithDoNotUpdateSessionLastAccess())
		require.NoError(t, err)
		assert.Equal(t, userID, retrievedSession.UserID)
		keyValue, ok := retrievedSession.GetAttributes()["key"].Value.(string)
		require.True(t, ok, "key value is not a string")
		assert.Equal(t, "value", keyValue)
		time.Sleep(1 * time.Second)
		sm.processLastAccessUpdates()

		// Test updating last accessed time
		oldLastAccessed := retrievedSession.LastAccessed

		fakeClock.Advance(time.Minute)
		retrievedSession, err = sm.GetSessionWithVersion(context.Background(), session.ID, 1)
		require.NoError(t, err)
		assert.True(t, retrievedSession.LastAccessed.After(oldLastAccessed))
	})

	// Test UpdateSession
	t.Run("UpdateSession", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "value", Marshaled: false},
		}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Advance the clock
		fakeClock.Advance(time.Second)

		retrievedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, 1)
		require.NoError(t, err)

		err = retrievedSession.UpdateAttribute("key", "new_value")
		require.NoError(t, err)

		err = retrievedSession.UpdateAttribute("new_key", "another_value")
		require.NoError(t, err)

		updatedSession, err := sm.UpdateSession(context.Background(), retrievedSession)
		require.NoError(t, err)
		assert.Equal(t, 2, updatedSession.Version)

		// Advance the clock
		fakeClock.Advance(time.Second)

		finalSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, 2)
		require.NoError(t, err)
		keyValue, ok := finalSession.GetAttributes()["key"].Value.(string)
		require.True(t, ok, "key value is not a string")
		assert.Equal(t, "new_value", keyValue)
		newKeyValue, ok := finalSession.GetAttributes()["new_key"].Value.(string)
		require.True(t, ok, "new_key value is not a string")
		assert.Equal(t, "another_value", newKeyValue)
		assert.Equal(t, 2, finalSession.Version)
	})

	// Test DeleteSession
	t.Run("DeleteSession", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "value", Marshaled: false},
		}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Advance the clock
		fakeClock.Advance(time.Second)

		err = sm.DeleteSession(context.Background(), session.ID)
		require.NoError(t, err)

		// Advance the clock
		fakeClock.Advance(time.Second)

		_, err = sm.GetSessionWithVersion(context.Background(), session.ID, 1)
		assert.Error(t, err)
	})

	// Test DeleteAllUserSessions
	t.Run("DeleteAllUserSessions", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "value", Marshaled: false},
		}

		session1, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Advance the clock
		fakeClock.Advance(time.Second)

		session2, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Advance the clock
		fakeClock.Advance(time.Second)

		err = sm.DeleteAllUserSessions(context.Background(), userID)
		require.NoError(t, err)

		// Advance the clock
		fakeClock.Advance(time.Second)

		_, err = sm.GetSessionWithVersion(context.Background(), session1.ID, 1)
		assert.Error(t, err)

		_, err = sm.GetSessionWithVersion(context.Background(), session2.ID, 1)
		assert.Error(t, err)
	})

	// Test enforceMaxSessions
	t.Run("EnforceMaxSessions", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "value", Marshaled: false},
		}

		session1, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)
		fakeClock.Advance(time.Second)

		session2, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)
		fakeClock.Advance(time.Second)

		session3, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)
		fakeClock.Advance(time.Second)

		// This should trigger enforceMaxSessions and delete the oldest session
		session4, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)
		fakeClock.Advance(time.Second)

		// Wait for the enforce method to finish
		time.Sleep(2 * time.Second)

		_, err = sm.GetSessionWithVersion(context.Background(), session1.ID, 1)
		assert.Error(t, err) // This session should have been deleted

		_, err = sm.GetSessionWithVersion(context.Background(), session2.ID, 1)
		assert.NoError(t, err)

		_, err = sm.GetSessionWithVersion(context.Background(), session3.ID, 1)
		assert.NoError(t, err)

		_, err = sm.GetSessionWithVersion(context.Background(), session4.ID, 1)
		assert.NoError(t, err)
	})

	// Test cleanupExpiredSessions
	t.Run("CleanupExpiredSessions", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "value", Marshaled: false},
		}

		// Create a session with a short expiration time
		prevSessionExpiration := cfg.SessionExpiration
		defer func() {
			cfg.SessionExpiration = prevSessionExpiration
		}()
		cfg.SessionExpiration = 1 * time.Minute
		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Advance the clock past the expiration time
		fakeClock.Advance(2 * time.Minute)

		// Manually trigger cleanup
		sm.processLastAccessUpdates()
		err = sm.cleanupExpiredSessions(context.Background())
		require.NoError(t, err)

		// The expired session should be deleted
		_, err = sm.GetSessionWithVersion(context.Background(), session.ID, 1)
		assert.Error(t, err)
	})

	// Test refreshCache
	t.Run("RefreshCache", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "value", Marshaled: false},
		}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Advance the clock
		fakeClock.Advance(time.Second)

		// Simulate cache invalidation
		sm.clearCache()

		// Refresh the cache
		err = sm.refreshCache(context.Background())
		require.NoError(t, err)

		// The session should be back in the cache
		retrievedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, 1)
		require.NoError(t, err)
		assert.NotNil(t, retrievedSession)
	})

	// Test DeleteAttribute
	t.Run("DeleteAttribute", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key1": {Value: "value1", Marshaled: false},
			"key2": {Value: "value2", Marshaled: false},
		}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Advance the clock
		fakeClock.Advance(time.Second)

		retrievedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, 1)
		require.NoError(t, err)

		retrievedSession.DeleteAttribute("key1")

		updatedSession, err := sm.UpdateSession(context.Background(), retrievedSession)
		require.NoError(t, err)

		// Advance the clock
		fakeClock.Advance(time.Second)

		finalSession, err := sm.GetSessionWithVersion(context.Background(), updatedSession.ID, updatedSession.Version)
		require.NoError(t, err)
		_, exists := finalSession.GetAttribute("key1")
		assert.False(t, exists)
		_, exists = finalSession.GetAttribute("key2")
		assert.True(t, exists)
	})
	// Test RemoveAllUserCachedSessionsFromAllNodes
	t.Run("RemoveAllUserCachedSessionsFromAllNodes", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "value", Marshaled: false},
		}

		session1, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)
		fakeClock.Advance(time.Second)

		session2, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)
		fakeClock.Advance(time.Second)

		err = sm.RemoveAllUserCachedSessionsFromAllNodes(userID)
		require.NoError(t, err)

		// Sessions should still exist in the database but not in the cache
		retrievedSession1, err := sm.GetSessionWithVersion(context.Background(), session1.ID, 1)
		require.NoError(t, err)
		assert.NotNil(t, retrievedSession1)

		retrievedSession2, err := sm.GetSessionWithVersion(context.Background(), session2.ID, 1)
		require.NoError(t, err)
		assert.NotNil(t, retrievedSession2)

		// Clear the cache again
		sm.clearCache()

		// Sessions should still be retrievable from the database
		retrievedSession1, err = sm.GetSessionWithVersion(context.Background(), session1.ID, 1)
		require.NoError(t, err)
		assert.NotNil(t, retrievedSession1)

		retrievedSession2, err = sm.GetSessionWithVersion(context.Background(), session2.ID, 1)
		require.NoError(t, err)
		assert.NotNil(t, retrievedSession2)
	})

	// Test EncodeSessionIDAndVersion and ParseSessionIDAndVersion
	t.Run("EncodeDecodeSessionIDAndVersion", func(t *testing.T) {
		sessionID := uuid.New()
		version := 42

		encoded := sm.EncodeSessionIDAndVersion(sessionID, version)
		decodedSessionID, decodedVersion, err := sm.ParseSessionIDAndVersion(encoded)

		require.NoError(t, err)
		assert.Equal(t, sessionID, decodedSessionID)
		assert.Equal(t, version, decodedVersion)
	})

	// Test session signing and verification
	t.Run("SessionSigningAndVerification", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "value", Marshaled: false},
		}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Sign the session ID
		encodedSessionID := sm.EncodeSessionIDAndVersion(session.ID, 1)
		encodedAndSigned, err := signerPool.SignAndEncode(encodedSessionID)
		require.NoError(t, err)

		// Verify the signature
		isValid, decodedSessionID, err := signerPool.VerifyAndDecode(encodedAndSigned)
		require.NoError(t, err)
		assert.True(t, isValid)
		assert.Equal(t, encodedSessionID, decodedSessionID)

		// Decode the session ID and version
		decodedID, version, err := sm.ParseSessionIDAndVersion(decodedSessionID)
		require.NoError(t, err)
		assert.Equal(t, session.ID, decodedID)
		assert.Equal(t, 1, version)

		// Verify that we can retrieve the session
		retrievedSession, err := sm.GetSessionWithVersion(context.Background(), decodedID, version)
		require.NoError(t, err)
		assert.Equal(t, userID, retrievedSession.UserID)
	})

	// Test expired attributes
	t.Run("ExpiredAttributes", func(t *testing.T) {
		err := sm.DeleteAllSessions(ctx)
		require.NoError(t, err)

		userID := uuid.New()
		now := fakeClock.Now()
		attributes := map[string]SessionAttributeValue{
			"permanent": {Value: "permanent_value", Marshaled: false, ExpiresAt: nil},
			"expiring":  {Value: "expiring_value", Marshaled: false, ExpiresAt: &now},
		}
		prevSessionExpiration := cfg.SessionExpiration
		defer func() {
			cfg.SessionExpiration = prevSessionExpiration
		}()
		cfg.SessionExpiration = 24 * time.Hour
		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Advance the clock past the expiration time
		fakeClock.Advance(2 * time.Second)

		sm.processLastAccessUpdates()
		sm.cleanupExpiredSessions(ctx)

		retrievedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, session.Version, WithForceRefresh())
		require.NoError(t, err)

		// The expiring attribute should be gone
		_, exists := retrievedSession.GetAttribute("expiring")
		assert.False(t, exists)

		// The permanent attribute should still be there
		permanentAttr, exists := retrievedSession.GetAttribute("permanent")
		assert.True(t, exists)
		permanentValue, ok := permanentAttr.Value.(string)
		require.True(t, ok, "permanent attribute value is not a string")
		assert.Equal(t, "permanent_value", permanentValue)

		// Add a new expiring attribute
		futureTime := fakeClock.Now().Add(time.Hour)
		err = retrievedSession.UpdateAttribute("new_expiring", "new_expiring_value", WithExpiresAt(futureTime))
		require.NoError(t, err)

		updatedSession, err := sm.UpdateSession(context.Background(), retrievedSession)
		require.NoError(t, err)

		// Verify the new expiring attribute
		finalSession, err := sm.GetSessionWithVersion(context.Background(), updatedSession.ID, updatedSession.Version)
		require.NoError(t, err)
		newExpiringAttr, exists := finalSession.GetAttribute("new_expiring")
		assert.True(t, exists)
		newExpiringValue, ok := newExpiringAttr.Value.(string)
		require.True(t, ok, "new_expiring attribute value is not a string")
		assert.Equal(t, "new_expiring_value", newExpiringValue)

		// Advance the clock past the new expiration time
		fakeClock.Advance(1*time.Hour + 5*time.Minute)

		_, err = sm.GetSessionWithVersion(context.Background(), updatedSession.ID, updatedSession.Version)
		require.NoError(t, err)
		time.Sleep(2 * time.Second)
		sm.processLastAccessUpdates()
		sm.cleanupExpiredSessions(ctx)

		// The new expiring attribute should now be gone
		expiredSession, err := sm.GetSessionWithVersion(context.Background(), finalSession.ID, finalSession.Version)
		require.NoError(t, err)
		_, exists = expiredSession.GetAttribute("new_expiring")
		assert.False(t, exists)
	})

	// Test concurrent session updates
	t.Run("ConcurrentSessionUpdates", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"counter": {Value: "0", Marshaled: false},
		}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		concurrentUpdates := 100
		done := make(chan bool)

		for i := 0; i < concurrentUpdates; i++ {
			go func() {
				defer func() { done <- true }()

				for retry := 0; retry < 15; retry++ {
					retrievedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, 1)
					require.NoError(t, err)

					counterAttr, ok := retrievedSession.GetAttribute("counter")
					require.True(t, ok)

					counterValue, ok := counterAttr.Value.(string)
					require.True(t, ok, "counter value is not a string")

					counter, err := strconv.Atoi(counterValue)
					require.NoError(t, err)

					err = retrievedSession.UpdateAttribute("counter", strconv.Itoa(counter+1))
					require.NoError(t, err)

					_, err = sm.UpdateSession(context.Background(), retrievedSession, WithCheckVersion())
					if err == nil {
						break
					}
					sleepDuration := time.Duration(rand.Intn(2000)) * time.Millisecond
					time.Sleep(sleepDuration)
				}
			}()
		}

		for i := 0; i < concurrentUpdates; i++ {
			<-done
		}
		time.Sleep(5 * time.Second)

		finalSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, 1)
		require.NoError(t, err)

		finalCounterAttr, ok := finalSession.GetAttribute("counter")
		require.True(t, ok)

		finalCounterValue, ok := finalCounterAttr.Value.(string)
		require.True(t, ok, "final counter value is not a string")

		finalCounter, err := strconv.Atoi(finalCounterValue)
		require.NoError(t, err)
		assert.Equal(t, concurrentUpdates, finalCounter)
	})
}

func TestSessionManagerWithRealClock(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new SessionManager with real clock
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CleanupInterval = 1 * time.Second
	cfg.LastAccessUpdateInterval = 1 * time.Second

	sm, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	// Test automatic cleanup of expired sessions
	t.Run("AutomaticCleanup", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "value", Marshaled: false},
		}
		prevSessionExpiration := cfg.SessionExpiration
		defer func() {
			cfg.SessionExpiration = prevSessionExpiration
		}()
		// Create a session with a short expiration time
		cfg.SessionExpiration = 2 * time.Second
		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Wait for the session to expire and be cleaned up
		time.Sleep(3 * time.Second)

		// The expired session should be deleted
		_, err = sm.GetSessionWithVersion(context.Background(), session.ID, 1)
		assert.Error(t, err)
	})

	// Test automatic last access time updates
	t.Run("AutomaticLastAccessUpdate", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "value", Marshaled: false},
		}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		initialSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, 1, WithDoNotUpdateSessionLastAccess())
		require.NoError(t, err)

		// Wait for the last access time to be updated
		time.Sleep(2 * time.Second)

		updatedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, 1)
		require.NoError(t, err)

		assert.True(t, updatedSession.LastAccessed.After(initialSession.LastAccessed))
	})
}

func TestSessionManagerEdgeCases(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true

	sm, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	// Test creating a session with empty attributes
	t.Run("CreateSessionWithEmptyAttributes", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, session.ID)

		retrievedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, 1)
		require.NoError(t, err)
		assert.Empty(t, retrievedSession.GetAttributes())
	})

	// Test updating a non-existent session
	t.Run("UpdateNonExistentSession", func(t *testing.T) {
		nonExistentSession := &Session{
			ID:      uuid.New(),
			UserID:  uuid.New(),
			Version: 1,
			sm:      sm,
		}

		_, err := sm.UpdateSession(context.Background(), nonExistentSession)
		assert.Error(t, err)
	})

	// Test deleting a non-existent session
	t.Run("DeleteNonExistentSession", func(t *testing.T) {
		err := sm.DeleteSession(context.Background(), uuid.New())
		assert.NoError(t, err) // Deleting a non-existent session should not return an error
	})

	// Test getting a session with an invalid UUID
	t.Run("GetSessionWithInvalidUUID", func(t *testing.T) {
		_, err := sm.GetSessionWithVersion(context.Background(), uuid.Nil, 1)
		assert.Error(t, err)
	})

	// Test encoding and decoding an invalid session ID
	t.Run("EncodeDecodeInvalidSessionID", func(t *testing.T) {
		_, _, err := sm.ParseSessionIDAndVersion("invalid_encoded_data")
		assert.Error(t, err)
	})
}

func TestSessionManagerPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 10000

	sm, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	// Test creating many sessions
	t.Run("CreateManySessions", func(t *testing.T) {
		numSessions := 10000
		userID := uuid.New()

		start := time.Now()
		for i := 0; i < numSessions; i++ {
			attributes := map[string]SessionAttributeValue{
				"key": {Value: fmt.Sprintf("value%d", i)},
			}
			_, err := sm.CreateSession(context.Background(), userID, attributes)
			require.NoError(t, err)
		}
		elapsed := time.Since(start)

		log.Printf("Created %d sessions in %s", numSessions, elapsed)
		assert.Less(t, elapsed, 30*time.Second) // Adjust this threshold as needed
	})

	// Test retrieving many sessions
	t.Run("RetrieveManySessionsWithCache", func(t *testing.T) {
		numSessions := 10000
		var sessionIDs []uuid.UUID

		// Create sessions
		start := time.Now()
		for i := 0; i < numSessions; i++ {
			attributes := map[string]SessionAttributeValue{
				"key": {Value: fmt.Sprintf("value%d", i)},
			}
			userID := uuid.New()
			session, err := sm.CreateSession(context.Background(), userID, attributes)
			require.NoError(t, err)
			sessionIDs = append(sessionIDs, session.ID)
		}
		elapsed := time.Since(start)
		log.Printf("Created %d sessions in %s", numSessions, elapsed)

		// Retrieve sessions (should be cached)
		start = time.Now()
		for _, sessionID := range sessionIDs {
			_, err := sm.GetSessionWithVersion(context.Background(), sessionID, 1)
			require.NoError(t, err)
		}
		elapsed = time.Since(start)

		log.Printf("Retrieved %d cached sessions in %s", numSessions, elapsed)
		assert.Less(t, elapsed, 10*time.Second) // Adjust this threshold as needed
	})
}

func TestHMACSHA256SignerPool(t *testing.T) {
	secret := "test_secret"
	maxPoolSize := 5

	t.Run("SignAndVerify", func(t *testing.T) {
		pool := NewHMACSHA256SignerPool([]byte(secret), maxPoolSize)
		message := "test message"

		signature, err := pool.SignAndEncode(message)
		require.NoError(t, err)

		isValid, decodedMessage, err := pool.VerifyAndDecode(signature)
		require.NoError(t, err)
		assert.True(t, isValid)
		assert.Equal(t, message, decodedMessage)
	})

	t.Run("InvalidSignature", func(t *testing.T) {
		pool := NewHMACSHA256SignerPool([]byte(secret), maxPoolSize)
		invalidSignature := "invalid_signature"

		isValid, _, err := pool.VerifyAndDecode(invalidSignature)
		require.Error(t, err)
		assert.False(t, isValid)
	})

	t.Run("ConcurrentUsage", func(t *testing.T) {
		pool := NewHMACSHA256SignerPool([]byte(secret), maxPoolSize)
		numGoroutines := 100
		message := "test message"

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				signature, err := pool.SignAndEncode(message)
				require.NoError(t, err)

				isValid, decodedMessage, err := pool.VerifyAndDecode(signature)
				require.NoError(t, err)
				assert.True(t, isValid)
				assert.Equal(t, message, decodedMessage)
			}()
		}

		wg.Wait()
	})
}

func TestConcurrentSessionManagers(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create multiple SessionManagers
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 1000
	cfg.NotifyOnUpdates = true

	numManagers := 3
	managers := make([]*SessionManager, numManagers)
	for i := 0; i < numManagers; i++ {
		sm, err := NewSessionManager(ctx, cfg, db)
		require.NoError(t, err)
		defer sm.Shutdown(context.Background())
		managers[i] = sm
	}

	// Test concurrent session creation, update, and notification
	t.Run("ConcurrentSessionCreationUpdateAndNotification", func(t *testing.T) {
		var wg sync.WaitGroup
		sessionCount := 100

		for i := 0; i < sessionCount; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				userID := uuid.New()
				attributes := map[string]SessionAttributeValue{
					"key": {Value: fmt.Sprintf("value%d", index)},
				}
				session, err := managers[index%numManagers].CreateSession(context.Background(), userID, attributes)
				require.NoError(t, err)

				// Wait a bit to allow notifications to propagate
				time.Sleep(100 * time.Millisecond)

				// Update the session
				err = session.UpdateAttribute("key", fmt.Sprintf("updated_value%d", index))
				require.NoError(t, err)
				updatedSession, err := managers[index%numManagers].UpdateSession(context.Background(), session)
				require.NoError(t, err)

				// Wait a bit to allow update notifications to propagate
				time.Sleep(100 * time.Millisecond)

				// Check if the updated session is available in all managers
				for j, sm := range managers {
					retrievedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, updatedSession.Version)
					require.NoError(t, err, "Manager %d failed to retrieve session", j)
					assert.Equal(t, session.ID, retrievedSession.ID)
					assert.Equal(t, fmt.Sprintf("updated_value%d", index), retrievedSession.GetAttributes()["key"].Value)
				}
			}(i)
		}

		wg.Wait()
	})
}

func TestSignSessionID(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true

	sm, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	// Create HMAC signer pool
	signerPool := NewHMACSHA256SignerPool([]byte("test_secret"), 10)

	t.Run("SignAndVerifySessionID", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "value"},
		}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Sign only the session ID
		signature, err := signerPool.SignAndEncode(session.ID.String())
		require.NoError(t, err)

		// Verify the signature
		isValid, decodedSessionID, err := signerPool.VerifyAndDecode(signature)
		require.NoError(t, err)
		assert.True(t, isValid)
		assert.Equal(t, session.ID.String(), decodedSessionID)

		// Retrieve the session using the decoded session ID
		retrievedSessionID, err := uuid.Parse(decodedSessionID)
		require.NoError(t, err)
		retrievedSession, err := sm.GetSessionWithVersion(context.Background(), retrievedSessionID, 1)
		require.NoError(t, err)
		assert.Equal(t, session.ID, retrievedSession.ID)
	})
}

func TestRefreshCache(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 100
	cfg.NotifyOnUpdates = false // Disable notifications for this test

	sm, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	t.Run("RefreshCacheAfterMissedUpdates", func(t *testing.T) {
		// Create a session
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "initial_value"},
		}
		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Update the session directly in the database to simulate a missed notification
		_, err = sm.db.ExecContext(ctx, fmt.Sprintf(`
            UPDATE %s
            SET "updated_at" = NOW(), "version" = "version" + 1
            WHERE "id" = $1
        `, sm.getTableName("sessions")), session.ID)
		require.NoError(t, err)

		// Update an attribute directly in the database
		_, err = sm.db.ExecContext(ctx, fmt.Sprintf(`
            UPDATE %s
            SET "value" = 'updated_value'
            WHERE "session_id" = $1 AND "key" = 'key'
        `, sm.getTableName("session_attributes")), session.ID)
		require.NoError(t, err)

		// Manually call refreshCache
		err = sm.refreshCache(context.Background())
		require.NoError(t, err)

		// Attempt to get the session (this should now return the updated session)
		refreshedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, 1)
		require.NoError(t, err)

		// Check if the refreshed session has the updated attribute
		attr, exists := refreshedSession.GetAttribute("key")
		assert.True(t, exists)
		assert.Equal(t, "updated_value", attr.Value)
		assert.Equal(t, 2, refreshedSession.Version)
	})
}

func TestOutOfSyncBehavior(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 100

	sm, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	t.Run("OutOfSyncForcesRefresh", func(t *testing.T) {
		// Create a session
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "initial_value"},
		}
		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Manually set outOfSync to true
		sm.mutex.Lock()
		sm.outOfSync = true
		sm.mutex.Unlock()
		defer func() {
			sm.mutex.Lock()
			sm.outOfSync = false
			sm.mutex.Unlock()
		}()

		// Update the session directly in the database
		_, err = sm.db.ExecContext(ctx, fmt.Sprintf(`
            UPDATE %s
            SET "updated_at" = NOW(), "version" = "version" + 1
            WHERE "id" = $1
        `, sm.getTableName("sessions")), session.ID)
		require.NoError(t, err)

		// Update an attribute directly in the database
		_, err = sm.db.ExecContext(ctx, fmt.Sprintf(`
            UPDATE %s
            SET "value" = 'updated_value'
            WHERE "session_id" = $1 AND "key" = 'key'
        `, sm.getTableName("session_attributes")), session.ID)
		require.NoError(t, err)

		// Attempt to get the session (this should force a refresh due to outOfSync)
		refreshedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, 1)
		require.NoError(t, err)

		// Check if the refreshed session has the updated attribute
		attr, exists := refreshedSession.GetAttribute("key")
		assert.True(t, exists)
		assert.Equal(t, "updated_value", attr.Value)
		assert.Equal(t, 2, refreshedSession.Version)
	})
}

func TestUpdateSessionWithCheckVersion(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 100

	sm, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	t.Run("UpdateSessionWithCheckVersionTrue", func(t *testing.T) {
		// Create a session
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "initial_value"},
		}
		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Update the session with checkVersion = true
		err = session.UpdateAttribute("key", "updated_value")
		require.NoError(t, err)
		updatedSession, err := sm.UpdateSession(context.Background(), session, WithCheckVersion())
		require.NoError(t, err)
		assert.Equal(t, 2, updatedSession.Version)

		// Try to update the session again with the old version
		err = session.UpdateAttribute("key", "another_value")
		require.NoError(t, err)
		_, err = sm.UpdateSession(context.Background(), session, WithCheckVersion())
		assert.Equal(t, ErrSessionVersionIsOutdated, err)
	})

	t.Run("UpdateSessionWithCheckVersionFalse", func(t *testing.T) {
		// Create a session
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "initial_value"},
		}
		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Update the session with checkVersion = false (default)
		err = session.UpdateAttribute("key", "updated_value")
		require.NoError(t, err)
		updatedSession, err := sm.UpdateSession(context.Background(), session)
		require.NoError(t, err)
		assert.Equal(t, 2, updatedSession.Version)

		// Try to update the session again with the old version
		err = session.UpdateAttribute("key", "another_value")
		require.NoError(t, err)
		finalSession, err := sm.UpdateSession(context.Background(), session)
		require.NoError(t, err)
		assert.Equal(t, 3, finalSession.Version)
		assert.Equal(t, "another_value", finalSession.GetAttributes()["key"].Value)
	})
}
func TestGetAttributeAndRetainUnmarshaled(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 100

	sm, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	// Create a test session with a complex attribute
	userID := uuid.New()
	type complexValueType struct {
		Name  string
		Age   int
		Roles []string
	}
	complexValue := complexValueType{
		Name:  "John Doe",
		Age:   30,
		Roles: []string{"admin", "user"},
	}
	complexValueJSON, err := json.Marshal(complexValue)
	require.NoError(t, err)

	attributes := map[string]SessionAttributeValue{
		"simpleKey":  {Value: "simpleValue", Marshaled: false},
		"complexKey": {Value: string(complexValueJSON), Marshaled: true},
	}

	session, err := sm.CreateSession(context.Background(), userID, attributes)
	require.NoError(t, err)

	t.Run("UnmarshalSimpleAttribute", func(t *testing.T) {
		var simpleValue string
		attr, err := session.GetAttributeAndRetainUnmarshaled("simpleKey", &simpleValue)
		require.NoError(t, err)
		assert.Equal(t, "simpleValue", simpleValue)
		assert.Equal(t, "simpleValue", attr.Value)
		assert.False(t, attr.Marshaled)
	})

	t.Run("UnmarshalComplexAttribute", func(t *testing.T) {
		var unmarshaledValue complexValueType
		attr, err := session.GetAttributeAndRetainUnmarshaled("complexKey", &unmarshaledValue)
		require.NoError(t, err)
		assert.Equal(t, complexValue, unmarshaledValue)
		assert.Equal(t, complexValue, attr.Value)
		assert.False(t, attr.Marshaled)

		// Check if the attribute is now unmarshaled in the session
		sessionAttr, ok := session.GetAttribute("complexKey")
		require.True(t, ok)
		assert.Equal(t, complexValue, sessionAttr.Value)
		assert.False(t, sessionAttr.Marshaled)
	})

	t.Run("RetainUnmarshaledValue", func(t *testing.T) {
		// First call to unmarshal
		var value1 complexValueType
		_, err := session.GetAttributeAndRetainUnmarshaled("complexKey", &value1)
		require.NoError(t, err)

		// Second call should return the already unmarshaled value
		var value2 complexValueType
		attr, err := session.GetAttributeAndRetainUnmarshaled("complexKey", &value2)
		require.NoError(t, err)
		assert.Equal(t, value1, value2)
		assert.False(t, attr.Marshaled)
	})

	t.Run("CacheUpdateWithUnmarshaledValue", func(t *testing.T) {
		// Force a cache update
		sm.clearCache()
		err := sm.refreshCache(context.Background())
		require.NoError(t, err)

		// Retrieve the session from cache
		cachedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, session.Version)
		require.NoError(t, err)

		// Unmarshal the complex attribute
		var unmarshaledValue complexValueType
		_, err = cachedSession.GetAttributeAndRetainUnmarshaled("complexKey", &unmarshaledValue)
		require.NoError(t, err)

		// Check if the cache was updated with the unmarshaled value
		sm.mutex.RLock()
		cachedItem, exists := sm.cache[session.ID]
		sm.mutex.RUnlock()
		require.True(t, exists)
		cachedAttr, ok := cachedItem.session.attributes["complexKey"]
		require.True(t, ok)
		assert.Equal(t, complexValue, cachedAttr.Value)
		assert.False(t, cachedAttr.Marshaled)
	})

	t.Run("ConcurrentUnmarshaling", func(t *testing.T) {
		var wg sync.WaitGroup
		concurrentAccesses := 10

		for i := 0; i < concurrentAccesses; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var unmarshaledValue complexValueType
				_, err := session.GetAttributeAndRetainUnmarshaled("complexKey", &unmarshaledValue)
				require.NoError(t, err)
				assert.Equal(t, complexValue, unmarshaledValue)
			}()
		}

		wg.Wait()

		// Verify that the attribute is unmarshaled in the session
		attr, ok := session.GetAttribute("complexKey")
		require.True(t, ok)
		assert.Equal(t, complexValue, attr.Value)
		assert.False(t, attr.Marshaled)
	})

	t.Run("UnmarshalNonExistentAttribute", func(t *testing.T) {
		var value string
		_, err := session.GetAttributeAndRetainUnmarshaled("nonExistentKey", &value)
		assert.Error(t, err)
		assert.Equal(t, ErrAttributeNotFound, err)
	})

	t.Run("UnmarshalInvalidJSON", func(t *testing.T) {
		// Update the session with an invalid JSON value
		err := session.UpdateAttribute("invalidJSON", "{invalid_json")
		require.NoError(t, err)
		updatedSession, err := sm.UpdateSession(context.Background(), session)
		require.NoError(t, err)

		var value map[string]interface{}
		_, err = updatedSession.GetAttributeAndRetainUnmarshaled("invalidJSON", &value)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot assign string") // this is the string, don't change.
	})
}

func TestSessionManagerResilience(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, pgConnString, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 100
	cfg.NotifyOnUpdates = true

	sm, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	// Create a test session
	userID := uuid.New()
	attributes := map[string]SessionAttributeValue{
		"key": {Value: "initial_value"},
	}
	session, err := sm.CreateSession(context.Background(), userID, attributes)
	require.NoError(t, err)

	// Simulate PostgreSQL dropping all connections
	t.Run("HandleConnectionDrop", func(t *testing.T) {
		// Create a new connection to execute the termination command
		terminateDB, err := sql.Open("pgx", pgConnString)
		require.NoError(t, err)
		defer terminateDB.Close()

		// Force disconnect all clients except our current connection
		_, err = terminateDB.Exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid()")
		require.NoError(t, err)

		// Close our termination connection
		terminateDB.Close()

		// Wait a bit for the SessionManager to detect the disconnection and re-establish the connection
		time.Sleep(5 * time.Second)

		// Try to get the session (this should trigger a refresh)
		retrievedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, session.Version, WithForceRefresh())
		require.NoError(t, err)
		assert.Equal(t, session.ID, retrievedSession.ID)
		assert.Equal(t, "initial_value", retrievedSession.GetAttributes()["key"].Value)
	})

	// Test out-of-sync scenario
	t.Run("HandleOutOfSync", func(t *testing.T) {
		// Update the session directly in the database
		_, err = db.ExecContext(ctx, fmt.Sprintf(`
			UPDATE %s
			SET "updated_at" = NOW(), "version" = "version" + 1
			WHERE "id" = $1
		`, sm.getTableName("sessions")), session.ID)
		require.NoError(t, err)

		// Update an attribute directly in the database
		_, err = db.ExecContext(ctx, fmt.Sprintf(`
			UPDATE %s
			SET "value" = 'updated_value'
			WHERE "session_id" = $1 AND "key" = 'key'
		`, sm.getTableName("session_attributes")), session.ID)
		require.NoError(t, err)

		// Create a new connection to execute the termination command
		terminateDB, err := sql.Open("pgx", pgConnString)
		require.NoError(t, err)
		defer terminateDB.Close()

		// Force disconnect all clients except our current connection
		_, err = terminateDB.Exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid()")
		require.NoError(t, err)

		// Close our termination connection
		terminateDB.Close()

		// Wait a bit for the SessionManager to detect the disconnection and re-establish the connection
		time.Sleep(5 * time.Second)

		// Try to get the session (this should trigger a refresh due to outOfSync)
		retrievedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, session.Version)
		require.NoError(t, err)

		// Check if the retrieved session has the updated attribute
		attr, exists := retrievedSession.GetAttribute("key")
		assert.True(t, exists)
		assert.Equal(t, "updated_value", attr.Value)
		assert.Equal(t, session.Version+1, retrievedSession.Version)

		// Verify that outOfSync is set back to false
		sm.mutex.RLock()
		assert.False(t, sm.outOfSync)
		sm.mutex.RUnlock()
	})
}
func startPostgresContainer(ctx context.Context) (testcontainers.Container, *sql.DB, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:13",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test_user",
			"POSTGRES_PASSWORD": "test_password",
			"POSTGRES_DB":       "test_db",
		},
		Cmd: []string{
			"postgres",
			"-c", "max_connections=200",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithStartupTimeout(60 * time.Second),
	}

	postgres, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to start postgres container: %v", err)
	}

	host, err := postgres.Host(ctx)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to get postgres host: %v", err)
	}

	port, err := postgres.MappedPort(ctx, "5432")
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to get postgres port: %v", err)
	}

	pgConnString := fmt.Sprintf("host=%s port=%d user=test_user password=test_password dbname=test_db sslmode=disable", host, port.Int())

	log.Printf("Attempting to connect with: %s", pgConnString)

	// Attempt to connect with retries
	var db *sql.DB
	err = retry(ctx, 30*time.Second, func() error {
		var err error
		db, err = sql.Open("pgx", pgConnString)
		if err != nil {
			log.Printf("Failed to connect, retrying: %v", err)
			return err
		}
		db.SetMaxOpenConns(200)
		return db.Ping()
	})

	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to connect to database after retries: %v", err)
	}

	// Verify max_connections setting
	var maxConnections int
	err = db.QueryRow("SHOW max_connections").Scan(&maxConnections)
	if err != nil {
		log.Printf("Warning: Could not verify max_connections: %v", err)
	} else {
		log.Printf("max_connections is set to: %d", maxConnections)
	}

	return postgres, db, pgConnString, nil
}

func retry(ctx context.Context, maxWait time.Duration, fn func() error) error {
	start := time.Now()
	for {
		err := fn()
		if err == nil {
			return nil
		}

		if time.Since(start) > maxWait {
			return fmt.Errorf("timeout after %v: %w", maxWait, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			// Continue with the next iteration
		}
	}

}

func TestCheckAttributeVersion(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 100

	sm, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	// Set up a fake clock for testing
	fakeClock := clockwork.NewFakeClock()
	sm.setClock(fakeClock)

	t.Run("SuccessfulAttributeUpdate", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key1": {Value: "value1", Marshaled: false},
		}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Update the attribute
		err = session.UpdateAttribute("key1", "value2")
		require.NoError(t, err)

		updatedSession, err := sm.UpdateSession(context.Background(), session, WithCheckAttributeVersion())
		require.NoError(t, err)

		// Verify the attribute was updated
		attr, exists := updatedSession.GetAttribute("key1")
		require.True(t, exists)
		assert.Equal(t, "value2", attr.Value)
		assert.Equal(t, 2, attr.Version) // Version should be incremented
	})

	t.Run("ConcurrentAttributeUpdateFailure", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key1": {Value: "value1", Marshaled: false},
		}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Simulate a concurrent update
		concurrentSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, session.Version)
		require.NoError(t, err)

		// Update the attribute in the original session
		err = session.UpdateAttribute("key1", "value2")
		require.NoError(t, err)
		_, err = sm.UpdateSession(context.Background(), session, WithCheckAttributeVersion())
		require.NoError(t, err)

		// Try to update the attribute in the concurrent session
		err = concurrentSession.UpdateAttribute("key1", "value3")
		require.NoError(t, err)
		_, err = sm.UpdateSession(context.Background(), concurrentSession, WithCheckAttributeVersion())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "attribute key1 version mismatch")
	})

	t.Run("MultipleAttributeUpdates", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key1": {Value: "value1", Marshaled: false},
			"key2": {Value: "value2", Marshaled: false},
		}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Update multiple attributes
		err = session.UpdateAttribute("key1", "new_value1")
		require.NoError(t, err)
		err = session.UpdateAttribute("key2", "new_value2")
		require.NoError(t, err)
		err = session.UpdateAttribute("key3", "new_value3")
		require.NoError(t, err)

		updatedSession, err := sm.UpdateSession(context.Background(), session, WithCheckAttributeVersion())
		require.NoError(t, err)

		// Verify all attributes were updated
		attr1, exists := updatedSession.GetAttribute("key1")
		require.True(t, exists)
		assert.Equal(t, "new_value1", attr1.Value)
		assert.Equal(t, 2, attr1.Version)

		attr2, exists := updatedSession.GetAttribute("key2")
		require.True(t, exists)
		assert.Equal(t, "new_value2", attr2.Value)
		assert.Equal(t, 2, attr2.Version)

		attr3, exists := updatedSession.GetAttribute("key3")
		require.True(t, exists)
		assert.Equal(t, "new_value3", attr3.Value)
		assert.Equal(t, 1, attr3.Version) // New attribute, version should be 1
	})

	t.Run("AttributeUpdateWithoutVersionCheck", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key1": {Value: "value1", Marshaled: false},
		}

		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Simulate a concurrent update
		concurrentSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, session.Version)
		require.NoError(t, err)

		// Update the attribute in the original session
		err = session.UpdateAttribute("key1", "value2")
		require.NoError(t, err)
		_, err = sm.UpdateSession(context.Background(), session)
		require.NoError(t, err)

		// Update the attribute in the concurrent session without version check
		err = concurrentSession.UpdateAttribute("key1", "value3")
		require.NoError(t, err)
		updatedSession, err := sm.UpdateSession(context.Background(), concurrentSession)
		require.NoError(t, err)

		// Verify the attribute was updated
		attr, exists := updatedSession.GetAttribute("key1")
		require.True(t, exists)
		assert.Equal(t, "value3", attr.Value)
		assert.Equal(t, 3, attr.Version) // Version should be incremented twice
	})
}

// Add these new tests to the existing session_test.go file

func TestDeleteAttributeFromAllUserSessions(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 100

	sm, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	// Set up a fake clock for testing
	fakeClock := clockwork.NewFakeClock()
	sm.setClock(fakeClock)

	t.Run("DeleteAttributeFromAllUserSessions", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key1": {Value: "value1", Marshaled: false},
			"key2": {Value: "value2", Marshaled: false},
		}

		// Create multiple sessions for the user
		session1, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		session2, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Delete the "key1" attribute from all user sessions
		err = sm.DeleteAttributeFromAllUserSessions(context.Background(), userID, "key1")
		require.NoError(t, err)

		// Verify that "key1" is deleted from both sessions
		updatedSession1, err := sm.GetSessionWithVersion(context.Background(), session1.ID, session1.Version)
		require.NoError(t, err)
		_, exists := updatedSession1.GetAttribute("key1")
		assert.False(t, exists)
		_, exists = updatedSession1.GetAttribute("key2")
		assert.True(t, exists)

		updatedSession2, err := sm.GetSessionWithVersion(context.Background(), session2.ID, session2.Version)
		require.NoError(t, err)
		_, exists = updatedSession2.GetAttribute("key1")
		assert.False(t, exists)
		_, exists = updatedSession2.GetAttribute("key2")
		assert.True(t, exists)
	})
}

func TestUserSessionsIndexMaintenance(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 100

	sm, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	// Set up a fake clock for testing
	fakeClock := clockwork.NewFakeClock()
	sm.setClock(fakeClock)

	t.Run("UserSessionsIndexMaintenance", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: "value", Marshaled: false},
		}

		// Create multiple sessions for the user
		session1, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		session2, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Verify that the userSessionsIndex contains both sessions
		sm.mutex.RLock()
		userSessions, exists := sm.userSessionsIndex[userID]
		sm.mutex.RUnlock()
		assert.True(t, exists)
		assert.Len(t, userSessions, 2)

		// Delete one session
		err = sm.DeleteSession(context.Background(), session1.ID)
		require.NoError(t, err)

		// Verify that the userSessionsIndex still contains one session
		sm.mutex.RLock()
		userSessions, exists = sm.userSessionsIndex[userID]
		sm.mutex.RUnlock()
		assert.True(t, exists)
		assert.Len(t, userSessions, 1)
		assert.Equal(t, session2.ID, userSessions[0])

		// Delete the second session
		err = sm.DeleteSession(context.Background(), session2.ID)
		require.NoError(t, err)

		// Verify that the userSessionsIndex no longer contains an entry for the user
		sm.mutex.RLock()
		_, exists = sm.userSessionsIndex[userID]
		sm.mutex.RUnlock()
		assert.False(t, exists)
	})
}

func TestMaxSessionLifetimeInCache(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new SessionManager with MaxSessionLifetimeInCache set
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 100
	cfg.MaxSessionLifetimeInCache = 2 * time.Second // Set a short duration for testing

	sm, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm.Shutdown(context.Background())

	// Set up a fake clock for testing
	fakeClock := clockwork.NewFakeClock()
	sm.setClock(fakeClock)

	// Create a test session
	userID := uuid.New()
	attributes := map[string]SessionAttributeValue{
		"key": {Value: "initial_value", Marshaled: false},
	}

	session, err := sm.CreateSession(context.Background(), userID, attributes)
	require.NoError(t, err)

	// Verify that the session is initially served from the cache
	cachedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, session.Version)
	require.NoError(t, err)
	assert.True(t, cachedSession.IsFromCache())

	// Advance the clock by 1 second (less than MaxSessionLifetimeInCache)
	fakeClock.Advance(1 * time.Second)

	// The session should still be served from the cache
	cachedSession, err = sm.GetSessionWithVersion(context.Background(), session.ID, session.Version)
	require.NoError(t, err)
	assert.True(t, cachedSession.IsFromCache())

	// Update the session in the database directly
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET "updated_at" = NOW(), "version" = "version" + 1
		WHERE "id" = $1
	`, sm.getTableName("sessions")), session.ID)
	require.NoError(t, err)

	// Update an attribute directly in the database
	_, err = db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET "value" = 'updated_value'
		WHERE "session_id" = $1 AND "key" = 'key'
	`, sm.getTableName("session_attributes")), session.ID)
	require.NoError(t, err)

	// Advance the clock past MaxSessionLifetimeInCache
	fakeClock.Advance(2 * time.Second)

	// The session should now be refreshed from the database
	refreshedSession, err := sm.GetSessionWithVersion(context.Background(), session.ID, session.Version)
	require.NoError(t, err)
	assert.False(t, refreshedSession.IsFromCache())
	assert.Equal(t, session.Version+1, refreshedSession.Version)

	// Verify that the attribute was updated
	attr, exists := refreshedSession.GetAttribute("key")
	assert.True(t, exists)
	assert.Equal(t, "updated_value", attr.Value)

	// The next immediate fetch should be from cache again
	cachedSession, err = sm.GetSessionWithVersion(context.Background(), session.ID, refreshedSession.Version)
	require.NoError(t, err)
	assert.True(t, cachedSession.IsFromCache())
}

func TestClearEntireCache(t *testing.T) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create multiple SessionManagers to simulate different nodes
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 100
	cfg.NotifyOnUpdates = true

	sm1, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm1.Shutdown(context.Background())

	sm2, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sm2.Shutdown(context.Background())

	// Create some sessions
	userID := uuid.New()
	attributes := map[string]SessionAttributeValue{
		"key": {Value: "value"},
	}

	session1, err := sm1.CreateSession(ctx, userID, attributes)
	require.NoError(t, err)

	session2, err := sm2.CreateSession(ctx, userID, attributes)
	require.NoError(t, err)

	// Ensure sessions are in cache
	cachedSession1, err := sm1.GetSession(ctx, session1.ID)
	require.NoError(t, err)
	assert.True(t, cachedSession1.IsFromCache())

	cachedSession2, err := sm2.GetSession(ctx, session2.ID)
	require.NoError(t, err)
	assert.True(t, cachedSession2.IsFromCache())

	// Clear entire cache
	err = sm1.ClearEntireCache(ctx)
	require.NoError(t, err)

	// Wait a bit for the notification to propagate
	time.Sleep(100 * time.Millisecond)

	// Check that sessions are no longer in cache
	refreshedSession1, err := sm1.GetSession(ctx, session1.ID)
	require.NoError(t, err)
	assert.False(t, refreshedSession1.IsFromCache())

	refreshedSession2, err := sm2.GetSession(ctx, session2.ID)
	require.NoError(t, err)
	assert.False(t, refreshedSession2.IsFromCache())
}
