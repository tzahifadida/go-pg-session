package gopgsession

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadmeExamples(t *testing.T) {
	// Start PostgreSQL container
	ctx := context.Background()
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)
	defer db.Close()

	// Create a new SessionManager
	cfg := DefaultConfig()
	cfg.MaxSessions = 10
	cfg.SessionExpiration = 24 * time.Hour // 1 day
	cfg.CreateSchemaIfMissing = true
	cfg.InactivityDuration = 2 * time.Hour // Sessions expire after 2 hours of inactivity

	sessionManager, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sessionManager.Shutdown(context.Background())

	t.Run("CreateSessionWithCustomOptions", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"role":        {Value: "admin"},
			"preferences": {Value: map[string]string{"theme": "dark"}},
		}

		customExpiry := time.Now().Add(48 * time.Hour)
		session, err := sessionManager.CreateSession(ctx, userID, attributes,
			WithCreateIncludeInactivity(false),
			WithCreateExpiresAt(customExpiry))
		require.NoError(t, err)
		assert.NotNil(t, session)
		assert.Equal(t, userID, session.UserID)
		assert.False(t, session.IncludeInactivity)
		assert.Equal(t, customExpiry.Unix(), session.ExpiresAt.Unix())

		// Verify attributes
		roleAttr, exists := session.GetAttribute("role")
		assert.True(t, exists)
		assert.Equal(t, "admin", roleAttr.Value)

		var preferences map[string]string
		_, err = session.GetAttributeAndRetainUnmarshaled("preferences", &preferences)
		require.NoError(t, err)
		assert.Equal(t, "dark", preferences["theme"])
	})

	t.Run("UpdateSessionExpiresAt", func(t *testing.T) {
		userID := uuid.New()
		session, err := sessionManager.CreateSession(ctx, userID, nil)
		require.NoError(t, err)

		newExpiresAt := time.Now().Add(24 * time.Hour)
		updatedSession, err := sessionManager.UpdateSession(ctx, session,
			WithUpdateExpiresAt(newExpiresAt))
		require.NoError(t, err)
		assert.Equal(t, newExpiresAt.Unix(), updatedSession.ExpiresAt.Unix())
	})

	t.Run("ToggleInactivityExpiration", func(t *testing.T) {
		userID := uuid.New()
		session, err := sessionManager.CreateSession(ctx, userID, nil)
		require.NoError(t, err)
		assert.True(t, session.IncludeInactivity) // Default should be true

		updatedSession, err := sessionManager.UpdateSession(ctx, session,
			WithUpdateIncludeInactivity(false))
		require.NoError(t, err)
		assert.False(t, updatedSession.IncludeInactivity)
	})

	t.Run("AttributeLevelExpiration", func(t *testing.T) {
		userID := uuid.New()
		session, err := sessionManager.CreateSession(ctx, userID, nil)
		require.NoError(t, err)

		// Grant elevated access for 15 minutes
		elevatedExpiry := time.Now().Add(15 * time.Minute)
		err = session.UpdateAttribute("elevated_access", "true", WithUpdateAttExpiresAt(elevatedExpiry))
		require.NoError(t, err)

		// Set a promotional offer that expires in 1 hour
		offerExpiry := time.Now().Add(1 * time.Hour)
		err = session.UpdateAttribute("promo_code", "FLASH_SALE_20", WithUpdateAttExpiresAt(offerExpiry))
		require.NoError(t, err)

		updatedSession, err := sessionManager.UpdateSession(ctx, session)
		require.NoError(t, err)

		// Verify attributes were set with expiration
		elevatedAccess, exists := updatedSession.GetAttribute("elevated_access")
		require.True(t, exists)
		assert.Equal(t, "true", elevatedAccess.Value)
		assert.NotNil(t, elevatedAccess.ExpiresAt)
		assert.Equal(t, elevatedExpiry.Unix(), elevatedAccess.ExpiresAt.Unix())

		promoCode, exists := updatedSession.GetAttribute("promo_code")
		require.True(t, exists)
		assert.Equal(t, "FLASH_SALE_20", promoCode.Value)
		assert.NotNil(t, promoCode.ExpiresAt)
		assert.Equal(t, offerExpiry.Unix(), promoCode.ExpiresAt.Unix())
	})

	t.Run("BasicUsage", func(t *testing.T) {
		// Creating a Session
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"role":        {Value: "admin"},
			"preferences": {Value: map[string]string{"theme": "dark"}},
		}

		session, err := sessionManager.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)
		assert.NotNil(t, session)

		// Retrieving a Session
		retrievedSession, err := sessionManager.GetSession(context.Background(), session.ID, WithGetDoNotUpdateSessionLastAccess())
		require.NoError(t, err)
		assert.Equal(t, userID, retrievedSession.UserID)

		// Updating a Session Attribute with Session-Level Version Check
		var preferences map[string]string
		_, err = retrievedSession.GetAttributeAndRetainUnmarshaled("preferences", &preferences)
		require.NoError(t, err)
		preferences["theme"] = "light"
		err = retrievedSession.UpdateAttribute("preferences", preferences)
		require.NoError(t, err)

		updatedSession, err := sessionManager.UpdateSession(context.Background(), retrievedSession, WithUpdateCheckVersion())
		require.NoError(t, err)

		// Verify the updated attribute
		var updatedPreferences map[string]string
		_, err = updatedSession.GetAttributeAndRetainUnmarshaled("preferences", &updatedPreferences)
		require.NoError(t, err)
		assert.Equal(t, "light", updatedPreferences["theme"])

		// Deleting a Session
		err = sessionManager.DeleteSession(context.Background(), updatedSession.ID)
		require.NoError(t, err)

		_, err = sessionManager.GetSession(context.Background(), updatedSession.ID)
		assert.Error(t, err)
	})

	t.Run("UpdateSessionWithAttributeVersionCheck", func(t *testing.T) {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"preferences": {Value: map[string]string{"theme": "dark", "language": "en"}},
		}

		session, err := sessionManager.CreateSession(context.Background(), userID, attributes)
		require.NoError(t, err)

		// Update the session with attribute version check
		var preferences map[string]string
		_, err = session.GetAttributeAndRetainUnmarshaled("preferences", &preferences)
		require.NoError(t, err)

		preferences["theme"] = "light"
		err = session.UpdateAttribute("preferences", preferences)
		require.NoError(t, err)

		updatedSession, err := sessionManager.UpdateSession(context.Background(), session, WithUpdateCheckAttributeVersion())
		require.NoError(t, err)

		// Verify the update
		var updatedPreferences map[string]string
		_, err = updatedSession.GetAttributeAndRetainUnmarshaled("preferences", &updatedPreferences)
		require.NoError(t, err)
		assert.Equal(t, "light", updatedPreferences["theme"])
		assert.Equal(t, "en", updatedPreferences["language"])
	})

	t.Run("LoginHandler", func(t *testing.T) {
		handler := loginHandler(sessionManager)

		req, err := http.NewRequest("POST", "/login", nil)
		require.NoError(t, err)
		req.Form = map[string][]string{
			"username": {"testuser"},
			"password": {"testpass"},
		}

		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "Login successful", rr.Body.String())

		cookies := rr.Result().Cookies()
		assert.Len(t, cookies, 1)
		assert.Equal(t, "session_id", cookies[0].Name)
		assert.NotEmpty(t, cookies[0].Value)
	})

	t.Run("LogoutHandler", func(t *testing.T) {
		// First, create a session
		userID := uuid.New()
		session, err := sessionManager.CreateSession(context.Background(), userID, nil)
		require.NoError(t, err)

		handler := logoutHandler(sessionManager)

		req, err := http.NewRequest("POST", "/logout", nil)
		require.NoError(t, err)
		req.AddCookie(&http.Cookie{Name: "session_id", Value: session.ID.String()})

		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "Logout successful", rr.Body.String())

		cookies := rr.Result().Cookies()
		assert.Len(t, cookies, 1)
		assert.Equal(t, "session_id", cookies[0].Name)
		assert.Equal(t, "", cookies[0].Value)
		assert.True(t, cookies[0].MaxAge < 0)
	})

	t.Run("UpdateUserPreferences", func(t *testing.T) {
		handler := updateUserPreferences(sessionManager)

		// First, create a session
		userID := uuid.New()
		initialPreferences := map[string]string{"theme": "dark"}
		session, err := sessionManager.CreateSession(context.Background(), userID, map[string]SessionAttributeValue{
			"preferences": {Value: initialPreferences},
		})
		require.NoError(t, err)

		req, err := http.NewRequest("POST", "/update-preferences", nil)
		require.NoError(t, err)
		req.AddCookie(&http.Cookie{Name: "session_id", Value: session.ID.String()})
		req.Form = map[string][]string{
			"theme": {"light"},
		}

		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "Preferences updated successfully", rr.Body.String())

		// Verify the update
		updatedSession, err := sessionManager.GetSession(context.Background(), session.ID)
		require.NoError(t, err)

		var updatedPreferences map[string]string
		_, err = updatedSession.GetAttributeAndRetainUnmarshaled("preferences", &updatedPreferences)
		require.NoError(t, err)
		assert.Equal(t, "light", updatedPreferences["theme"])
	})

	t.Run("AddToCartHandler", func(t *testing.T) {
		handler := addToCartHandler(sessionManager)

		// First, create a session without a cart
		userID := uuid.New()
		session, err := sessionManager.CreateSession(context.Background(), userID, nil)
		require.NoError(t, err)

		req, err := http.NewRequest("POST", "/add-to-cart", nil)
		require.NoError(t, err)
		req.AddCookie(&http.Cookie{Name: "session_id", Value: session.ID.String()})
		req.Form = map[string][]string{
			"item_id": {"123"},
		}

		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "Item added to cart", rr.Body.String())

		// Verify the cart update
		updatedSession, err := sessionManager.GetSession(context.Background(), session.ID)
		require.NoError(t, err)
		var cartItems []string
		_, err = updatedSession.GetAttributeAndRetainUnmarshaled("cart", &cartItems)
		require.NoError(t, err)
		assert.Contains(t, cartItems, "123")

		// Add another item to the cart
		req, err = http.NewRequest("POST", "/add-to-cart", nil)
		require.NoError(t, err)
		req.AddCookie(&http.Cookie{Name: "session_id", Value: session.ID.String()})
		req.Form = map[string][]string{
			"item_id": {"456"},
		}

		rr = httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "Item added to cart", rr.Body.String())

		// Verify the cart update again
		updatedSession, err = sessionManager.GetSession(context.Background(), session.ID)
		require.NoError(t, err)
		_, err = updatedSession.GetAttributeAndRetainUnmarshaled("cart", &cartItems)
		require.NoError(t, err)
		assert.Contains(t, cartItems, "123")
		assert.Contains(t, cartItems, "456")
		assert.Len(t, cartItems, 2)
	})

	t.Run("UpdateUserSessionWithSessionLevelVersionCheck", func(t *testing.T) {
		handler := updateUserSessionWithVersionCheck(sessionManager)

		// First, create a session
		userID := uuid.New()
		initialSession, err := sessionManager.CreateSession(context.Background(), userID, map[string]SessionAttributeValue{
			"last_access": {Value: time.Now().Add(-1 * time.Hour)},
			"page_views":  {Value: 5},
		})
		require.NoError(t, err)

		req, err := http.NewRequest("POST", "/update-session", nil)
		require.NoError(t, err)
		req.AddCookie(&http.Cookie{Name: "session_id", Value: initialSession.ID.String()})

		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "Session updated successfully", rr.Body.String())

		// Verify the update
		updatedSession, err := sessionManager.GetSession(context.Background(), initialSession.ID)
		require.NoError(t, err)

		var lastAccess time.Time
		_, err = updatedSession.GetAttributeAndRetainUnmarshaled("last_access", &lastAccess)
		require.NoError(t, err)
		assert.True(t, lastAccess.After(time.Now().Add(-1*time.Minute)), "Last access should be updated")

		var pageViews int
		_, err = updatedSession.GetAttributeAndRetainUnmarshaled("page_views", &pageViews)
		require.NoError(t, err)
		assert.Equal(t, 6, pageViews, "Page views should be incremented")
	})
}

func TestSignedCookiesExample(t *testing.T) {
	// Start PostgreSQL container
	ctx := context.Background()
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)
	defer db.Close()

	// Create a new SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true

	sessionManager, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sessionManager.Shutdown(context.Background())

	// Initialize the signer
	signerPool := NewHMACSHA256SignerPool([]byte("test-secret-key"), 10)

	// Test login handler with signed cookie
	t.Run("LoginHandlerWithSignedCookie", func(t *testing.T) {
		handler := loginHandlerWithSignedCookie(sessionManager, signerPool)

		req, err := http.NewRequest("POST", "/login", nil)
		require.NoError(t, err)
		req.Form = map[string][]string{
			"username": {"testuser"},
			"password": {"testpass"},
		}

		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Equal(t, "Login successful", rr.Body.String())

		cookies := rr.Result().Cookies()
		require.Len(t, cookies, 1)
		assert.Equal(t, "session_id", cookies[0].Name)
		assert.NotEmpty(t, cookies[0].Value)

		// Verify cookie format (sessionID.signature)
		parts := strings.Split(cookies[0].Value, ".")
		assert.Len(t, parts, 2)
	})

	// Test get session with signed cookie
	t.Run("GetSessionWithSignedCookie", func(t *testing.T) {
		// First, create a session with a signed cookie
		loginHandler := loginHandlerWithSignedCookie(sessionManager, signerPool)
		loginReq, _ := http.NewRequest("POST", "/login", nil)
		loginReq.Form = map[string][]string{
			"username": {"testuser"},
			"password": {"testpass"},
		}
		loginRR := httptest.NewRecorder()
		loginHandler.ServeHTTP(loginRR, loginReq)

		// Now, try to get the session using the signed cookie
		getSessionReq, _ := http.NewRequest("GET", "/get-session", nil)
		for _, cookie := range loginRR.Result().Cookies() {
			getSessionReq.AddCookie(cookie)
		}

		session, err := getSessionWithSignedCookie(sessionManager, signerPool, getSessionReq)
		require.NoError(t, err)
		assert.NotNil(t, session)

		// Verify session data
		var username string
		_, err = session.GetAttributeAndRetainUnmarshaled("username", &username)
		require.NoError(t, err)
		assert.Equal(t, "testuser", username)
	})

	// Test with invalid signature
	t.Run("GetSessionWithInvalidSignature", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/get-session", nil)
		req.AddCookie(&http.Cookie{
			Name:  "session_id",
			Value: "invalid-session-id.invalid-signature",
		})

		_, err := getSessionWithSignedCookie(sessionManager, signerPool, req)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to verify")
	})
}

func TestDistributedLockExamples(t *testing.T) {
	// Setup
	ctx := context.Background()
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)
	defer db.Close()

	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true

	sessionManager, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sessionManager.Shutdown(context.Background())

	t.Run("BasicLockUsage", func(t *testing.T) {
		userID := uuid.New()
		session, err := sessionManager.CreateSession(ctx, userID, nil)
		require.NoError(t, err)

		lock := sessionManager.NewDistributedLock(session.ID, "critical-operation", nil)

		err = lock.Lock(ctx)
		require.NoError(t, err)

		// Simulate critical operation
		time.Sleep(100 * time.Millisecond)

		err = lock.Unlock(ctx)
		require.NoError(t, err)
	})

	t.Run("CustomLockConfiguration", func(t *testing.T) {
		userID := uuid.New()
		session, err := sessionManager.CreateSession(ctx, userID, nil)
		require.NoError(t, err)

		config := &DistributedLockConfig{
			MaxRetries:        5,
			RetryDelay:        100 * time.Millisecond,
			HeartbeatInterval: 1 * time.Second,
			LeaseTime:         5 * time.Second,
		}

		lock := sessionManager.NewDistributedLock(session.ID, "custom-lock", config)

		err = lock.Lock(ctx)
		require.NoError(t, err)

		// Simulate operation
		time.Sleep(200 * time.Millisecond)

		err = lock.Unlock(ctx)
		require.NoError(t, err)
	})

	t.Run("ExtendingLease", func(t *testing.T) {
		userID := uuid.New()
		session, err := sessionManager.CreateSession(ctx, userID, nil)
		require.NoError(t, err)

		config := &DistributedLockConfig{
			LeaseTime: 2 * time.Second,
		}

		lock := sessionManager.NewDistributedLock(session.ID, "extend-lease-lock", config)

		err = lock.Lock(ctx)
		require.NoError(t, err)

		// Simulate long-running operation
		for i := 0; i < 3; i++ {
			time.Sleep(1 * time.Second)
			err = lock.ExtendLease(ctx, 2*time.Second)
			require.NoError(t, err)
		}

		err = lock.Unlock(ctx)
		require.NoError(t, err)
	})

	t.Run("HandlingLockAcquisitionFailures", func(t *testing.T) {
		userID := uuid.New()
		session, err := sessionManager.CreateSession(ctx, userID, nil)
		require.NoError(t, err)

		lock1 := sessionManager.NewDistributedLock(session.ID, "conflicting-lock", nil)
		lock2 := sessionManager.NewDistributedLock(session.ID, "conflicting-lock", nil)

		err = lock1.Lock(ctx)
		require.NoError(t, err)

		err = lock2.Lock(ctx)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrLockAlreadyHeld))

		err = lock1.Unlock(ctx)
		require.NoError(t, err)
	})

	t.Run("ConcurrentLockAttempts", func(t *testing.T) {
		userID := uuid.New()
		session, err := sessionManager.CreateSession(ctx, userID, nil)
		require.NoError(t, err)

		var wg sync.WaitGroup
		successCount := 0
		attemptCount := 5
		var mu sync.Mutex

		for i := 0; i < attemptCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				lock := sessionManager.NewDistributedLock(session.ID, "concurrent-lock", nil)
				err := lock.Lock(ctx)
				if err == nil {
					mu.Lock()
					successCount++
					mu.Unlock()
					time.Sleep(100 * time.Millisecond)
					lock.Unlock(ctx)
				}
			}()
		}

		wg.Wait()
		assert.Equal(t, 1, successCount, "Only one goroutine should have acquired the lock")
	})
}

func TestPerformCriticalOperation(t *testing.T) {
	// Setup
	ctx := context.Background()
	postgres, db, _, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)
	defer db.Close()

	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true

	sessionManager, err := NewSessionManager(ctx, cfg, db)
	require.NoError(t, err)
	defer sessionManager.Shutdown(context.Background())

	userID := uuid.New()
	session, err := sessionManager.CreateSession(ctx, userID, nil)
	require.NoError(t, err)

	err = performCriticalOperation(sessionManager, session.ID)
	require.NoError(t, err)
}

// Handler implementations

func loginHandler(sm *SessionManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		username := r.FormValue("username")
		//password := r.FormValue("password")

		userID := uuid.New() // In a real scenario, this would be fetched from a database

		attributes := map[string]SessionAttributeValue{
			"username":   {Value: username},
			"last_login": {Value: time.Now().Format(time.RFC3339)},
		}
		session, err := sm.CreateSession(r.Context(), userID, attributes)
		if err != nil {
			http.Error(w, "Failed to create session", http.StatusInternalServerError)
			return
		}

		http.SetCookie(w, &http.Cookie{
			Name:     "session_id",
			Value:    session.ID.String(),
			HttpOnly: true,
			Secure:   true,
			SameSite: http.SameSiteStrictMode,
		})

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Login successful"))
	}
}

func logoutHandler(sm *SessionManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sessionCookie, err := r.Cookie("session_id")
		if err != nil {
			http.Error(w, "No session found", http.StatusBadRequest)
			return
		}

		sessionID, err := uuid.Parse(sessionCookie.Value)
		if err != nil {
			http.Error(w, "Invalid session ID", http.StatusBadRequest)
			return
		}

		err = sm.DeleteSession(r.Context(), sessionID)
		if err != nil {
			http.Error(w, "Failed to delete session", http.StatusInternalServerError)
			return
		}

		http.SetCookie(w, &http.Cookie{
			Name:     "session_id",
			Value:    "",
			HttpOnly: true,
			Secure:   true,
			SameSite: http.SameSiteStrictMode,
			MaxAge:   -1,
		})

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Logout successful"))
	}
}

func updateUserPreferences(sm *SessionManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sessionCookie, err := r.Cookie("session_id")
		if err != nil {
			http.Error(w, "No session found", http.StatusBadRequest)
			return
		}

		sessionID, err := uuid.Parse(sessionCookie.Value)
		if err != nil {
			http.Error(w, "Invalid session ID", http.StatusBadRequest)
			return
		}

		newTheme := r.FormValue("theme")

		maxRetries := 3
		for attempt := 0; attempt < maxRetries; attempt++ {
			var session *Session
			var err error

			if attempt == 0 {
				session, err = sm.GetSession(r.Context(), sessionID)
			} else {
				session, err = sm.GetSession(r.Context(), sessionID, WithGetForceRefresh())
			}

			if err != nil {
				http.Error(w, "Failed to retrieve session", http.StatusInternalServerError)
				return
			}

			var preferences map[string]string
			_, err = session.GetAttributeAndRetainUnmarshaled("preferences", &preferences)
			if err != nil {
				http.Error(w, "Failed to get preferences", http.StatusInternalServerError)
				return
			}

			preferences["theme"] = newTheme
			err = session.UpdateAttribute("preferences", preferences)
			if err != nil {
				http.Error(w, "Failed to update preferences", http.StatusInternalServerError)
				return
			}

			_, err = sm.UpdateSession(r.Context(), session, WithUpdateCheckAttributeVersion())
			if err != nil {
				if err.Error() == "attribute preferences version mismatch" {
					// Specific attribute version conflict, retry
					continue
				}
				http.Error(w, "Failed to save session", http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Preferences updated successfully"))
			return
		}

		http.Error(w, "Failed to update preferences after max retries", http.StatusConflict)
	}
}

func addToCartHandler(sm *SessionManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sessionCookie, err := r.Cookie("session_id")
		if err != nil {
			http.Error(w, "No session found", http.StatusBadRequest)
			return
		}

		sessionID, err := uuid.Parse(sessionCookie.Value)
		if err != nil {
			http.Error(w, "Invalid session ID", http.StatusBadRequest)
			return
		}

		itemID := r.FormValue("item_id")

		maxRetries := 3
		for attempt := 0; attempt < maxRetries; attempt++ {
			var session *Session
			var err error

			if attempt == 0 {
				session, err = sm.GetSession(r.Context(), sessionID)
			} else {
				session, err = sm.GetSession(r.Context(), sessionID, WithGetForceRefresh())
			}
			if err != nil {
				http.Error(w, "Failed to retrieve session", http.StatusInternalServerError)
				return
			}

			var cartItems []string
			attr, exists := session.GetAttribute("cart")
			if exists {
				cartItems, _ = attr.Value.([]string)
			} else {
				// Cart doesn't exist yet, create a new one
				cartItems = []string{}
			}

			cartItems = append(cartItems, itemID)

			err = session.UpdateAttribute("cart", cartItems)
			if err != nil {
				http.Error(w, "Failed to update cart", http.StatusInternalServerError)
				return
			}

			_, err = sm.UpdateSession(r.Context(), session, WithUpdateCheckAttributeVersion())
			if err != nil {
				if err.Error() == "attribute cart version mismatch" {
					continue // Retry
				}
				http.Error(w, "Failed to save session", http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Item added to cart"))
			return
		}

		http.Error(w, "Failed to add item to cart after max retries", http.StatusConflict)
	}
}

func updateUserSessionWithVersionCheck(sm *SessionManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sessionCookie, err := r.Cookie("session_id")
		if err != nil {
			http.Error(w, "No session found", http.StatusBadRequest)
			return
		}
		sessionID, _ := uuid.Parse(sessionCookie.Value)

		maxRetries := 3
		for attempt := 0; attempt < maxRetries; attempt++ {
			session, err := sm.GetSession(r.Context(), sessionID)
			if err != nil {
				http.Error(w, "Failed to retrieve session", http.StatusInternalServerError)
				return
			}

			// Update multiple session attributes
			err = session.UpdateAttribute("last_access", time.Now())
			if err != nil {
				http.Error(w, "Failed to update last access", http.StatusInternalServerError)
				return
			}

			var pageViews int
			_, err = session.GetAttributeAndRetainUnmarshaled("page_views", &pageViews)
			if err != nil {
				http.Error(w, "Failed to get page views", http.StatusInternalServerError)
				return
			}
			err = session.UpdateAttribute("page_views", pageViews+1)
			if err != nil {
				http.Error(w, "Failed to update page views", http.StatusInternalServerError)
				return
			}

			// Try to update with session version check
			_, err = sm.UpdateSession(r.Context(), session, WithUpdateCheckVersion())
			if errors.Is(err, ErrSessionVersionIsOutdated) {
				// Version conflict, retry
				continue
			}
			if err != nil {
				http.Error(w, "Failed to save session", http.StatusInternalServerError)
				return
			}

			// Success
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Session updated successfully"))
			return
		}

		http.Error(w, "Failed to update session after max retries", http.StatusConflict)
	}
}

func loginHandlerWithSignedCookie(sm *SessionManager, signerPool *HMACSHA256SignerPool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		username := r.FormValue("username")
		//password := r.FormValue("password")

		userID := uuid.New() // In a real scenario, this would be fetched from a database

		attributes := map[string]SessionAttributeValue{
			"username":   {Value: username},
			"last_login": {Value: time.Now().Format(time.RFC3339)},
		}
		session, err := sm.CreateSession(r.Context(), userID, attributes)
		if err != nil {
			http.Error(w, "Failed to create session", http.StatusInternalServerError)
			return
		}

		// Sign and encode the session ID
		signedSessionID, err := signerPool.SignAndEncode(session.ID.String())
		if err != nil {
			http.Error(w, "Failed to sign session", http.StatusInternalServerError)
			return
		}

		// Set the signed session cookie
		http.SetCookie(w, &http.Cookie{
			Name:     "session_id",
			Value:    signedSessionID,
			HttpOnly: true,
			Secure:   true,
			SameSite: http.SameSiteStrictMode,
		})

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Login successful"))
	}
}

func getSessionWithSignedCookie(sm *SessionManager, signerPool *HMACSHA256SignerPool, r *http.Request) (*Session, error) {
	cookie, err := r.Cookie("session_id")
	if err != nil {
		return nil, err
	}

	// Verify the signature and decode the session ID
	isValid, sessionIDString, err := signerPool.VerifyAndDecode(cookie.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to verify and decode session data: %w", err)
	}
	if !isValid {
		return nil, fmt.Errorf("invalid session signature")
	}

	// Parse the session ID
	sessionID, err := uuid.Parse(sessionIDString)
	if err != nil {
		return nil, fmt.Errorf("invalid session ID: %w", err)
	}

	// Retrieve the session
	return sm.GetSession(r.Context(), sessionID)
}

func performCriticalOperation(sm *SessionManager, sessionID uuid.UUID) error {
	lock := sm.NewDistributedLock(sessionID, "critical-operation", nil)

	err := lock.Lock(context.Background())
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer lock.Unlock(context.Background())

	// Simulate critical operation
	time.Sleep(100 * time.Millisecond)

	return nil
}
