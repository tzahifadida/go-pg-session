package gopgsession

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadmeExamples(t *testing.T) {
	// Start PostgreSQL container
	ctx := context.Background()
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
		retrievedSession, err := sessionManager.GetSession(context.Background(), session.ID, WithDoNotUpdateSessionLastAccess())
		require.NoError(t, err)
		assert.Equal(t, userID, retrievedSession.UserID)

		// Updating a Session Attribute
		var preferences map[string]string
		_, err = retrievedSession.GetAttributeAndRetainUnmarshaled("preferences", &preferences)
		require.NoError(t, err)
		preferences["theme"] = "light"
		err = retrievedSession.UpdateAttribute("preferences", preferences, nil)
		require.NoError(t, err)

		updatedSession, err := sessionManager.UpdateSession(context.Background(), retrievedSession, WithCheckVersion())
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

		// First, create a session
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
	})
}

func TestSignedCookiesExample(t *testing.T) {
	// Start PostgreSQL container
	ctx := context.Background()
	postgres, pgConnString, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)

	// Create a new SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true

	sessionManager, err := NewSessionManager(cfg, pgConnString)
	require.NoError(t, err)
	defer sessionManager.Shutdown(context.Background())

	// Initialize the signer
	signerPool := NewHMACSHA256SignerPool("test-secret-key", 10)

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
		assert.Contains(t, err.Error(), "invalid session signature")
	})
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
				session, err = sm.GetSession(r.Context(), sessionID, WithForceRefresh())
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
			err = session.UpdateAttribute("preferences", preferences, nil)
			if err != nil {
				http.Error(w, "Failed to update preferences", http.StatusInternalServerError)
				return
			}

			_, err = sm.UpdateSession(r.Context(), session, WithCheckVersion())
			if errors.Is(err, ErrSessionVersionIsOutdated) {
				continue
			}
			if err != nil {
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
				session, err = sm.GetSession(r.Context(), sessionID, WithForceRefresh())
			}

			if err != nil {
				http.Error(w, "Failed to retrieve session", http.StatusInternalServerError)
				return
			}

			var cartItems []string
			_, err = session.GetAttributeAndRetainUnmarshaled("cart", &cartItems)
			if err != nil && err.Error() != "attribute cart not found" {
				http.Error(w, "Failed to get cart", http.StatusInternalServerError)
				return
			}
			cartItems = append(cartItems, itemID)

			err = session.UpdateAttribute("cart", cartItems, nil)
			if err != nil {
				http.Error(w, "Failed to update cart", http.StatusInternalServerError)
				return
			}

			_, err = sm.UpdateSession(r.Context(), session, WithCheckVersion())
			if errors.Is(err, ErrSessionVersionIsOutdated) {
				continue
			}
			if err != nil {
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

		// Sign the session ID
		sessionIDString := session.ID.String()
		signature, err := signerPool.Sign(sessionIDString)
		if err != nil {
			http.Error(w, "Failed to sign session", http.StatusInternalServerError)
			return
		}

		// Set the signed session cookie
		http.SetCookie(w, &http.Cookie{
			Name:     "session_id",
			Value:    sessionIDString + "." + signature,
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

	parts := strings.Split(cookie.Value, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid session cookie format")
	}

	sessionIDString, signature := parts[0], parts[1]

	// Verify the signature
	isValid, _, err := signerPool.Verify(sessionIDString, signature)
	if err != nil || !isValid {
		return nil, fmt.Errorf("invalid session signature")
	}

	// Parse the session ID
	sessionID, err := uuid.Parse(sessionIDString)
	if err != nil {
		return nil, fmt.Errorf("invalid session ID")
	}

	// Retrieve the session
	return sm.GetSession(r.Context(), sessionID)
}

// startPostgresContainer function should be implemented here or imported from your test utilities
