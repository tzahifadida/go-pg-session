# go-pg-session

`go-pg-session` is a distributed session management library that uses PostgreSQL's LISTEN/NOTIFY mechanism to handle session updates across multiple nodes. It leverages the `pgln` library to quickly recover from disconnections, ensuring robust and reliable session management.

⭐️ **Star This Project** ⭐️

If you find this project helpful, please give it a star on GitHub! Your support is greatly appreciated.

## Table of Contents

- [Background](#background)
- [Features](#features)
- [Benchmark Results](#benchmark-results)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [Choosing Between CheckVersion and CheckAttributeVersion](#choosing-between-checkversion-and-checkattributeversion)
- [Distributed Locks](#distributed-locks)
- [Performance Considerations](#performance-considerations)
- [Exported Functions and Configuration](#exported-functions-and-configuration)
- [Contributing](#contributing)
- [License](#license)

## Background

The `go-pg-session` library was created with the goal of combining the speed of JWTs with the simplicity and security of a backend session library. Recognizing that sessions are typically slow to change per user and are mainly read rather than written, this library implements an eventually consistent memory caching strategy on each node.

This approach offers a hybrid solution that leverages the benefits of both cookie-based and server-side session management:

- **Security**:
    - **Minimized Data Exposure**: Only the session identifier is stored in the cookie, minimizing exposure to sensitive data.
    - **Server-Side Data Integrity**: Actual session data is stored and managed server-side, ensuring data integrity and security.

- **Scalability**:
    - **Lightweight Cookies**: The cookie remains lightweight as it only contains the session identifier. This significantly reduces data transfer costs, which are often the highest expense in modern web applications.

- **Performance**:
    - **Efficient Session Retrieval**: Session data can be fetched quickly using the identifier, and caching strategies are employed to optimize performance.
    - **In-Memory Read Operations**: Unlike Redis-based solutions, read operations are performed directly from memory, making them even faster than Redis reads. This approach provides ultra-low latency for session data access.

- **Flexibility**:
    - **No Size Limitations**: The database can store large and complex session data without the size limitations of cookies.
    - **Persistent Storage**: Sessions can persist across server restarts and crashes.

- **Simplified Infrastructure**:
    - **No Additional Caching Servers**: Unlike solutions that rely on Redis, `go-pg-session` eliminates the need for additional Redis servers and complex Redis cluster management. This simplifies the infrastructure, reducing operational complexity and costs.
    - **Leveraging Existing Database**: By using PostgreSQL for both data storage and real-time notifications, the solution minimizes the number of components in the system architecture.

## Features

- **Distributed Session Management**: Uses PostgreSQL LISTEN/NOTIFY for real-time session updates.
- **Quick Recovery**: Utilizes the `pgln` library to handle disconnections efficiently.
- **Session Caching**: In-memory caching of sessions with LRU eviction policy.
- **Session Expiration**: Automatically expires sessions based on configured durations.
- **Periodic Cleanup**: Periodically cleans up expired sessions.
- **Efficient Last Access Update**: Accumulates last access times and updates them in batches at a predefined interval, reducing performance hits during session retrieval.
- **Highly Configurable**: Various settings can be customized via a configuration structure.
- **Optimistic Locking**:
    - **Session-Level Versioning**: Supports version-based optimistic locking for entire sessions.
    - **Attribute-Level Versioning**: Option to check versions of individual session attributes, providing fine-grained control over concurrent modifications.
- **Attribute-Level Expiration**: Individual session attributes can have their own expiration times, providing fine-grained control over data lifecycle.
- **Distributed Locks**: Provides a mechanism for coordinating access to shared resources across multiple nodes.

### Attribute-Level Expiration: A Powerful Feature

The attribute-level expiration feature in `go-pg-session` offers several significant benefits:

1. **Fine-Grained Data Control**: Unlike whole-session expiration, attribute expiration allows you to set different lifetimes for different pieces of data within a session. This is particularly useful for managing sensitive or temporary information.

2. **Enhanced Security**: Sensitive data can be automatically removed from the session after a short period, reducing the window of vulnerability without affecting the overall session.

3. **Compliance with Data Regulations**: For applications that need to comply with data protection regulations (like GDPR), attribute expiration provides a mechanism to ensure that certain types of data are not retained longer than necessary.

4. **Optimized Storage**: By allowing frequently changing or temporary data to expire automatically, you can keep your session data lean and relevant, potentially improving performance and reducing storage costs.

5. **Flexible User Experiences**: You can implement features that require temporary elevated permissions or time-limited offers without compromising on security or user convenience.

Example use cases:

- **Elevated Security (Step-Up Authentication)**: After a user performs a sensitive action requiring additional authentication, you can store a temporary "elevated_access" attribute that automatically expires after a short period:

  ```go
  // Grant elevated access for 15 minutes after step-up authentication
  elevatedExpiry := time.Now().Add(15 * time.Minute)
  session.UpdateAttribute("elevated_access", "true", gopgsession.WithExpiresAt(elevatedExpiry))
  ```

- **Limited-Time Offers**: Store time-sensitive promotional codes or offers that automatically expire:

  ```go
  // Set a promotional offer that expires in 1 hour
  offerExpiry := time.Now().Add(1 * time.Hour)
  session.UpdateAttribute("promo_code", "FLASH_SALE_20", gopgsession.WithExpiresAt(offerExpiry))
  ```

- **Abandoned UI Processes**: For multi-step processes in your UI, store temporary state that cleans itself up if the user abandons the process:

  ```go
  // Store temporary form data for a multi-step process, expires in 30 minutes
  formExpiry := time.Now().Add(30 * time.Minute)
  session.UpdateAttribute("temp_form_data", jsonEncodedFormData, gopgsession.WithExpiresAt(formExpiry))
  ```

- **Temporary Access Tokens**: Store short-lived access tokens for external services:

  ```go
  // Store an API token that expires in 5 minutes
  tokenExpiry := time.Now().Add(5 * time.Minute)
  session.UpdateAttribute("external_api_token", "token123", gopgsession.WithExpiresAt(tokenExpiry))
  ```

By leveraging attribute-level expiration, you can implement these features securely and efficiently, automatically cleaning up sensitive or temporary data without manual intervention or complex cleanup processes. This feature allows for more nuanced session management, enhancing both security and user experience in your application.

## Benchmark Results

The following table compares the performance of `go-pg-session` (PostgreSQL-based) with a Redis-based session management solution. These benchmarks were run on an ARM64 Darwin system.

| Operation     | Storage                    | Operations/sec | Nanoseconds/op |
|---------------|----------------------------|---------------:|---------------:|
| GetSession    | PostgreSQL + go-pg-session |        326,712 |          3,061 |
| GetSession    | Redis                      |         38,567 |         25,929 |
| UpdateSession | PostgreSQL + go-pg-session |          2,137 |        467,921 |
| UpdateSession | Redis                      |         19,515 |         51,243 |

### Key Observations:

1. **Read Performance (GetSession)**:
    - `go-pg-session` outperforms Redis by a significant margin, processing about 8.5 times more read operations per second.
    - The latency for read operations with `go-pg-session` is about 8.5 times lower than Redis.

2. **Write Performance (UpdateSession)**:
    - Redis shows better performance for write operations, processing about 9 times more updates per second.
    - The latency for write operations with Redis is about 9 times lower than `go-pg-session`.

3. **Overall Performance**:
    - `go-pg-session` excels in read-heavy scenarios, which aligns with typical session management use cases where reads are far more frequent than writes.
    - The PostgreSQL-based solution offers a better balance between read and write performance, making it suitable for a wide range of applications.

These results demonstrate that `go-pg-session` is particularly well-suited for applications with high read volumes, offering superior performance for session retrieval operations. While Redis shows better performance for write operations, the overall balance and especially the read performance of `go-pg-session` make it an excellent choice for most session management scenarios.

Remember that performance can vary based on hardware, network conditions, and specific use cases. It's always recommended to benchmark in your specific environment for the most accurate results.

Note: A would be go-pg-session for redis (or go-redis-session) could outperform go-pg-session when writing for sure, but the aim of this library is to use only PostgreSQL without additional services like redis. Moreover, since sessions are mostly read oriented, this is less of an advantage. It is possible that a go-redis-session would be great for developers who use redis already and may be developed if there is a demand.

## Installation

To install the package, run:

```sh
go get github.com/tzahifadida/go-pg-session
```

## Configuration

Create a configuration using the `Config` struct. You can use the `DefaultConfig` function to get a default configuration and modify it as needed.

### `Config` Fields

- `MaxSessions`: Maximum number of concurrent sessions allowed per user.
- `MaxAttributeLength`: Maximum length of session attributes.
- `SessionExpiration`: Duration after which a session expires.
- `InactivityDuration`: Duration of inactivity after which a session expires.
- `CleanupInterval`: Interval at which expired sessions are cleaned up.
- `CacheSize`: Size of the in-memory cache for sessions.
- `TablePrefix`: Prefix for the table names used in the database.
- `SchemaName`: Name of the schema used in the database.
- `CreateSchemaIfMissing`: Flag to create the schema if it is missing.
- `LastAccessUpdateInterval`: Interval for updating the last access time of sessions.
- `LastAccessUpdateBatchSize`: Batch size for updating last access times.
- `NotifyOnUpdates`: Flag to enable/disable notifications on updates.
- `MaxSessionLifetimeInCache`: Maximum duration a session can remain in the cache before being refreshed from the database.

```go
cfg := gopgsession.DefaultConfig()
cfg.MaxSessions = 10
cfg.SessionExpiration = 24 * time.Hour // 1 day
cfg.CreateSchemaIfMissing = true
cfg.MaxSessionLifetimeInCache = 1 * time.Hour // Force refresh after 1 hour
```

### MaxSessionLifetimeInCache

The `MaxSessionLifetimeInCache` setting allows you to control how long a session can remain in the cache before it's forcefully refreshed from the database. This feature helps ensure that any manual changes made to the session in the database (e.g., forced logouts) are reflected in the cache within a reasonable timeframe.

- If set to a positive duration, sessions older than this duration will be refreshed from the database on the next access.
- If set to 0 or a negative value, this feature is disabled, and sessions will remain in the cache until explicitly invalidated or evicted.

By default, this value is set to 48 hours. Adjust this setting based on your application's needs and the frequency of out-of-band session modifications.

## Usage Examples

### Initialization

Initialize a `SessionManager` with the configuration and a PostgreSQL database connection.

```go
import (
"database/sql"
_ "github.com/jackc/pgx/v5/stdlib"
"github.com/tzahifadida/go-pg-session"
)

// Open a database connection
db, err := sql.Open("pgx", "postgres://username:password@localhost/dbname?sslmode=disable")
if err != nil {
log.Fatalf("Failed to open database: %v", err)
}
defer db.Close()

// Create a new SessionManager
sessionManager, err := gopgsession.NewSessionManager(ctx, cfg, db)
if err != nil {
log.Fatalf("Failed to initialize session manager: %v", err)
}
defer sessionManager.Shutdown(context.Background())
```

### Creating a Session

Create a session for a user with initial attributes.

```go
ctx := context.Background()
userID := uuid.New()
attributes := map[string]gopgsession.SessionAttributeValue{
"role":        {Value: "admin"},
"preferences": {Value: map[string]string{"theme": "dark"}},
}

session, err := sessionManager.CreateSession(ctx, userID, attributes)
if err != nil {
log.Fatalf("Failed to create session: %v", err)
}

log.Printf("Created session with ID: %s", session.ID)
```

### Retrieving a Session

Retrieve a session by its ID. You can use `GetSession` with optional parameters for more control.

```go
session, err := sessionManager.GetSession(ctx, sessionID)
if err != nil {
log.Fatalf("Failed to retrieve session: %v", err)
}

// With options
session, err := sessionManager.GetSession(ctx, sessionID, gopgsession.WithDoNotUpdateSessionLastAccess(), gopgsession.WithForceRefresh())
if err != nil {
log.Fatalf("Failed to retrieve session: %v", err)
}

var preferences map[string]string
_, err = session.GetAttributeAndRetainUnmarshaled("preferences", &preferences)
if err != nil {
log.Fatalf("Failed to get preferences: %v", err)
}

log.Printf("User preferences: %v", preferences)
```

### Updating a Session with Version Check

Update a session while ensuring version consistency at the session level.

```go
session, err := sessionManager.GetSession(ctx, sessionID)
if err != nil {
log.Fatalf("Failed to retrieve session: %v", err)
}

err = session.UpdateAttribute("last_access", time.Now())
if err != nil {
log.Fatalf("Failed to update session attribute: %v", err)
}

if err != nil {
log.Fatalf("Failed to update session attribute: %v", err)
}

updatedSession, err := sessionManager.UpdateSession(ctx, session, gopgsession.WithCheckVersion())
if err != nil {
log.Fatalf("Failed to update session: %v", err)
}
```

### Updating a Session with Attribute Version Check

Update a session while ensuring version consistency at the attribute level.

```go
session, err := sessionManager.GetSession(ctx, sessionID)
if err != nil {
log.Fatalf("Failed to retrieve session: %v", err)
}

var preferences map[string]string
_, err = session.GetAttributeAndRetainUnmarshaled("preferences", &preferences)
if err != nil {
log.Fatalf("Failed to get preferences: %v", err)
}

preferences["theme"] = "light"
err = session.UpdateAttribute("preferences", preferences)
if err != nil {
log.Fatalf("Failed to update session attribute: %v", err)
}

updatedSession, err := sessionManager.UpdateSession(ctx, session, gopgsession.WithCheckAttributeVersion())
if err != nil {
log.Fatalf("Failed to update session: %v", err)
}
```

### Deleting a Session

Delete a session by its ID.

```go
err = sessionManager.DeleteSession(ctx, sessionID)
if err != nil {
log.Fatalf("Failed to delete session: %v", err)
}

log.Printf("Deleted session with ID: %s", sessionID)
```

### Login and Logout Handlers

Here are examples of login and logout handlers that demonstrate session creation and deletion:

```go
func loginHandler(sm *gopgsession.SessionManager) http.HandlerFunc {
return func(w http.ResponseWriter, r *http.Request) {
// Authenticate user (simplified for example)
username := r.FormValue("username")
password := r.FormValue("password")

userID, err := authenticateUser(username, password)
if err != nil {
http.Error(w, "Authentication failed", http.StatusUnauthorized)
return
}

// Create new session
attributes := map[string]gopgsession.SessionAttributeValue{
"username":    {Value: username},
"last_login":  {Value: time.Now().Format(time.RFC3339)},
}
session, err := sm.CreateSession(r.Context(), userID, attributes)
if err != nil {
http.Error(w, "Failed to create session", http.StatusInternalServerError)
return
}

// Set session cookie
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

func logoutHandler(sm *gopgsession.SessionManager) http.HandlerFunc {
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

// Delete the session
err = sm.DeleteSession(r.Context(), sessionID)
if err != nil {
http.Error(w, "Failed to delete session", http.StatusInternalServerError)
return
}

// Clear the session cookie
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
```

### Concurrent Updates Example

Here's a refined example of handling concurrent updates to a session, useful for scenarios like a shopping cart:

```go
func addToCartHandler(sm *gopgsession.SessionManager) http.HandlerFunc {
return func(w http.ResponseWriter, r *http.Request) {
sessionID, _ := uuid.Parse(r.Cookie("session_id").Value)
itemID := r.FormValue("item_id")

maxRetries := 3
for attempt := 0; attempt < maxRetries; attempt++ {
var session *gopgsession.Session
var err error

if attempt == 0 {
session, err = sm.GetSession(r.Context(), sessionID)
} else {
session, err = sm.GetSession(r.Context(), sessionID, gopgsession.WithForceRefresh())
}

if err != nil {
http.Error(w, "Failed to retrieve session", http.StatusInternalServerError)
return
}

// Business logic: Update cart
var cartItems []string
_, err = session.GetAttributeAndRetainUnmarshaled("cart", &cartItems)
if err != nil && err.Error() != "attribute cart not found" {
http.Error(w, "Failed to get cart", http.StatusInternalServerError)
return
}
cartItems = append(cartItems, itemID)

err = session.UpdateAttribute("cart", cartItems)
if err != nil {
http.Error(w, "Failed to update cart", http.StatusInternalServerError)
return
}

_, err = sm.UpdateSession(r.Context(), session, gopgsession.WithCheckAttributeVersion())
if err != nil {
if err.Error() == "attribute cart version mismatch" {
continue // Retry
}
http.Error(w, "Failed to save session", http.StatusInternalServerError)
return
}

// Success
w.WriteHeader(http.StatusOK)
w.Write([]byte("Item added to cart"))
return
}

http.Error(w, "Failed to add item to cart after max retries", http.StatusConflict)
}
}
```

### Signed Cookies for DDOS Mitigation

To reduce overhead when handling requests for non-existent sessions (which could be part of a DDOS attack), you can sign the session ID cookie. This allows you to verify the signature before attempting to retrieve the session from the database or cache. The `HMACSHA256SignerPool` provides `SignAndEncode` and `VerifyAndDecode` methods for this purpose.

Here's an example of how to implement this:

```go
import (
"github.com/tzahifadida/go-pg-session"
"github.com/google/uuid"
"fmt"
)

// Initialize the signer
secret := []byte("your-secret-key")
signerPool := gopgsession.NewHMACSHA256SignerPool(secret, 10)

// When creating a session and setting the cookie
func loginHandler(sm *gopgsession.SessionManager) http.HandlerFunc {
return func(w http.ResponseWriter, r *http.Request) {
// ... (authentication logic)

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

// When verifying and retrieving a session
func getSession(sm *gopgsession.SessionManager, r *http.Request) (*gopgsession.Session, error) {
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
// Retrieve the session
return sm.GetSession(r.Context(), sessionID)
}
```

This approach leverages the `HMACSHA256SignerPool`'s methods for signing and verifying session information:

1. `SignAndEncode`: This method signs the given session ID and encodes it into a single string.
2. `VerifyAndDecode`: This method verifies the signature and decodes the original session ID.

By using these methods, you ensure that the session ID is signed, providing an additional layer of security. This approach can significantly reduce the impact of DDOS attacks targeting your session management system by allowing you to quickly reject invalid or tampered requests without querying the database or cache.

Key benefits of this approach:

1. **Efficiency**: The signature can be verified without accessing the database, reducing load on your backend for invalid requests.
2. **Security**: The signed session ID prevents tampering and forgery attempts.
3. **Simplicity**: By focusing on just the session ID, the implementation remains straightforward and easy to understand.

Remember that while this method provides an extra layer of security, it's still important to implement other security best practices, such as using HTTPS, setting appropriate cookie flags, and implementing proper session management on the server side.

### Deleting Attributes from All User Sessions

The `DeleteAttributeFromAllUserSessions` method allows you to remove a specific attribute from all active sessions belonging to a particular user. This is particularly useful in scenarios where you need to update user-wide information that affects all sessions, such as changing user roles, permissions, or any other global user settings.

Here's an example of how to use this method:

```go
func updateUserRoles(sm *gopgsession.SessionManager, userID uuid.UUID) error {
    // Delete the 'roles' attribute from all of the user's sessions
    err := sm.DeleteAttributeFromAllUserSessions(context.Background(), userID, "roles")
    if err != nil {
        return fmt.Errorf("failed to delete roles attribute: %w", err)
    }

    // The 'roles' attribute will be recalculated the next time each session is accessed
    return nil
}
```

#### Use Case: Updating User Roles or Permissions

When you change a user's roles, permissions, or any other security-related attributes, it's crucial to ensure that these changes are reflected across all active sessions for that user. Here's how you might use `DeleteAttributeFromAllUserSessions` in such a scenario:

1. Update the user's roles in your user management system.
2. Call `DeleteAttributeFromAllUserSessions` to remove the 'roles' attribute from all active sessions.
3. The next time each session is accessed, your application will recalculate and set the 'roles' attribute based on the latest information.

```go
func updateUserRolesAndSessions(sm *gopgsession.SessionManager, userID uuid.UUID, newRoles []string) error {
    // Update roles in the user management system
    err := updateUserRolesInDatabase(userID, newRoles)
    if err != nil {
        return fmt.Errorf("failed to update user roles: %w", err)
    }

    // Delete the 'roles' attribute from all active sessions
    err = sm.DeleteAttributeFromAllUserSessions(context.Background(), userID, "roles")
    if err != nil {
        return fmt.Errorf("failed to delete roles attribute from sessions: %w", err)
    }

    log.Printf("Updated roles for user %s and cleared roles attribute from all sessions", userID)
    return nil
}

// In your session retrieval logic
func getSessionWithRoles(sm *gopgsession.SessionManager, sessionID uuid.UUID) (*gopgsession.Session, error) {
    session, err := sm.GetSession(context.Background(), sessionID)
    if err != nil {
        return nil, fmt.Errorf("failed to get session: %w", err)
    }

    // Check if roles need to be recalculated
    _, exists := session.GetAttribute("roles")
    if !exists {
        // Roles attribute doesn't exist, so recalculate it
        roles, err := getUserRolesFromDatabase(session.UserID)
        if err != nil {
            return nil, fmt.Errorf("failed to get user roles: %w", err)
        }

        err = session.UpdateAttribute("roles", roles)
        if err != nil {
            return nil, fmt.Errorf("failed to update roles attribute: %w", err)
        }

        // Save the updated session
        session, err = sm.UpdateSession(context.Background(), session)
        if err != nil {
            return nil, fmt.Errorf("failed to save updated session: %w", err)
        }
    }

    return session, nil
}
```

This approach ensures that:

1. Role changes are immediately effective across all sessions.
2. The performance impact is minimized, as roles are only recalculated when a session is actually used.
3. There's no need to track down and update every active session immediately, which could be resource-intensive.

By using `DeleteAttributeFromAllUserSessions`, you can efficiently handle scenarios where global user attributes need to be updated across all active sessions, ensuring consistency and security in your application.

## Choosing Between CheckVersion and CheckAttributeVersion

The `go-pg-session` library offers two types of version checking for optimistic locking:

1. **Session-Level Version Checking** (`WithCheckVersion()`):
    - Checks the version of the entire session.
    - Useful when you want to ensure the entire session hasn't been modified since it was retrieved.

2. **Attribute-Level Version Checking** (`WithCheckAttributeVersion()`):
    - Checks the version of individual attributes being updated.
    - Provides finer-grained control over concurrent modifications.
    - Useful when different parts of your application may be updating different attributes concurrently.

Choose the appropriate version checking method based on your specific use case:

- Use `WithCheckVersion()` when:
    - You need to ensure the entire session state is as expected before making updates.
    - You want to detect any changes to the session, even to attributes you're not currently modifying.

- Use `WithCheckAttributeVersion()` when:
    - You want to allow concurrent updates to different attributes of the same session.
    - You need more granular conflict detection and resolution.
    - You're updating a specific attribute and don't care about changes to other attributes.

Remember, you should choose either `WithCheckVersion()` or `WithCheckAttributeVersion()`, not both, as `WithCheckVersion()` implicitly checks all attribute versions as well.

## Distributed Locks

The `go-pg-session` library provides a distributed locking mechanism that allows you to coordinate access to shared resources across multiple nodes in a distributed system. This feature is particularly useful for scenarios where you need to ensure that only one process or node can access or modify a specific resource at a time.

### Key Features

- Lease-based locking with automatic expiration
- Heartbeat mechanism to maintain locks
- Configurable retry and timeout settings
- Support for extending lock lease time

### Use Cases

- Coordinating access to shared resources in a distributed system
- Implementing distributed cron jobs or scheduled tasks
- Ensuring single-writer scenarios in multi-node deployments
- Preventing race conditions in distributed workflows

### Basic Usage

Here's a basic example of how to use the distributed lock:

```go
func performCriticalOperation(sm *gopgsession.SessionManager, sessionID uuid.UUID) error {
// Create a new distributed lock
lock := sm.NewDistributedLock(sessionID, "critical-operation", nil)

// Attempt to acquire the lock
err := lock.Lock(context.Background())
if err != nil {
return fmt.Errorf("failed to acquire lock: %w", err)
}
defer lock.Unlock(context.Background())

// Perform your critical operation here
// ...

return nil
}
```

### Advanced Usage

#### Custom Configuration

You can customize the lock behavior by providing a `DistributedLockConfig`:

```go
config := &gopgsession.DistributedLockConfig{
MaxRetries:        5,
RetryDelay:        100 * time.Millisecond,
HeartbeatInterval: 5 * time.Second,
LeaseTime:         30 * time.Second,
}

lock := sm.NewDistributedLock(sessionID, "custom-lock", config)
```

#### Extending Lease Time

If your operation takes longer than expected, you can extend the lease time:

```go
func longRunningOperation(sm *gopgsession.SessionManager, sessionID uuid.UUID) error {
lock := sm.NewDistributedLock(sessionID, "long-operation", nil)

err := lock.Lock(context.Background())
if err != nil {
return fmt.Errorf("failed to acquire lock: %w", err)
}
defer lock.Unlock(context.Background())

// Start the operation
for {
// Do some work...

// Extend the lease if needed
err := lock.ExtendLease(context.Background(), 30*time.Second)
if err != nil {
return fmt.Errorf("failed to extend lease: %w", err)
}

// Check if the operation is complete
if isComplete() {
break
}
}

return nil
}
```

#### Handling Lock Acquisition Failures

In some cases, you might want to handle scenarios where the lock can't be acquired:

```go
func tryOperation(sm *gopgsession.SessionManager, sessionID uuid.UUID) error {
lock := sm.NewDistributedLock(sessionID, "try-operation", nil)

err := lock.Lock(context.Background())
if err != nil {
if errors.Is(err, gopgsession.ErrLockAlreadyHeld) {
// Handle the case where the lock is already held
return fmt.Errorf("operation already in progress")
}
return fmt.Errorf("failed to acquire lock: %w", err)
}
defer lock.Unlock(context.Background())

// Perform the operation
// ...

return nil
}
```

### Best Practices

1. **Always use deferred unlock**: Use `defer lock.Unlock(ctx)` immediately after acquiring the lock to ensure it's released even if there's a panic.

2. **Use appropriate timeout**: Set a context timeout when acquiring locks to prevent indefinite waiting.

3. **Handle errors properly**: Check for and handle all potential errors, including lock acquisition failures and unlock errors.

4. **Use unique resource names**: Ensure that your resource names (second parameter in `NewDistributedLock`) are unique and descriptive for the operation you're protecting.

5. **Minimize lock duration**: Keep the critical section as short as possible to reduce contention.

6. **Consider using try-lock pattern**: In some scenarios, it might be better to fail fast if a lock can't be acquired immediately, rather than waiting.

By using the distributed lock feature of `go-pg-session`, you can coordinate operations across multiple instances of your application, ensuring data consistency and preventing race conditions in distributed environments.

## Performance Considerations

The distributed memory cache in `go-pg-session` provides excellent read performance, as most session retrievals will be served from memory. Write operations are managed efficiently through batched updates and PostgreSQL's NOTIFY mechanism.

In high-traffic scenarios, consider adjusting the following configuration parameters:

- `CacheSize`: Increase this value to cache more sessions in memory, reducing database reads.
- `LastAccessUpdateInterval` and `LastAccessUpdateBatchSize`: Tune these values to optimize the frequency and size of batched last-access updates.
- `CleanupInterval`: Adjust this value to balance between timely session cleanup and database load.

Remember to monitor your PostgreSQL server's performance and scale it accordingly as your application grows.

When using version checking (either session-level or attribute-level), keep in mind:

- It provides protection against concurrent modifications but may increase the complexity of conflict resolution.
- In high-concurrency scenarios, it may lead to more update conflicts, requiring careful retry logic in your application.
- The performance impact is generally minimal, but extensive use in extremely high-traffic applications should be monitored.
- Attribute-level version checking can offer more granular control and potentially reduce conflicts in scenarios where different attributes are often updated independently.

## Exported Functions and Configuration

### `NewSessionManager`

Initializes a new `SessionManager` with the given configuration and PostgreSQL database connection.

```go
func NewSessionManager(ctx context.Context, cfg *Config, db *sql.DB) (*SessionManager, error)
```

### `CreateSession`

Creates a new session for the specified user with given attributes.

```go
func (sm *SessionManager) CreateSession(ctx context.Context, userID uuid.UUID, attributes map[string]SessionAttributeValue) (*Session, error)
```

### `GetSession`

Retrieves a session by its ID with optional parameters.

```go
func (sm *SessionManager) GetSession(ctx context.Context, sessionID uuid.UUID, options ...SessionOption) (*Session, error)
```

### `UpdateSession`

Updates the session in the database with any changed attributes. Supports session-level or attribute-level version checking and other options.

```go
func (sm *SessionManager) UpdateSession(ctx context.Context, session *Session, options ...UpdateSessionOption) (*Session, error)
```

#### Update Session Options

- `WithCheckVersion()`: Enables version checking for the entire session.
- `WithCheckAttributeVersion()`: Enables version checking for individual attributes.
- `WithDoNotNotify()`: Disables notifications for this update operation.

Usage examples:

```go
// Session-level version checking
updatedSession, err := sm.UpdateSession(ctx, session, gopgsession.WithCheckVersion())

// Attribute-level version checking
updatedSession, err := sm.UpdateSession(ctx, session, gopgsession.WithCheckAttributeVersion())
```

### `DeleteSession`

Deletes a session by its ID.

```go
func (sm *SessionManager) DeleteSession(ctx context.Context, sessionID uuid.UUID) error
```

### `DeleteAllUserSessions`

Deletes all sessions for a given user.

```go
func (sm *SessionManager) DeleteAllUserSessions(ctx context.Context, userID uuid.UUID) error
```

### `Shutdown`

Shuts down the session manager gracefully, ensuring all ongoing operations are completed.

```go
func (sm *SessionManager) Shutdown(ctx context.Context) error
```

### `ClearEntireCache`

Clears all sessions from the cache on the current node and notifies other nodes to do the same.

```go
func (sm *SessionManager) ClearEntireCache(ctx context.Context) error
```

This method is particularly useful in scenarios where you need to invalidate all cached sessions across all nodes, such as:

- After a major data migration
- In response to a security event
- When deploying significant changes that affect session data

Usage example:

```go
err := sessionManager.ClearEntireCache(context.Background())
```

### Session Methods

#### `UpdateAttribute`

Updates or sets an attribute for the session.

```go
func (s *Session) UpdateAttribute(key string, value interface{}, options ...UpdateAttributeOption) error
```

#### Update Attribute Options

- `WithExpiresAt(time.Time)`: Sets an expiration time for the attribute.

Usage examples:

```go
// Update an attribute without expiration
err := session.UpdateAttribute("theme", "dark")

// Update an attribute with expiration
expiresAt := time.Now().Add(24 * time.Hour)
err := session.UpdateAttribute("temporary_flag", true, gopgsession.WithExpiresAt(expiresAt))
```

#### `DeleteAttribute`

Deletes an attribute from the session.

```go
func (s *Session) DeleteAttribute(key string) error
```

#### `GetAttributes`

Returns all attributes of the session.

```go
func (s *Session) GetAttributes() map[string]SessionAttributeValue
```

#### `GetAttribute`

Retrieves a specific attribute from the session.

```go
func (s *Session) GetAttribute(key string) (SessionAttributeValue, bool)
```

#### `GetAttributeAndRetainUnmarshaled`

Retrieves a specific attribute, unmarshals it if necessary, and retains the unmarshaled value in memory for future use.

```go
func (s *Session) GetAttributeAndRetainUnmarshaled(key string, v interface{}) (SessionAttributeValue, error)
```

This method is particularly useful for attributes that are frequently accessed and expensive to unmarshal. It provides the following benefits:

1. **Lazy Unmarshaling**: Attributes are unmarshaled only when requested, saving processing time for unused attributes.
2. **Caching**: Once unmarshaled, the value is retained in memory, improving performance for subsequent accesses.
3. **Type Safety**: The method uses Go's type system to unmarshal into the correct type.
4. **Efficiency**: For string attributes, it avoids unnecessary unmarshaling.
5. **Thread-Safety**: The method is safe to use in concurrent environments.

Usage example:

```go
var preferences struct {
Theme string `json:"theme"`
Language string `json:"language"`
}

attr, err := session.GetAttributeAndRetainUnmarshaled("preferences", &preferences)
if err != nil {
log.Printf("Failed to get preferences: %v", err)
return
}

log.Printf("User preferences: Theme=%s, Language=%s", preferences.Theme, preferences.Language)

// The unmarshaled value is now cached in the session for future use
```

### Distributed Lock Methods

#### `NewDistributedLock`

Creates a new distributed lock for a given session and resource.

```go
func (sm *SessionManager) NewDistributedLock(sessionID uuid.UUID, resourceName string, config *DistributedLockConfig) *DistributedLock
```

#### `Lock`

Attempts to acquire the distributed lock.

```go
func (dl *DistributedLock) Lock(ctx context.Context) error
```

#### `Unlock`

Releases the distributed lock.

```go
func (dl *DistributedLock) Unlock(ctx context.Context) error
```

#### `ExtendLease`

Extends the lease time of the lock.

```go
func (dl *DistributedLock) ExtendLease(ctx context.Context, duration time.Duration) error
```

## Contributing

Contributions to `go-pg-session` are welcome! Here are some ways you can contribute:

1. Report bugs or request features by opening an issue.
2. Improve documentation.
3. Submit pull requests with bug fixes or new features.

Please ensure that your code adheres to the existing style and that all tests pass before submitting a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Thank you for using `go-pg-session`! If you have any questions, suggestions, or encounter any issues, please don't hesitate to open an issue on the GitHub repository. Your feedback and contributions are greatly appreciated and help make this library better for everyone.