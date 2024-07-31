# gopgsession

`gopgsession` is a distributed session management library that uses PostgreSQL's LISTEN/NOTIFY mechanism to handle session updates across multiple nodes. It leverages the `pgln` library to quickly recover from disconnections, ensuring robust and reliable session management.

⭐️ **Star This Project** ⭐️

If you find this project helpful, please give it a star on GitHub! Your support is greatly appreciated.

## Features

- **Distributed Session Management**: Uses PostgreSQL LISTEN/NOTIFY for real-time session updates.
- **Quick Recovery**: Utilizes the `pgln` library to handle disconnections efficiently.
- **Session Caching**: In-memory caching of sessions with LRU eviction policy.
- **Session Expiration**: Automatically expires sessions based on configured durations.
- **Periodic Cleanup**: Periodically cleans up expired sessions.
- **Efficient Last Access Update**: Accumulates last access times and updates them in batches at a predefined interval, reducing performance hits during session retrieval.
- **Highly Configurable**: Various settings can be customized via a configuration structure.

## Installation

To install the package, run:

```sh
go get github.com/tzahifadida/gopgsession
```

## Usage

### Configuration

Create a configuration using the `Config` struct. You can use the `DefaultConfig` function to get a default configuration and modify it as needed.

#### `Config` Fields

- `MaxSessions`: Maximum number of sessions allowed per user.
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

```go
cfg := gopgsession.DefaultConfig()
cfg.MaxSessions = 10
cfg.SessionExpiration = 24 * time.Hour // 1 day
cfg.CreateSchemaIfMissing = true
```

### Initialization

Initialize a `SessionManager` with the configuration and a PostgreSQL connection string.

```go
pgxConnectionString := "postgres://username:password@localhost/dbname?sslmode=disable"
sessionManager, err := gopgsession.NewSessionManager(cfg, pgxConnectionString)
if err != nil {
    log.Fatalf("Failed to initialize session manager: %v", err)
}
```

### Creating a Session

Create a session for a user with initial attributes.

```go
ctx := context.Background()
userID := uuid.New()
attributes := map[string]interface{}{
    "role": "admin",
    "preferences": map[string]string{
        "theme": "dark",
    },
}

sessionID, err := sessionManager.CreateSession(ctx, userID, attributes)
if err != nil {
    log.Fatalf("Failed to create session: %v", err)
}

log.Printf("Created session with ID: %s", sessionID)
```

### Retrieving a Session

Retrieve a session by its ID.

```go
session, err := sessionManager.GetSession(ctx, sessionID, true)
if err != nil {
    log.Fatalf("Failed to retrieve session: %v", err)
}

log.Printf("Retrieved session for user ID: %s", session.UserID)
```

### Updating a Session Attribute

Update an attribute in a session.

```go
err = session.UpdateAttribute("preferences", map[string]string{"theme": "light"})
if err != nil {
    log.Fatalf("Failed to update session attribute: %v", err)
}

err = sessionManager.UpdateSession(ctx, session)
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

### Shutdown

Shutdown the session manager gracefully.

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

err = sessionManager.Shutdown(ctx)
if err != nil {
    log.Fatalf("Failed to shutdown session manager: %v", err)
}

log.Println("Session manager shutdown successfully")
```

## Exported Functions and Configuration

### `NewSessionManager`

Initializes a new `SessionManager` with the given configuration and PostgreSQL connection string.

```go
func NewSessionManager(cfg *Config, pgxConnectionString string) (*SessionManager, error)
```

### `CreateSession`

Creates a new session for the specified user with given attributes.

```go
func (sm *SessionManager) CreateSession(ctx context.Context, userID uuid.UUID, attributes map[string]interface{}) (uuid.UUID, error)
```

### `GetSession`

Retrieves a session by its ID. Optionally updates the last access time.

```go
func (sm *SessionManager) GetSession(ctx context.Context, sessionID uuid.UUID, updateSessionAccess bool) (*Session, error)
```

### `UpdateSession`

Updates the session in the database with any changed attributes.

```go
func (sm *SessionManager) UpdateSession(ctx context.Context, session *Session) error
```

### `DeleteSession`

Deletes a session by its ID.

```go
func (sm *SessionManager) DeleteSession(ctx context.Context, sessionID uuid.UUID) error
```

### `Shutdown`

Shuts down the session manager gracefully, ensuring all ongoing operations are completed.

```go
func (sm *SessionManager) Shutdown(ctx context.Context) error
```

## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests.

## License

This project is licensed under the MIT License.

---

For more details and examples, please refer to the `session_test.go` file in the repository.