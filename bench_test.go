package gopgsession

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func BenchmarkConcurrentGetSession(b *testing.B) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, pgConnString, err := startPostgresContainer(ctx)
	require.NoError(b, err)
	defer postgres.Terminate(ctx)

	// Create SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 10000

	sm, err := NewSessionManager(cfg, pgConnString)
	require.NoError(b, err)
	defer sm.Shutdown(context.Background())

	// Create test sessions
	numSessions := 1000
	sessionIDs := make([]uuid.UUID, numSessions)
	for i := 0; i < numSessions; i++ {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: fmt.Sprintf("value%d", i)},
		}
		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(b, err)
		sessionIDs[i] = session.ID
	}

	b.ResetTimer()

	b.Run("ConcurrentGetSession", func(b *testing.B) {
		var wg sync.WaitGroup
		wg.Add(b.N)

		start := time.Now()

		for i := 0; i < b.N; i++ {
			go func() {
				defer wg.Done()
				sessionID := sessionIDs[rand.Intn(numSessions)]
				_, err := sm.GetSession(context.Background(), sessionID)
				if err != nil {
					b.Error(err)
				}
			}()
		}

		wg.Wait()

		elapsed := time.Since(start)
		opsPerSecond := float64(b.N) / elapsed.Seconds()
		nsPerOp := float64(elapsed.Nanoseconds()) / float64(b.N)

		b.ReportMetric(opsPerSecond, "ops/sec")
		b.ReportMetric(nsPerOp, "ns/op")

		// Print results to console
		log.Printf("BenchmarkConcurrentGetSession:\n")
		log.Printf("  Iterations: %d\n", b.N)
		log.Printf("  Total time: %v\n", elapsed)
		log.Printf("  Ops/sec: %.2f\n", opsPerSecond)
		log.Printf("  Ns/op: %.2f\n", nsPerOp)
	})
}

func BenchmarkConcurrentUpdateSession(b *testing.B) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, pgConnString, err := startPostgresContainer(ctx)
	require.NoError(b, err)
	defer postgres.Terminate(ctx)

	// Create SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 10000

	sm, err := NewSessionManager(cfg, pgConnString)
	require.NoError(b, err)
	defer sm.Shutdown(context.Background())

	// Create test sessions
	numSessions := 1000
	sessionIDs := make([]uuid.UUID, numSessions)
	for i := 0; i < numSessions; i++ {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: fmt.Sprintf("value%d", i)},
		}
		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(b, err)
		sessionIDs[i] = session.ID
	}

	b.ResetTimer()

	b.Run("ConcurrentUpdateSession", func(b *testing.B) {
		var wg sync.WaitGroup
		wg.Add(b.N)

		start := time.Now()

		for i := 0; i < b.N; i++ {
			go func(i int) {
				defer wg.Done()
				sessionID := sessionIDs[rand.Intn(numSessions)]
				session, err := sm.GetSession(context.Background(), sessionID)
				if err != nil {
					b.Error(err)
					return
				}

				err = session.UpdateAttribute("key", fmt.Sprintf("updatedvalue%d", i), nil)
				if err != nil {
					b.Error(err)
					return
				}

				_, err = sm.UpdateSession(context.Background(), session)
				if err != nil {
					b.Error(err)
					return
				}
			}(i)
		}

		wg.Wait()

		elapsed := time.Since(start)
		opsPerSecond := float64(b.N) / elapsed.Seconds()
		nsPerOp := float64(elapsed.Nanoseconds()) / float64(b.N)

		b.ReportMetric(opsPerSecond, "ops/sec")
		b.ReportMetric(nsPerOp, "ns/op")

		// Print results to console
		log.Printf("BenchmarkConcurrentUpdateSession:\n")
		log.Printf("  Iterations: %d\n", b.N)
		log.Printf("  Total time: %v\n", elapsed)
		log.Printf("  Ops/sec: %.2f\n", opsPerSecond)
		log.Printf("  Ns/op: %.2f\n", nsPerOp)
	})
}

type RedisSessionManager struct {
	client *redis.Client
}

func NewRedisSessionManager(addr string) *RedisSessionManager {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisSessionManager{client: client}
}

func (rsm *RedisSessionManager) CreateSession(ctx context.Context, userID uuid.UUID, attributes map[string]string) (string, error) {
	sessionID := uuid.New().String()
	pipe := rsm.client.Pipeline()
	pipe.HSet(ctx, sessionID, attributes)
	pipe.Expire(ctx, sessionID, 24*time.Hour) // Set expiration to 24 hours
	_, err := pipe.Exec(ctx)
	return sessionID, err
}

func (rsm *RedisSessionManager) GetSession(ctx context.Context, sessionID string) (map[string]string, error) {
	return rsm.client.HGetAll(ctx, sessionID).Result()
}

func (rsm *RedisSessionManager) UpdateSession(ctx context.Context, sessionID string, attributes map[string]string) error {
	return rsm.client.HSet(ctx, sessionID, attributes).Err()
}

func startRedisContainer(ctx context.Context) (testcontainers.Container, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "redis:6",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", fmt.Errorf("failed to start redis container: %v", err)
	}

	host, err := redisC.Host(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get redis host: %v", err)
	}

	port, err := redisC.MappedPort(ctx, "6379")
	if err != nil {
		return nil, "", fmt.Errorf("failed to get redis port: %v", err)
	}

	redisAddr := fmt.Sprintf("%s:%d", host, port.Int())
	return redisC, redisAddr, nil
}

func BenchmarkSessionManagers(b *testing.B) {
	ctx := context.Background()

	// Start PostgreSQL container
	postgres, pgConnString, err := startPostgresContainer(ctx)
	require.NoError(b, err)
	defer postgres.Terminate(ctx)

	// Start Redis container
	redis, redisAddr, err := startRedisContainer(ctx)
	require.NoError(b, err)
	defer redis.Terminate(ctx)

	// Create SessionManager
	cfg := DefaultConfig()
	cfg.CreateSchemaIfMissing = true
	cfg.CacheSize = 10000

	sm, err := NewSessionManager(cfg, pgConnString)
	require.NoError(b, err)
	defer sm.Shutdown(context.Background())

	// Create RedisSessionManager
	rsm := NewRedisSessionManager(redisAddr)

	// Create test sessions
	numSessions := 1000
	pgSessionIDs := make([]uuid.UUID, numSessions)
	redisSessionIDs := make([]string, numSessions)

	for i := 0; i < numSessions; i++ {
		userID := uuid.New()
		attributes := map[string]SessionAttributeValue{
			"key": {Value: fmt.Sprintf("value%d", i)},
		}
		session, err := sm.CreateSession(context.Background(), userID, attributes)
		require.NoError(b, err)
		pgSessionIDs[i] = session.ID

		redisAttributes := map[string]string{
			"key": fmt.Sprintf("value%d", i),
		}
		redisSessionID, err := rsm.CreateSession(context.Background(), userID, redisAttributes)
		require.NoError(b, err)
		redisSessionIDs[i] = redisSessionID
	}

	b.Run("GetSession", func(b *testing.B) {
		b.Run("PostgreSQL", func(b *testing.B) {
			benchmarkGetSession(b, sm, pgSessionIDs)
		})
		b.Run("Redis", func(b *testing.B) {
			benchmarkGetSessionRedis(b, rsm, redisSessionIDs)
		})
	})

	b.Run("UpdateSession", func(b *testing.B) {
		b.Run("PostgreSQL", func(b *testing.B) {
			benchmarkUpdateSession(b, sm, pgSessionIDs)
		})
		b.Run("Redis", func(b *testing.B) {
			benchmarkUpdateSessionRedis(b, rsm, redisSessionIDs)
		})
	})
}

func benchmarkGetSession(b *testing.B, sm *SessionManager, sessionIDs []uuid.UUID) {
	var wg sync.WaitGroup
	wg.Add(b.N)

	start := time.Now()

	for i := 0; i < b.N; i++ {
		go func() {
			defer wg.Done()
			sessionID := sessionIDs[rand.Intn(len(sessionIDs))]
			_, err := sm.GetSession(context.Background(), sessionID)
			if err != nil {
				b.Error(err)
			}
		}()
	}

	wg.Wait()

	elapsed := time.Since(start)
	opsPerSecond := float64(b.N) / elapsed.Seconds()
	nsPerOp := float64(elapsed.Nanoseconds()) / float64(b.N)

	b.ReportMetric(opsPerSecond, "ops/sec")
	b.ReportMetric(nsPerOp, "ns/op")

	fmt.Printf("PostgreSQL GetSession Benchmark:\n")
	fmt.Printf("  Iterations: %d\n", b.N)
	fmt.Printf("  Total time: %v\n", elapsed)
	fmt.Printf("  Ops/sec: %.2f\n", opsPerSecond)
	fmt.Printf("  Ns/op: %.2f\n", nsPerOp)
}

func benchmarkGetSessionRedis(b *testing.B, rsm *RedisSessionManager, sessionIDs []string) {
	var wg sync.WaitGroup
	wg.Add(b.N)

	start := time.Now()

	for i := 0; i < b.N; i++ {
		go func() {
			defer wg.Done()
			sessionID := sessionIDs[rand.Intn(len(sessionIDs))]
			_, err := rsm.GetSession(context.Background(), sessionID)
			if err != nil {
				b.Error(err)
			}
		}()
	}

	wg.Wait()

	elapsed := time.Since(start)
	opsPerSecond := float64(b.N) / elapsed.Seconds()
	nsPerOp := float64(elapsed.Nanoseconds()) / float64(b.N)

	b.ReportMetric(opsPerSecond, "ops/sec")
	b.ReportMetric(nsPerOp, "ns/op")

	fmt.Printf("Redis GetSession Benchmark:\n")
	fmt.Printf("  Iterations: %d\n", b.N)
	fmt.Printf("  Total time: %v\n", elapsed)
	fmt.Printf("  Ops/sec: %.2f\n", opsPerSecond)
	fmt.Printf("  Ns/op: %.2f\n", nsPerOp)
}

func benchmarkUpdateSession(b *testing.B, sm *SessionManager, sessionIDs []uuid.UUID) {
	var wg sync.WaitGroup
	wg.Add(b.N)

	start := time.Now()

	for i := 0; i < b.N; i++ {
		go func(i int) {
			defer wg.Done()
			sessionID := sessionIDs[rand.Intn(len(sessionIDs))]
			session, err := sm.GetSession(context.Background(), sessionID)
			if err != nil {
				b.Error(err)
				return
			}

			err = session.UpdateAttribute("key", fmt.Sprintf("updatedvalue%d", i), nil)
			if err != nil {
				b.Error(err)
				return
			}

			_, err = sm.UpdateSession(context.Background(), session)
			if err != nil {
				b.Error(err)
				return
			}
		}(i)
	}

	wg.Wait()

	elapsed := time.Since(start)
	opsPerSecond := float64(b.N) / elapsed.Seconds()
	nsPerOp := float64(elapsed.Nanoseconds()) / float64(b.N)

	b.ReportMetric(opsPerSecond, "ops/sec")
	b.ReportMetric(nsPerOp, "ns/op")

	fmt.Printf("PostgreSQL UpdateSession Benchmark:\n")
	fmt.Printf("  Iterations: %d\n", b.N)
	fmt.Printf("  Total time: %v\n", elapsed)
	fmt.Printf("  Ops/sec: %.2f\n", opsPerSecond)
	fmt.Printf("  Ns/op: %.2f\n", nsPerOp)
}

func benchmarkUpdateSessionRedis(b *testing.B, rsm *RedisSessionManager, sessionIDs []string) {
	var wg sync.WaitGroup
	wg.Add(b.N)

	start := time.Now()

	for i := 0; i < b.N; i++ {
		go func(i int) {
			defer wg.Done()
			sessionID := sessionIDs[rand.Intn(len(sessionIDs))]
			attributes, err := rsm.GetSession(context.Background(), sessionID)
			if err != nil {
				b.Error(err)
				return
			}

			attributes["key"] = fmt.Sprintf("updatedvalue%d", i)

			err = rsm.UpdateSession(context.Background(), sessionID, attributes)
			if err != nil {
				b.Error(err)
				return
			}
		}(i)
	}

	wg.Wait()

	elapsed := time.Since(start)
	opsPerSecond := float64(b.N) / elapsed.Seconds()
	nsPerOp := float64(elapsed.Nanoseconds()) / float64(b.N)

	b.ReportMetric(opsPerSecond, "ops/sec")
	b.ReportMetric(nsPerOp, "ns/op")

	fmt.Printf("Redis UpdateSession Benchmark:\n")
	fmt.Printf("  Iterations: %d\n", b.N)
	fmt.Printf("  Total time: %v\n", elapsed)
	fmt.Printf("  Ops/sec: %.2f\n", opsPerSecond)
	fmt.Printf("  Ns/op: %.2f\n", nsPerOp)
}
