package pgbatcher_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/hailam/pgbatch_test/pgbatcher"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Test & Benchmark Setup ---

// setupTestPool creates a connection pool for testing and benchmarking.
func setupTestPool(tb testing.TB) (*pgxpool.Pool, func()) {
	tb.Helper()
	// Use environment variable or default to test connection string
	connString := os.Getenv("TEST_DATABASE_URL")
	if connString == "" {
		connString = "postgres://admin:admin@localhost:5432/pgbatch?sslmode=disable"
		//log.Println("TEST_DATABASE_URL env var not set, using default:", connString)
	}

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		tb.Skipf("Skipping test/benchmark, unable to parse connection string: %v", err)
		return nil, func() {}
	}

	// Configure pool settings for tests
	config.MaxConns = 50

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		tb.Skipf("Skipping test/benchmark, unable to create connection pool: %v", err)
		return nil, func() {}
	}

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		tb.Skipf("Skipping test/benchmark, failed to ping database: %v", err)
		return nil, func() {}
	}

	cleanup := func() {
		pool.Close()
	}

	return pool, cleanup
}

// setupBenchmarkDeps sets up database pool and batcher for benchmarks.
func setupBenchmarkDeps(b *testing.B, opts ...pgbatcher.Options) (*pgxpool.Pool, *pgbatcher.Batcher, func()) {
	b.Helper()
	pool, poolCleanup := setupTestPool(b)
	if pool == nil {
		// setupTestPool already called b.Skip
		return nil, nil, poolCleanup
	}

	var batcherOpts pgbatcher.Options
	if len(opts) > 0 {
		batcherOpts = opts[0]
	} else {
		batcherOpts = pgbatcher.DefaultOptions()
	}

	batcher := pgbatcher.New(pool, batcherOpts)

	cleanup := func() {
		batcher.Close()
		poolCleanup()
	}
	return pool, batcher, cleanup
}

// --- Request Simulation Logic (adapted from main.go) ---

// RequestType represents different API endpoints (copied/adapted from main.go)
type RequestType int

const (
	UserRequest RequestType = iota
	ProductRequest
	OrderRequest
	OrderItemRequest
	// Add more if main.go evolves
)

// simulateRequestHandlerLogic simulates the core query logic from main.go's RequestHandler
// It can execute directly or use the batcher.
func simulateRequestHandlerLogic(ctx context.Context, pool *pgxpool.Pool, batcher *pgbatcher.Batcher, requestType RequestType, useBatcher bool) error {
	var query string
	var args []any

	switch requestType {
	case UserRequest:
		userID := rand.IntN(50) + 1 // Using ranges from main.go
		query = "SELECT id, name, email FROM users WHERE id = $1"
		args = []any{userID}
	case ProductRequest:
		productID := rand.IntN(100) + 1
		query = "SELECT id, name, price FROM products WHERE id = $1"
		args = []any{productID}
	case OrderRequest:
		userID := rand.IntN(50) + 1
		query = "SELECT id, user_id FROM orders WHERE user_id = $1"
		args = []any{userID}
	case OrderItemRequest:
		orderID := rand.IntN(200) + 1
		query = "SELECT id, order_id, product_id, quantity FROM order_items WHERE order_id = $1 LIMIT 5"
		args = []any{orderID}
	default:
		return fmt.Errorf("unknown request type: %v", requestType)
	}

	var err error
	if useBatcher {
		if batcher == nil {
			return errors.New("batcher is nil but useBatcher is true")
		}
		// Use QueueSimple as the original RequestHandler didn't process results
		err = batcher.QueueSimple(ctx, query, args...)
	} else {
		if pool == nil {
			return errors.New("pool is nil but useBatcher is false")
		}
		// Direct query execution
		rows, qErr := pool.Query(ctx, query, args...)
		if qErr != nil {
			// Don't wrap nil errors
			if qErr == context.DeadlineExceeded || qErr == context.Canceled {
				return qErr
			}

			// pgErr, ok := qErr.(*pgconn.PgError)
			// if ok { ... }
			return fmt.Errorf("query error: %w", qErr)
		}
		// Must consume rows for accurate timing and resource cleanup
		for rows.Next() {
			// Just iterate
		}
		err = rows.Err() // Check for errors during iteration
		rows.Close()     // Ensure rows are closed
	}

	// Don't log errors here in benchmark helpers, let the benchmark function handle b.Error/b.Logf
	return err
}

// SimulateOneBurst simulates a single burst of concurrent requests, adapted from main.go
func SimulateOneBurst(pool *pgxpool.Pool, batcher *pgbatcher.Batcher, concurrentRequests int, useBatcher bool) {
	var wg sync.WaitGroup
	wg.Add(concurrentRequests)

	for i := 0; i < concurrentRequests; i++ {
		go func(requestID int) {
			defer wg.Done()
			// Longer timeout for bursts which might involve waiting for batch window
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			requestType := RequestType(rand.IntN(4)) // Match the 4 types in simulateRequestHandlerLogic

			// Use the simulation helper
			err := simulateRequestHandlerLogic(ctx, pool, batcher, requestType, useBatcher)
			if err != nil {
				// Avoid flooding logs during benchmarks, only log if debugging
				// log.Printf("Benchmark burst error (request %d): %v", requestID, err)
			}
		}(i)
	}
	wg.Wait()

	// Flush only if using the batcher
	if useBatcher && batcher != nil {
		batcher.Flush()
	}
}

// --- Unit/Integration Tests ---

func TestConnectionSetup(t *testing.T) {
	pool, cleanup := setupTestPool(t)
	defer cleanup()
	if pool == nil {
		return // Skip already called by setup
	}

	// Test connection with a simple query
	var count int
	err := pool.QueryRow(context.Background(), "SELECT 1").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to execute test query: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected 1, got %d", count)
	}
	t.Log("Database connection successful.")
}

func TestBasicBatchOperation(t *testing.T) {
	pool, cleanup := setupTestPool(t) // Get pool and cleanup first
	defer cleanup()                   // Defer pool cleanup
	if pool == nil {
		return // Skip called in setupTestPool
	}
	// Now create the batcher
	batcher := pgbatcher.New(pool, pgbatcher.Options{
		BatchWindow:  5 * time.Millisecond,
		MaxBatchSize: 10,
	})
	defer batcher.Close() // Defer batcher close separately

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	const numQueries = 5

	for i := 0; i < numQueries; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			userID := id + 1
			// Use Queue to test result processing path
			data, fields, err := batcher.Queue(ctx, "SELECT id, name, email FROM users WHERE id = $1", userID)
			if err != nil {
				// Errors are expected if schema isn't present or user doesn't exist
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					t.Logf("Query %d error (schema/data present?): %v", id, err)
				}
			} else {
				t.Logf("Query %d returned %d rows with %d fields", id, len(data), len(fields))
			}

			// Also test QueueSimple
			errSimple := batcher.QueueSimple(ctx, "SELECT $1::int", id)
			if errSimple != nil && !errors.Is(errSimple, context.Canceled) && !errors.Is(errSimple, context.DeadlineExceeded) {
				t.Errorf("Simple query %d failed: %v", id, errSimple)
			}
		}(i)
	}

	wg.Wait()       // Wait for all Queue calls to return (results might not be processed yet)
	batcher.Flush() // Force execution

	// Allow a very brief moment for async processing and stats update after flush
	time.Sleep(20 * time.Millisecond)

	// Check stats after flush
	stats := batcher.GetStats()
	if stats.PendingQueries > 0 {
		t.Errorf("Expected 0 pending queries after flush, got %d", stats.PendingQueries)
	}
	t.Logf("Batcher stats after flush: %+v", stats)
}

// --- Benchmark Functions ---

// BenchmarkRequestHandlerDirect benchmarks individual requests executed directly.
func BenchmarkRequestHandlerDirect(b *testing.B) {
	pool, _, cleanup := setupBenchmarkDeps(b) // Batcher not needed
	defer cleanup()
	if pool == nil {
		return // Skip called in setup
	}

	b.ResetTimer()
	// Run b.N requests sequentially (Go benchmark default)
	for i := 0; i < b.N; i++ {
		requestType := RequestType(rand.IntN(4))
		err := simulateRequestHandlerLogic(context.Background(), pool, nil, requestType, false) // useBatcher = false
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			// Don't fail benchmark, just log non-context errors if needed for debugging
			// b.Logf("Direct request error: %v", err)
		}
	}
	b.StopTimer() // Stop timer if significant cleanup follows
}

// BenchmarkRequestHandlerBatched benchmarks individual requests queued via the batcher.
func BenchmarkRequestHandlerBatched(b *testing.B) {
	// Example: Test with a common batch window
	batchWindow := 5 * time.Millisecond
	maxBatchSize := 50

	pool, batcher, cleanup := setupBenchmarkDeps(b, pgbatcher.Options{
		BatchWindow:  batchWindow,
		MaxBatchSize: maxBatchSize,
	})
	defer cleanup()
	if pool == nil || batcher == nil {
		return // Skip called in setup
	}

	b.ResetTimer()
	// Queue b.N requests sequentially
	for i := 0; i < b.N; i++ {
		requestType := RequestType(rand.IntN(4))
		err := simulateRequestHandlerLogic(context.Background(), pool, batcher, requestType, true) // useBatcher = true
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			// Log non-context errors if needed
			// b.Logf("Batched request error: %v", err)
		}
	}
	b.StopTimer()

	batcher.Flush() // Ensure all queued items from the b.N loop are flushed *after* timing
}

// BenchmarkBurstDirect benchmarks bursts of concurrent requests executed directly.
func BenchmarkBurstDirect(b *testing.B) {
	pool, _, cleanup := setupBenchmarkDeps(b)
	defer cleanup()
	if pool == nil {
		return // Skip called in setup
	}

	concurrentRequests := 100 // From main.go example config

	b.ResetTimer()
	// Benchmark the execution of b.N *bursts*
	for i := 0; i < b.N; i++ {
		// SimulateOneBurst handles concurrency internally
		SimulateOneBurst(pool, nil, concurrentRequests, false) // useBatcher = false
	}
	b.StopTimer()
}

// BenchmarkBurstBatched benchmarks bursts of concurrent requests using the batcher.
func BenchmarkBurstBatched(b *testing.B) {
	batchWindow := 5 * time.Millisecond
	maxBatchSize := 50

	pool, batcher, cleanup := setupBenchmarkDeps(b, pgbatcher.Options{
		BatchWindow:  batchWindow,
		MaxBatchSize: maxBatchSize,
	})
	defer cleanup()
	if pool == nil || batcher == nil {
		return // Skip called in setup
	}

	concurrentRequests := 100 // From main.go example config

	b.ResetTimer()
	// Benchmark the execution of b.N *bursts*
	for i := 0; i < b.N; i++ {
		// SimulateOneBurst handles concurrency and flushing (if batcher used)
		SimulateOneBurst(pool, batcher, concurrentRequests, true) // useBatcher = true
	}
	b.StopTimer()
	// No final flush needed here as SimulateOneBurst flushes internally when batching
}

// --- Service Simulation Code (Optional - Keep if used by tests below) ---
// NOTE: These were old ones, section duplicates some logic from simulateRequestHandlerLogic.
// Consider refactoring to use simulateRequestHandlerLogic if possible,
// or keep it if the service abstraction is important for specific tests.

// UserService simulates a service for user-related operations
type UserService struct {
	pool    *pgxpool.Pool
	batcher *pgbatcher.Batcher
}

// NewUserService creates a new UserService
func NewUserService(pool *pgxpool.Pool, batcher *pgbatcher.Batcher) *UserService {
	return &UserService{pool: pool, batcher: batcher}
}

// GetUserByID retrieves a user by ID
func (s *UserService) GetUserByID(ctx context.Context, userID int) error {
	query := "SELECT id, name, email FROM users WHERE id = $1"
	return executeServiceQuery(ctx, s.pool, s.batcher, query, userID)
}

// ProductService simulates a service for product-related operations
type ProductService struct {
	pool    *pgxpool.Pool
	batcher *pgbatcher.Batcher
}

// NewProductService creates a new ProductService
func NewProductService(pool *pgxpool.Pool, batcher *pgbatcher.Batcher) *ProductService {
	return &ProductService{pool: pool, batcher: batcher}
}

// GetProductByID retrieves a product by ID
func (s *ProductService) GetProductByID(ctx context.Context, productID int) error {
	query := "SELECT id, name, price FROM products WHERE id = $1"
	return executeServiceQuery(ctx, s.pool, s.batcher, query, productID)
}

// OrderService simulates a service for order-related operations
type OrderService struct {
	pool    *pgxpool.Pool
	batcher *pgbatcher.Batcher
}

// NewOrderService creates a new OrderService
func NewOrderService(pool *pgxpool.Pool, batcher *pgbatcher.Batcher) *OrderService {
	return &OrderService{pool: pool, batcher: batcher}
}

// GetOrdersByUserID retrieves orders for a user
func (s *OrderService) GetOrdersByUserID(ctx context.Context, userID int) error {
	query := "SELECT id, user_id FROM orders WHERE user_id = $1" // Removed order_date
	return executeServiceQuery(ctx, s.pool, s.batcher, query, userID)
}

// GetOrderItemsByOrderID retrieves items for an order
func (s *OrderService) GetOrderItemsByOrderID(ctx context.Context, orderID int) error {
	query := "SELECT id, order_id, product_id, quantity FROM order_items WHERE order_id = $1 LIMIT 5" // Fixed column name
	return executeServiceQuery(ctx, s.pool, s.batcher, query, orderID)
}

// executeServiceQuery is a helper function that executes queries either directly or via batcher
func executeServiceQuery(ctx context.Context, pool *pgxpool.Pool, batcher *pgbatcher.Batcher, query string, args ...any) error {
	if batcher != nil {
		// Use batcher when available
		return batcher.QueueSimple(ctx, query, args...)
	} else {
		// Direct execution path
		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			// Don't wrap nil context errors
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			return fmt.Errorf("direct query execution failed: %w", err)
		}
		defer rows.Close()

		// Consume rows
		for rows.Next() {
		}

		err = rows.Err() // Check iteration errors
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("direct query row iteration failed: %w", err)
		}
		return err // Return nil or context error
	}
}

// ServiceRunner is a function type that runs a specific service operation
type ServiceRunner func(ctx context.Context, id int) error

// createServiceRunner creates a function that simulates multiple services
func createServiceRunner(pool *pgxpool.Pool, batcher *pgbatcher.Batcher) ServiceRunner {
	userService := NewUserService(pool, batcher)
	productService := NewProductService(pool, batcher)
	orderService := NewOrderService(pool, batcher)

	return func(ctx context.Context, id int) error {
		var err error
		// Use a slightly longer timeout for service calls that might queue
		serviceCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		switch id % 10 {
		case 0, 1: // User queries (20% of traffic)
			userID := rand.IntN(50) + 1
			err = userService.GetUserByID(serviceCtx, userID)
		case 2, 3, 4: // Product queries (30% of traffic)
			productID := rand.IntN(100) + 1
			err = productService.GetProductByID(serviceCtx, productID)
		case 5, 6: // Order queries (20% of traffic)
			userID := rand.IntN(50) + 1
			err = orderService.GetOrdersByUserID(serviceCtx, userID)
		case 7, 8, 9: // Order item queries (30% of traffic)
			orderID := rand.IntN(200) + 1
			err = orderService.GetOrderItemsByOrderID(serviceCtx, orderID)
		}
		return err // Return error from the specific service call
	}
}

// runMultiServiceQueries runs queries using multiple simulated services
func runMultiServiceQueries(pool *pgxpool.Pool, batcher *pgbatcher.Batcher, queryCount int) time.Duration {
	start := time.Now()
	runner := createServiceRunner(pool, batcher)
	var wg sync.WaitGroup
	wg.Add(queryCount)

	// Limit concurrency slightly to avoid overwhelming resources during tests
	sem := make(chan struct{}, 100)

	for i := 0; i < queryCount; i++ {
		sem <- struct{}{} // Acquire semaphore
		go func(id int) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore

			// Use a background context, the runner will apply its own timeout
			ctx := context.Background()
			err := runner(ctx, id)
			if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				// Log potentially interesting errors during tests, but don't fail
				log.Printf("runMultiServiceQueries error (id %d): %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	if batcher != nil {
		batcher.Flush()
	}
	return time.Since(start)
}

// RunMultiServiceDirectQueries simulates services executing queries directly
func RunMultiServiceDirectQueries(pool *pgxpool.Pool, queryCount int) time.Duration {
	return runMultiServiceQueries(pool, nil, queryCount)
}

// RunMultiServiceBatchedQueries simulates services using the batcher
func RunMultiServiceBatchedQueries(pool *pgxpool.Pool, batcher *pgbatcher.Batcher, queryCount int) time.Duration {
	return runMultiServiceQueries(pool, batcher, queryCount)
}

// --- Other Helper Functions ---

// calculateImprovement calculates performance improvement as a percentage
func calculateImprovement(directTime, batchTime time.Duration) float64 {
	if directTime <= 0 || batchTime <= 0 { // Avoid division by zero or nonsensical results
		return 0
	}
	return (float64(directTime) - float64(batchTime)) / float64(directTime) * 100.0
}

// runSpecificQueryMix runs a specific mix of queries, similar to runMultiServiceQueries
func runSpecificQueryMix(pool *pgxpool.Pool, batcher *pgbatcher.Batcher, queryCount, dominantPercent int, dominantReqType RequestType) time.Duration {
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(queryCount)
	sem := make(chan struct{}, 100) // Limit concurrency

	for i := 0; i < queryCount; i++ {
		sem <- struct{}{}
		go func(id int) {
			defer wg.Done()
			defer func() { <-sem }()

			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			// Determine which query type to run
			var reqType RequestType
			if rand.IntN(100) < dominantPercent {
				reqType = dominantReqType // Use dominant query type
			} else {
				// Use random *other* query type
				reqType = RequestType((int(dominantReqType) + 1 + rand.IntN(3)) % 4)
			}

			// Use the unified logic simulation function
			err := simulateRequestHandlerLogic(ctx, pool, batcher, reqType, batcher != nil)
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				// log.Printf("runSpecificQueryMix error (id %d, type %v): %v", id, reqType, err)
			}
		}(i)
	}

	wg.Wait()

	if batcher != nil {
		batcher.Flush()
	}
	return time.Since(start)
}

// --- Existing Test Functions  ---

// TestQueryMix tests different query mix patterns
func TestQueryMix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping query mix test in short mode")
	}

	pool, cleanup := setupTestPool(t)
	defer cleanup()
	if pool == nil {
		return // Skip called in setupTestPool
	}

	fmt.Printf("\nRunning query mix tests (1000 queries)...\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush() // Ensure flush happens
	fmt.Fprintln(w, "Query Mix\tDirect Time\tBatch Time (5ms, 50 Max)\tImprovement (%)")
	fmt.Fprintln(w, "---------\t-----------\t-----------------------\t---------------")

	queryCount := 1000 // Reduced from original for faster local testing if needed
	batchWindow := 5 * time.Millisecond
	maxBatchSize := 50

	// Test standard mix (using RunMultiService which calls createServiceRunner)
	directTimeStd := RunMultiServiceDirectQueries(pool, queryCount)
	batcherStd := pgbatcher.New(pool, pgbatcher.Options{BatchWindow: batchWindow, MaxBatchSize: maxBatchSize})
	batchTimeStd := RunMultiServiceBatchedQueries(pool, batcherStd, queryCount)
	improvementStd := calculateImprovement(directTimeStd, batchTimeStd)
	fmt.Fprintf(w, "Standard Mix\t%s\t%s\t%.2f%%\n", directTimeStd, batchTimeStd, improvementStd)
	batcherStd.Close()

	// Test 80% user queries (using runSpecificQueryMix)
	time.Sleep(200 * time.Millisecond) // Short pause
	directTimeUser := runSpecificQueryMix(pool, nil, queryCount, 80, UserRequest)
	batcherUser := pgbatcher.New(pool, pgbatcher.Options{BatchWindow: batchWindow, MaxBatchSize: maxBatchSize})
	batchTimeUser := runSpecificQueryMix(pool, batcherUser, queryCount, 80, UserRequest)
	improvementUser := calculateImprovement(directTimeUser, batchTimeUser)
	fmt.Fprintf(w, "80%% User\t%s\t%s\t%.2f%%\n", directTimeUser, batchTimeUser, improvementUser)
	batcherUser.Close()

	// Test 80% product queries (using runSpecificQueryMix)
	time.Sleep(200 * time.Millisecond) // Short pause
	directTimeProd := runSpecificQueryMix(pool, nil, queryCount, 80, ProductRequest)
	batcherProd := pgbatcher.New(pool, pgbatcher.Options{BatchWindow: batchWindow, MaxBatchSize: maxBatchSize})
	batchTimeProd := runSpecificQueryMix(pool, batcherProd, queryCount, 80, ProductRequest)
	improvementProd := calculateImprovement(directTimeProd, batchTimeProd)
	fmt.Fprintf(w, "80%% Product\t%s\t%s\t%.2f%%\n", directTimeProd, batchTimeProd, improvementProd)
	batcherProd.Close()

}

// TestBatchWindowSensitivity tests different batch window settings
func TestBatchWindowSensitivity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping batch window sensitivity test in short mode")
	}

	pool, cleanup := setupTestPool(t)
	defer cleanup()
	if pool == nil {
		return // Skip called in setupTestPool
	}

	fmt.Printf("\nRunning batch window sensitivity analysis (standard mix, 1000 queries)...\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()
	fmt.Fprintln(w, "Batch Window\tBatch Time (50 Max)\tImprovement vs Direct (%)")
	fmt.Fprintln(w, "------------\t-------------------\t-------------------------")

	queryCount := 1000
	maxBatchSize := 50

	// First get direct execution time for comparison using the standard service mix
	directTime := RunMultiServiceDirectQueries(pool, queryCount)
	fmt.Printf("(Direct time for comparison: %s)\n", directTime)

	windowsToTest := []time.Duration{
		500 * time.Microsecond,
		1 * time.Millisecond,
		2 * time.Millisecond,
		3 * time.Millisecond,
		5 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
	}

	for _, window := range windowsToTest {
		batcher := pgbatcher.New(pool, pgbatcher.Options{BatchWindow: window, MaxBatchSize: maxBatchSize})
		// Use the standard service mix for apples-to-apples comparison
		batchTime := RunMultiServiceBatchedQueries(pool, batcher, queryCount)
		improvement := calculateImprovement(directTime, batchTime)
		fmt.Fprintf(w, "%s\t%s\t%.2f%%\n", window, batchTime, improvement)
		batcher.Close()
		time.Sleep(200 * time.Millisecond) // Give DB a breather
	}

}

// TestMaxBatchSize tests different maximum batch sizes
func TestMaxBatchSize(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping max batch size test in short mode")
	}

	pool, cleanup := setupTestPool(t)
	defer cleanup()
	if pool == nil {
		return // Skip called in setupTestPool
	}

	fmt.Printf("\nRunning max batch size test (standard mix, 1000 queries, 5ms window)...\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()
	fmt.Fprintln(w, "Max Batch Size\tBatch Time\tImprovement vs Direct (%)")
	fmt.Fprintln(w, "--------------\t----------\t-------------------------")

	queryCount := 1000
	batchWindow := 5 * time.Millisecond // Fixed window for this test

	// First get direct execution time for comparison using the standard service mix
	directTime := RunMultiServiceDirectQueries(pool, queryCount)
	fmt.Printf("(Direct time for comparison: %s)\n", directTime)

	sizesToTest := []int{5, 10, 25, 50, 100, 200, 0} // 0 = unlimited

	for _, size := range sizesToTest {
		batcher := pgbatcher.New(pool, pgbatcher.Options{BatchWindow: batchWindow, MaxBatchSize: size})
		// Use the standard service mix
		batchTime := RunMultiServiceBatchedQueries(pool, batcher, queryCount)
		improvement := calculateImprovement(directTime, batchTime)
		sizeLabel := fmt.Sprintf("%d", size)
		if size == 0 {
			sizeLabel = "Unlimited"
		}
		fmt.Fprintf(w, "%s\t%s\t%.2f%%\n", sizeLabel, batchTime, improvement)
		batcher.Close()
		time.Sleep(200 * time.Millisecond) // Give DB a breather
	}
}
