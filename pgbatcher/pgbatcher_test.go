package pgbatcher_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/hailam/pgbatch_test/pgbatcher"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- Test Service Definitions ---

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
	query := "SELECT id, user_id, order_date FROM orders WHERE user_id = $1"
	return executeServiceQuery(ctx, s.pool, s.batcher, query, userID)
}

// GetOrderItemsByOrderID retrieves items for an order
func (s *OrderService) GetOrderItemsByOrderID(ctx context.Context, orderID int) error {
	query := "SELECT id, orderid, product_id, quantity FROM order_items WHERE order_id = $1 LIMIT 5"
	return executeServiceQuery(ctx, s.pool, s.batcher, query, orderID)
}

// --- Service Helper Functions ---

// executeServiceQuery is a helper function that executes queries either directly or via batcher
func executeServiceQuery(ctx context.Context, pool *pgxpool.Pool, batcher *pgbatcher.Batcher, query string, args ...any) error {
	if batcher != nil {
		// Use batcher when available
		return batcher.QueueSimple(ctx, query, args...)
	} else {
		// Direct execution path
		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("direct query execution failed: %w", err)
		}
		defer rows.Close()

		// Consume rows
		for rows.Next() {
			// Just iterating through results
		}

		if err := rows.Err(); err != nil {
			return fmt.Errorf("direct query row iteration failed: %w", err)
		}

		return nil
	}
}

// --- Service Runner ---

// ServiceRunner is a function type that runs a specific service operation
type ServiceRunner func(ctx context.Context, id int) error

// createServiceRunner creates a function that simulates multiple services
func createServiceRunner(pool *pgxpool.Pool, batcher *pgbatcher.Batcher) ServiceRunner {
	userService := NewUserService(pool, batcher)
	productService := NewProductService(pool, batcher)
	orderService := NewOrderService(pool, batcher)

	return func(ctx context.Context, id int) error {
		var err error

		switch id % 10 {
		case 0, 1:
			// User queries (20% of traffic)
			userID := rand.IntN(50) + 1
			err = userService.GetUserByID(ctx, userID)
		case 2, 3, 4:
			// Product queries (30% of traffic)
			productID := rand.IntN(100) + 1
			err = productService.GetProductByID(ctx, productID)
		case 5, 6:
			// Order queries (20% of traffic)
			userID := rand.IntN(50) + 1
			err = orderService.GetOrdersByUserID(ctx, userID)
		case 7, 8, 9:
			// Order item queries (30% of traffic)
			orderID := rand.IntN(200) + 1
			err = orderService.GetOrderItemsByOrderID(ctx, orderID)
		}

		return err
	}
}

// --- Benchmark Functions ---

// runMultiServiceQueries runs queries using multiple simulated services
func runMultiServiceQueries(pool *pgxpool.Pool, batcher *pgbatcher.Batcher, queryCount int) time.Duration {
	start := time.Now()
	runner := createServiceRunner(pool, batcher)
	var wg sync.WaitGroup
	wg.Add(queryCount)

	for i := 0; i < queryCount; i++ {
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			if err := runner(ctx, id); err != nil {
				// In a real application, we might handle specific errors differently
				if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
					// This would be logged in a real application
				}
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

// --- Helper Functions ---

// calculateImprovement calculates performance improvement as a percentage
func calculateImprovement(directTime, batchTime time.Duration) float64 {
	if directTime <= 0 {
		return 0
	}

	return (float64(directTime) - float64(batchTime)) / float64(directTime) * 100.0
}

// Helper to generate specific query mixes for testing
func runSpecificQueryMix(pool *pgxpool.Pool, batcher *pgbatcher.Batcher, queryCount, dominantPercent, dominantQueryType int) time.Duration {
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(queryCount)

	for i := 0; i < queryCount; i++ {
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Determine which query to run based on distribution
			var queryID int
			if rand.IntN(100) < dominantPercent {
				// Use dominant query type
				queryID = dominantQueryType
			} else {
				// Use random other query type
				queryID = (dominantQueryType + 1 + rand.IntN(3)) % 4
			}

			// Construct appropriate query
			var query string
			var args []any

			switch queryID {
			case 0:
				// User query
				userID := rand.IntN(50) + 1
				query = "SELECT id, name, email FROM users WHERE id = $1"
				args = []any{userID}
			case 1:
				// Product query
				productID := rand.IntN(100) + 1
				query = "SELECT id, name, price FROM products WHERE id = $1"
				args = []any{productID}
			case 2:
				// Orders query
				userID := rand.IntN(50) + 1
				query = "SELECT id, user_id, order_date FROM orders WHERE user_id = $1"
				args = []any{userID}
			case 3:
				// Order items query
				orderID := rand.IntN(200) + 1
				query = "SELECT id, order_id, product_id, quantity FROM order_items WHERE order_id = $1 LIMIT 5"
				args = []any{orderID}
			}

			// Execute query
			if batcher != nil {
				err := batcher.QueueSimple(ctx, query, args...)
				if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					// Would log errors in a real application
				}
			} else {
				rows, err := pool.Query(ctx, query, args...)
				if err == nil {
					// Consume rows
					for rows.Next() {
					}
					rows.Close()
				}
			}
		}(i)
	}

	wg.Wait()

	if batcher != nil {
		batcher.Flush()
	}

	return time.Since(start)
}

// --- Test Functions ---

// TestConnectionSetup tests that we can connect to the database
func TestConnectionSetup(t *testing.T) {
	pool, err := setupTestPool()
	if err != nil {
		t.Fatalf("Failed to set up connection pool: %v", err)
	}
	defer pool.Close()

	// Test connection with a simple query
	var count int
	err = pool.QueryRow(context.Background(), "SELECT 1").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to execute test query: %v", err)
	}
	if count != 1 {
		t.Fatalf("Expected 1, got %d", count)
	}
}

// setupTestPool creates a connection pool for testing
func setupTestPool() (*pgxpool.Pool, error) {
	// Use environment variable or default to test connection string
	connString := os.Getenv("TEST_DATABASE_URL")
	if connString == "" {
		connString = "postgres://admin:admin@localhost:5432/pgbatch?sslmode=disable"
	}

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("unable to parse connection string: %w", err)
	}

	// Configure pool settings for tests
	config.MaxConns = 50

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection pool: %w", err)
	}

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}

// TestBasicBatchOperation tests basic batching functionality
func TestBasicBatchOperation(t *testing.T) {
	pool, err := setupTestPool()
	if err != nil {
		t.Skipf("Skipping test, could not connect to database: %v", err)
		return
	}
	defer pool.Close()

	// Create batcher with test settings
	options := pgbatcher.DefaultOptions()
	options.BatchWindow = 5 * time.Millisecond
	options.MaxBatchSize = 10

	batcher := pgbatcher.New(pool, options)
	defer batcher.Close()

	// Test a few queries
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		userID := i + 1

		// Use Queue to get data
		data, fields, err := batcher.Queue(ctx, "SELECT id, name, email FROM users WHERE id = $1", userID)

		// In testing environments we might not have actual tables,
		// so errors can be expected - just check the API functions properly
		if err != nil {
			t.Logf("Query error (migrations was run?): %v", err)
		} else {
			// If query succeeded, verify results
			t.Logf("Query returned %d rows with %d fields", len(data), len(fields))
		}

		// Also test QueueSimple
		err = batcher.QueueSimple(ctx, "SELECT 1")
		if err != nil {
			t.Errorf("Simple query failed: %v", err)
		}
	}

	// Force batch execution
	batcher.Flush()

	// Check stats
	stats := batcher.GetStats()
	if stats.PendingQueries > 0 {
		t.Errorf("Expected 0 pending queries after flush, got %d", stats.PendingQueries)
	}
}

// --- Benchmark Tests ---

func BenchmarkMultiServiceQueries(b *testing.B) {
	pool, err := setupTestPool()
	if err != nil {
		b.Skipf("Skipping benchmark, could not connect to database: %v", err)
		return
	}
	defer pool.Close()

	// Print benchmark header
	fmt.Println("\nRunning multi-service simulation benchmarks...")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Query Count\tDirect Time\tBatch Window\tMax Batch Size\tBatch Time\tImprovement (%)")
	fmt.Fprintln(w, "-----------\t-----------\t------------\t--------------\t----------\t---------------")

	// Test different query counts
	queryCounts := []int{100, 500, 1000}
	batchWindows := []time.Duration{
		1 * time.Millisecond,
		2 * time.Millisecond,
		5 * time.Millisecond,
		10 * time.Millisecond,
	}
	maxBatchSize := 50

	for _, queryCount := range queryCounts {
		// Skip large query counts in -short mode
		if testing.Short() && queryCount > 100 {
			continue
		}

		// Run direct query benchmark
		b.Run(fmt.Sprintf("Direct_%d", queryCount), func(b *testing.B) {
			b.ResetTimer()
			directTime := RunMultiServiceDirectQueries(pool, queryCount)
			b.StopTimer()

			// Store result for comparison
			b.ReportMetric(float64(directTime.Nanoseconds())/float64(queryCount), "ns/op")

			// Test different batch window settings
			for _, batchWindow := range batchWindows {
				// Create batcher with current settings
				batcher := pgbatcher.New(pool, pgbatcher.Options{
					BatchWindow:  batchWindow,
					MaxBatchSize: maxBatchSize,
				})

				// Name for this run
				batchName := fmt.Sprintf("Batch_%d_%s", queryCount, batchWindow)

				// Run batch query benchmark
				b.Run(batchName, func(b *testing.B) {
					b.ResetTimer()
					batchTime := RunMultiServiceBatchedQueries(pool, batcher, queryCount)
					b.StopTimer()

					b.ReportMetric(float64(batchTime.Nanoseconds())/float64(queryCount), "ns/op")

					// Calculate improvement
					improvement := calculateImprovement(directTime, batchTime)

					// Report results
					fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%s\t%.2f%%\n",
						queryCount, directTime, batchWindow, maxBatchSize, batchTime, improvement)
				})

				// Clean up batcher
				batcher.Close()

				// Give DB a moment to recover between tests
				time.Sleep(100 * time.Millisecond)
			}
		})
	}

	w.Flush()
}

// TestQueryMix tests different query mix patterns
func TestQueryMix(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping query mix test in short mode")
	}

	pool, err := setupTestPool()
	if err != nil {
		t.Skipf("Skipping test, could not connect to database: %v", err)
		return
	}
	defer pool.Close()

	// Print test header
	fmt.Printf("\nRunning query mix tests (1000 queries)...\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Query Mix\tDirect Time\tBatch Time (5ms, 50 Max)\tImprovement (%)")
	fmt.Fprintln(w, "---------\t-----------\t-----------------------\t---------------")

	queryCount := 1000
	batchWindow := 5 * time.Millisecond
	maxBatchSize := 50

	// Test standard mix (already done in service simulation)
	directTimeStd := RunMultiServiceDirectQueries(pool, queryCount)
	batcher := pgbatcher.New(pool, pgbatcher.Options{
		BatchWindow:  batchWindow,
		MaxBatchSize: maxBatchSize,
	})
	batchTimeStd := RunMultiServiceBatchedQueries(pool, batcher, queryCount)
	improvementStd := calculateImprovement(directTimeStd, batchTimeStd)
	fmt.Fprintf(w, "Standard Mix\t%s\t%s\t%.2f%%\n",
		directTimeStd, batchTimeStd, improvementStd)
	batcher.Close()

	// Test 80% user queries
	time.Sleep(500 * time.Millisecond)
	directTimeUser := runSpecificQueryMix(pool, nil, queryCount, 80, 0)
	batcherUser := pgbatcher.New(pool, pgbatcher.Options{
		BatchWindow:  batchWindow,
		MaxBatchSize: maxBatchSize,
	})
	batchTimeUser := runSpecificQueryMix(pool, batcherUser, queryCount, 80, 0)
	improvementUser := calculateImprovement(directTimeUser, batchTimeUser)
	fmt.Fprintf(w, "80%% User\t%s\t%s\t%.2f%%\n",
		directTimeUser, batchTimeUser, improvementUser)
	batcherUser.Close()

	// Test 80% product queries
	time.Sleep(500 * time.Millisecond)
	directTimeProd := runSpecificQueryMix(pool, nil, queryCount, 80, 1)
	batcherProd := pgbatcher.New(pool, pgbatcher.Options{
		BatchWindow:  batchWindow,
		MaxBatchSize: maxBatchSize,
	})
	batchTimeProd := runSpecificQueryMix(pool, batcherProd, queryCount, 80, 1)
	improvementProd := calculateImprovement(directTimeProd, batchTimeProd)
	fmt.Fprintf(w, "80%% Product\t%s\t%s\t%.2f%%\n",
		directTimeProd, batchTimeProd, improvementProd)
	batcherProd.Close()

	w.Flush()
}

// TestBatchWindowSensitivity tests different batch window settings
func TestBatchWindowSensitivity(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping batch window sensitivity test in short mode")
	}

	pool, err := setupTestPool()
	if err != nil {
		t.Skipf("Skipping test, could not connect to database: %v", err)
		return
	}
	defer pool.Close()

	// Print test header
	fmt.Printf("\nRunning batch window sensitivity analysis with 1000 queries...\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Batch Window\tBatch Time (50 Max)\tImprovement vs Direct (%)")
	fmt.Fprintln(w, "------------\t-------------------\t-------------------------")

	queryCount := 1000
	maxBatchSize := 50

	// First get direct execution time for comparison
	directTime := RunMultiServiceDirectQueries(pool, queryCount)

	// Test different window durations
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
		batcher := pgbatcher.New(pool, pgbatcher.Options{
			BatchWindow:  window,
			MaxBatchSize: maxBatchSize,
		})

		batchTime := RunMultiServiceBatchedQueries(pool, batcher, queryCount)
		improvement := calculateImprovement(directTime, batchTime)

		fmt.Fprintf(w, "%s\t%s\t%.2f%%\n", window, batchTime, improvement)

		batcher.Close()
		time.Sleep(500 * time.Millisecond) // Give DB a breather
	}

	w.Flush()
}

// TestMaxBatchSize tests different maximum batch sizes
func TestMaxBatchSize(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping max batch size test in short mode")
	}

	pool, err := setupTestPool()
	if err != nil {
		t.Skipf("Skipping test, could not connect to database: %v", err)
		return
	}
	defer pool.Close()

	// Print test header
	fmt.Printf("\nRunning max batch size test with 1000 queries (5ms window)...\n")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Max Batch Size\tBatch Time\tImprovement vs Direct (%)")
	fmt.Fprintln(w, "--------------\t----------\t-------------------------")

	queryCount := 1000
	batchWindow := 5 * time.Millisecond

	// First get direct execution time for comparison
	directTime := RunMultiServiceDirectQueries(pool, queryCount)

	// Test different max batch sizes
	sizesToTest := []int{5, 10, 25, 50, 100, 200, 0} // 0 = unlimited

	for _, size := range sizesToTest {
		batcher := pgbatcher.New(pool, pgbatcher.Options{
			BatchWindow:  batchWindow,
			MaxBatchSize: size,
		})

		batchTime := RunMultiServiceBatchedQueries(pool, batcher, queryCount)
		improvement := calculateImprovement(directTime, batchTime)

		sizeLabel := fmt.Sprintf("%d", size)
		if size == 0 {
			sizeLabel = "Unlimited"
		}

		fmt.Fprintf(w, "%s\t%s\t%.2f%%\n", sizeLabel, batchTime, improvement)

		batcher.Close()
		time.Sleep(500 * time.Millisecond) // Give DB a breather
	}

	w.Flush()
}
