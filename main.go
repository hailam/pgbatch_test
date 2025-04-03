package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// QueryResult holds the processed result of a query executed within a batch
type QueryResult struct {
	// Rows pgx.Rows // rows when to be consumed would be nil so, we don't send them
	Data   [][]any                   // Holds slices of row values
	Fields []pgconn.FieldDescription // Holds column information
	Err    error                     // Holds any error during execution or processing
}

// QueryRequest represents a query to be batched
type QueryRequest struct {
	SQL        string
	Args       []any
	ResultChan chan QueryResult // Channel to send the *processed* result back
}

// BatchService handles batching database queries
type BatchService struct {
	pool         *pgxpool.Pool
	batchWindow  time.Duration
	maxBatchSize int

	mutex       sync.Mutex
	batch       []QueryRequest
	timer       *time.Timer
	timerActive bool
}

// NewBatchService creates a new batching service
func NewBatchService(pool *pgxpool.Pool, batchWindow time.Duration, maxBatchSize int) *BatchService {
	bs := &BatchService{
		pool:         pool,
		batchWindow:  batchWindow,
		maxBatchSize: maxBatchSize,
		batch:        make([]QueryRequest, 0),
		timerActive:  false,
	}
	// Pre-allocate batch slice slightly if maxBatchSize is known and reasonable
	if maxBatchSize > 0 && maxBatchSize < 1000 {
		bs.batch = make([]QueryRequest, 0, maxBatchSize)
	}
	return bs
}

// EnqueueQuery adds a query to the batch and waits for its processed result.
// It no longer returns pgx.Rows. The caller only gets an error or nil.
// If data is needed, it would be added to QueryResult and returned differently.
func (s *BatchService) EnqueueQuery(ctx context.Context, sql string, args ...any) error {
	resultChan := make(chan QueryResult, 1) // Buffered channel for the result

	s.mutex.Lock()

	s.batch = append(s.batch, QueryRequest{
		SQL:        sql,
		Args:       args,
		ResultChan: resultChan,
	})

	batchFull := s.maxBatchSize > 0 && len(s.batch) >= s.maxBatchSize
	executeNow := false

	if !s.timerActive && !batchFull {
		s.timer = time.AfterFunc(s.batchWindow, s.executeBatch)
		s.timerActive = true
	}

	if batchFull { // Execute if full, regardless of timer state (it might have just fired)
		if s.timerActive && s.timer != nil {
			s.timer.Stop()
		}
		s.timerActive = false // Will be reset by executeBatch if needed
		executeNow = true
	}

	s.mutex.Unlock()

	if executeNow {
		s.executeBatch()
	}

	select {
	case <-ctx.Done():
		// Context cancelled before result received.
		// The query might still execute in the batch.
		return ctx.Err() // Return context error
	case result := <-resultChan:
		// Return the error received from the batch execution/processing.
		return result.Err
		// NOTE: If you needed the data, you would access result.Data here.
		// For this benchmark, we primarily care about the error status.
	}
}

// executeBatch sends the current batch, processes results, and sends back via channels.
func (s *BatchService) executeBatch() {
	s.mutex.Lock()
	if len(s.batch) == 0 {
		s.timerActive = false
		s.mutex.Unlock()
		return
	}

	currentBatch := s.batch
	// Reset batch, preserving capacity
	s.batch = make([]QueryRequest, 0, cap(s.batch))
	s.timerActive = false
	s.mutex.Unlock()

	// --- Correct Batch Execution & Processing ---
	batch := &pgx.Batch{}
	for _, req := range currentBatch {
		batch.Queue(req.SQL, req.Args...)
	}

	// Use a background context potentially with a timeout for the batch itself
	// batchCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel()
	// br := s.pool.SendBatch(batchCtx, batch)

	br := s.pool.SendBatch(context.Background(), batch)
	defer br.Close() // Close batch results when done processing all queries in this batch

	// Process results IN ORDER and send back through channels
	for _, req := range currentBatch {
		rows, err := br.Query() // Get results for the next query in the batch sequence

		var queryData [][]any
		var fields []pgconn.FieldDescription
		var processingErr error

		if err != nil {
			// Error executing this specific query in the batch
			// Send error immediately. No rows to process.
			// Need to ensure rows are closed if non-nil, pgx might do this on Query error, but belt-and-suspenders.
			if rows != nil {
				rows.Close()
			}
			req.ResultChan <- QueryResult{Err: err}
			continue // Move to the next query result in the batch
		}

		// --- Process Rows ---
		fields = rows.FieldDescriptions()
		queryData = make([][]any, 0) // Initialize slice

		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				processingErr = fmt.Errorf("error reading row values: %w", err)
				break // Stop processing rows for this query on error
			}
			// Append a copy or the direct slice
			queryData = append(queryData, values)
		}

		// Check for errors during iteration (e.g., network issues)
		if iterErr := rows.Err(); iterErr != nil && processingErr == nil {
			processingErr = fmt.Errorf("error iterating rows: %w", iterErr)
		}

		rows.Close() // IMPORTANT: Close rows for this query *before* getting the next one

		// Send processed result (data + fields + error)
		// Combine execution/processing errors. Prioritize processing errors if they occurred.
		finalErr := err // err from br.Query() - might be nil
		if processingErr != nil {
			finalErr = processingErr // Overwrite with processing error
		}
		req.ResultChan <- QueryResult{
			Data:   queryData,
			Fields: fields,
			Err:    finalErr,
		}
		// --- End Process Rows ---
	}
	// --- End Correct Batch Execution & Processing ---
}

// ForceFlush immediately executes any pending queries in the batch
func (s *BatchService) ForceFlush() {
	s.mutex.Lock()
	if s.timerActive && s.timer != nil {
		if !s.timer.Stop() {
			// Timer already fired and executeBatch might be running,
			// but executeBatch handles locking and checking len(s.batch).
			// We might risk a double execution trigger, but the lock
			// and len check in executeBatch should make it safe (one will find batch empty).
		}
		s.timerActive = false
	}
	s.mutex.Unlock()
	s.executeBatch()
}

// --- Benchmarking Functions ---

// RunDirectQueries executes queries directly without batching
func RunDirectQueries(pool *pgxpool.Pool, queryCount int) time.Duration {
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(queryCount)

	for i := 0; i < queryCount; i++ {
		go func(id int) {
			defer wg.Done()
			query, args := generateQuery(id)
			rows, err := pool.Query(context.Background(), query, args...)
			if err != nil {
				return // Log/count errors if needed
			}
			defer rows.Close()
			// Consume rows (like original code)
			for rows.Next() {
			}
			_ = rows.Err() // Check iteration error
		}(i)
	}

	wg.Wait()
	return time.Since(start)
}

// RunBatchedQueries executes queries through the batch service
func RunBatchedQueries(batchService *BatchService, queryCount int) time.Duration {
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(queryCount)

	for i := 0; i < queryCount; i++ {
		go func(id int) {
			defer wg.Done()
			query, args := generateQuery(id)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// EnqueueQuery now only returns an error
			err := batchService.EnqueueQuery(ctx, query, args...)
			if err != nil {
				// Error could be context cancellation or batch execution/processing error
				// fmt.Printf("Error executing batched query: %v\n", err)
				return
			}
			// No rows object to process here anymore, it's handled in executeBatch
		}(i)
	}

	wg.Wait()
	batchService.ForceFlush() // Ensure final batch executes
	return time.Since(start)
}

// Helper to generate consistent queries for benchmarks
func generateQuery(id int) (sql string, args []any) {

	switch id % 4 {
	case 0:
		userID := rand.IntN(50) + 1
		sql = "SELECT id, name, email FROM users WHERE id = $1"
		args = []any{userID}
	case 1:
		productID := rand.IntN(100) + 1
		sql = "SELECT id, name, price FROM products WHERE id = $1"
		args = []any{productID}
	case 2:
		userID := rand.IntN(50) + 1
		sql = "SELECT id, user_id, order_date FROM orders WHERE user_id = $1"
		args = []any{userID}
	case 3:
		orderID := rand.IntN(200) + 1
		sql = "SELECT id, order_id, product_id, quantity FROM order_items WHERE order_id = $1 LIMIT 5"
		args = []any{orderID}
	}
	return sql, args
}

// --- Multi-Service Simulation ---

type ServiceRunner func(ctx context.Context, id int) error

func createServiceRunner(pool *pgxpool.Pool, bs *BatchService) ServiceRunner {
	userService := NewUserService(pool, bs)
	productService := NewProductService(pool, bs)
	orderService := NewOrderService(pool, bs)

	return func(ctx context.Context, id int) error {
		var err error

		switch id % 10 {
		case 0, 1:
			userID := rand.IntN(50) + 1
			err = userService.GetUserByID(ctx, userID)
		case 2, 3, 4:
			productID := rand.IntN(100) + 1
			err = productService.GetProductByID(ctx, productID)
		case 5, 6:
			userID := rand.IntN(50) + 1
			err = orderService.GetOrdersByUserID(ctx, userID)
		case 7, 8, 9:
			orderID := rand.IntN(200) + 1
			err = orderService.GetOrderItemsByOrderID(ctx, orderID)
		}
		return err
	}
}

func runMultiServiceQueries(pool *pgxpool.Pool, bs *BatchService, queryCount int) time.Duration {
	start := time.Now()
	runner := createServiceRunner(pool, bs)
	var wg sync.WaitGroup
	wg.Add(queryCount)

	for i := 0; i < queryCount; i++ {
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := runner(ctx, id); err != nil {
				// Optionally log errors, especially context deadline exceeded vs other errors
				// if errors.Is(err, context.DeadlineExceeded) { // Check specific errors if needed }
			}
		}(i)
	}
	wg.Wait()
	if bs != nil {
		bs.ForceFlush()
	}
	return time.Since(start)
}

// RunMultiServiceDirectQueries simulates multiple services executing queries directly
func RunMultiServiceDirectQueries(pool *pgxpool.Pool, queryCount int) time.Duration {
	return runMultiServiceQueries(pool, nil, queryCount)
}

// RunMultiServiceBatchedQueries simulates multiple services using the batch service
func RunMultiServiceBatchedQueries(pool *pgxpool.Pool, batchService *BatchService, queryCount int) time.Duration {
	return runMultiServiceQueries(pool, batchService, queryCount)
}

// --- Service Definitions (simplified, use helper for query execution) ---

// executeServiceQuery now only returns an error, mirroring EnqueueQuery's change
func executeServiceQuery(ctx context.Context, pool *pgxpool.Pool, bs *BatchService, query string, args ...any) error {
	if bs != nil {
		// Call EnqueueQuery which now returns only an error
		err := bs.EnqueueQuery(ctx, query, args...)
		if err != nil {
			// Return specific context errors if needed, otherwise wrap
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err // Return context error directly
			}
			return fmt.Errorf("batched query execution failed: %w", err)
		}
		return nil // Success
	} else {
		// Direct execution path remains the same conceptually, but consume rows here
		rows, err := pool.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("direct query execution failed: %w", err)
		}
		defer rows.Close()
		// Consume rows
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("direct query row iteration failed: %w", err)
		}
		return nil // Success
	}
}

type UserService struct {
	pool *pgxpool.Pool
	bs   *BatchService
}

func NewUserService(pool *pgxpool.Pool, batchService *BatchService) *UserService {
	return &UserService{pool: pool, bs: batchService}
}
func (s *UserService) GetUserByID(ctx context.Context, userID int) error {
	query := "SELECT id, name, email FROM users WHERE id = $1"
	return executeServiceQuery(ctx, s.pool, s.bs, query, userID)
}

type ProductService struct {
	pool *pgxpool.Pool
	bs   *BatchService
}

func NewProductService(pool *pgxpool.Pool, batchService *BatchService) *ProductService {
	return &ProductService{pool: pool, bs: batchService}
}
func (s *ProductService) GetProductByID(ctx context.Context, productID int) error {
	query := "SELECT id, name, price FROM products WHERE id = $1"
	return executeServiceQuery(ctx, s.pool, s.bs, query, productID)
}

type OrderService struct {
	pool *pgxpool.Pool
	bs   *BatchService
}

func NewOrderService(pool *pgxpool.Pool, batchService *BatchService) *OrderService {
	return &OrderService{pool: pool, bs: batchService}
}
func (s *OrderService) GetOrdersByUserID(ctx context.Context, userID int) error {
	query := "SELECT id, user_id, order_date FROM orders WHERE user_id = $1"
	return executeServiceQuery(ctx, s.pool, s.bs, query, userID)
}
func (s *OrderService) GetOrderItemsByOrderID(ctx context.Context, orderID int) error {
	query := "SELECT id, order_id, product_id, quantity FROM order_items WHERE order_id = $1 LIMIT 5"
	return executeServiceQuery(ctx, s.pool, s.bs, query, orderID)
}

// --- Main Execution ---

func main() {
	// Connect to PostgreSQL
	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		connString = "postgres://admin:admin@localhost:5432/pgbatch?sslmode=disable"
		fmt.Println("DATABASE_URL env var not set, using default:", connString)
	}

	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to parse connection string: %v\n", err)
		os.Exit(1)
	}
	config.MaxConns = 50 // Example: Set max connections

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to create connection pool: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	err = pool.Ping(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to ping database: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Connected to PostgreSQL database successfully!")

	// --- Run Benchmarks ---
	queryCounts := []int{100, 500, 1000, 2500}
	batchWindows := []time.Duration{
		1 * time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond,
	}
	defaultMaxBatchSize := 50

	fmt.Println("\nRunning simple benchmarks (Mixed Queries)...")
	wSimple := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.Debug) // Use Debug for alignment markers
	fmt.Fprintln(wSimple, "Query Count\tDirect Time\tBatch Window\tMax Batch Size\tBatch Time\tImprovement (%)")
	fmt.Fprintln(wSimple, "-----------\t-----------\t------------\t--------------\t----------\t---------------")

	for _, queryCount := range queryCounts {
		directTime := RunDirectQueries(pool, queryCount)

		for _, batchWindow := range batchWindows {
			batchService := NewBatchService(pool, batchWindow, defaultMaxBatchSize)
			batchTime := RunBatchedQueries(batchService, queryCount)
			improvement := calculateImprovement(directTime, batchTime)
			fmt.Fprintf(wSimple, "%d\t%s\t%s\t%d\t%s\t%.2f%%\n",
				queryCount, directTime, batchWindow, defaultMaxBatchSize, batchTime, improvement)
		}
	}
	wSimple.Flush()

	fmt.Println("\nRunning multi-service simulation benchmarks...")
	wMulti := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.Debug)
	fmt.Fprintln(wMulti, "Query Count\tDirect Time\tBatch Window\tMax Batch Size\tBatch Time\tImprovement (%)")
	fmt.Fprintln(wMulti, "-----------\t-----------\t------------\t--------------\t----------\t---------------")

	multiServiceQueryCounts := []int{500, 1000, 5000}
	for _, queryCount := range multiServiceQueryCounts {
		directTime := RunMultiServiceDirectQueries(pool, queryCount)

		for _, batchWindow := range batchWindows {
			batchService := NewBatchService(pool, batchWindow, defaultMaxBatchSize)
			batchTime := RunMultiServiceBatchedQueries(pool, batchService, queryCount)
			improvement := calculateImprovement(directTime, batchTime)
			fmt.Fprintf(wMulti, "%d\t%s\t%s\t%d\t%s\t%.2f%%\n",
				queryCount, directTime, batchWindow, defaultMaxBatchSize, batchTime, improvement)
		}
	}
	wMulti.Flush()

	// --- Run Additional Tests ---
	fmt.Println("\n--- Starting Additional Tests ---")
	runPoolSizeTests(config, 1000)
	runQueryMixTests(pool, 1000) // This should now work without panic
	runExtremeConcurrencyTest(pool, defaultMaxBatchSize, []int{1000, 2500, 5000})
	runBatchWindowSensitivityTest(pool, 1000)
	runMaxBatchSizeTest(pool, 1000, 5*time.Millisecond)

	fmt.Println("\n--- Benchmarks Complete ---")
}

// --- Helper Functions (Moved outside main) ---

func calculateImprovement(directTime, batchTime time.Duration) float64 {
	if directTime <= 0 && batchTime <= 0 {
		return 0 // No time taken, no improvement
	}
	if directTime <= 0 && batchTime > 0 {
		return -100.0 // Went from zero time to some time (infinitely worse)
	}
	if batchTime <= 0 { // directTime > 0
		return 100.0 // Went from some time to zero time (100% improvement or infinite?) -> Cap at 100%
	}

	// Calculate improvement relative to direct time
	// Original formula: (float64(directTime-batchTime) / float64(directTime)) * 100.0
	improvement := (float64(directTime) - float64(batchTime)) / float64(directTime) * 100.0
	return improvement

}

// Helper function to run specific query mix (now uses error return only for batching)
func runSpecificQueryMix(pool *pgxpool.Pool, batchService *BatchService, queryCount int, dominantPercent int, dominantType int) time.Duration {
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(queryCount)

	for i := 0; i < queryCount; i++ {
		go func(id int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			var query string
			var args []any
			useDominantQuery := rand.IntN(100) < dominantPercent

			if useDominantQuery {
				query, args = generateQuery(dominantType)
			} else {
				otherType := (dominantType + 1 + rand.IntN(3)) % 4
				query, args = generateQuery(otherType)
			}

			var err error
			if batchService != nil {
				// Batch path: Only get error back
				err = batchService.EnqueueQuery(ctx, query, args...)
			} else {
				// Direct path: Still need to consume rows
				var rows pgx.Rows
				rows, err = pool.Query(ctx, query, args...)
				if err == nil && rows != nil { // Process only if no error and rows valid

					for rows.Next() {
						//
					}
					iterErr := rows.Err()
					rows.Close() // Close after processing
					// Prioritize iteration error if execution error was nil
					if iterErr != nil {
						err = iterErr
					}
				} else if rows != nil {
					rows.Close() // Ensure close even if initial query had error
				}
			}

			if err != nil {
				// Log/count errors if needed
			}
		}(i)
	}

	wg.Wait()
	if batchService != nil {
		batchService.ForceFlush()
	}
	return time.Since(start)
}

// Function to run a series of tests with increasing connection pool sizes
func runPoolSizeTests(baseConfig *pgxpool.Config, queryCount int) {
	fmt.Printf("\nRunning pool size tests with %d queries (Multi-Service)...\n", queryCount)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Pool Size\tDirect Time\tBatch Time (2ms, 50 Max)\tImprovement (%)")
	fmt.Fprintln(w, "---------\t-----------\t-----------------------\t---------------")

	poolSizes := []int{5, 10, 25, 50, 100}
	batchWindow := 2 * time.Millisecond
	maxBatchSize := 50

	for _, size := range poolSizes {
		currentConfig := baseConfig.Copy()
		currentConfig.MaxConns = int32(size)
		if currentConfig.MinConns > currentConfig.MaxConns {
			currentConfig.MinConns = currentConfig.MaxConns
		}

		sizedPool, err := pgxpool.NewWithConfig(context.Background(), currentConfig)
		if err != nil {
			fmt.Printf("! WARN: Failed to create pool with size %d: %v\n", size, err)
			continue // Skip this size if pool creation fails
		}

		directTime := RunMultiServiceDirectQueries(sizedPool, queryCount)
		batchService := NewBatchService(sizedPool, batchWindow, maxBatchSize)
		batchTime := RunMultiServiceBatchedQueries(sizedPool, batchService, queryCount)
		improvement := calculateImprovement(directTime, batchTime)

		fmt.Fprintf(w, "%d\t%s\t%s\t%.2f%%\n", size, directTime, batchTime, improvement)

		sizedPool.Close()
		time.Sleep(1 * time.Second) // Still pause between tests
	}
	// Flush only ONCE after the loop finishes
	w.Flush()
}

// Function to benchmark query mixes
func runQueryMixTests(pool *pgxpool.Pool, queryCount int) {
	fmt.Printf("\nRunning query mix tests (%d queries, Multi-Service)...\n", queryCount)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Query Mix\tDirect Time\tBatch Time (2ms, 50 Max)\tImprovement (%)")
	fmt.Fprintln(w, "---------\t-----------\t-----------------------\t---------------")

	batchWindow := 2 * time.Millisecond
	maxBatchSize := 50

	// --- Standard mix ---
	directTimeStd := RunMultiServiceDirectQueries(pool, queryCount)
	batchServiceStd := NewBatchService(pool, batchWindow, maxBatchSize)
	batchTimeStd := RunMultiServiceBatchedQueries(pool, batchServiceStd, queryCount)
	improvementStd := calculateImprovement(directTimeStd, batchTimeStd)
	fmt.Fprintf(w, "Standard Mix\t%s\t%s\t%.2f%%\n", directTimeStd, batchTimeStd, improvementStd)

	// --- 80% User Lookups ---
	time.Sleep(500 * time.Millisecond) // Optional pause between sub-tests
	directTimeSimilar := runSpecificQueryMix(pool, nil, queryCount, 80, 0)
	batchServiceSimilar := NewBatchService(pool, batchWindow, maxBatchSize)
	batchTimeSimilar := runSpecificQueryMix(pool, batchServiceSimilar, queryCount, 80, 0)
	improvementSimilar := calculateImprovement(directTimeSimilar, batchTimeSimilar)
	fmt.Fprintf(w, "80%% User\t%s\t%s\t%.2f%%\n", directTimeSimilar, batchTimeSimilar, improvementSimilar)

	// --- 80% Product lookups ---
	time.Sleep(500 * time.Millisecond) // Optional pause
	directTimeProd := runSpecificQueryMix(pool, nil, queryCount, 80, 1)
	batchServiceProd := NewBatchService(pool, batchWindow, maxBatchSize)
	batchTimeProd := runSpecificQueryMix(pool, batchServiceProd, queryCount, 80, 1)
	improvementProd := calculateImprovement(directTimeProd, batchTimeProd)
	fmt.Fprintf(w, "80%% Product\t%s\t%s\t%.2f%%\n", directTimeProd, batchTimeProd, improvementProd)

	w.Flush()
}

// Function to test batching with extreme concurrency
func runExtremeConcurrencyTest(pool *pgxpool.Pool, maxBatchSize int, concurrencyLevels []int) {
	fmt.Println("\nRunning extreme concurrency tests (Multi-Service)...")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Concurrency\tDirect Time\tBatch Time (2ms, 50 Max)\tImprovement (%)")
	fmt.Fprintln(w, "-----------\t-----------\t-----------------------\t---------------")

	batchWindow := 2 * time.Millisecond

	for _, concurrency := range concurrencyLevels {
		directTime := RunMultiServiceDirectQueries(pool, concurrency)
		batchService := NewBatchService(pool, batchWindow, maxBatchSize)
		batchTime := RunMultiServiceBatchedQueries(pool, batchService, concurrency)
		improvement := calculateImprovement(directTime, batchTime)

		fmt.Fprintf(w, "%d\t%s\t%s\t%.2f%%\n", concurrency, directTime, batchTime, improvement)

		time.Sleep(1 * time.Second) // kuda pause between tests
	}
	w.Flush()
}

// Function to run a batch window sensitivity analysis
func runBatchWindowSensitivityTest(pool *pgxpool.Pool, queryCount int) {
	fmt.Printf("\nRunning batch window sensitivity analysis with %d queries (Multi-Service)...\n", queryCount)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Batch Window\tBatch Time (50 Max)\tImprovement vs Direct (%)")
	fmt.Fprintln(w, "------------\t-------------------\t-------------------------")

	directTime := RunMultiServiceDirectQueries(pool, queryCount)
	maxBatchSize := 50

	windowsToTest := []time.Duration{
		500 * time.Microsecond, 1 * time.Millisecond, 2 * time.Millisecond,
		3 * time.Millisecond, 5 * time.Millisecond, 8 * time.Millisecond,
		10 * time.Millisecond, 15 * time.Millisecond, 20 * time.Millisecond,
	}

	for _, bWindow := range windowsToTest {
		bs := NewBatchService(pool, bWindow, maxBatchSize)
		batchTime := RunMultiServiceBatchedQueries(pool, bs, queryCount)
		improvement := calculateImprovement(directTime, batchTime)

		fmt.Fprintf(w, "%s\t%s\t%.2f%%\n", bWindow, batchTime, improvement)

		time.Sleep(500 * time.Millisecond) // Still pause
	}
	w.Flush()
}

// Function to test effect of max batch size
func runMaxBatchSizeTest(pool *pgxpool.Pool, queryCount int, batchWindow time.Duration) {
	fmt.Printf("\nRunning Max Batch Size test with %d queries (Multi-Service, %s window)...\n", queryCount, batchWindow)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Max Batch Size\tDirect Time\tBatch Time\tImprovement (%)")
	fmt.Fprintln(w, "--------------\t-----------\t----------\t---------------")

	directTime := RunMultiServiceDirectQueries(pool, queryCount)

	batchSizesToTest := []int{5, 10, 25, 50, 100, 200, 0} // 0 means unlimited

	for _, maxBatchSize := range batchSizesToTest {
		bs := NewBatchService(pool, batchWindow, maxBatchSize)
		batchTime := RunMultiServiceBatchedQueries(pool, bs, queryCount)
		improvement := calculateImprovement(directTime, batchTime)

		sizeLabel := fmt.Sprintf("%d", maxBatchSize)
		if maxBatchSize == 0 {
			sizeLabel = "Unlimited"
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%.2f%%\n", sizeLabel, directTime, batchTime, improvement)
		// w.Flush()
		time.Sleep(500 * time.Millisecond) // Still pause
	}
	w.Flush()
}
