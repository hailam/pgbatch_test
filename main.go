package main

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/hailam/pgbatch_test/pgbatcher"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Global database connection and batcher
var (
	pool    *pgxpool.Pool
	batcher *pgbatcher.Batcher
)

// RequestType represents different API endpoints
type RequestType int

const (
	UserRequest RequestType = iota
	ProductRequest
	OrderRequest
	OrderItemRequest
)

// SimulationConfig holds configuration for the simulation
type SimulationConfig struct {
	ConcurrentRequests int
	RequestsPerBurst   int
	BurstCount         int
	TimeBetweenBursts  time.Duration
	DirectMode         bool
	BatchWindow        time.Duration // Added BatchWindow to config
}

// RequestHandler simulates handling a request in a goroutine
func RequestHandler(ctx context.Context, requestID int, requestType RequestType, useBatcher bool) error {
	start := time.Now()

	// Simulate different queries based on request type
	var query string
	var args []any

	switch requestType {
	case UserRequest:
		userID := rand.IntN(50) + 1
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
	}

	var err error

	if useBatcher {
		// Use the shared batcher
		_, _, err = batcher.Queue(ctx, query, args...)
	} else {
		// Direct query execution
		rows, qErr := pool.Query(ctx, query, args...)
		if qErr != nil {
			return fmt.Errorf("query error: %w", qErr)
		}

		// Must consume rows
		for rows.Next() {
			// Just iterate
		}

		err = rows.Err()
		rows.Close()
	}

	duration := time.Since(start)

	// In a real app, we would handle errors and process data here
	if err != nil {
		return fmt.Errorf("request %d failed: %w", requestID, err)
	}

	// Uncomment to see individual request times
	fmt.Printf("Request %d completed in %v\n", requestID, duration)

	return nil
}

// SimulateBurst simulates a burst of concurrent HTTP requests
func SimulateBurst(burstID int, config SimulationConfig) time.Duration {
	fmt.Printf("Starting burst %d with %d concurrent requests...\n",
		burstID, config.ConcurrentRequests)

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(config.ConcurrentRequests)

	// Launch concurrent goroutines to simulate HTTP handlers
	for i := 0; i < config.ConcurrentRequests; i++ {
		go func(requestID int) {
			defer wg.Done()

			// Create a context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Choose a random request type (simulating different API endpoints)
			requestType := RequestType(rand.IntN(4))

			// Process request
			err := RequestHandler(ctx, requestID, requestType, !config.DirectMode)
			if err != nil {
				log.Printf("Error: %v", err)
			}
		}(i)
	}

	// Wait for all requests to complete
	wg.Wait()

	// If using batcher, flush any pending queries
	if !config.DirectMode {
		batcher.Flush()
	}

	duration := time.Since(start)
	fmt.Printf("Burst %d completed in %v\n", burstID, duration)
	return duration
}

// RunSimulation runs a complete simulation
func RunSimulation(config SimulationConfig) time.Duration {
	mode := "DIRECT"
	if !config.DirectMode {
		mode = fmt.Sprintf("BATCHED (window: %v)", config.BatchWindow)
	}

	fmt.Printf("\n--- Starting %s mode simulation ---\n", mode)
	fmt.Printf("Concurrent requests per burst: %d\n", config.ConcurrentRequests)
	fmt.Printf("Total bursts: %d\n", config.BurstCount)
	fmt.Printf("Time between bursts: %v\n", config.TimeBetweenBursts)

	totalStart := time.Now()

	// Run multiple bursts
	var totalBurstTime time.Duration

	for i := 0; i < config.BurstCount; i++ {
		burstTime := SimulateBurst(i+1, config)
		totalBurstTime += burstTime

		// Wait between bursts
		if i < config.BurstCount-1 && config.TimeBetweenBursts > 0 {
			time.Sleep(config.TimeBetweenBursts)
		}
	}

	totalTime := time.Since(totalStart)
	requestCount := config.ConcurrentRequests * config.BurstCount
	reqPerSec := float64(requestCount) / totalTime.Seconds()
	avgRequestTime := totalBurstTime / time.Duration(config.BurstCount)

	fmt.Printf("\n--- %s mode simulation results ---\n", mode)
	fmt.Printf("Total requests: %d\n", requestCount)
	fmt.Printf("Total time: %v\n", totalTime)
	fmt.Printf("Average burst time: %v\n", avgRequestTime)
	fmt.Printf("Requests per second: %.2f\n", reqPerSec)

	return totalTime
}

// setupBatcher creates a new batcher with the given batch window
func setupBatcher(batchWindow time.Duration) *pgbatcher.Batcher {
	options := pgbatcher.DefaultOptions()
	options.BatchWindow = batchWindow
	options.MaxBatchSize = 50
	options.MaxConcurrentBatches = 5

	return pgbatcher.New(pool, options)
}

// setupDatabase initializes database connection
func setupDatabase() error {
	// Get connection string from environment or use default
	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		connString = "postgres://admin:admin@localhost:5432/pgbatch?sslmode=disable"
		fmt.Println("DATABASE_URL env var not set, using default:", connString)
	}

	// Parse configuration
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return fmt.Errorf("unable to parse connection string: %w", err)
	}

	// Configure pool
	config.MaxConns = 50

	// Create connection pool
	pool, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return fmt.Errorf("unable to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(context.Background()); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	fmt.Println("Connected to PostgreSQL database successfully!")
	return nil
}

// getTimingColor returns ANSI color codes for timing comparison visualization
func getTimingColor(improvement float64) string {
	if improvement >= 30 {
		return "\033[1;32m" // Bold green
	} else if improvement >= 10 {
		return "\033[32m" // Green
	} else if improvement >= 0 {
		return "\033[33m" // Yellow
	} else {
		return "\033[31m" // Red
	}
}

// resetColor returns ANSI reset code
func resetColor() string {
	return "\033[0m"
}

func main() {
	// Setup database connection
	if err := setupDatabase(); err != nil {
		log.Fatalf("Failed to set up database: %v", err)
	}
	defer pool.Close()

	// base simulation parameters
	baseConfig := SimulationConfig{
		ConcurrentRequests: 100, // per burst
		BurstCount:         20,
		TimeBetweenBursts:  6 * time.Millisecond,
	}

	batchWindows := []time.Duration{
		1 * time.Millisecond,
		2 * time.Millisecond,
		5 * time.Millisecond,
		10 * time.Millisecond,
		20 * time.Millisecond,
		50 * time.Millisecond,
	}

	// Store results for comparison
	type SimulationResult struct {
		config        SimulationConfig
		executionTime time.Duration
		reqPerSec     float64
	}
	results := make([]SimulationResult, 0, len(batchWindows)+1)

	// First run simulation with direct queries
	directConfig := baseConfig
	directConfig.DirectMode = true
	directConfig.BatchWindow = 0

	directTime := RunSimulation(directConfig)
	directReqPerSec := float64(baseConfig.ConcurrentRequests*baseConfig.BurstCount) / directTime.Seconds()

	results = append(results, SimulationResult{
		config:        directConfig,
		executionTime: directTime,
		reqPerSec:     directReqPerSec,
	})

	// Short pause between simulation types
	time.Sleep(5 * time.Second)

	// Run simulations with different batch windows
	for _, window := range batchWindows {
		fmt.Printf("\n\n--- TESTING BATCH WINDOW: %v ---\n", window)

		// Create a new batcher with this window
		if batcher != nil {
			batcher.Close()
		}
		batcher = setupBatcher(window)

		// Configure and run this simulation
		batchedConfig := baseConfig
		batchedConfig.DirectMode = false
		batchedConfig.BatchWindow = window
		batchedConfig.TimeBetweenBursts = window + 1*time.Millisecond // Ensure enough time between bursts

		batchedTime := RunSimulation(batchedConfig)
		batchedReqPerSec := float64(baseConfig.ConcurrentRequests*baseConfig.BurstCount) / batchedTime.Seconds()

		results = append(results, SimulationResult{
			config:        batchedConfig,
			executionTime: batchedTime,
			reqPerSec:     batchedReqPerSec,
		})

		// Short pause between simulations
		time.Sleep(3 * time.Second)
	}

	// Close the last batcher
	if batcher != nil {
		batcher.Close()
	}

	// Print comprehensive comparison
	fmt.Println("\n\n=== COMPARISON OF ALL SIMULATIONS ===")
	colWidthMode := 28
	colWidthTime := 18
	colWidthRPS := 15
	colWidthImp := 15

	fmt.Printf("%-*s %-*s %-*s %-*s\n",
		colWidthMode, "Mode",
		colWidthTime, "Execution Time",
		colWidthRPS, "Requests/sec",
		colWidthImp, "Improvement")
	fmt.Println("----------------------------------------------------------------------------")

	directResult := results[0]
	fmt.Printf("%-*s %-*v %-*.2f %*s\n",
		colWidthMode, "DIRECT",
		colWidthTime, directResult.executionTime,
		colWidthRPS, directResult.reqPerSec,
		colWidthImp, "-")

	for i := 1; i < len(results); i++ {
		result := results[i]
		timeImprovement := (float64(directResult.executionTime) - float64(result.executionTime)) / float64(directResult.executionTime) * 100
		//throughputImprovement := (result.reqPerSec - directResult.reqPerSec) / directResult.reqPerSec * 100
		colorCode := getTimingColor(timeImprovement)
		resetCode := resetColor()

		modeStr := fmt.Sprintf("BATCHED (window: %v)", result.config.BatchWindow)
		improvementStr := fmt.Sprintf("%s%.2f%%%s", colorCode, timeImprovement, resetCode)

		fmt.Printf("%-*s %-*v %-*.2f %s\n",
			colWidthMode, modeStr,
			colWidthTime, result.executionTime,
			colWidthRPS, result.reqPerSec,
			improvementStr)
	}

	// Find optimal batch window
	var bestResult SimulationResult
	bestImprovement := -100.0

	for i := 1; i < len(results); i++ {
		improvement := (float64(directResult.executionTime) - float64(results[i].executionTime)) / float64(directResult.executionTime) * 100
		if improvement > bestImprovement {
			bestImprovement = improvement
			bestResult = results[i]
		}
	}

	fmt.Printf("\n\n=== SUMMARY ===\n")
	fmt.Printf("Best performance: %s%.2f%%%s improvement with batch window of %v\n",
		getTimingColor(bestImprovement),
		bestImprovement,
		resetColor(),
		bestResult.config.BatchWindow)
	fmt.Printf("Execution time: %v vs. %v (direct)\n", bestResult.executionTime, directResult.executionTime)
	fmt.Printf("Throughput: %.2f req/sec vs. %.2f req/sec (direct)\n", bestResult.reqPerSec, directResult.reqPerSec)
}
