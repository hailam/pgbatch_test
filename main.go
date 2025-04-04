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
		mode = "BATCHED"
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

// setupDatabase initializes database connection and batcher
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

	// Create batcher with configured options
	options := pgbatcher.DefaultOptions()
	options.BatchWindow = 5 * time.Millisecond
	options.MaxBatchSize = 50
	options.MaxConcurrentBatches = 5

	batcher = pgbatcher.New(pool, options)

	fmt.Println("Connected to PostgreSQL database successfully!")
	return nil
}

func main() {
	// Setup database connection
	if err := setupDatabase(); err != nil {
		log.Fatalf("Failed to set up database: %v", err)
	}
	defer pool.Close()
	defer batcher.Close()

	// Configure simulation
	baseConfig := SimulationConfig{
		ConcurrentRequests: 100, // Simulate 100 concurrent HTTP requests
		BurstCount:         20,  // Run 5 bursts
		TimeBetweenBursts:  100 * time.Millisecond,
	}

	// First run simulation with direct queries
	directConfig := baseConfig
	directConfig.DirectMode = true
	directTime := RunSimulation(directConfig)

	// Short pause
	time.Sleep(5 * time.Second)

	// Then run simulation with batched queries
	batchedConfig := baseConfig
	batchedConfig.DirectMode = false
	batchedTime := RunSimulation(batchedConfig)

	// Compare results
	improvement := (float64(directTime) - float64(batchedTime)) / float64(directTime) * 100

	fmt.Println("\n--- COMPARISON ---")
	fmt.Printf("Direct execution: %v\n", directTime)
	fmt.Printf("Batched execution: %v\n", batchedTime)
	fmt.Printf("Time improvement: %.2f%%\n", improvement)

	directReqPerSec := float64(baseConfig.ConcurrentRequests*baseConfig.BurstCount) / directTime.Seconds()
	batchedReqPerSec := float64(baseConfig.ConcurrentRequests*baseConfig.BurstCount) / batchedTime.Seconds()
	throughputImprovement := (batchedReqPerSec - directReqPerSec) / directReqPerSec * 100

	fmt.Printf("Direct throughput: %.2f req/sec\n", directReqPerSec)
	fmt.Printf("Batched throughput: %.2f req/sec\n", batchedReqPerSec)
	fmt.Printf("Throughput improvement: %.2f%%\n", throughputImprovement)
}
