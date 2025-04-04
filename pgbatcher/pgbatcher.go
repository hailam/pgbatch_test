// Package pgbatcher provides an efficient way to batch PostgreSQL queries.
// It transparently collects individual queries over a specified time window
// and executes them as a group to reduce database round trips.
package pgbatcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// QueryResult holds the processed result of a query executed within a batch
type QueryResult struct {
	// The raw data returned by the query
	Data [][]any
	// Column metadata
	Fields []pgconn.FieldDescription
	// Any error that occurred during execution
	Err error
}

// QueryRequest represents a query to be batched
type QueryRequest struct {
	SQL        string
	Args       []any
	ResultChan chan QueryResult
}

// Options contains configuration options for the Batcher
type Options struct {
	// How long to wait before executing a batch of queries (default: 5ms)
	BatchWindow time.Duration
	// Maximum number of queries in a batch before forcing execution (default: 50, 0 = unlimited)
	MaxBatchSize int
	// Maximum number of concurrent batches to execute (default: 5)
	MaxConcurrentBatches int
}

// DefaultOptions returns the recommended default options
func DefaultOptions() Options {
	return Options{
		BatchWindow:          5 * time.Millisecond,
		MaxBatchSize:         50,
		MaxConcurrentBatches: 5,
	}
}

// Batcher handles batching database queries
type Batcher struct {
	pool    *pgxpool.Pool
	options Options

	mutex       sync.Mutex
	batch       []QueryRequest
	timer       *time.Timer
	timerActive bool
	closed      bool

	// Semaphore to limit concurrent batch executions
	batchSemaphore chan struct{}
}

// New creates a new Batcher with the provided connection pool and options
func New(pool *pgxpool.Pool, options Options) *Batcher {
	// Use reasonable defaults if not specified
	if options.BatchWindow <= 0 {
		options.BatchWindow = DefaultOptions().BatchWindow
	}
	if options.MaxConcurrentBatches <= 0 {
		options.MaxConcurrentBatches = DefaultOptions().MaxConcurrentBatches
	}

	batcher := &Batcher{
		pool:           pool,
		options:        options,
		batch:          make([]QueryRequest, 0, options.MaxBatchSize),
		timerActive:    false,
		closed:         false,
		batchSemaphore: make(chan struct{}, options.MaxConcurrentBatches),
	}
	return batcher
}

// Queue adds a query to the batch and returns a QueryResult through the provided context
func (b *Batcher) Queue(ctx context.Context, sql string, args ...any) ([][]any, []pgconn.FieldDescription, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	resultChan := make(chan QueryResult, 1)

	b.mutex.Lock()
	if b.closed {
		b.mutex.Unlock()
		return nil, nil, errors.New("batcher is closed")
	}

	b.batch = append(b.batch, QueryRequest{
		SQL:        sql,
		Args:       args,
		ResultChan: resultChan,
	})

	// Check if batch should be executed immediately
	batchFull := b.options.MaxBatchSize > 0 && len(b.batch) >= b.options.MaxBatchSize
	executeNow := false

	// Start timer if not already running
	if !b.timerActive && !batchFull {
		b.timer = time.AfterFunc(b.options.BatchWindow, b.executeBatch)
		b.timerActive = true
	}

	// Execute immediately if batch is full
	if batchFull {
		if b.timerActive && b.timer != nil {
			b.timer.Stop()
		}
		b.timerActive = false
		executeNow = true
	}

	b.mutex.Unlock()

	if executeNow {
		b.executeBatch()
	}

	// Wait for results
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case result := <-resultChan:
		return result.Data, result.Fields, result.Err
	}
}

// QueueSimple adds a query to the batch but only returns the error
// This is useful when you don't need the returned data
func (b *Batcher) QueueSimple(ctx context.Context, sql string, args ...any) error {
	_, _, err := b.Queue(ctx, sql, args...)
	return err
}

// executeBatch processes the current batch of queries
func (b *Batcher) executeBatch() {
	b.mutex.Lock()
	// Skip if no queries or already closed
	if len(b.batch) == 0 || b.closed {
		b.timerActive = false
		b.mutex.Unlock()
		return
	}

	// Grab the current batch and reset
	currentBatch := b.batch
	b.batch = make([]QueryRequest, 0, cap(b.batch))
	b.timerActive = false
	b.mutex.Unlock()

	// Acquire a semaphore slot for this batch execution
	b.batchSemaphore <- struct{}{}

	// Execute batch in a separate goroutine
	go func(batch []QueryRequest) {
		defer func() {
			// Release semaphore when done
			<-b.batchSemaphore
		}()

		// Prepare the pgx batch
		pgxBatch := &pgx.Batch{}
		for _, req := range batch {
			pgxBatch.Queue(req.SQL, req.Args...)
		}

		// Use a background context with reasonable timeout for batch execution
		execCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Send batch to database
		br := b.pool.SendBatch(execCtx, pgxBatch)
		defer br.Close()

		// Process each query's result
		for _, req := range batch {
			rows, err := br.Query()

			var queryData [][]any
			var fields []pgconn.FieldDescription

			if err != nil {
				// Error executing query
				if rows != nil {
					rows.Close()
				}
				req.ResultChan <- QueryResult{Err: err}
				continue
			}

			// Process rows
			fields = rows.FieldDescriptions()
			queryData = make([][]any, 0)

			for rows.Next() {
				values, err := rows.Values()
				if err != nil {
					req.ResultChan <- QueryResult{
						Data:   queryData,
						Fields: fields,
						Err:    fmt.Errorf("error reading row values: %w", err),
					}
					rows.Close()
					continue
				}
				queryData = append(queryData, values)
			}

			// Check for iteration errors
			err = rows.Err()
			rows.Close()

			// Send back results
			req.ResultChan <- QueryResult{
				Data:   queryData,
				Fields: fields,
				Err:    err,
			}
		}
	}(currentBatch)
}

// Flush forces immediate execution of any pending queries
func (b *Batcher) Flush() {
	b.mutex.Lock()

	// No-op if no pending queries or already closed
	if len(b.batch) == 0 || b.closed {
		b.mutex.Unlock()
		return
	}

	// Stop the timer if active
	if b.timerActive && b.timer != nil {
		b.timer.Stop()
		b.timerActive = false
	}

	b.mutex.Unlock()

	// Execute the batch
	b.executeBatch()
}

// Close stops accepting new queries and flushes any pending queries
func (b *Batcher) Close() {
	b.mutex.Lock()
	if b.closed {
		b.mutex.Unlock()
		return
	}

	b.closed = true

	// Stop timer if active
	if b.timerActive && b.timer != nil {
		b.timer.Stop()
		b.timerActive = false
	}

	// Check if there are pending queries
	hasPendingQueries := len(b.batch) > 0
	b.mutex.Unlock()

	// Flush remaining queries
	if hasPendingQueries {
		b.executeBatch()
	}
}

// Stats represents current Batcher statistics
type Stats struct {
	PendingQueries       int
	ActiveBatches        int
	MaxConcurrentBatches int
}

// GetStats returns current statistics about the batcher
func (b *Batcher) GetStats() Stats {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return Stats{
		PendingQueries:       len(b.batch),
		ActiveBatches:        len(b.batchSemaphore),
		MaxConcurrentBatches: cap(b.batchSemaphore),
	}
}
