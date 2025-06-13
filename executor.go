package keyedexecutor

import (
	"container/list"
	"context"
	"hash/fnv"
	"sync"
)

// KeyedExecutor allows execution of functions with specific keys of type K.
// Functions with the same key are executed sequentially, while
// functions with different keys can be executed concurrently.
// Keys are hashed to a fixed number of buckets, each with its own worker.
type KeyedExecutor[K comparable] struct {
	mu          sync.Mutex
	taskQueues  []*list.List    // Queue of tasks for each bucket
	workers     []chan struct{} // Signal channels for each worker
	hashBuckets int             // Number of hash buckets (same as worker count)
	stopChan    chan struct{}   // For shutdown
	wg          sync.WaitGroup  // To track active workers
}

// Task interface that all task types must implement
type Task interface {
	Execute()
}

// SimpleTask represents a simple function with no arguments and no return value
type SimpleTask struct {
	fn func()
}

func (t *SimpleTask) Execute() {
	t.fn()
}

// ContextTask represents a function that takes a context
type ContextTask struct {
	fn  func(ctx context.Context)
	ctx context.Context
}

func (t *ContextTask) Execute() {
	t.fn(t.ctx)
}

// ErrorTask represents a function that returns an error
type ErrorTask struct {
	fn      func() error
	errChan chan error
}

func (t *ErrorTask) Execute() {
	err := t.fn()
	if t.errChan != nil {
		t.errChan <- err
		close(t.errChan)
	}
}

// ContextErrorTask represents a function that takes a context and returns an error
type ContextErrorTask struct {
	fn      func(ctx context.Context) error
	ctx     context.Context
	errChan chan error
}

func (t *ContextErrorTask) Execute() {
	err := t.fn(t.ctx)
	if t.errChan != nil {
		t.errChan <- err
		close(t.errChan)
	}
}

// ResultTask represents a function that returns a generic result
type ResultTask[T any] struct {
	fn         func() T
	resultChan chan T
}

func (t *ResultTask[T]) Execute() {
	result := t.fn()
	if t.resultChan != nil {
		t.resultChan <- result
		close(t.resultChan)
	}
}

// ContextResultTask represents a function that takes a context and returns a generic result
type ContextResultTask[T any] struct {
	fn         func(ctx context.Context) T
	ctx        context.Context
	resultChan chan T
}

func (t *ContextResultTask[T]) Execute() {
	result := t.fn(t.ctx)
	if t.resultChan != nil {
		t.resultChan <- result
		close(t.resultChan)
	}
}

// ResultErrorTask represents a function that returns both a generic result and an error
type ResultErrorTask[T any] struct {
	fn         func() (T, error)
	resultChan chan Result[T]
}

func (t *ResultErrorTask[T]) Execute() {
	result, err := t.fn()
	if t.resultChan != nil {
		t.resultChan <- Result[T]{Value: result, Err: err}
		close(t.resultChan)
	}
}

// ContextResultErrorTask represents a function that takes a context and returns both a generic result and an error
type ContextResultErrorTask[T any] struct {
	fn         func(ctx context.Context) (T, error)
	ctx        context.Context
	resultChan chan Result[T]
}

func (t *ContextResultErrorTask[T]) Execute() {
	result, err := t.fn(t.ctx)
	if t.resultChan != nil {
		t.resultChan <- Result[T]{Value: result, Err: err}
		close(t.resultChan)
	}
}

// Result wraps a generic result value and a potential error
type Result[T any] struct {
	Value T
	Err   error
}

// Config contains configuration options for KeyedExecutor
type Config struct {
	WorkerCount int // Number of worker goroutines (and hash buckets)
}

// DefaultConfig returns a default configuration
func DefaultConfig() Config {
	return Config{
		WorkerCount: 16, // Default to 16 workers/buckets
	}
}

// New creates a new KeyedExecutor with keys of type K.
func New[K comparable](config ...Config) *KeyedExecutor[K] {
	cfg := DefaultConfig()
	if len(config) > 0 {
		cfg = config[0]
	}

	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = DefaultConfig().WorkerCount
	}

	ke := &KeyedExecutor[K]{
		taskQueues:  make([]*list.List, cfg.WorkerCount),
		workers:     make([]chan struct{}, cfg.WorkerCount),
		hashBuckets: cfg.WorkerCount,
		stopChan:    make(chan struct{}),
	}

	// Initialize queues and start workers
	for i := 0; i < cfg.WorkerCount; i++ {
		ke.taskQueues[i] = list.New()
		ke.workers[i] = make(chan struct{}, 1)

		// Start the worker goroutine
		ke.wg.Add(1)
		go ke.workerLoop(i)
	}

	return ke
}

// Execute schedules a simple function with no arguments or return values
func (e *KeyedExecutor[K]) Execute(key K, fn func()) {
	task := &SimpleTask{fn: fn}
	e.scheduleTask(key, task)
}

// ExecuteWithContext schedules a function that takes a context
func (e *KeyedExecutor[K]) ExecuteWithContext(key K, ctx context.Context, fn func(context.Context)) {
	task := &ContextTask{fn: fn, ctx: ctx}
	e.scheduleTask(key, task)
}

// ExecuteWithError schedules a function that returns an error
// Returns a channel that will receive the error when the function completes
func (e *KeyedExecutor[K]) ExecuteWithError(key K, fn func() error) <-chan error {
	errChan := make(chan error, 1)
	task := &ErrorTask{fn: fn, errChan: errChan}
	e.scheduleTask(key, task)
	return errChan
}

// ExecuteWithContextError schedules a function that takes a context and returns an error
// Returns a channel that will receive the error when the function completes
func (e *KeyedExecutor[K]) ExecuteWithContextError(key K, ctx context.Context, fn func(context.Context) error) <-chan error {
	errChan := make(chan error, 1)
	task := &ContextErrorTask{fn: fn, ctx: ctx, errChan: errChan}
	e.scheduleTask(key, task)
	return errChan
}

// scheduleTask adds a task to the appropriate queue and signals the worker
func (e *KeyedExecutor[K]) scheduleTask(key K, task Task) {
	// Hash the key to determine which bucket/worker to use
	bucketIndex := e.hashKey(key)

	e.mu.Lock()
	// Add the task to the appropriate queue
	queue := e.taskQueues[bucketIndex]
	queue.PushBack(task)
	e.mu.Unlock()

	// Signal the worker that a task is available
	select {
	case e.workers[bucketIndex] <- struct{}{}:
		// Signal sent
	default:
		// Worker is already notified or busy, no need to send again
	}
}

// hashKey determines which bucket/worker should handle this key
func (e *KeyedExecutor[K]) hashKey(key K) int {
	// Convert key to bytes for hashing
	h := fnv.New32a()

	// Use type switch to handle common key types efficiently
	switch k := any(key).(type) {
	case string:
		h.Write([]byte(k))
	case int:
		var buf [4]byte
		buf[0] = byte(k)
		buf[1] = byte(k >> 8)
		buf[2] = byte(k >> 16)
		buf[3] = byte(k >> 24)
		h.Write(buf[:])
	case int64:
		var buf [8]byte
		buf[0] = byte(k)
		buf[1] = byte(k >> 8)
		buf[2] = byte(k >> 16)
		buf[3] = byte(k >> 24)
		buf[4] = byte(k >> 32)
		buf[5] = byte(k >> 40)
		buf[6] = byte(k >> 48)
		buf[7] = byte(k >> 56)
		h.Write(buf[:])
	default:
		// Fall back to string conversion for other types
		h.Write([]byte(any(key).(string)))
	}

	// Calculate bucket index from hash
	return int(h.Sum32()) % e.hashBuckets
}

// workerLoop processes tasks for a specific bucket
func (e *KeyedExecutor[K]) workerLoop(bucketIndex int) {
	defer e.wg.Done()

	for {
		// Wait for a signal or shutdown
		select {
		case <-e.workers[bucketIndex]:
			// Process task
		case <-e.stopChan:
			return
		}

		// Process all available tasks in the queue
		for {
			e.mu.Lock()
			queue := e.taskQueues[bucketIndex]

			// Check if there are tasks to process
			if queue.Len() == 0 {
				e.mu.Unlock()
				break
			}

			// Get the next task
			element := queue.Front()
			task := element.Value.(Task)
			queue.Remove(element)
			e.mu.Unlock()

			// Execute the task
			task.Execute()
		}
	}
}

// Shutdown stops the executor and waits for all workers to complete
func (e *KeyedExecutor[K]) Shutdown() {
	close(e.stopChan)
	e.wg.Wait()
}

// Stats returns current statistics about the executor state
func (e *KeyedExecutor[K]) Stats() (workerCount int, totalPendingTasks int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	workerCount = e.hashBuckets
	for _, queue := range e.taskQueues {
		totalPendingTasks += queue.Len()
	}

	return workerCount, totalPendingTasks
}
