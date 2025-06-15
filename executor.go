// Package keyedexecutor provides a generic, key-based concurrent task execution framework.
// Tasks with the same key are executed sequentially, while tasks with different keys can execute concurrently.
// It is useful for enforcing per-key serialization (e.g., per-user order placement) while maintaining global concurrency.
package keyedexecutor

import (
	"container/list"
	"context"
	"hash/fnv"
	"sync"
)

// Result wraps a generic return value with an error.
// It is used for communicating the outcome of async tasks with result.
type Result[R any] struct {
	Value R
	Err   error
}

// KeyedExecutor manages the concurrent execution of tasks partitioned by key.
// Tasks sharing the same key are executed serially in the order they are submitted.
// Internally, keys are hashed into a fixed number of buckets, each processed by a dedicated worker goroutine.
type KeyedExecutor[K comparable, R any] struct {
	mu          sync.Mutex      // protects access to taskQueues
	taskQueues  []*list.List    // per-bucket task queues
	workers     []chan struct{} // per-bucket wake-up channels
	hashBuckets int             // number of hash buckets
	stopChan    chan struct{}   // signals all workers to shut down
	wg          sync.WaitGroup  // waits for all workers to complete
}

// Task represents a unit of executable logic.
type Task interface {
	Execute()
}

// SimpleTask executes a no-argument function with no return value.
type SimpleTask struct {
	fn func()
}

func (t *SimpleTask) Execute() {
	t.fn()
}

// ContextTask executes a function with context injection, useful for timeouts or cancellation.
type ContextTask struct {
	fn  func(ctx context.Context)
	ctx context.Context
}

func (t *ContextTask) Execute() {
	t.fn(t.ctx)
}

// ErrorTask executes a function returning an error, and reports the result through an error channel.
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

// ContextErrorTask executes a context-aware function returning an error,
// and reports the result through an error channel.
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

// GenericResultTask executes a function returning a typed result and error,
// and delivers the result through a typed channel.
type GenericResultTask[R any] struct {
	fn      func() (R, error)
	resultC chan Result[R]
}

func (t *GenericResultTask[R]) Execute() {
	res, err := t.fn()
	t.resultC <- Result[R]{Value: res, Err: err}
	close(t.resultC)
}

// ContextGenericResultTask executes a context-aware function returning a typed result and error,
// and delivers the result through a typed channel.
type ContextGenericResultTask[R any] struct {
	fn      func(ctx context.Context) (R, error)
	ctx     context.Context
	resultC chan Result[R]
}

func (t *ContextGenericResultTask[R]) Execute() {
	res, err := t.fn(t.ctx)
	t.resultC <- Result[R]{Value: res, Err: err}
	close(t.resultC)
}

// Config contains optional parameters for KeyedExecutor initialization.
type Config struct {
	WorkerCount int // number of worker goroutines to spin up (i.e., hash buckets)
}

// DefaultConfig returns the recommended default configuration with 16 workers.
func DefaultConfig() Config {
	return Config{WorkerCount: 16}
}

// New creates a new KeyedExecutor instance with optional configuration.
// If no config is provided, DefaultConfig is used.
// WorkerCount must be positive, or DefaultConfig will be enforced.
func New[K comparable, R any](config ...Config) *KeyedExecutor[K, R] {
	cfg := DefaultConfig()
	if len(config) > 0 {
		cfg = config[0]
	}
	if cfg.WorkerCount <= 0 {
		cfg = DefaultConfig()
	}

	ke := &KeyedExecutor[K, R]{
		taskQueues:  make([]*list.List, cfg.WorkerCount),
		workers:     make([]chan struct{}, cfg.WorkerCount),
		hashBuckets: cfg.WorkerCount,
		stopChan:    make(chan struct{}),
	}

	for i := 0; i < cfg.WorkerCount; i++ {
		ke.taskQueues[i] = list.New()
		ke.workers[i] = make(chan struct{}, 1)
		ke.wg.Add(1)
		go ke.workerLoop(i)
	}

	return ke
}

// Execute schedules a simple task (no context, no result) for the given key.
func (e *KeyedExecutor[K, R]) Execute(key K, fn func()) {
	task := &SimpleTask{fn: fn}
	e.scheduleTask(key, task)
}

// ExecuteWithContext schedules a context-aware task for the given key.
func (e *KeyedExecutor[K, R]) ExecuteWithContext(key K, ctx context.Context, fn func(context.Context)) {
	task := &ContextTask{fn: fn, ctx: ctx}
	e.scheduleTask(key, task)
}

// ExecuteWithError schedules a task that returns an error for the given key.
// Returns a channel that receives the task's error result.
func (e *KeyedExecutor[K, R]) ExecuteWithError(key K, fn func() error) <-chan error {
	errChan := make(chan error, 1)
	task := &ErrorTask{fn: fn, errChan: errChan}
	e.scheduleTask(key, task)
	return errChan
}

// ExecuteWithContextError schedules a context-aware task that returns an error.
// Returns a channel that receives the task's error result.
func (e *KeyedExecutor[K, R]) ExecuteWithContextError(key K, ctx context.Context, fn func(context.Context) error) <-chan error {
	errChan := make(chan error, 1)
	task := &ContextErrorTask{fn: fn, ctx: ctx, errChan: errChan}
	e.scheduleTask(key, task)
	return errChan
}

// ExecuteWithResult schedules a typed result task and returns a channel for the result.
func (e *KeyedExecutor[K, R]) ExecuteWithResult(key K, fn func() (R, error)) <-chan Result[R] {
	resultC := make(chan Result[R], 1)
	task := &GenericResultTask[R]{fn: fn, resultC: resultC}
	e.scheduleTask(key, task)
	return resultC
}

// ExecuteWithContextResult schedules a context-aware typed result task and returns a channel for the result.
func (e *KeyedExecutor[K, R]) ExecuteWithContextResult(key K, ctx context.Context, fn func(context.Context) (R, error)) <-chan Result[R] {
	resultC := make(chan Result[R], 1)
	task := &ContextGenericResultTask[R]{fn: fn, ctx: ctx, resultC: resultC}
	e.scheduleTask(key, task)
	return resultC
}

// scheduleTask hashes the key to a bucket, enqueues the task, and wakes up the corresponding worker if idle.
func (e *KeyedExecutor[K, R]) scheduleTask(key K, task Task) {
	bucketIndex := e.hashKey(key)
	e.mu.Lock()
	queue := e.taskQueues[bucketIndex]
	queue.PushBack(task)
	e.mu.Unlock()

	select {
	case e.workers[bucketIndex] <- struct{}{}:
	default: // worker already awake
	}
}

// hashKey computes a deterministic hash bucket index from the key.
func (e *KeyedExecutor[K, R]) hashKey(key K) int {
	h := fnv.New32a()

	// Simple type assertions for common keys
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
		for i := 0; i < 8; i++ {
			buf[i] = byte(k >> (8 * i))
		}
		h.Write(buf[:])
	default:
		// fallback: use string representation
		h.Write([]byte(any(key).(string)))
	}

	return int(h.Sum32()) % e.hashBuckets
}

// workerLoop is the main processing loop for each bucket worker.
// It processes tasks serially from the queue until shutdown.
func (e *KeyedExecutor[K, R]) workerLoop(bucketIndex int) {
	defer e.wg.Done()
	for {
		select {
		case <-e.workers[bucketIndex]: // woken up to process
		case <-e.stopChan: // global shutdown
			return
		}
		for {
			e.mu.Lock()
			queue := e.taskQueues[bucketIndex]
			if queue.Len() == 0 {
				e.mu.Unlock()
				break
			}
			elem := queue.Front()
			task := elem.Value.(Task)
			queue.Remove(elem)
			e.mu.Unlock()
			task.Execute()
		}
	}
}

// Shutdown stops all worker goroutines and waits for them to exit.
// Pending tasks may still be executed before workers terminate.
func (e *KeyedExecutor[K, R]) Shutdown() {
	close(e.stopChan)
	e.wg.Wait()
}

// Stats returns the number of buckets and total number of queued tasks across all buckets.
func (e *KeyedExecutor[K, R]) Stats() (buckets int, pending int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	buckets = e.hashBuckets
	for _, q := range e.taskQueues {
		pending += q.Len()
	}
	return
}
