// Package keyedexecutor provides a generic, key-based concurrent task execution framework.
// Tasks with the same key are executed sequentially, while tasks with different keys can execute concurrently.
// This version assigns a dedicated slot (queue + worker) per key, avoiding cross-key serialization caused by hashing.
package keyedexecutor

import (
	"container/list"
	"context"
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
// Internally, each key gets its own slot (queue + worker). The worker drains the queue and
// exits when the queue becomes empty. New tasks for that key will spawn a new worker on demand.
type KeyedExecutor[K comparable, R any] struct {
	mu       sync.Mutex
	slots    map[K]*slot // per-key task slots
	stopChan chan struct{}

	// wg waits for all per-key workers to exit.
	wg sync.WaitGroup
	// tasksWG waits for all scheduled tasks to finish Execute().
	tasksWG sync.WaitGroup
}

type slot struct {
	queue  *list.List
	active bool // whether a worker goroutine is currently running for this slot
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

// Config is kept for API compatibility. Currently there are no tunables.
// Future options could include idle timeouts, caps on active slots, etc.
type Config struct{}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config { return Config{} }

// New creates a new KeyedExecutor instance.
// Per-key workers are created on demand and exit when their queue becomes empty.
func New[K comparable, R any](config ...Config) *KeyedExecutor[K, R] {
	_ = DefaultConfig()
	if len(config) > 0 {
		_ = config[0]
	}
	return &KeyedExecutor[K, R]{
		slots:    make(map[K]*slot),
		stopChan: make(chan struct{}),
	}
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

// scheduleTask enqueues the task into the per-key slot, creating a slot and worker if needed.
// The per-key worker drains tasks serially and exits when the queue is empty.
func (e *KeyedExecutor[K, R]) scheduleTask(key K, task Task) {
	e.mu.Lock()
	s := e.slots[key]
	if s == nil {
		s = &slot{
			queue:  list.New(),
			active: false,
		}
		e.slots[key] = s
	}
	s.queue.PushBack(task)
	// track pending tasks
	e.tasksWG.Add(1)
	// Start a worker if there isn't one already running for this key.
	if !s.active {
		s.active = true
		e.wg.Add(1)
		go e.workerLoopForKey(key, s)
	}
	e.mu.Unlock()
}

// workerLoopForKey processes tasks for a single key serially.
// It drains the queue and exits when no more tasks are pending.
// On exit, it removes the slot from the executor (to free memory) and marks it inactive.
func (e *KeyedExecutor[K, R]) workerLoopForKey(key K, s *slot) {
	defer e.wg.Done()

	for {
		var task Task

		e.mu.Lock()
		if s.queue.Len() == 0 {
			// No more tasks; clean up the slot and exit.
			// Only delete if the current slot is still the one registered for the key.
			if curr, ok := e.slots[key]; ok && curr == s {
				delete(e.slots, key)
			}
			// Mark inactive for safety; though slot is removed from map.
			s.active = false
			e.mu.Unlock()
			return
		}
		elem := s.queue.Front()
		task = elem.Value.(Task)
		s.queue.Remove(elem)
		e.mu.Unlock()

		// Execute outside the lock to allow enqueuing new tasks concurrently.
		func() {
			defer e.tasksWG.Done()
			task.Execute()
		}()
	}
}

// Shutdown waits for all tasks to complete and all per-key workers to finish.
// Pending tasks continue to run to completion. New tasks should not be enqueued after Shutdown.
func (e *KeyedExecutor[K, R]) Shutdown() {
	close(e.stopChan)
	// Wait for all scheduled tasks to finish executing.
	e.tasksWG.Wait()
	// Then ensure all workers have exited and slots are cleaned up.
	e.wg.Wait()
}

// Stats returns the number of active slots (keys with pending or running tasks)
// and the total number of queued tasks across all slots.
func (e *KeyedExecutor[K, R]) Stats() (slots int, pending int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	slots = len(e.slots)
	for _, s := range e.slots {
		pending += s.queue.Len()
	}
	return
}
