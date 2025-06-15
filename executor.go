package keyedexecutor

import (
	"container/list"
	"context"
	"hash/fnv"
	"sync"
)

// Result wraps a generic result with an error
type Result[R any] struct {
	Value R
	Err   error
}

// KeyedExecutor allows execution of functions with specific keys of type K.
// Functions with the same key are executed sequentially, while
// functions with different keys can be executed concurrently.
// Keys are hashed to a fixed number of buckets, each with its own worker.
type KeyedExecutor[K comparable, R any] struct {
	mu          sync.Mutex
	taskQueues  []*list.List
	workers     []chan struct{}
	hashBuckets int
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

// Task interface
type Task interface {
	Execute()
}

// SimpleTask runs a func()
type SimpleTask struct {
	fn func()
}

func (t *SimpleTask) Execute() {
	t.fn()
}

// ContextTask runs a func(ctx)
type ContextTask struct {
	fn  func(ctx context.Context)
	ctx context.Context
}

func (t *ContextTask) Execute() {
	t.fn(t.ctx)
}

// ErrorTask runs a func() error
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

// ContextErrorTask runs a func(ctx) error
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

// GenericResultTask returns (R, error)
type GenericResultTask[R any] struct {
	fn      func() (R, error)
	resultC chan Result[R]
}

func (t *GenericResultTask[R]) Execute() {
	res, err := t.fn()
	t.resultC <- Result[R]{Value: res, Err: err}
	close(t.resultC)
}

// ContextGenericResultTask returns (R, error) with ctx
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

// Config for KeyedExecutor

type Config struct {
	WorkerCount int
}

func DefaultConfig() Config {
	return Config{WorkerCount: 16}
}

func New[K comparable, R any](config ...Config) *KeyedExecutor[K, R] {
	cfg := DefaultConfig()
	if len(config) > 0 {
		cfg = config[0]
	}
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = DefaultConfig().WorkerCount
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

func (e *KeyedExecutor[K, R]) Execute(key K, fn func()) {
	task := &SimpleTask{fn: fn}
	e.scheduleTask(key, task)
}

func (e *KeyedExecutor[K, R]) ExecuteWithContext(key K, ctx context.Context, fn func(context.Context)) {
	task := &ContextTask{fn: fn, ctx: ctx}
	e.scheduleTask(key, task)
}

func (e *KeyedExecutor[K, R]) ExecuteWithError(key K, fn func() error) <-chan error {
	errChan := make(chan error, 1)
	task := &ErrorTask{fn: fn, errChan: errChan}
	e.scheduleTask(key, task)
	return errChan
}

func (e *KeyedExecutor[K, R]) ExecuteWithContextError(key K, ctx context.Context, fn func(context.Context) error) <-chan error {
	errChan := make(chan error, 1)
	task := &ContextErrorTask{fn: fn, ctx: ctx, errChan: errChan}
	e.scheduleTask(key, task)
	return errChan
}

func (e *KeyedExecutor[K, R]) ExecuteWithResult(key K, fn func() (R, error)) <-chan Result[R] {
	resultC := make(chan Result[R], 1)
	task := &GenericResultTask[R]{fn: fn, resultC: resultC}
	e.scheduleTask(key, task)
	return resultC
}

func (e *KeyedExecutor[K, R]) ExecuteWithContextResult(key K, ctx context.Context, fn func(context.Context) (R, error)) <-chan Result[R] {
	resultC := make(chan Result[R], 1)
	task := &ContextGenericResultTask[R]{fn: fn, ctx: ctx, resultC: resultC}
	e.scheduleTask(key, task)
	return resultC
}

func (e *KeyedExecutor[K, R]) scheduleTask(key K, task Task) {
	bucketIndex := e.hashKey(key)
	e.mu.Lock()
	queue := e.taskQueues[bucketIndex]
	queue.PushBack(task)
	e.mu.Unlock()

	select {
	case e.workers[bucketIndex] <- struct{}{}:
	default:
	}
}

func (e *KeyedExecutor[K, R]) hashKey(key K) int {
	h := fnv.New32a()
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
		h.Write([]byte(any(key).(string)))
	}

	return int(h.Sum32()) % e.hashBuckets
}

func (e *KeyedExecutor[K, R]) workerLoop(bucketIndex int) {
	defer e.wg.Done()
	for {
		select {
		case <-e.workers[bucketIndex]:
		case <-e.stopChan:
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

func (e *KeyedExecutor[K, R]) Shutdown() {
	close(e.stopChan)
	e.wg.Wait()
}

func (e *KeyedExecutor[K, R]) Stats() (int, int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	count := e.hashBuckets
	total := 0
	for _, q := range e.taskQueues {
		total += q.Len()
	}
	return count, total
}
