package keyedexecutor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestKeyedExecutor_Execute(t *testing.T) {
	executor := New[string]()
	defer executor.Shutdown()

	var counter int
	var mu sync.Mutex

	// Execute multiple tasks with same key (sequential execution)
	for i := 0; i < 10; i++ {
		executor.Execute("same-key", func() {
			mu.Lock()
			counter++
			mu.Unlock()
		})
	}

	// Wait for tasks to complete
	time.Sleep(100 * time.Millisecond)

	if counter != 10 {
		t.Errorf("Expected counter to be 10, got %d", counter)
	}
}

func TestKeyedExecutor_ExecuteWithContext(t *testing.T) {
	executor := New[string]()
	defer executor.Shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	executor.ExecuteWithContext("test-key", ctx, func(ctx context.Context) {
		defer wg.Done()

		select {
		case <-ctx.Done():
			t.Error("Context should not have expired yet")
		case <-time.After(100 * time.Millisecond):
			// Expected behavior
		}
	})

	wg.Wait()
}

func TestKeyedExecutor_ExecuteWithError(t *testing.T) {
	executor := New[int]()
	defer executor.Shutdown()

	expectedErr := errors.New("test error")

	errChan := executor.ExecuteWithError(42, func() error {
		return expectedErr
	})

	err := <-errChan
	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
}

func TestKeyedExecutor_ExecuteWithContextError(t *testing.T) {
	executor := New[int]()
	defer executor.Shutdown()

	ctx := context.Background()
	expectedErr := errors.New("test context error")

	errChan := executor.ExecuteWithContextError(123, ctx, func(ctx context.Context) error {
		return expectedErr
	})

	err := <-errChan
	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
}

func TestKeyedExecutor_ConcurrentExecution(t *testing.T) {
	executor := New[string]()
	defer executor.Shutdown()

	var wg sync.WaitGroup
	const numKeys = 5
	const tasksPerKey = 10

	results := make([]int, numKeys)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprint("a", i)
		wg.Add(tasksPerKey)

		for j := 0; j < tasksPerKey; j++ {
			executor.Execute(key, func(keyIndex int) func() {
				return func() {
					defer wg.Done()
					results[keyIndex]++
					time.Sleep(10 * time.Millisecond)
				}
			}(i))
		}
	}

	wg.Wait()

	for i, count := range results {
		if count != tasksPerKey {
			t.Errorf("Key %c expected %d executions, got %d", 'a'+i, tasksPerKey, count)
		}
	}
}

func TestKeyedExecutor_Stats(t *testing.T) {
	cfg := Config{WorkerCount: 8}
	executor := New[string](cfg)
	defer executor.Shutdown()

	workers, pending := executor.Stats()
	if workers != 8 {
		t.Errorf("Expected 8 workers, got %d", workers)
	}
	if pending != 0 {
		t.Errorf("Expected 0 pending tasks, got %d", pending)
	}

	// Add some tasks and check stats again
	var wg sync.WaitGroup
	wg.Add(1)

	blockCh := make(chan struct{})

	executor.Execute("key1", func() {
		<-blockCh // Block until we unblock
		wg.Done()
	})

	// Schedule more tasks
	for i := 0; i < 5; i++ {
		executor.Execute("key1", func() {})
	}

	// Give tasks time to be queued
	time.Sleep(10 * time.Millisecond)

	_, pending = executor.Stats()
	if pending < 5 { // At least 5 tasks should be pending (might be 6 if first one is still queued)
		t.Errorf("Expected at least 5 pending tasks, got %d", pending)
	}

	// Unblock the first task
	close(blockCh)
	wg.Wait()

	// Wait for all tasks to complete
	time.Sleep(100 * time.Millisecond)

	_, pending = executor.Stats()
	if pending != 0 {
		t.Errorf("Expected 0 pending tasks, got %d", pending)
	}
}
