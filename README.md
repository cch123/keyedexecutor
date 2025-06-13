!!!! Not Production Ready

# KeyedExecutor

KeyedExecutor is a Go library for concurrent task execution with key-based grouping. It ensures that tasks with the same key are executed sequentially, while tasks with different keys can run in parallel.

## Features

- Execute tasks concurrently with controlled parallelism
- Group-based task scheduling using keys
- Tasks with the same key are executed sequentially
- Tasks with different keys can run in parallel
- Simple, intuitive API

## Installation

```bash
go get github.com/cch123/keyedexecutor
```

## Usage

```go
package main

import (
	"context"
	"fmt"
	"time"

	exec "github.com/cch123/keyedexecutor"
)

func main() {
	// Create a new executor with a maximum of 10 concurrent tasks
	executor := exec.New[string](exec.Config{
		WorkerCount: 4,
	})

	// Execute a task with key "user1"
	executor.ExecuteWithContext("user1", context.Background(), func(ctx context.Context) {
		fmt.Println("Processing task for user1")
		time.Sleep(100 * time.Millisecond)
	})

	// Tasks with the same key run sequentially
	executor.Execute("user1", func() {
		fmt.Println("Another task for user1, runs after the first one")
	})

	// Tasks with different keys can run in parallel
	executor.ExecuteWithError("user2", func() error {
		fmt.Println("Processing task for user2 (can run in parallel with user1 tasks)")
		return nil
	})

	fmt.Println(executor.Stats())
}

```

## API Documentation

### Creating a New Executor

```go
// Create a new executor with a specified maximum number of concurrent tasks
executor := keyedexecutor.New[int](keyedexecutor.Config{100})
```

### Executing Tasks

```go
// Execute a task with a specific key
executor.Execute(key string, task func(context.Context) error)

// Execute a task with a specific key and context
executor.ExecuteWithContext(ctx context.Context, key string, task func(context.Context) error)
```

### Waiting for Tasks to Complete

```go
// Wait for all tasks to complete
executor.Wait()

// Wait for all tasks to complete with a timeout
executor.WaitWithTimeout(timeout time.Duration) error
```

## License

[MIT License](LICENSE)
