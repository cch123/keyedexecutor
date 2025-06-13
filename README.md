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
	"errors"
	"fmt"
	"time"

	"github.com/cch123/keyedexecutor"
)

func main() {
	// Create a new executor with a maximum of 10 concurrent tasks
	exec := keyedexecutor.New[string](keyedexecutor.Config{
		WorkerCount: 4,
	})

	// Execute a task with key "user1"
	ctx := context.Background()
	ctx = context.WithValue(ctx, "user", "user1")
	exec.ExecuteWithContext("user1", ctx, func(ctx context.Context) {
		fmt.Println("Processing task for user1")
		time.Sleep(100 * time.Millisecond)
		fmt.Println("Processing task for user1, read ctx value, User:", ctx.Value("user"))
	})

	// Tasks with the same key run sequentially
	exec.Execute("user1", func() {
		fmt.Println("Another task for user1, runs after the first one")
	})

	// Tasks with different keys can run in parallel
	err := <-exec.ExecuteWithError("user2", func() error {
		fmt.Println("Processing task for user2 (can run in parallel with user1 tasks)")
		return errors.New("example error")
	})
	fmt.Println("err returned", err)

	err = <-exec.ExecuteWithContextError("user3", ctx, func(ctx context.Context) error {
		fmt.Println("Processing task for user3 (can run in parallel with user1 tasks)")

		time.Sleep(200 * time.Millisecond)
		fmt.Println("Processing task for user3, read ctx value, User:", ctx.Value("user"))
		return errors.New("example error for user3")
	})

	fmt.Println("err returned for user3", err)

	fmt.Printf("executor stats ")
	workerCount, pendingTasks := exec.Stats()
	fmt.Println("worker count:", workerCount, "pending tasks:", pendingTasks)
	time.Sleep(time.Second)
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

## License

[MIT License](LICENSE)
