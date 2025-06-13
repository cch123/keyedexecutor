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
go get github.com/yourusername/keyedexecutor
```

## Usage

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/yourusername/keyedexecutor"
)

func main() {
    // Create a new executor with a maximum of 10 concurrent tasks
    executor := keyedexecutor.New(10)

    // Execute a task with key "user1"
    executor.Execute("user1", func(ctx context.Context) error {
        fmt.Println("Processing task for user1")
        time.Sleep(100 * time.Millisecond)
        return nil
    })

    // Tasks with the same key run sequentially
    executor.Execute("user1", func(ctx context.Context) error {
        fmt.Println("Another task for user1, runs after the first one")
        return nil
    })

    // Tasks with different keys can run in parallel
    executor.Execute("user2", func(ctx context.Context) error {
        fmt.Println("Processing task for user2 (can run in parallel with user1 tasks)")
        return nil
    })

    // Wait for all tasks to complete
    executor.Wait()
}
```

## API Documentation

### Creating a New Executor

```go
// Create a new executor with a specified maximum number of concurrent tasks
executor := keyedexecutor.New(maxConcurrency)
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
