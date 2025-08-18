package keyedexecutor

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func noop() {
	time.Sleep(10 * time.Millisecond) // 模拟一些工作
}

func waitUntil(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// 同一个 key 串行且保持提交顺序
func TestSameKeySerialOrder(t *testing.T) {
	e := New[string, any]()

	const n = 200
	var mu sync.Mutex
	order := make([]int, 0, n)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		e.Execute("user-1", func() {
			defer wg.Done()
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
		})
	}
	wg.Wait()

	// 期望严格按 0..n-1 顺序
	for i := 0; i < n; i++ {
		if order[i] != i {
			t.Fatalf("order broken at %d: got %d want %d", i, order[i], i)
		}
	}

	// 完成后应清理 slot
	waitUntil(t, func() bool {
		slots, _ := e.Stats()
		return slots == 0
	}, 2*time.Second)

	e.Shutdown()
}

// 不同 key 可以并行
func TestDifferentKeysParallel(t *testing.T) {
	e := New[string, any]()

	var running int32
	var maxRunning int32

	started := make(chan struct{}, 2)
	release := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(2)

	run := func(key string) {
		e.Execute(key, func() {
			defer wg.Done()
			c := atomic.AddInt32(&running, 1)
			for {
				// 更新最大并发
				old := atomic.LoadInt32(&maxRunning)
				if c > old && atomic.CompareAndSwapInt32(&maxRunning, old, c) {
					break
				}
				if c <= old {
					break
				}
			}
			started <- struct{}{}
			<-release
			atomic.AddInt32(&running, -1)
		})
	}

	go run("user-A")
	go run("user-B")

	// 等两个任务都启动
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first task did not start in time")
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("second task did not start in time")
	}

	// 应该出现并发=2（不同 key）
	if atomic.LoadInt32(&maxRunning) < 2 {
		t.Fatalf("expected parallelism across keys, maxRunning=%d", atomic.LoadInt32(&maxRunning))
	}

	close(release)
	wg.Wait()

	// 清理
	waitUntil(t, func() bool {
		slots, _ := e.Stats()
		return slots == 0
	}, 2*time.Second)
	e.Shutdown()
}

// 同一 key 任务不会并发执行（保护串行约束）
func TestNoParallelOnSameKey(t *testing.T) {
	e := New[string, any]()

	var inSection int32 // 进入临界区计数
	errCh := make(chan error, 1)

	var wg sync.WaitGroup
	wg.Add(2)

	run := func(id int) {
		e.Execute("same-key", func() {
			defer wg.Done()
			if atomic.AddInt32(&inSection, 1) != 1 {
				// 发现并发进入
				errCh <- fmt.Errorf("tasks for same key ran in parallel (task %d)", id)
				return
			}
			time.Sleep(50 * time.Millisecond) // 模拟执行
			atomic.AddInt32(&inSection, -1)
		})
	}

	go run(1)
	go run(2)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case err := <-errCh:
		t.Fatal(err)
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("tasks did not complete in time")
	}

	e.Shutdown()
}

// 执行完成后 slot 会被回收；同一 key 再来可重新创建并运行
func TestSlotLifecycleAndRecreate(t *testing.T) {
	e := New[string, any]()

	var wg sync.WaitGroup

	wg.Add(1)
	e.Execute("reuser", func() { defer wg.Done() })
	wg.Wait()

	// 等待 slot 清理
	waitUntil(t, func() bool {
		slots, _ := e.Stats()
		return slots == 0
	}, 2*time.Second)

	// 再来一个同 key 任务
	wg.Add(1)
	e.Execute("reuser", func() { defer wg.Done() })
	wg.Wait()

	// 再次清理
	waitUntil(t, func() bool {
		slots, _ := e.Stats()
		return slots == 0
	}, 2*time.Second)

	e.Shutdown()
}

// ExecuteWithResult / ExecuteWithContextResult 功能正确
func TestExecuteWithResult(t *testing.T) {
	e := New[string, int]()

	resC := e.ExecuteWithResult("k", func() (int, error) {
		return 42, nil
	})
	res := <-resC
	if res.Err != nil || res.Value != 42 {
		t.Fatalf("unexpected result: %+v", res)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resC2 := e.ExecuteWithContextResult("k", ctx, func(ctx context.Context) (int, error) {
		return 7, nil
	})
	res2 := <-resC2
	if res2.Err != nil || res2.Value != 7 {
		t.Fatalf("unexpected ctx result: %+v", res2)
	}

	e.Shutdown()
}

// Shutdown 会等待在途任务执行完成
func TestShutdownWaitsForInFlight(t *testing.T) {
	e := New[string, any]()

	var wg sync.WaitGroup
	const n = 5
	wg.Add(n)
	for i := 0; i < n; i++ {
		e.Execute("key", func() {
			defer wg.Done()
			time.Sleep(50 * time.Millisecond)
		})
	}

	// 并行等待 Shutdown 和任务完成，避免使用 default 引发的竞态
	done := make(chan struct{})
	go func() {
		e.Shutdown()
		close(done)
	}()

	wgDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(wgDone)
	}()

	select {
	case <-done:
		// Shutdown 返回了，确保任务也已完成（给一点缓冲时间）
		select {
		case <-wgDone:
			// ok
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Shutdown returned before tasks completed")
		}
	case <-wgDone:
		// 任务先完成，则确保 Shutdown 随后也能尽快返回
		select {
		case <-done:
			// ok
		case <-time.After(1 * time.Second):
			t.Fatal("Shutdown did not return after tasks completed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for shutdown or tasks to complete")
	}
}

func BenchmarkPerKey_SingleKey(b *testing.B) {
	e := New[string, any]()
	defer e.Shutdown()

	var wg sync.WaitGroup
	wg.Add(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Execute("user-1", func() {
			noop()
			wg.Done()
		})
	}
	wg.Wait()
}

func BenchmarkPerKey_100Keys(b *testing.B) {
	e := New[string, any]()
	defer e.Shutdown()

	const keys = 100
	var wg sync.WaitGroup
	wg.Add(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("u-%d", i%keys)
		e.Execute(k, func() {
			noop()
			wg.Done()
		})
	}
	wg.Wait()
}

func BenchmarkPerKey_Parallel_100Keys(b *testing.B) {
	e := New[string, any]()
	defer e.Shutdown()

	const keys = 100
	var outstanding int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		local := 0
		for pb.Next() {
			local++
			atomic.AddInt64(&outstanding, 1)
			k := fmt.Sprintf("u-%d", local%keys)
			e.Execute(k, func() {
				noop()
				atomic.AddInt64(&outstanding, -1)
			})
		}
	})

	// 等待所有已提交任务完成
	for atomic.LoadInt64(&outstanding) != 0 {
		runtime.Gosched()
	}
}

func BenchmarkPerKey_Parallel_1kKeys(b *testing.B) {
	e := New[string, any]()
	defer e.Shutdown()

	const keys = 1000
	var outstanding int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		local := 0
		for pb.Next() {
			local++
			atomic.AddInt64(&outstanding, 1)
			k := fmt.Sprintf("u-%d", local%keys)
			e.Execute(k, func() {
				// 模拟一点点计算负载，避免纯调度过于乐观
				x := 0
				for i := 0; i < 16; i++ {
					x += i
				}
				_ = x
				atomic.AddInt64(&outstanding, -1)
			})
		}
	})

	for atomic.LoadInt64(&outstanding) != 0 {
		runtime.Gosched()
	}
}

func BenchmarkPerKey_WithResult(b *testing.B) {
	e := New[string, int]()
	defer e.Shutdown()

	const keys = 64
	var wg sync.WaitGroup
	wg.Add(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("u-%d", i%keys)
		ch := e.ExecuteWithResult(k, func() (int, error) {
			// 一点点计算
			x := 0
			for j := 0; j < 8; j++ {
				x += j
			}
			return x, nil
		})
		go func() {
			<-ch
			wg.Done()
		}()
	}
	wg.Wait()
}
