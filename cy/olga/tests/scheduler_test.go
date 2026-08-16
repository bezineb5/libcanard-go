package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/opencyphal/cy-go/olga"
)

// mockNow is a mock time function for testing.
var mockNow int64

func mockNowFunc() int64 {
	return mockNow
}

// TestSchedulerBasic tests basic scheduling functionality.
func TestSchedulerBasic(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	// Schedule a task
	called := false
	task := s.Schedule(100, func() {
		called = true
	})

	if task == nil {
		t.Error("Schedule returned nil task")
		return
	}

	// Task should not have been called yet
	if called {
		t.Error("Task was called before deadline")
	}

	// Advance time and run once
	mockNow = 200
	s.RunOnce()

	// Task should have been called
	if !called {
		t.Error("Task was not called after deadline")
	}
}

// TestSchedulerMultipleTasks tests scheduling multiple tasks.
func TestSchedulerMultipleTasks(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	var callOrder []int

	// Schedule multiple tasks with different deadlines
	s.Schedule(300, func() {
		callOrder = append(callOrder, 1)
	})

	s.Schedule(100, func() {
		callOrder = append(callOrder, 2)
	})

	s.Schedule(200, func() {
		callOrder = append(callOrder, 3)
	})

	// Advance time and run all tasks
	mockNow = 400
	for s.Len() > 0 {
		s.RunOnce()
	}

	// Check call order (should be sorted by deadline)
	if len(callOrder) != 3 {
		t.Errorf("Expected 3 calls, got %d", len(callOrder))
		return
	}

	// Tasks should be called in order of their deadlines
	expected := []int{2, 3, 1}
	for i := range expected {
		if callOrder[i] != expected[i] {
			t.Errorf("Expected call order %v, got %v", expected, callOrder)
		}
	}
}

// TestSchedulerCancel tests canceling a scheduled task.
func TestSchedulerCancel(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	called := false
	task := s.Schedule(100, func() {
		called = true
	})

	// Cancel the task
	task.Active = false
	s.Cancel(task)

	// Advance time and run
	mockNow = 200
	s.RunOnce()

	// Task should not have been called
	if called {
		t.Error("Canceled task was called")
	}
}

// TestSchedulerRepeating tests a repeating task.
func TestSchedulerRepeating(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	callCount := 0
	var task *olga.Task

	task = s.Schedule(50, func() {
		callCount++
		// Reschedule ourselves
		if callCount < 5 {
			s.Reschedule(task, mockNow+50)
		}
	})

	// Advance time and run multiple times
	for i := 0; i < 5; i++ {
		mockNow += 50
		s.RunOnce()
	}

	// Should have been called 5 times
	if callCount != 5 {
		t.Errorf("Expected 5 calls, got %d", callCount)
	}
}

// TestSchedulerNow tests scheduling a task for immediate execution.
func TestSchedulerNow(t *testing.T) {
	mockNow = 100
	s := olga.NewWithNowFunc(mockNowFunc)

	called := false
	s.Schedule(mockNow, func() {
		called = true
	})

	// Run once
	s.RunOnce()

	if !called {
		t.Error("Task scheduled for now was not called")
	}
}

// TestSchedulerEmptySpin tests running with no tasks.
func TestSchedulerEmptySpin(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	// Run with no tasks - should do nothing
	n := s.RunOnce()
	if n != 0 {
		t.Errorf("Expected 0 tasks run, got %d", n)
	}
}

// TestSchedulerTaskOrdering tests that tasks are executed in deadline order.
func TestSchedulerTaskOrdering(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	var executionOrder []int

	// Schedule tasks with different deadlines
	for i := 0; i < 10; i++ {
		deadline := int64(i * 10)
		s.Schedule(deadline, func(i int) func() {
			return func() {
				executionOrder = append(executionOrder, i)
			}
		}(i))
	}

	// Advance time and run all tasks
	mockNow = 150
	for s.Len() > 0 {
		s.RunOnce()
	}

	// Check that tasks were executed in order
	if len(executionOrder) != 10 {
		t.Errorf("Expected 10 tasks to execute, got %d", len(executionOrder))
		return
	}

	for i := range executionOrder {
		if executionOrder[i] != i {
			t.Errorf("Expected task %d to execute at position %d, got %d",
				i, i, executionOrder[i])
		}
	}
}

// TestSchedulerTaskContext tests passing context to tasks.
func TestSchedulerTaskContext(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	testValue := "hello world"
	var receivedValue string

	s.Schedule(10, func() {
		receivedValue = testValue
	})

	mockNow = 50
	s.RunOnce()

	if receivedValue != testValue {
		t.Errorf("Expected %q, got %q", testValue, receivedValue)
	}
}

// TestSchedulerMultipleSchedulers tests that multiple schedulers work independently.
func TestSchedulerMultipleSchedulers(t *testing.T) {
	mockNow = 0
	s1 := olga.NewWithNowFunc(mockNowFunc)
	s2 := olga.NewWithNowFunc(mockNowFunc)

	var s1Called, s2Called bool

	s1.Schedule(50, func() {
		s1Called = true
	})

	s2.Schedule(50, func() {
		s2Called = true
	})

	// Run s1
	mockNow = 100
	s1.RunOnce()

	if !s1Called {
		t.Error("s1 task was not called")
	}
	if s2Called {
		t.Error("s2 task was called (should be independent)")
	}

	// Run s2
	mockNow = 100
	s2.RunOnce()

	if !s2Called {
		t.Error("s2 task was not called")
	}
}

// TestSchedulerCancelAll tests canceling all tasks.
func TestSchedulerCancelAll(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	callCount := 0

	// Schedule many tasks
	for i := 0; i < 10; i++ {
		s.Schedule(100, func() {
			callCount++
		})
	}

	// Cancel all
	s.Clear()

	// Advance time and run
	mockNow = 200
	s.RunOnce()

	// No tasks should have been called
	if callCount != 0 {
		t.Errorf("Expected 0 calls after Clear, got %d", callCount)
	}
}

// TestSchedulerReschedule tests rescheduling a task.
func TestSchedulerReschedule(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	callCount := 0

	task := s.Schedule(100, func() {
		callCount++
	})

	// Run past first deadline
	mockNow = 150
	s.RunOnce()

	if callCount != 1 {
		t.Errorf("Expected 1 call, got %d", callCount)
	}

	// Reschedule for another 100
	s.Reschedule(task, 250)

	// Run past second deadline
	mockNow = 300
	s.RunOnce()

	if callCount != 2 {
		t.Errorf("Expected 2 calls, got %d", callCount)
	}
}

// TestSchedulerLen tests the Len method.
func TestSchedulerLen(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	if s.Len() != 0 {
		t.Errorf("New scheduler should have length 0, got %d", s.Len())
	}

	// Add 5 tasks
	for i := 0; i < 5; i++ {
		s.Schedule(int64(i*10), func() {})
	}

	if s.Len() != 5 {
		t.Errorf("Expected length 5, got %d", s.Len())
	}

	// Run one task
	mockNow = 50
	s.RunOnce()

	if s.Len() != 4 {
		t.Errorf("Expected length 4 after running one task, got %d", s.Len())
	}
}

// TestSchedulerEmpty tests the Empty method.
func TestSchedulerEmpty(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	if !s.Empty() {
		t.Error("New scheduler should be empty")
	}

	s.Schedule(100, func() {})
	if s.Empty() {
		t.Error("Scheduler with one task should not be empty")
	}

	mockNow = 200
	s.RunOnce()
	if !s.Empty() {
		t.Error("Scheduler should be empty after running all tasks")
	}
}

// TestSchedulerNextDeadline tests the NextDeadline method.
func TestSchedulerNextDeadline(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	// Empty scheduler
	if !s.Empty() {
		t.Error("Scheduler should be empty")
	}

	// Add tasks with different deadlines
	s.Schedule(100, func() {})
	s.Schedule(50, func() {})
	s.Schedule(200, func() {})

	next := s.NextDeadline()
	if next != 50 {
		t.Errorf("Expected next deadline 50, got %d", next)
	}

	// Run the first task
	mockNow = 100
	s.RunOnce()

	next = s.NextDeadline()
	if next != 100 {
		t.Errorf("Expected next deadline 100, got %d", next)
	}
}

// TestSchedulerStop tests stopping a running scheduler.
func TestSchedulerStop(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	// Start the scheduler in a goroutine
	done := make(chan struct{})
	go func() {
		s.Run()
		close(done)
	}()

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Stop it
	s.Stop()

	// Wait for it to finish
	select {
	case <-done:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Error("Scheduler did not stop in time")
	}
}

// TestSchedulerConcurrent tests concurrent scheduling.
func TestSchedulerConcurrent(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	// Schedule many tasks concurrently
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		go func(i int) {
			defer wg.Done()
			s.Schedule(int64(i), func() {})
		}(i)
	}

	// Wait for all to be scheduled
	wg.Wait()

	if s.Len() != 100 {
		t.Errorf("Expected 100 tasks, got %d", s.Len())
	}
}

// TestSchedulerRunUntil tests RunUntil.
func TestSchedulerRunUntil(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	callCount := 0

	// Schedule tasks at different times
	for i := 0; i < 10; i++ {
		s.Schedule(int64(i*10), func() {
			callCount++
		})
	}

	// Run until time 50
	err := s.RunUntil(50)
	if err != nil {
		t.Errorf("RunUntil returned error: %v", err)
	}

	// Should have run tasks with deadlines <= 50
	// That's tasks at 0, 10, 20, 30, 40, 50 = 6 tasks
	if callCount != 6 {
		t.Errorf("Expected 6 calls, got %d", callCount)
	}

	// Should have 4 tasks remaining (60, 70, 80, 90)
	if s.Len() != 4 {
		t.Errorf("Expected 4 tasks remaining, got %d", s.Len())
	}
}

// BenchmarkSchedulerSchedule benchmarks scheduling performance.
func BenchmarkSchedulerSchedule(b *testing.B) {
	s := olga.New()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		task := s.Schedule(int64(i), func() {})
		s.Cancel(task)
	}
}

// BenchmarkSchedulerRun benchmarks running tasks.
func BenchmarkSchedulerRun(b *testing.B) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	// Schedule N tasks
	for i := 0; i < b.N; i++ {
		s.Schedule(int64(i), func() {})
	}

	b.ResetTimer()
	mockNow = int64(b.N)
	for s.Len() > 0 {
		s.RunOnce()
	}
}
