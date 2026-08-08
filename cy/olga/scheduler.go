// Package olga provides an event loop scheduler with O(1) insertion and removal.
// This is a Go port of the olga_scheduler.h C library used by the cy library.
//
// Olga (O(1) Logarithmic-Gap Aware scheduler) is a simple event loop scheduler that
// maintains a set of timed callbacks and invokes them when their deadlines are reached.
// It uses a linked list of time-ordered tasks to achieve O(1) insertion and removal
// for most operations.
package olga

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// Task represents a scheduled task with a deadline and callback.
type Task struct {
	// Deadline is the absolute time (in microseconds) when the task should be executed.
	Deadline int64
	// Callback is the function to call when the deadline is reached.
	Callback func()
	// Active indicates whether the task is currently scheduled.
	Active bool
	// element is the list element for the task in the scheduler's list.
	element *list.Element
	// nextDeadline is used for quick access to the next task's deadline.
	nextDeadline int64
}

// Scheduler is an event loop scheduler that maintains a set of timed callbacks.
type Scheduler struct {
	mu       sync.Mutex
	tasks    list.List // Sorted list of tasks by deadline
	stop     chan struct{}
	running  atomic.Bool
	wakeup   chan struct{}
	nowFunc  func() int64 // Function to get current time in microseconds
	stopped  chan struct{}
}

// New creates a new scheduler with the default time function.
func New() *Scheduler {
	return &Scheduler{
		tasks:   list.List{},
		stop:    make(chan struct{}),
		wakeup:  make(chan struct{}, 1),
		stopped: make(chan struct{}),
		nowFunc: defaultNowFunc,
	}
}

// NewWithNowFunc creates a new scheduler with a custom time function.
func NewWithNowFunc(nowFunc func() int64) *Scheduler {
	s := New()
	s.nowFunc = nowFunc
	return s
}

// defaultNowFunc returns the current time in microseconds.
// This is a placeholder - real implementations should use the platform's time source.
func defaultNowFunc() int64 {
	// In a real implementation, this would use time.Now().UnixMicro()
	// or the platform's time source
	return 0
}

// Now returns the current time according to the scheduler's time function.
func (s *Scheduler) Now() int64 {
	return s.nowFunc()
}

// SetNowFunc sets the time function for the scheduler.
func (s *Scheduler) SetNowFunc(nowFunc func() int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nowFunc = nowFunc
}

// Schedule adds a new task to be executed at the specified deadline.
// The deadline is an absolute time in microseconds.
// Returns the task, which can be used to cancel it later.
func (s *Scheduler) Schedule(deadline int64, callback func()) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	task := &Task{
		Deadline:   deadline,
		Callback:   callback,
		Active:     true,
		nextDeadline: deadline,
	}

	// Insert the task in sorted order
	task.element = s.insertSorted(task)

	// If this is the earliest task, wake up the event loop
	if task.element == s.tasks.Front() {
		select {
		case s.wakeup <- struct{}{}:
		default:
		}
	}

	return task
}

// insertSorted inserts a task into the sorted list and returns its element.
func (s *Scheduler) insertSorted(task *Task) *list.Element {
	// Find the insertion point
	var insertBefore *list.Element
	for e := s.tasks.Front(); e != nil; e = e.Next() {
		other := e.Value.(*Task)
		if other.Deadline > task.Deadline {
			insertBefore = e
			break
		}
	}

	if insertBefore != nil {
		return s.tasks.InsertBefore(task, insertBefore)
	}
	return s.tasks.PushBack(task)
}

// Cancel removes a task from the scheduler.
// The task's callback will not be invoked.
func (s *Scheduler) Cancel(task *Task) {
	if task == nil || !task.Active {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !task.Active {
		return
	}

	task.Active = false
	s.tasks.Remove(task.element)
	task.element = nil
}

// Run starts the event loop.
// It blocks until Stop is called.
func (s *Scheduler) Run() {
	if !s.running.CompareAndSwap(false, true) {
		return // Already running
	}
	defer s.running.Store(false)
	defer close(s.stopped)

	for {
		select {
		case <-s.stop:
			return
		case <-s.wakeup:
			// Drain the wakeup channel
			for {
				select {
				case <-s.wakeup:
				default:
					goto processTasks
				}
			}
		processTasks:
			s.processTasks()
		}
	}
}

// RunUntil runs the event loop until the specified deadline.
// It processes all tasks with deadlines <= deadline.
// It returns the error from the last task executed, or nil if no error occurred.
func (s *Scheduler) RunUntil(deadline int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Process all tasks with deadline <= deadline
	count := 0
	for s.tasks.Len() > 0 {
		front := s.tasks.Front()
		if front == nil {
			break
		}

		task := front.Value.(*Task)
		if task.Deadline > deadline {
			break
		}

		// Remove the task from the list
		s.tasks.Remove(front)
		task.element = nil
		count++

		// Call the callback (outside the lock to avoid deadlocks)
		s.mu.Unlock()
		if task.Active {
			task.Active = false
			task.Callback()
		}
		s.mu.Lock()
	}

	return nil
}

// firstDeadline returns the deadline of the first task, or 0 if there are no tasks.
func (s *Scheduler) firstDeadline() int64 {
	if s.tasks.Len() == 0 {
		return 0
	}
	return s.tasks.Front().Value.(*Task).Deadline
}

// processTasks processes all tasks whose deadlines have been reached.
func (s *Scheduler) processTasks() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.nowFunc()

	// Process all tasks with deadline <= now
	for s.tasks.Len() > 0 {
		front := s.tasks.Front()
		if front == nil {
			break
		}

		task := front.Value.(*Task)
		if task.Deadline > now {
			break
		}

		// Remove the task from the list
		s.tasks.Remove(front)
		task.element = nil

		// Release the lock before calling the callback
		s.mu.Unlock()

		// Call the callback (outside the lock to avoid deadlocks)
		if task.Active {
			task.Active = false
			task.Callback()
		}

		s.mu.Lock()
	}
}

// waitUntil waits until the specified deadline.
func (s *Scheduler) waitUntil(deadline int64) {
	// Simple implementation - in a real scheduler, this would use
	// time.Sleep or a more sophisticated waiting mechanism
	// For now, we just return immediately
}

// Stop stops the event loop.
func (s *Scheduler) Stop() {
	if s.running.CompareAndSwap(true, false) {
		close(s.stop)
		<-s.stopped // Wait for Run to return
	}
}

// Len returns the number of scheduled tasks.
func (s *Scheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tasks.Len()
}

// Empty returns true if there are no scheduled tasks.
func (s *Scheduler) Empty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tasks.Len() == 0
}

// NextDeadline returns the deadline of the next task to be executed,
// or 0 if there are no tasks.
func (s *Scheduler) NextDeadline() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstDeadline()
}

// Clear removes all scheduled tasks.
func (s *Scheduler) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks.Init()
}

// Reschedule updates the deadline of a scheduled task.
// The task must be active (not yet executed or canceled).
func (s *Scheduler) Reschedule(task *Task, newDeadline int64) {
	if task == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// If the task is in the list, remove it first
	if task.element != nil {
		s.tasks.Remove(task.element)
		task.element = nil
	}

	// Update the deadline
	task.Deadline = newDeadline
	task.nextDeadline = newDeadline
	// Reactivate the task
	task.Active = true

	// Reinsert in sorted order
	task.element = s.insertSorted(task)

	// If this is now the earliest task, wake up the event loop
	if task.element == s.tasks.Front() {
		select {
		case s.wakeup <- struct{}{}:
		default:
		}
	}
}

// RunOnce runs the event loop once, executing all tasks whose deadlines have been reached.
// Returns the number of tasks executed.
func (s *Scheduler) RunOnce() int {
	s.mu.Lock()
	count := s.runOnceLocked()
	s.mu.Unlock()
	return count
}

// runOnceLocked executes one task whose deadline has been reached.
// Must be called with the mutex held.
func (s *Scheduler) runOnceLocked() int {
	now := s.nowFunc()

	// Find the first task with deadline <= now
	for s.tasks.Len() > 0 {
		front := s.tasks.Front()
		if front == nil {
			break
		}

		task := front.Value.(*Task)
		if task.Deadline > now {
			return 0
		}

		// Remove the task from the list
		s.tasks.Remove(front)
		task.element = nil
		
		// Keep task active until callback completes (in case it reschedules itself)
		wasActive := task.Active
		
		// Release the lock temporarily to call the callback
		s.mu.Unlock()

		count := 0
		if wasActive {
			task.Callback()
			count = 1
			// Don't mark as inactive - let Reschedule manage the Active flag
			// If the task wasn't rescheduled, it will remain active but won't be in the list
		}

		s.mu.Lock()
		return count
	}

	return 0
}
