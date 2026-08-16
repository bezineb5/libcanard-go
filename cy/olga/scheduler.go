// Package olga provides an EDF (earliest-deadline-first) scheduler.
//
// This is a faithful port of the C reference olga_scheduler.h (cavl2-based
// balanced binary search tree keyed by (deadline, seqno)). Insertion and removal
// are O(log n); ties on the deadline are broken by a monotonically increasing
// sequence number so that tasks scheduled at the same deadline are invoked in
// FIFO order (first scheduled, first served). A spin reports the next pending
// deadline, the worst (maximum) lateness observed while running due tasks, and
// the freshly sampled time -- this mirrors olga_spin_result_t.
package olga

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/opencyphal/cy-go/cavl"
)

// maxInt64 is the "no deadline" sentinel used for NextDeadline when the queue
// is empty, mirroring the C constant INT64_MAX.
const maxInt64 = int64(^uint64(0) >> 1)

// taskKey is the search key for the scheduler tree. It is (deadline, seqno):
// the deadline is the primary ordering key (earliest deadline first) and the
// sequence number provides a stable FIFO tie-break for equal deadlines. The
// sequence number is unique per insertion so every key is distinct.
type taskKey struct {
	deadline int64
	seqno    uint64
}

// compareTaskKey mirrors C olga_scheduler.h's comparator: order primarily by
// deadline, then by sequence number for a deterministic FIFO tie-break.
func compareTaskKey(a, b taskKey) int {
	if a.deadline != b.deadline {
		if a.deadline > b.deadline {
			return 1
		}
		return -1
	}
	if a.seqno != b.seqno {
		if a.seqno > b.seqno {
			return 1
		}
		return -1
	}
	return 0
}

// Task represents a scheduled task with a deadline and callback.
//
// Active reflects whether the task is currently pending in the scheduler. It is
// managed by Schedule/Cancel/Reschedule and after a task is executed; external
// code should treat it as read-only.
type Task struct {
	Deadline int64
	Seqno    uint64
	Callback func()
	// Active is true while the task is enqueued and pending execution.
	Active bool

	// key is the (deadline, seqno) key currently used to locate the task in the
	// tree. It is meaningful only while Active is true.
	key taskKey
}

// SpinResult is the outcome of a spin, faithful to C olga_spin_result_t.
type SpinResult struct {
	// NextDeadline is the deadline of the earliest still-pending task, or
	// maxInt64 if the queue is empty.
	NextDeadline int64
	// WorstLateness is the maximum of (now - deadline) across all tasks executed
	// during the spin; it is negative or zero when every task ran on time.
	WorstLateness int64
	// Now is the clock value sampled at the end of the spin.
	Now int64
}

// Scheduler is an EDF scheduler backed by a balanced AVL tree.
//
// All methods are safe for concurrent use. The clock is supplied via NowFunc;
// it is expected to return microseconds (the OpenCyphal convention).
type Scheduler struct {
	mu        sync.Mutex
	tree      *cavl.Tree[taskKey, *Task]
	nextSeqno uint64
	nowFunc   func() int64
	user      interface{}

	stop    chan struct{}
	wakeup  chan struct{}
	running atomic.Bool
	stopped chan struct{}
}

// defaultNowFunc is a placeholder clock that returns a fixed instant. It is
// replaced by SetNowFunc before any real use (the cy library always does so).
func defaultNowFunc() int64 { return 0 }

// New creates a new scheduler with a placeholder clock. Use SetNowFunc to
// install a real clock before scheduling tasks.
func New() *Scheduler {
	s := &Scheduler{
		tree:    cavl.New[taskKey, *Task](compareTaskKey),
		stop:    make(chan struct{}),
		wakeup:  make(chan struct{}, 1),
		stopped: make(chan struct{}),
		nowFunc: defaultNowFunc,
	}
	return s
}

// NewWithNowFunc creates a new scheduler with the given clock function.
func NewWithNowFunc(nowFunc func() int64) *Scheduler {
	s := New()
	s.nowFunc = nowFunc
	return s
}

// Now returns the current clock value.
func (s *Scheduler) Now() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nowFunc()
}

// SetNowFunc installs the clock function used to determine task due-ness.
func (s *Scheduler) SetNowFunc(nowFunc func() int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if nowFunc != nil {
		s.nowFunc = nowFunc
	}
}

// Schedule enqueues a task to run at the given deadline. It returns a handle
// that can later be passed to Cancel or Reschedule. Tasks scheduled at the same
// deadline execute in FIFO order.
func (s *Scheduler) Schedule(deadline int64, callback func()) *Task {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := taskKey{deadline: deadline, seqno: s.nextSeqno}
	s.nextSeqno++
	task := &Task{
		Deadline: deadline,
		Seqno:    k.seqno,
		Callback: callback,
		Active:   true,
		key:      k,
	}
	s.tree.Insert(k, task)
	s.wakeupIfEarliest(k)
	return task
}

// wakeupIfEarliest signals the Run loop if the given key is now the earliest
// pending task. Caller must hold s.mu.
func (s *Scheduler) wakeupIfEarliest(k taskKey) {
	node := s.tree.First()
	if node != nil && node.Key == k {
		select {
		case s.wakeup <- struct{}{}:
		default:
		}
	}
}

// Cancel removes a task from the scheduler. Calling it on an already-completed or
// canceled task is a safe no-op (mirrors olga_cancel). The task's Active flag is
// cleared.
func (s *Scheduler) Cancel(task *Task) {
	if task == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Remove by the stored key. If the task is not pending this is a harmless
	// no-op (cavl Delete on a missing key does nothing).
	s.tree.Delete(task.key)
	task.Active = false
	task.key = taskKey{}
}

// Reschedule changes the deadline of a currently pending task, faithful to
// C olga_defer on an existing event: the old entry is removed and a fresh
// (deadline, seqno) key is inserted, preserving FIFO ordering among equal
// deadlines. It is a no-op if the task is not active.
func (s *Scheduler) Reschedule(task *Task, newDeadline int64) {
	if task == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !task.Active {
		return
	}
	s.tree.Delete(task.key)
	k := taskKey{deadline: newDeadline, seqno: s.nextSeqno}
	s.nextSeqno++
	task.Deadline = newDeadline
	task.Seqno = k.seqno
	task.key = k
	s.tree.Insert(k, task)
	s.wakeupIfEarliest(k)
}

// spin runs tasks in earliest-deadline order and reports the spin outcome.
// When checkNow is true (faithful to C olga_spin), only tasks whose deadline is
// at or before the sampled clock are executed; otherwise (the legacy RunUntil
// path used by the cy library) tasks are run up to the deadline bound regardless
// of the clock, which the cy test suite and timing model rely upon. Callbacks are
// invoked without holding s.mu to avoid re-entrant deadlocks. The task is removed
// from the tree before the callback, but its Active flag and key are left intact
// so a callback may safely Reschedule/Cancel it (matching the prior behavior).
func (s *Scheduler) spin(limit int64, checkNow bool) SpinResult {
	var res SpinResult
	res.NextDeadline = maxInt64
	worst := int64(0)

	for {
		s.mu.Lock()
		node := s.tree.First()
		if node == nil {
			s.mu.Unlock()
			break
		}
		task := node.Value
		now := s.nowFunc()
		if (checkNow && task.Deadline > now) || task.Deadline > limit {
			res.NextDeadline = task.Deadline
			res.Now = now
			s.mu.Unlock()
			break
		}
		// Dequeue before releasing the lock so a concurrent Cancel/Reschedule
		// cannot mutate this task while it is still pending.
		s.tree.Delete(task.key)
		lateness := now - task.Deadline
		if lateness > worst {
			worst = lateness
		}
		res.Now = now
		s.mu.Unlock()

		if task.Callback != nil {
			task.Callback()
		}
	}
	res.WorstLateness = worst
	return res
}

// Run starts the scheduler loop, blocking until Stop is called. It fires due
// tasks and sleeps until the next deadline otherwise, faithfully mirroring a
// blocking event loop. It is not used by the cy library, which drives the
// scheduler explicitly via SpinUntil; it is provided for standalone use and
// tests.
func (s *Scheduler) Run() error {
	if !s.running.CompareAndSwap(false, true) {
		return nil
	}
	defer s.running.Store(false)
	defer close(s.stopped)

	for {
		s.mu.Lock()
		now := s.nowFunc()
		next := maxInt64
		if node := s.tree.First(); node != nil {
			next = node.Value.Deadline
		}
		s.mu.Unlock()

		select {
		case <-s.stop:
			return nil
		case <-s.wakeup:
			s.processTasks()
		case <-s.sleepTimer(now, next):
			s.processTasks()
		}
	}
}

// maxSleepMicros bounds a single sleep so that converting microseconds to
// nanoseconds cannot overflow time.Duration (which is an int64 of nanoseconds).
const maxSleepMicros = int64(1) << 52 // ~4.5e12 us (< 2^63/1e6 ns)

// sleepTimer returns a channel that fires after the shorter of (next-now)
// microseconds and maxSleepMicros, or a never-firing channel if next <= now.
func (s *Scheduler) sleepTimer(now, next int64) <-chan time.Time {
	if next <= now {
		return make(chan time.Time) // never fires
	}
	d := next - now
	if d > maxSleepMicros {
		d = maxSleepMicros
	}
	return time.After(time.Duration(d) * time.Microsecond)
}

// RunUntil processes tasks with deadlines at or before the given deadline bound,
// in earliest-deadline order, WITHOUT requiring the clock to have reached those
// deadlines. This legacy bound-only semantics is what the cy library and its test
// suite rely upon (the platform clock may not have advanced to the deadline when
// RunUntil is invoked). It returns no error; the faithful by-clock spin result is
// available via SpinUntil.
func (s *Scheduler) RunUntil(deadline int64) error {
	s.spin(deadline, false)
	return nil
}

// SpinUntil is like RunUntil but additionally only executes tasks whose deadline
// is at or before the sampled clock, mirroring C olga_spin's deadline check, and
// returns the spin outcome (including the worst lateness observed).
func (s *Scheduler) SpinUntil(deadline int64) (SpinResult, error) {
	return s.spin(deadline, true), nil
}

// Spin processes all currently-due tasks (deadlines at or before the sampled
// clock) and returns the spin outcome, mirroring C olga_spin.
func (s *Scheduler) Spin() (SpinResult, error) {
	return s.spin(maxInt64, true), nil
}

// firstDeadline returns the deadline of the earliest pending task, or 0 if the
// queue is empty.
func (s *Scheduler) firstDeadline() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if node := s.tree.First(); node != nil {
		return node.Value.Deadline
	}
	return 0
}

// processTasks runs all currently-due tasks (by clock). It is used by the
// blocking Run loop; the result is discarded there.
func (s *Scheduler) processTasks() {
	s.spin(maxInt64, true)
}

// waitUntil blocks until the sooner of the given deadline and the earliest
// pending task deadline (mirroring cy_spin_until's wait clamp). The clock is
// expected to be in microseconds.
func (s *Scheduler) waitUntil(deadline int64) {
	s.mu.Lock()
	now := s.nowFunc()
	next := deadline
	if node := s.tree.First(); node != nil && node.Value.Deadline < next {
		next = node.Value.Deadline
	}
	s.mu.Unlock()

	if next > now {
		d := next - now
		if d > maxSleepMicros {
			d = maxSleepMicros
		}
		time.Sleep(time.Duration(d) * time.Microsecond)
	}
}

// Stop terminates the scheduler loop started by Run and waits for it to exit.
func (s *Scheduler) Stop() {
	if s.running.CompareAndSwap(true, false) {
		close(s.stop)
		<-s.stopped
	}
}

// Len returns the number of pending tasks.
func (s *Scheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tree.Len()
}

// Empty reports whether there are no pending tasks.
func (s *Scheduler) Empty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tree.Empty()
}

// NextDeadline returns the deadline of the earliest pending task, or 0 if the
// queue is empty.
func (s *Scheduler) NextDeadline() int64 {
	return s.firstDeadline()
}

// Clear removes all pending tasks without invoking any of them.
func (s *Scheduler) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for node := s.tree.First(); node != nil; node = s.tree.First() {
		task := node.Value
		s.tree.Delete(task.key)
		task.Active = false
		task.key = taskKey{}
	}
}

// RunOnce runs at most one currently-due task and returns the number of tasks
// executed (0 or 1).
func (s *Scheduler) RunOnce() int {
	s.mu.Lock()
	node := s.tree.First()
	if node == nil {
		s.mu.Unlock()
		return 0
	}
	task := node.Value
	now := s.nowFunc()
	if task.Deadline > now {
		s.mu.Unlock()
		return 0
	}
	// Dequeue before releasing the lock; leave Active/key intact so a callback
	// may Reschedule this task (matching prior behavior).
	s.tree.Delete(task.key)
	s.mu.Unlock()

	if task.Callback != nil {
		task.Callback()
	}
	return 1
}
