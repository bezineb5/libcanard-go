// Additional tests for the faithful AVL-based scheduler: FIFO tie-break for
// equal deadlines and worst-lateness tracking in the spin result.

package tests

import (
	"testing"

	"github.com/opencyphal/cy-go/olga"
)

// TestSchedulerFIFOTieBreak verifies that tasks scheduled at the same deadline
// are executed in the order they were scheduled (FIFO), faithful to the C
// (deadline, seqno) ordering with a monotonic sequence number.
func TestSchedulerFIFOTieBreak(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	var order []int
	for i := 0; i < 5; i++ {
		i := i
		// All scheduled at the same deadline with distinct registration order.
		s.Schedule(100, func() {
			order = append(order, i)
		})
	}

	mockNow = 100
	res, err := s.Spin()
	if err != nil {
		t.Fatalf("Spin returned error: %v", err)
	}
	if len(order) != 5 {
		t.Fatalf("expected 5 calls, got %d", len(order))
	}
	for i := range order {
		if order[i] != i {
			t.Errorf("FIFO violated: position %d executed task %d (want %d)", i, order[i], i)
		}
	}
	// Executed on time => no lateness.
	if res.WorstLateness != 0 {
		t.Errorf("expected worst lateness 0, got %d", res.WorstLateness)
	}
}

// TestSchedulerWorstLateness verifies that Spin reports the maximum lateness
// observed across executed tasks, faithful to olga_spin_result_t.worst_lateness.
func TestSchedulerWorstLateness(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	var count int
	s.Schedule(10, func() { count++ })
	s.Schedule(20, func() { count++ })
	s.Schedule(30, func() { count++ })

	// Run everything at t=100: lateness = 100-deadline => 90, 80, 70.
	mockNow = 100
	res, err := s.Spin()
	if err != nil {
		t.Fatalf("Spin returned error: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 calls, got %d", count)
	}
	if res.WorstLateness != 90 {
		t.Errorf("expected worst lateness 90, got %d", res.WorstLateness)
	}
	if res.Now != 100 {
		t.Errorf("expected Now 100, got %d", res.Now)
	}
	if res.NextDeadline != maxInt64Sentinel {
		t.Errorf("expected empty NextDeadline sentinel, got %d", res.NextDeadline)
	}
}

// TestSchedulerSpinUntilBound verifies that SpinUntil does not execute tasks
// beyond the provided deadline bound even when they are due.
func TestSchedulerSpinUntilBound(t *testing.T) {
	mockNow = 0
	s := olga.NewWithNowFunc(mockNowFunc)

	var before, after int
	s.Schedule(10, func() { before++ })
	s.Schedule(50, func() { after++ })

	// Run until t=30 only.
	mockNow = 100
	res, err := s.SpinUntil(30)
	if err != nil {
		t.Fatalf("SpinUntil returned error: %v", err)
	}
	if before != 1 {
		t.Errorf("expected the due-before-30 task to run, got %d", before)
	}
	if after != 0 {
		t.Errorf("expected the after-30 task to NOT run, got %d", after)
	}
	if res.NextDeadline != 50 {
		t.Errorf("expected next deadline 50, got %d", res.NextDeadline)
	}
	if s.Len() != 1 {
		t.Errorf("expected 1 remaining task, got %d", s.Len())
	}
}

// maxInt64Sentinel mirrors the sentinel used by the scheduler for an empty queue.
const maxInt64Sentinel = int64(^uint64(0) >> 1)
