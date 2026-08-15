package cy

import (
	"testing"

	"github.com/opencyphal/cy-go/olga"
)

// newTestSubscriberCy builds a minimal Cy sufficient for exercising subscriber
// delivery/reordering without a real transport.
func newTestSubscriberCy() *Cy {
	return &Cy{olga: olga.New()}
}

func TestReorderingStateInOrderDelivery(t *testing.T) {
	sub := newSubscriber(newTestSubscriberCy(), &Topic{name: "a", hash: 1}, 256, 100000)
	var got []uint64
	sub.SetCallback(func(a *Arrival) { got = append(got, a.Breadcrumb.MessageTag) })

	rr := &reorderingState{
		subscriber:  sub,
		remoteID:    1,
		topicHash:   1,
		slots:       make(map[uint64]*reorderingSlot),
		tagBaseline: 0, // identity mapping for a deterministic test
	}
	msg := func(tag uint64) MessageTS {
		return MessageTS{Timestamp: Microsecond(tag), Content: NewMessage([]byte{byte(tag)})}
	}

	if !rr.push(1, PriorityNominal, msg(1)) {
		t.Fatal("tag 1 should be accepted (fast path)")
	}
	if !rr.push(3, PriorityNominal, msg(3)) {
		t.Fatal("tag 3 should be accepted (interned; gap at 2)")
	}
	if !rr.push(2, PriorityNominal, msg(2)) {
		t.Fatal("tag 2 should be accepted (closes the gap and flushes 3)")
	}
	if rr.push(2, PriorityNominal, msg(2)) {
		t.Fatal("tag 2 duplicate should be late-dropped")
	}
	if rr.push(1, PriorityNominal, msg(1)) {
		t.Fatal("tag 1 older than last ejected should be late-dropped")
	}

	want := []uint64{1, 2, 3}
	if len(got) != len(want) {
		t.Fatalf("delivered %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("delivered %v, want %v", got, want)
		}
	}
}

func TestReorderingLateDropNoAck(t *testing.T) {
	sub := newSubscriber(newTestSubscriberCy(), &Topic{name: "a", hash: 1}, 256, 100000)
	rr := &reorderingState{
		subscriber:  sub,
		remoteID:    1,
		topicHash:   1,
		slots:       make(map[uint64]*reorderingSlot),
		tagBaseline: 0,
	}
	msg := func(tag uint64) MessageTS {
		return MessageTS{Timestamp: Microsecond(tag), Content: NewMessage([]byte{byte(tag)})}
	}
	// Deliver 1, 2, 3 in order.
	if !rr.push(1, PriorityNominal, msg(1)) {
		t.Fatal("tag 1")
	}
	if !rr.push(2, PriorityNominal, msg(2)) {
		t.Fatal("tag 2")
	}
	if !rr.push(3, PriorityNominal, msg(3)) {
		t.Fatal("tag 3")
	}
	// Tag 1 again is a late drop and must NOT be acknowledged.
	if rr.push(1, PriorityNominal, msg(1)) {
		t.Fatal("re-arrived old tag must be late-dropped (no ack)")
	}
}

func TestArrivalMoveAndCount(t *testing.T) {
	sub := newSubscriber(newTestSubscriberCy(), &Topic{name: "a", hash: 1}, 256, -1)
	f := sub.Future()
	if f.Done() {
		t.Fatal("future must not be done before any message")
	}
	if f.ArrivalCount() != 0 {
		t.Fatal("count should start at zero")
	}

	sub.notify(&Arrival{
		Message:    MessageTS{Timestamp: 100, Content: NewMessage([]byte{1})},
		Breadcrumb: Breadcrumb{Cy: sub.cy, MessageTag: 7},
	})
	if !f.Done() {
		t.Fatal("future must be done after an arrival")
	}
	if f.ArrivalCount() != 1 {
		t.Fatalf("count = %d, want 1", f.ArrivalCount())
	}
	a := f.ArrivalMove()
	if a == nil || a.Breadcrumb.MessageTag != 7 {
		t.Fatalf("ArrivalMove returned %v", a)
	}
	// Consuming the arrival flips the sampling-port future back to pending.
	if f.Done() {
		t.Fatal("future must flip back to pending after ArrivalMove")
	}
	if f.ArrivalCount() != 1 {
		t.Fatalf("count = %d, want 1 after move", f.ArrivalCount())
	}
}

func TestLivenessMonitoring(t *testing.T) {
	sub := newSubscriber(newTestSubscriberCy(), &Topic{name: "a", hash: 1}, 256, -1)
	f := sub.Future()
	sub.SetLivenessTimeout(1000)

	// While the timer is armed and no message has arrived, the future is pending.
	if f.Done() {
		t.Fatal("future must be pending while liveness armed and no message")
	}
	if f.Error() != OK {
		t.Fatalf("error = %v, want OK", f.Error())
	}

	// Simulate the liveness timer firing.
	sub.onLivenessTimeout()
	if !f.Done() {
		t.Fatal("future must be done after liveness timeout")
	}
	if f.Error() != ErrLiveness {
		t.Fatalf("error = %v, want ErrLiveness", f.Error())
	}

	// A fresh message clears the error and the future is done again.
	sub.notify(&Arrival{
		Message:    MessageTS{Timestamp: 5000, Content: NewMessage([]byte{2})},
		Breadcrumb: Breadcrumb{Cy: sub.cy, MessageTag: 9},
	})
	if f.Error() != OK {
		t.Fatalf("error = %v, want OK after message", f.Error())
	}
	if !f.Done() {
		t.Fatal("future must be done after a fresh message")
	}
}

func TestSubscriberNameAndSubstitutions(t *testing.T) {
	cy := newTestSubscriberCy()

	ps := newPatternSubscriber(cy, "sensors/>", 256)
	if !ps.IsPattern() {
		t.Fatal("expected pattern subscriber")
	}
	if ps.Name() != "sensors/>" {
		t.Fatalf("Name() = %q, want %q", ps.Name(), "sensors/>")
	}
	if ps.Pattern() != "sensors/>" {
		t.Fatal("Pattern() mismatch")
	}
	if !ps.IsOrdered() {
		// pattern subscribers are unordered by default
	}
	ok, subs := ps.Match("sensors/a/b/c")
	if !ok || len(subs) != 1 || subs[0] != "a/b/c" {
		t.Fatalf("Match sensors/> against sensors/a/b/c = %v %v", ok, subs)
	}
	// Non-trailing multi-wildcard: a/>/c matches a/b/d/c with substitution b/d.
	ok, subs = MatchPattern("a/>/c", "a/b/d/c")
	if !ok || len(subs) != 1 || subs[0] != "b/d" {
		t.Fatalf("Match a/>/c against a/b/d/c = %v %v", ok, subs)
	}
	if ps.Substitutions(&Topic{name: "sensors/a/b/c", hash: 1}) == nil {
		t.Fatal("Substitutions should be non-nil for a pattern subscriber")
	}

	// Verbatim subscriber.
	vs := newSubscriber(cy, &Topic{name: "x/y", hash: 2}, 256, -1)
	if vs.IsPattern() {
		t.Fatal("verbatim subscriber must not report as pattern")
	}
	if vs.Name() != "x/y" {
		t.Fatalf("Name() = %q, want %q", vs.Name(), "x/y")
	}
	if vs.Substitutions(&Topic{name: "x/y", hash: 2}) != nil {
		t.Fatal("Substitutions must be nil for a verbatim subscriber")
	}
	if ok, _ := vs.Match("x/y"); !ok {
		t.Fatal("verbatim subscriber must match its own topic name")
	}
}

func TestTopicReliableDedup(t *testing.T) {
	topic := &Topic{name: "a", hash: 1, dedup: make(map[uint64]*dedupState)}
	if topic.dedupCheck(1, 5, 0) {
		t.Fatal("empty dedup must not report a hit")
	}
	topic.dedupCommit(1, 5, 0)
	if !topic.dedupCheck(1, 5, 0) {
		t.Fatal("committed tag 5 must be a duplicate")
	}
	topic.dedupCommit(1, 6, 0)
	if !topic.dedupCheck(1, 6, 0) {
		t.Fatal("tag 6 must be a duplicate")
	}
	// Tag 5 is still within the window (rev 1 relative to frontier 6).
	if !topic.dedupCheck(1, 5, 0) {
		t.Fatal("tag 5 must still be in the window")
	}
	// Tag 4 was never received, so it is not a duplicate even though it is in range.
	if topic.dedupCheck(1, 4, 0) {
		t.Fatal("tag 4 was never received and must not be a duplicate")
	}
	// Jump far ahead: the window resets and only the new tag is remembered.
	topic.dedupCommit(1, 600, 0)
	if topic.dedupCheck(1, 6, 0) {
		t.Fatal("tag 6 must be forgotten after a far-forward commit")
	}
	if !topic.dedupCheck(1, 600, 0) {
		t.Fatal("tag 600 must be a duplicate")
	}
	// Different remote has independent state.
	if topic.dedupCheck(2, 5, 0) {
		t.Fatal("remote 2 must have independent dedup state")
	}
}
