package cy

import (
	"strings"
	"testing"
)

// TestDiagAddRemove verifies that listeners are dispatched while installed and
// not dispatched after removal, and that duplicate installs are a no-op.
func TestDiagAddRemove(t *testing.T) {
	cy, _ := newTestCy(t)

	var count int
	d := &Diag{VTable: &DiagVTable{
		AsyncError: func(_ *Diag, _ *Topic, _ Error, _ string) { count++ },
	}}
	cy.DiagAdd(d)
	cy.DiagAsyncError(nil, ErrMemory)
	if count != 1 {
		t.Fatalf("expected 1 dispatch after add, got %d", count)
	}

	// Duplicate add must not double-register.
	cy.DiagAdd(d)
	cy.DiagAsyncError(nil, ErrMemory)
	if count != 2 {
		t.Fatalf("expected 2 dispatches (duplicate add is no-op), got %d", count)
	}

	cy.DiagRemove(d)
	cy.DiagAsyncError(nil, ErrMemory)
	if count != 2 {
		t.Fatalf("expected no dispatch after remove, got %d", count)
	}

	// Removing twice is a no-op.
	cy.DiagRemove(d)
	cy.DiagAsyncError(nil, ErrMemory)
	if count != 2 {
		t.Fatalf("expected no dispatch after second remove, got %d", count)
	}
}

// TestDiagAsyncErrorDispatch verifies the async-error callback receives the
// correct error code and an identifying call site.
func TestDiagAsyncErrorDispatch(t *testing.T) {
	cy, _ := newTestCy(t)

	var (
		gotErr   Error
		gotSite  string
		gotTopic *Topic
	)
	cy.DiagAdd(&Diag{VTable: &DiagVTable{
		AsyncError: func(_ *Diag, topic *Topic, err Error, site string) {
			gotErr = err
			gotSite = site
			gotTopic = topic
		},
	}})

	// OK must be ignored (no dispatch).
	cy.DiagAsyncError(nil, OK)
	if gotErr != 0 {
		t.Fatalf("OK should not dispatch, got err=%v", gotErr)
	}

	cy.DiagAsyncError(nil, ErrMemory)
	if gotErr != ErrMemory {
		t.Errorf("got err %v, want ErrMemory", gotErr)
	}
	if gotTopic != nil {
		t.Errorf("got topic %v, want nil", gotTopic)
	}
	if gotSite == "" {
		t.Errorf("expected non-empty call site")
	}
	if !strings.Contains(gotSite, "diag_internal_test.go") {
		t.Errorf("expected site to reference the test file, got %q", gotSite)
	}
}

// TestDiagTopicCreatedAndReallocated verifies that creating a topic fires both
// TopicCreated and TopicReallocated with the committed allocation state.
func TestDiagTopicCreatedAndReallocated(t *testing.T) {
	cy, _ := newTestCy(t)

	var (
		created   *Topic
		reallocID uint32
		reallocEv uint32
	)
	cy.DiagAdd(&Diag{VTable: &DiagVTable{
		TopicCreated: func(_ *Diag, topic *Topic) { created = topic },
		TopicReallocated: func(_ *Diag, topic *Topic, subjectID uint32, evictions uint32) {
			reallocID = subjectID
			reallocEv = evictions
		},
	}})

	topic, err := cy.FindOrCreateTopic("diagnostics/created", 64)
	if err != nil {
		t.Fatalf("FindOrCreateTopic failed: %v", err)
	}
	if created != topic {
		t.Errorf("TopicCreated did not fire for the created topic")
	}
	if reallocID != topic.SubjectID() || reallocEv != topic.Evictions() {
		t.Errorf("TopicReallocated state mismatch: got (%d,%d) want (%d,%d)",
			reallocID, reallocEv, topic.SubjectID(), topic.Evictions())
	}
}

// TestDiagTopicDestroyed verifies destruction is reported.
func TestDiagTopicDestroyed(t *testing.T) {
	cy, _ := newTestCy(t)

	var destroyed *Topic
	cy.DiagAdd(&Diag{VTable: &DiagVTable{
		TopicDestroyed: func(_ *Diag, topic *Topic) { destroyed = topic },
	}})

	topic, err := cy.FindOrCreateTopic("diagnostics/destroy", 64)
	if err != nil {
		t.Fatalf("FindOrCreateTopic failed: %v", err)
	}
	cy.mu.Lock()
	cy.destroyTopic(topic)
	cy.mu.Unlock()

	if destroyed != topic {
		t.Errorf("TopicDestroyed did not fire for the destroyed topic")
	}
}

// TestDiagGossipProcessed verifies a processed gossip is reported even when there
// is no locally known topic (topic == nil).
func TestDiagGossipProcessed(t *testing.T) {
	cy, _ := newTestCy(t)

	var (
		gpTopic *Topic
		gpName  string
		gpHash  uint64
	)
	cy.DiagAdd(&Diag{VTable: &DiagVTable{
		GossipProcessed: func(_ *Diag, topic *Topic, name string, hash uint64) {
			gpTopic = topic
			gpName = name
			gpHash = hash
		},
	}})

	const hash = uint64(0xABCDEF)
	cy.mu.Lock()
	cy.gossip.ProcessGossip(0, hash, 0, []byte("some/topic"), 0, gossipBroadcast, cy.Now())
	cy.mu.Unlock()

	if gpTopic != nil {
		t.Errorf("expected nil topic for unknown gossip, got %v", gpTopic)
	}
	if gpName != "some/topic" {
		t.Errorf("got name %q, want %q", gpName, "some/topic")
	}
	if gpHash != hash {
		t.Errorf("got hash %d, want %d", gpHash, hash)
	}
}

// TestDiagAsyncErrorOnScoutFailure verifies that a failed scout (resubscription)
// during a consensus update surfaces an async error and keeps the pattern in the
// pending-scout set for retry (C: ON_ASYNC_ERROR_IF after do_send_scout).
func TestDiagAsyncErrorOnScoutFailure(t *testing.T) {
	cy, plat := newTestCy(t)
	plat.failSends = true

	var asyncErr Error
	cy.DiagAdd(&Diag{VTable: &DiagVTable{
		AsyncError: func(_ *Diag, _ *Topic, err Error, _ string) { asyncErr = err },
	}})

	if _, err := cy.Subscribe("diag/pat*", 64); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if asyncErr != ErrMedia {
		t.Errorf("expected ErrMedia async error on scout failure, got %v", asyncErr)
	}
	if pending := cy.PendingScouts(); len(pending) == 0 {
		t.Errorf("expected the pattern to remain pending after a failed scout")
	}
}
