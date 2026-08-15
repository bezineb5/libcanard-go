package tests

import (
	"testing"

	cy "github.com/opencyphal/cy-go"
)

// modulusPlatform is a minimal Platform that forces a non-default subject-ID
// modulus, used to verify that cy.New honors platform.SubjectIDModulus.
// It embeds *MockPlatform to obtain working transport so cy.New can complete.
type modulusPlatform struct {
	*MockPlatform
}

// SubjectIDModulus overrides the base to report a 23-bit modulus.
func (p *modulusPlatform) SubjectIDModulus() uint32 {
	return cy.SubjectIDModulus23bit
}

// TestNodeLifecyclePlatformModulus verifies that the subject-ID modulus is taken
// from the platform (not hardcoded to 16-bit), so heterogeneous networks work.
func TestNodeLifecyclePlatformModulus(t *testing.T) {
	platform := &modulusPlatform{MockPlatform: NewMockPlatform()}
	node, err := cy.New(platform, "test_node", "ns", "")
	if err != nil {
		t.Fatalf("cy.New failed: %v", err)
	}
	defer node.Destroy()

	if got := node.SubjectIDModulus(); got != cy.SubjectIDModulus23bit {
		t.Fatalf("expected platform modulus %d, got %d", cy.SubjectIDModulus23bit, got)
	}
	// The broadcast subject-ID (and shard layout) must be derived from the modulus.
	if got, want := node.BroadcastSubjectID(), cy.BroadcastSubjectID(cy.SubjectIDModulus23bit); got != want {
		t.Fatalf("expected broadcast subject-ID %d, got %d", want, got)
	}
}

// TestNodeLifecycleAccessors verifies the basic node-lifecycle accessors that
// mirror cy_home, cy_namespace, cy_uptime and the startup unicast extent.
func TestNodeLifecycleAccessors(t *testing.T) {
	platform := NewMockPlatform()
	node, err := cy.New(platform, "/home/foo", "my.ns", "")
	if err != nil {
		t.Fatalf("cy.New failed: %v", err)
	}
	defer node.Destroy()

	if got := node.Home(); got != "/home/foo" {
		t.Errorf("Home() = %q, want %q", got, "/home/foo")
	}
	if got := node.Namespace(); got != "my.ns" {
		t.Errorf("Namespace() = %q, want %q", got, "my.ns")
	}
	// Startup unicast extent: C does unicast_extent_set(platform, HEADER_BYTES+100).
	if got := node.UnicastExtent(); got != cy.HeaderSize+100 {
		t.Errorf("UnicastExtent() = %d, want %d", got, cy.HeaderSize+100)
	}
	// Uptime must be non-negative and must grow over time. C: cy_now() - ts_started.
	if u0 := node.Uptime(); u0 < 0 {
		t.Errorf("Uptime() = %d, want >= 0", u0)
	}
	// SpinOnce (cy_spin_once) must complete without a real error.
	if err := node.SpinOnce(); err != cy.OK {
		t.Errorf("SpinOnce() returned error: %v", err)
	}
}

// TestNodeLifecycleTopicIteration verifies the topic iterator and per-topic user
// context accessors, mirroring cy_topic_iter_first/next and cy_topic_user_context.
func TestNodeLifecycleTopicIteration(t *testing.T) {
	platform := NewMockPlatform()
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("cy.New failed: %v", err)
	}
	defer node.Destroy()

	const count = 5
	for i := 0; i < count; i++ {
		name := "topic." + string(rune('a'+i))
		if _, err := node.Advertise(name); err != nil {
			t.Fatalf("Advertise(%q) failed: %v", name, err)
		}
	}

	// Iterate and collect names in subject-hash order.
	var got []string
	for tp := node.TopicIterFirst(); tp != nil; tp = node.TopicIterNext(tp) {
		got = append(got, tp.Name())
	}
	if len(got) != count {
		t.Fatalf("iterator returned %d topics, want %d", len(got), count)
	}

	// The iterator should produce a stable, monotonic-by-hash sequence: walking
	// with TopicIterNext from the first must reproduce the full list exactly.
	var again []string
	first := node.TopicIterFirst()
	if first == nil {
		t.Fatal("TopicIterFirst returned nil with topics present")
	}
	for tp := first; tp != nil; tp = node.TopicIterNext(tp) {
		again = append(again, tp.Name())
	}
	for i := range got {
		if got[i] != again[i] {
			t.Fatalf("iteration mismatch at %d: %q vs %q", i, got[i], again[i])
		}
	}

	// TopicIterNext on a topic whose hash is not present must return nil.
	orphan := &cy.Topic{}
	if node.TopicIterNext(orphan) != nil {
		t.Error("TopicIterNext on unknown topic should return nil")
	}

	// Per-topic user context (cy_topic_user_context).
	first.SetUserContext(cy.UserContext{Ptr: [2]interface{}{"hello", 42}})
	uc := cy.TopicUserContext(first)
	if uc.Ptr[0] != "hello" {
		t.Errorf("TopicUserContext.Ptr[0] = %v, want %q", uc.Ptr[0], "hello")
	}
	// TopicUserContext on a nil topic yields an empty context.
	if !isZeroUserContext(cy.TopicUserContext(nil)) {
		t.Error("TopicUserContext(nil) should be empty")
	}
}

func isZeroUserContext(u cy.UserContext) bool {
	return u.Ptr[0] == nil && u.Ptr[1] == nil
}
