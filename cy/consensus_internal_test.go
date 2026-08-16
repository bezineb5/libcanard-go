package cy

import (
	"testing"
	"unsafe"
)

// testPlatform is a minimal in-package platform that actually creates subject
// writers/readers, so cy.New() succeeds and consensus tests can run. It allows
// controlling the clock via Now().
type testPlatform struct {
	PlatformBase
	nowValue  Microsecond
	failSends bool
}

type testWriter struct{ subjectID uint32 }

func (w *testWriter) SubjectID() uint32 { return w.subjectID }

type testReader struct {
	subjectID uint32
	extent    int
}

func (r *testReader) SubjectID() uint32 { return r.subjectID }
func (r *testReader) Extent() int       { return r.extent }
func (r *testReader) SetExtent(e int)   { r.extent = e }

func (p *testPlatform) NewSubjectWriter(subjectID uint32) (SubjectWriter, error) {
	return &testWriter{subjectID: subjectID}, nil
}

func (p *testPlatform) NewSubjectReader(subjectID uint32, extent int) (SubjectReader, error) {
	return &testReader{subjectID: subjectID, extent: extent}, nil
}

func (p *testPlatform) Now() Microsecond { return p.nowValue }
func (p *testPlatform) Destroy()         {}
func (p *testPlatform) Random() uint64   { return 1 }
func (p *testPlatform) Realloc(ptr unsafe.Pointer, size int) unsafe.Pointer {
	if size == 0 {
		return nil
	}
	if ptr == nil {
		b := make([]byte, size)
		return unsafe.Pointer(&b[0])
	}
	return ptr
}

// SubjectWriterSend overrides PlatformBase to optionally fail on demand.
func (p *testPlatform) SubjectWriterSend(w SubjectWriter, deadline Microsecond, priority Priority, message []byte) error {
	if p.failSends {
		return ErrMedia
	}
	return nil
}

// Unicast overrides PlatformBase to optionally fail on demand.
func (p *testPlatform) Unicast(lane Lane, deadline Microsecond, message []byte) error {
	if p.failSends {
		return ErrMedia
	}
	return nil
}

func newTestCy(t *testing.T) (*Cy, *testPlatform) {
	t.Helper()
	plat := &testPlatform{nowValue: 10 * MEGA}
	cy, err := New(plat, "n", "", "")
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	return cy, plat
}

// TestLog2FloorAndLageToUS verifies the log-age primitives match C semantics.
func TestLog2FloorAndLageToUS(t *testing.T) {
	if got := Log2Floor(0); got != -1 {
		t.Errorf("Log2Floor(0) = %d, want -1", got)
	}
	if got := Log2Floor(1); got != 0 {
		t.Errorf("Log2Floor(1) = %d, want 0", got)
	}
	if got := Log2Floor(8); got != 3 {
		t.Errorf("Log2Floor(8) = %d, want 3", got)
	}
	if got := LageToUS(-1); got != 0 {
		t.Errorf("LageToUS(-1) = %d, want 0", got)
	}
	if got := LageToUS(0); got != MEGA {
		t.Errorf("LageToUS(0) = %d, want 1 second (MEGA=%d)", got, MEGA)
	}
	// LageToUS(10) = 2^10 seconds = 1024 * 1e6 us.
	if got := LageToUS(10); got != Microsecond(1024)*MEGA {
		t.Errorf("LageToUS(10) = %d, want %d", got, Microsecond(1024)*MEGA)
	}
}

// TestIsValidSubjectIDModulusInternal checks the modulus precondition.
func TestIsValidSubjectIDModulusInternal(t *testing.T) {
	cases := []struct {
		modulus uint32
		want    bool
	}{
		{57203, true},      // default 16-bit modulus (prime, mod 4 == 3).
		{8378431, true},    // 23-bit modulus.
		{4294954663, true}, // 32-bit modulus.
		{1024, false},      // too small, not prime.
		{57204, false},     // not prime / wrong congruence.
		{4, false},         // mod 4 == 0.
		{7, false},         // prime but mod 4 != 3 and below minimum.
	}
	for _, tc := range cases {
		if got := IsValidSubjectIDModulus(tc.modulus); got != tc.want {
			t.Errorf("IsValidSubjectIDModulus(%d) = %v, want %v", tc.modulus, got, tc.want)
		}
	}
}

// TestBroadcastSubjectIDAndShardCount verifies the broadcast/shard layout.
func TestBroadcastSubjectIDAndShardCount(t *testing.T) {
	cy, _ := newTestCy(t)
	defer cy.Destroy()

	mod := cy.subjectIDModulus
	if cy.broadcastSubjectID != BroadcastSubjectID(mod) {
		t.Errorf("broadcastSubjectID %d != BroadcastSubjectID(%d) %d",
			cy.broadcastSubjectID, mod, BroadcastSubjectID(mod))
	}
	if cy.gossip.ShardCount() != GossipShardCount(mod) {
		t.Errorf("shardCount %d != GossipShardCount(%d) %d",
			cy.gossip.ShardCount(), mod, GossipShardCount(mod))
	}
	// The broadcast subject must sit above the max valid subject-ID and the shards.
	if cy.broadcastSubjectID <= SubjectIDMax(mod) {
		t.Errorf("broadcast subject %d must exceed max subject %d", cy.broadcastSubjectID, SubjectIDMax(mod))
	}
	// A shard subject must sit between the max valid subject-ID and broadcast.
	shard0 := cy.shardSubjectFor(0)
	if shard0 <= SubjectIDMax(mod) || shard0 >= cy.broadcastSubjectID {
		t.Errorf("shard0 subject %d not between max %d and broadcast %d",
			shard0, SubjectIDMax(mod), cy.broadcastSubjectID)
	}
}

// TestLogAgeDerivedFromOrigin verifies the topic log-age is derived from tsOrigin.
func TestLogAgeDerivedFromOrigin(t *testing.T) {
	cy, _ := newTestCy(t)
	defer cy.Destroy()
	now := cy.Now()
	topic, err := cy.findOrCreateTopic("a", 256)
	if err != nil {
		t.Fatal(err)
	}
	// Freshly created topic: tsOrigin == now -> LAGEMin.
	if got := topic.Lage(now); got != LAGEMin {
		t.Errorf("fresh Lage = %d, want LAGEMin %d", got, LAGEMin)
	}
	// Set an explicit age of 5 (2^5 seconds in the past).
	topic.SetOriginAt(now, 5, 0)
	if got := topic.Lage(now); got != 5 {
		t.Errorf("Lage after SetOriginAt = %d, want 5", got)
	}
}

// TestLeftWins verifies the conflict comparator: older (higher log-age) wins,
// ties broken by smaller hash.
func TestLeftWins(t *testing.T) {
	cy, _ := newTestCy(t)
	defer cy.Destroy()
	now := cy.Now()
	older, _ := cy.findOrCreateTopic("older", 64)
	younger, _ := cy.findOrCreateTopic("younger", 64)
	// Make 'older' genuinely older (higher log-age).
	older.SetOriginAt(now, 10, 0)
	younger.SetOriginAt(now, 5, 0)
	if !cy.leftWins(older, now, younger.Lage(now), younger.hash) {
		t.Error("expected older topic to win")
	}
	if cy.leftWins(younger, now, older.Lage(now), older.hash) {
		t.Error("expected younger topic to lose")
	}
	// Equal age: smaller hash wins.
	older.SetOriginAt(now, 5, 0)
	younger.SetOriginAt(now, 5, 0)
	want := older.hash < younger.hash
	if got := cy.leftWins(older, now, younger.Lage(now), younger.hash); got != want {
		t.Errorf("equal-age left_wins = %v, want %v (older.hash=%d younger.hash=%d)",
			got, want, older.hash, younger.hash)
	}
}

// TestTopicAllocateCollision verifies collision arbitration: when two local
// topics collide on a subject-ID, one re-evicts so no two topics share a slot.
func TestTopicAllocateCollision(t *testing.T) {
	cy, _ := newTestCy(t)
	defer cy.Destroy()
	now := cy.Now()

	a, err := cy.findOrCreateTopic("alpha", 256)
	if err != nil {
		t.Fatal(err)
	}
	b, err := cy.findOrCreateTopic("beta", 256)
	if err != nil {
		t.Fatal(err)
	}
	if a.subjectID == b.subjectID {
		t.Fatalf("alpha and beta already collide without forcing: %d", a.subjectID)
	}

	// Force a collision deterministically: give 'b' the same hash as 'a' so that,
	// at the same eviction counter, both compute the same subject-ID. (In practice
	// hashes differ, but this exercises the collision-arbitration path directly.)
	b.hash = a.hash
	// Trigger allocation of 'b' at eviction 0, which now collides with 'a'.
	cy.topicAllocate(b, b.evictions, now)

	// After arbitration, the two topics must occupy distinct subject-IDs and the
	// index must remain consistent (each topic is the occupant of its own slot).
	if a.subjectID == b.subjectID {
		t.Errorf("topics still collide after arbitration: %d", a.subjectID)
	}
	if cy.topicsBySubjectID[a.subjectID] != a {
		t.Errorf("index inconsistent for alpha at %d", a.subjectID)
	}
	if cy.topicsBySubjectID[b.subjectID] != b {
		t.Errorf("index inconsistent for beta at %d", b.subjectID)
	}
	for sid, top := range cy.topicsBySubjectID {
		if top.subjectID != sid {
			t.Errorf("topic at %d has subjectID %d", sid, top.subjectID)
		}
	}
}

// TestTopicAllocatePinned verifies pinned topics are not indexed by subject-ID
// and keep their pinned subject-ID across allocation.
func TestTopicAllocatePinned(t *testing.T) {
	cy, _ := newTestCy(t)
	defer cy.Destroy()
	now := cy.Now()

	pinned, err := cy.findOrCreateTopic("pinned#1234", 256)
	if err != nil {
		t.Fatal(err)
	}
	if !pinned.pinned {
		t.Fatal("expected pinned topic")
	}
	if pinned.subjectID != 1234 {
		t.Errorf("pinned subject-ID = %d, want 1234", pinned.subjectID)
	}
	// Re-allocate the pinned topic; it must keep its pinned subject-ID.
	cy.topicAllocate(pinned, ^uint32(1234), now)
	if pinned.subjectID != 1234 {
		t.Errorf("pinned subject-ID changed after reallocate: %d", pinned.subjectID)
	}
}
