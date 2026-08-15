// Package tests provides tests for the CRDT implementation.
// These tests validate the Conflict-free Replicated Data Type for topic allocation.
package tests

import (
	"testing"

	"github.com/opencyphal/cy-go"
)

// TestCRDTAddTopic tests adding a topic to the CRDT.
func TestCRDTAddTopic(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	crdt := node.CRDT()
	if crdt == nil {
		t.Fatal("Expected CRDT instance, got nil")
	}
	
	// Add a topic
	hash := uint64(0x123456789ABCDEF0)
	name := "test.topic"
	state := crdt.AddTopic(name, hash)
	
	if state == nil {
		t.Fatal("Expected state, got nil")
	}
	
	if state.Hash() != hash {
		t.Errorf("Expected hash %d, got %d", hash, state.Hash())
	}
	
	if state.Name() != name {
		t.Errorf("Expected name %s, got %s", name, state.Name())
	}
	
	// Adding the same topic again should return the same state
	state2 := crdt.AddTopic(name, hash)
	if state2 != state {
		t.Error("Expected same state for duplicate add")
	}
}

// TestCRDTGetTopic tests getting a topic from the CRDT.
func TestCRDTGetTopic(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	crdt := node.CRDT()
	
	// Add a topic
	hash := uint64(0x123456789ABCDEF0)
	name := "test.topic"
	crdt.AddTopic(name, hash)
	
	// Get the topic
	state, ok := crdt.GetTopic(hash)
	if !ok {
		t.Fatal("Expected to find topic")
	}
	
	if state.Hash() != hash {
		t.Errorf("Expected hash %d, got %d", hash, state.Hash())
	}
	
	// Get a non-existent topic
	_, ok = crdt.GetTopic(0)
	if ok {
		t.Error("Expected not to find non-existent topic")
	}
}

// TestCRDTRemoveTopic tests removing a topic from the CRDT.
func TestCRDTRemoveTopic(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	crdt := node.CRDT()
	
	// Add a topic
	hash := uint64(0x123456789ABCDEF0)
	crdt.AddTopic("test.topic", hash)
	
	// Verify it exists
	_, ok := crdt.GetTopic(hash)
	if !ok {
		t.Fatal("Expected to find topic before removal")
	}
	
	// Remove the topic
	crdt.RemoveTopic(hash)
	
	// Verify it's gone
	_, ok = crdt.GetTopic(hash)
	if ok {
		t.Error("Expected topic to be removed")
	}
}

// TestCRDTComputeSubjectID tests subject-ID computation.
func TestCRDTComputeSubjectID(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	crdt := node.CRDT()
	
	// Test with different hashes and evictions
	testCases := []struct {
		hash      uint64
		evictions uint32
		name      string
	}{
		{0x123456789ABCDEF0, 0, "test1"},
		{0xFEDCBA9876543210, 0, "test2"},
		{0x123456789ABCDEF0, 1, "test3"},
		{0x123456789ABCDEF0, 2, "test4"},
	}
	
	for _, tc := range testCases {
		subjectID := crdt.ComputeSubjectID(tc.hash, tc.evictions)
		
		// Subject-ID should be > SubjectIDPinnedMax for normal topics
		if subjectID <= cy.SubjectIDPinnedMax {
			t.Errorf("Expected subject-ID > %d for hash %d, evictions %d, got %d", 
				cy.SubjectIDPinnedMax, tc.hash, tc.evictions, subjectID)
		}
		
		t.Logf("Hash: 0x%016X, Evictions: %d, SubjectID: %d", tc.hash, tc.evictions, subjectID)
	}
}

// TestCRDTPinnedTopic tests pinned topic handling.
func TestCRDTPinnedTopic(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	crdt := node.CRDT()
	
	// Add a pinned topic
	hash := uint64(0x123456789ABCDEF0)
	name := "pinned.topic"
	pinnedSubjectID := uint32(12345)
	state := crdt.AddPinnedTopic(name, hash, pinnedSubjectID)
	
	if state == nil {
		t.Fatal("Expected state, got nil")
	}
	
	if !state.Pinned() {
		t.Error("Expected pinned topic")
	}
	
	if state.PinnedSubjectID() != pinnedSubjectID {
		t.Errorf("Expected pinned subject-ID %d, got %d", pinnedSubjectID, state.PinnedSubjectID())
	}
	
	// Check IsPinned
	if !crdt.IsPinned(hash) {
		t.Error("Expected topic to be pinned")
	}
	
	// Check GetPinnedSubjectID
	subjectID, ok := crdt.GetPinnedSubjectID(hash)
	if !ok {
		t.Error("Expected to get pinned subject-ID")
	}
	if subjectID != pinnedSubjectID {
		t.Errorf("Expected pinned subject-ID %d, got %d", pinnedSubjectID, subjectID)
	}
	
	// Non-pinned topic
	hash2 := uint64(0xFEDCBA9876543210)
	crdt.AddTopic("normal.topic", hash2)
	
	if crdt.IsPinned(hash2) {
		t.Error("Expected normal topic not to be pinned")
	}
	
	_, ok = crdt.GetPinnedSubjectID(hash2)
	if ok {
		t.Error("Expected no pinned subject-ID for normal topic")
	}
}

// TestCRDTMerge tests CRDT merge logic.
func TestCRDTMerge(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	crdt := node.CRDT()
	
	// Add a topic
	hash := uint64(0x123456789ABCDEF0)
	crdt.AddTopic("test.topic", hash)
	
	// Get the current state
	state, _ := crdt.GetTopic(hash)
	originalLogAge := state.LogAge()
	originalEvictions := state.Evictions()
	
	// Merge with older log-age (should not update)
	updated := crdt.Merge(hash, originalLogAge-1, originalEvictions)
	if updated {
		t.Error("Expected no update when merging with older log-age")
	}
	
	// Merge with newer log-age (should update)
	updated = crdt.Merge(hash, originalLogAge+1, originalEvictions)
	if !updated {
		t.Error("Expected update when merging with newer log-age")
	}
	
	// Verify the state was updated
	state, _ = crdt.GetTopic(hash)
	if state.LogAge() != originalLogAge+1 {
		t.Errorf("Expected log-age %d, got %d", originalLogAge+1, state.LogAge())
	}
	
	// Merge with same log-age but higher evictions (should update)
	updated = crdt.Merge(hash, originalLogAge+1, originalEvictions+1)
	if !updated {
		t.Error("Expected update when merging with same log-age but higher evictions")
	}
	
	// Verify the state was updated
	state, _ = crdt.GetTopic(hash)
	if state.Evictions() != originalEvictions+1 {
		t.Errorf("Expected evictions %d, got %d", originalEvictions+1, state.Evictions())
	}
}

// TestCRDTIncrementEvictions tests eviction counter incrementing.
func TestCRDTIncrementEvictions(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	crdt := node.CRDT()
	
	// Add a topic
	hash := uint64(0x123456789ABCDEF0)
	crdt.AddTopic("test.topic", hash)
	
	// Get initial evictions
	state, _ := crdt.GetTopic(hash)
	initialEvictions := state.Evictions()
	
	// Increment evictions
	crdt.IncrementEvictions(hash)
	
	// Verify evictions were incremented
	state, _ = crdt.GetTopic(hash)
	if state.Evictions() != initialEvictions+1 {
		t.Errorf("Expected evictions %d, got %d", initialEvictions+1, state.Evictions())
	}
	
	// Verify log-age was reset
	if state.LogAge() != cy.LAGEMin {
		t.Errorf("Expected log-age %d, got %d", cy.LAGEMin, state.LogAge())
	}
}

// TestCRDTUpdateLogAge tests log-age updating.
func TestCRDTUpdateLogAge(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	crdt := node.CRDT()
	
	// Add a topic
	hash := uint64(0x123456789ABCDEF0)
	crdt.AddTopic("test.topic", hash)
	
	// Get initial log-age
	state, _ := crdt.GetTopic(hash)
	initialLogAge := state.LogAge()
	
	// Update log-age
	newLogAge := initialLogAge + 5
	crdt.UpdateLogAge(hash, newLogAge)
	
	// Verify log-age was updated
	state, _ = crdt.GetTopic(hash)
	if state.LogAge() != newLogAge {
		t.Errorf("Expected log-age %d, got %d", newLogAge, state.LogAge())
	}
}

// TestCRDTGossipWireRoundTrip tests the C-compatible gossip wire format
// (24-byte header + name) round-trips through Marshal/Parse.
func TestCRDTGossipWireRoundTrip(t *testing.T) {
	lage := int8(7)
	hash := uint64(0x123456789ABCDEF0)
	evictions := uint32(42)
	name := "sensors/temperature"

	wire := cy.MarshalGossipMessage(lage, hash, uint64(evictions), name)
	if len(wire) != cy.HeaderSize+len(name) {
		t.Fatalf("Expected wire length %d, got %d", cy.HeaderSize+len(name), len(wire))
	}
	// The header type byte must be the gossip type.
	if cy.HeaderType(wire[0]) != cy.HeaderTypeGossip {
		t.Errorf("Expected gossip header type, got %d", wire[0])
	}
	// The trailing name length byte (offset 23) must match.
	if wire[23] != byte(len(name)) {
		t.Errorf("Expected name length %d at byte 23, got %d", len(name), wire[23])
	}

	parsed, err := cy.ParseGossipMessage(wire)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	if parsed.Lage != lage || parsed.Hash != hash || parsed.Evictions != evictions || parsed.Name != name {
		t.Errorf("Round-trip mismatch: got %+v", parsed)
	}
}

// TestCRDTGetAllTopicStates tests getting all topic states.
func TestCRDTGetAllTopicStates(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	crdt := node.CRDT()
	
	// Add multiple topics
	hashes := []uint64{0x123456789ABCDEF0, 0xFEDCBA9876543210, 0x1122334455667788}
	for i, hash := range hashes {
		crdt.AddTopic("topic."+string(rune('a'+i)), hash)
	}
	
	// Get all topic states
	states := crdt.GetAllTopicStates()
	if len(states) != len(hashes) {
		t.Errorf("Expected %d states, got %d", len(hashes), len(states))
	}
	
	// Verify all hashes are present
	hashSet := make(map[uint64]bool)
	for _, state := range states {
		hashSet[state.Hash()] = true
	}
	
	for _, hash := range hashes {
		if !hashSet[hash] {
			t.Errorf("Expected hash %d to be in states", hash)
		}
	}
}

// TestCRDTReset tests resetting the CRDT.
func TestCRDTReset(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	crdt := node.CRDT()
	
	// Add topics
	crdt.AddTopic("topic.a", 0x123456789ABCDEF0)
	crdt.AddTopic("topic.b", 0xFEDCBA9876543210)
	
	// Verify topics exist
	if len(crdt.GetAllTopicStates()) != 2 {
		t.Error("Expected 2 topics")
	}
	
	// Reset
	crdt.Reset()
	
	// Verify topics are gone
	if len(crdt.GetAllTopicStates()) != 0 {
		t.Error("Expected 0 topics after reset")
	}
}

// TestCRDTSubjectIDModulus tests subject-ID modulus configuration and validation.
func TestCRDTSubjectIDModulus(t *testing.T) {
	platform := NewMockPlatform()

	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()

	crdt := node.CRDT()

	// The default modulus must be valid (prime, mod 4 == 3, >= 16-bit modulus).
	defaultModulus := crdt.SubjectIDModulus()
	if defaultModulus == 0 {
		t.Error("Expected non-zero default modulus")
	}
	if !cy.IsValidSubjectIDModulus(defaultModulus) {
		t.Errorf("Default modulus %d should be valid", defaultModulus)
	}

	// A valid alternative modulus (the 23-bit constant) is accepted.
	if err := node.SetSubjectIDModulus(cy.SubjectIDModulus23bit); err != nil {
		t.Fatalf("Expected to accept valid modulus, got: %v", err)
	}
	if crdt.SubjectIDModulus() != cy.SubjectIDModulus23bit {
		t.Errorf("Expected modulus %d, got %d", cy.SubjectIDModulus23bit, crdt.SubjectIDModulus())
	}

	// An invalid modulus (not prime, too small) is rejected and leaves the
	// previous modulus in place.
	if err := node.SetSubjectIDModulus(1024); err == nil {
		t.Error("Expected rejection of invalid modulus 1024")
	}
	if crdt.SubjectIDModulus() != cy.SubjectIDModulus23bit {
		t.Errorf("Modulus should be unchanged after invalid set, got %d", crdt.SubjectIDModulus())
	}
}

// TestCRDTMultipleTopics tests CRDT with multiple topics.
func TestCRDTMultipleTopics(t *testing.T) {
	platform := NewMockPlatform()
	
	node, err := cy.New(platform, "test_node", "", "")
	if err != nil {
		t.Fatalf("Failed to create Cy instance: %v", err)
	}
	defer node.Destroy()
	
	crdt := node.CRDT()
	
	// Add many topics
	const numTopics = 100
	for i := 0; i < numTopics; i++ {
		hash := uint64(i)
		name := "topic." + string(rune('a'+i%26))
		crdt.AddTopic(name, hash)
	}
	
	// Verify all topics are present
	states := crdt.GetAllTopicStates()
	if len(states) != numTopics {
		t.Errorf("Expected %d topics, got %d", numTopics, len(states))
	}
	
	// Verify all hashes are unique
	hashSet := make(map[uint64]bool)
	for _, state := range states {
		if hashSet[state.Hash()] {
			t.Error("Found duplicate hash")
		}
		hashSet[state.Hash()] = true
	}
}
