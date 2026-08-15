package cy

import (
	"fmt"
	"strings"

	"go.dw1.io/rapidhash"
)

// Topic represents a Cyphal topic.
// Topics are identified by their name and are assigned a subject-ID through
// a distributed CRDT-based allocation protocol.
type Topic struct {
	// name is the topic name (e.g., "sensors/temperature").
	name string
	// hash is the hash of the topic name.
	hash uint64
	// subjectID is the subject-ID assigned to this topic.
	subjectID uint32
	// pinned indicates whether this topic has a pinned subject-ID.
	pinned bool
	// pinnedSubjectID is the explicitly pinned subject-ID (if pinned).
	pinnedSubjectID uint32

	// CRDT state for topic allocation
	// logAge is the log2 of seconds since topic creation.
	logAge int32
	// evictions is the number of times this topic has been evicted and recreated.
	evictions uint32

	// extent is the maximum message size for this topic.
	extent int

	// refcount is the number of references to this topic.
	refcount int
}

// newTopic creates a new topic with the specified name.
func newTopic(name string, modulus uint32) (*Topic, error) {
	if name == "" {
		return nil, ErrName
	}

	// Normalize the name
	name = NormalizeTopicName(name)

	// Compute hash
	hash := HashString(name)

	// Check if this is a pinned topic
	pinned, pinnedSubjectID := ParsePinnedTopic(name)

	topic := &Topic{
		name:    name,
		hash:    hash,
		pinned:  pinned,
		logAge:  LAGEMin,
		evictions: 0,
		extent: 0,
		refcount: 1,
	}

	// Assign subject-ID
	if pinned {
		topic.subjectID = pinnedSubjectID
		topic.pinnedSubjectID = pinnedSubjectID
	} else {
		topic.subjectID = ComputeSubjectID(hash, 0, modulus)
	}

	return topic, nil
}


// ParsePinnedTopic checks if a topic name has a pinned subject-ID.
// Pinned topics have the format "name#1234" where 1234 is the subject-ID.
func ParsePinnedTopic(name string) (bool, uint32) {
	// Find the last '#' in the name
	idx := strings.LastIndex(name, "#")
	if idx == -1 {
		return false, 0
	}

	// Extract the subject-ID part
	subjectIDStr := name[idx+1:]
	
	// Check if it's a valid number
	var subjectID uint64
	_, err := fmt.Sscanf(subjectIDStr, "%d", &subjectID)
	if err != nil || subjectID > SubjectIDPinnedMax {
		// Not a valid pinned topic
		return false, 0
	}

	// Check for leading zeros (not allowed)
	if len(subjectIDStr) > 1 && subjectIDStr[0] == '0' {
		return false, 0
	}

	// The actual topic name is the part before the '#'
	// But we need to check if it's empty
	topicName := name[:idx]
	if topicName == "" {
		return false, 0
	}

	return true, uint32(subjectID)
}

// ComputeSubjectID computes the subject-ID for a topic based on its hash and evictions.
// This implements the quadratic probing formula from the Cyphal specification.
func ComputeSubjectID(hash uint64, evictions uint32, modulus uint32) uint32 {
	// For pinned topics, the subject-ID is explicitly set
	// For normal topics, use the CRDT formula:
	// subject_id = CY_SUBJECT_ID_PINNED_MAX + 1
	//              + (((hash mod modulus) + ((evictions mod modulus)^2 mod modulus)) mod modulus)
	
	hashMod := hash % uint64(modulus)
	evictMod := uint64(evictions) % uint64(modulus)
	probe := (hashMod + (evictMod*evictMod)%uint64(modulus)) % uint64(modulus)
	
	return SubjectIDPinnedMax + 1 + uint32(probe)
}

// HashString computes a hash for a string using rapidhash.
func HashString(s string) uint64 {
	return rapidhash.Hash([]byte(s))
}

// Name returns the topic name.
func (t *Topic) Name() string {
	return t.name
}

// Hash returns the topic hash.
func (t *Topic) Hash() uint64 {
	return t.hash
}

// SubjectID returns the topic's subject-ID.
func (t *Topic) SubjectID() uint32 {
	return t.subjectID
}

// Pinned returns whether this topic has a pinned subject-ID.
func (t *Topic) Pinned() bool {
	return t.pinned
}

// PinnedSubjectID returns the explicitly pinned subject-ID, or 0 if not pinned.
func (t *Topic) PinnedSubjectID() uint32 {
	return t.pinnedSubjectID
}

// LogAge returns the log-age of the topic.
func (t *Topic) LogAge() int32 {
	return t.logAge
}

// Evictions returns the eviction count of the topic.
func (t *Topic) Evictions() uint32 {
	return t.evictions
}

// Extent returns the maximum message size for this topic.
func (t *Topic) Extent() int {
	return t.extent
}

// SetExtent sets the maximum message size for this topic.
func (t *Topic) SetExtent(extent int) {
	t.extent = extent
}

// Refcount returns the number of references to this topic.
func (t *Topic) Refcount() int {
	return t.refcount
}

// IncRef increases the reference count.
func (t *Topic) IncRef() {
	t.refcount++
}

// DecRef decreases the reference count.
func (t *Topic) DecRef() {
	if t.refcount > 0 {
		t.refcount--
	}
}

// UpdateState updates the topic's CRDT state.
// This is called when receiving gossip information about the topic.
func (t *Topic) UpdateState(otherLogAge int32, otherEvictions uint32) bool {
	// CRDT merge: older log-age wins, ties broken by larger eviction counter
	if otherLogAge < t.logAge {
		// Other is older, we win - no change
		return false
	} else if otherLogAge > t.logAge {
		// Other is newer, they win
		t.logAge = otherLogAge
		t.evictions = otherEvictions
		// Recompute subject-ID if not pinned
		if !t.pinned {
			// Would need modulus from Cy instance
			// For now, just mark that an update occurred
			// t.subjectID = ComputeSubjectID(t.hash, t.evictions, modulus)
		}
		return true
	} else {
		// Same log-age, larger eviction counter wins
		if otherEvictions > t.evictions {
			t.evictions = otherEvictions
			if !t.pinned {
				// Would need modulus from Cy instance
				// For now, just mark that an update occurred
				// t.subjectID = ComputeSubjectID(t.hash, t.evictions, modulus)
			}
			return true
		}
		// We win or it's a tie with same or smaller evictions
		return false
	}
}

// UpdateStateWithCRDT updates the topic state from CRDT state.
// This recomputes the subject-ID if the topic is not pinned.
func (t *Topic) UpdateStateWithCRDT(otherLogAge int32, otherEvictions uint32, crdt *CRDT) bool {
	// First update the state
	updated := t.UpdateState(otherLogAge, otherEvictions)
	
	if updated && !t.pinned && crdt != nil {
		// Recompute subject-ID
		t.subjectID = crdt.ComputeSubjectID(t.hash, t.evictions)
	}
	
	return updated
}

// State returns the current CRDT state of the topic.
func (t *Topic) State() (logAge int32, evictions uint32) {
	return t.logAge, t.evictions
}

// SetState sets the CRDT state of the topic.
func (t *Topic) SetState(logAge int32, evictions uint32) {
	t.logAge = logAge
	t.evictions = evictions
}

// GossipData returns the data to include in gossip messages for this topic.
// This is the CRDT state that needs to be propagated.
func (t *Topic) GossipData() []byte {
	// Format: [hash:8][logAge:4][evictions:4]
	data := make([]byte, 16)
	
	// Write hash (8 bytes, little-endian)
	for i := 0; i < 8; i++ {
		data[i] = byte(t.hash >> (i * 8))
	}
	
	// Write logAge (4 bytes, little-endian, signed)
	for i := 0; i < 4; i++ {
		data[8+i] = byte(uint32(t.logAge) >> (i * 8))
	}
	
	// Write evictions (4 bytes, little-endian)
	for i := 0; i < 4; i++ {
		data[12+i] = byte(t.evictions >> (i * 8))
	}
	
	return data
}

// ParseGossipData parses gossip data and updates the topic state.
func (t *Topic) ParseGossipData(data []byte) bool {
	if len(data) < 16 {
		return false
	}
	
	// Parse hash
	var hash uint64
	for i := 0; i < 8; i++ {
		hash |= uint64(data[i]) << (i * 8)
	}
	
	// If hash doesn't match, this gossip is for a different topic
	if hash != t.hash {
		return false
	}
	
	// Parse logAge
	var logAge uint32
	for i := 0; i < 4; i++ {
		logAge |= uint32(data[8+i]) << (i * 8)
	}
	
	// Parse evictions
	var evictions uint32
	for i := 0; i < 4; i++ {
		evictions |= uint32(data[12+i]) << (i * 8)
	}
	
	// Update state
	return t.UpdateState(int32(logAge), evictions)
}

// String returns a string representation of the topic.
func (t *Topic) String() string {
	if t == nil {
		return "<nil>"
	}
	pinnedStr := ""
	if t.pinned {
		pinnedStr = fmt.Sprintf(" (pinned:%d)", t.pinnedSubjectID)
	}
	return fmt.Sprintf("Topic{name:%q, hash:0x%016x, subjectID:%d%s}",
		t.name, t.hash, t.subjectID, pinnedStr)
}
