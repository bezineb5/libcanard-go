package cy

import (
	"sync"
)

// CRDT implements Conflict-free Replicated Data Type for topic allocation.
// This ensures that all nodes in the network eventually agree on topic-to-subject-ID mappings.
// The CRDT uses a hybrid approach combining:
// 1. Deterministic subject-ID computation based on topic hash and eviction counter
// 2. Gossip-based synchronization of topic states across nodes
// 3. Conflict resolution based on log-age and eviction counters
type CRDT struct {
	mu sync.RWMutex

	// cy is the parent Cy instance.
	cy *Cy

	// topics maps topic hashes to their CRDT state.
	topics map[uint64]*TopicState

	// subjectIDModulus is the modulus for subject-ID computation.
	subjectIDModulus uint32

	// gossipInterval is how often to gossip topic states (in microseconds).
	gossipInterval Microsecond

	// lastGossipTime is the last time gossip was sent.
	lastGossipTime Microsecond

	// pendingGossip tracks topics that need to be gossiped.
	pendingGossip []uint64
}

// TopicState represents the CRDT state for a topic.
type TopicState struct {
	// hash is the hash of the topic name.
	hash uint64
	// name is the topic name.
	name string
	// logAge is the log2 of seconds since topic creation.
	logAge int32
	// evictions is the number of times this topic has been evicted and recreated.
	evictions uint32
	// pinned indicates if this topic has a pinned subject-ID.
	pinned bool
	// pinnedSubjectID is the explicitly pinned subject-ID.
	pinnedSubjectID uint32
}

// Hash returns the topic hash.
func (s *TopicState) Hash() uint64 {
	return s.hash
}

// Name returns the topic name.
func (s *TopicState) Name() string {
	return s.name
}

// LogAge returns the log-age.
func (s *TopicState) LogAge() int32 {
	return s.logAge
}

// Evictions returns the eviction count.
func (s *TopicState) Evictions() uint32 {
	return s.evictions
}

// Pinned returns true if this is a pinned topic.
func (s *TopicState) Pinned() bool {
	return s.pinned
}

// PinnedSubjectID returns the pinned subject-ID.
func (s *TopicState) PinnedSubjectID() uint32 {
	return s.pinnedSubjectID
}

// NewTopicState creates a new topic state.
func NewTopicState(name string, hash uint64, logAge int32, evictions uint32) *TopicState {
	return &TopicState{
		name:    name,
		hash:    hash,
		logAge:  logAge,
		evictions: evictions,
		pinned:  false,
	}
}

// NewPinnedTopicState creates a new pinned topic state.
// For pinned topics, the evictions field is set to UINT32_MAX - pinnedSubjectID.
// This ensures that ComputeSubjectID returns the pinnedSubjectID.
func NewPinnedTopicState(name string, hash uint64, pinnedSubjectID uint32) *TopicState {
	return &TopicState{
		name:            name,
		hash:            hash,
		logAge:          LAGEMin,
		evictions:       ^pinnedSubjectID, // UINT32_MAX - pinnedSubjectID
		pinned:          true,
		pinnedSubjectID: pinnedSubjectID,
	}
}

// NewCRDT creates a new CRDT instance.
func NewCRDT(cy *Cy, modulus uint32) *CRDT {
	return &CRDT{
		cy:              cy,
		topics:          make(map[uint64]*TopicState),
		subjectIDModulus: modulus,
	}
}

// SetSubjectIDModulus sets the subject-ID modulus.
func (c *CRDT) SetSubjectIDModulus(modulus uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subjectIDModulus = modulus
}

// SubjectIDModulus returns the subject-ID modulus.
func (c *CRDT) SubjectIDModulus() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.subjectIDModulus
}

// AddTopic adds a new topic to the CRDT state.
func (c *CRDT) AddTopic(name string, hash uint64) *TopicState {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if the topic already exists
	if state, ok := c.topics[hash]; ok {
		return state
	}

	// Create a new topic state
	state := NewTopicState(name, hash, LAGEMin, 0)
	c.topics[hash] = state

	return state
}

// GetTopic returns the topic state for a given hash.
func (c *CRDT) GetTopic(hash uint64) (*TopicState, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	state, ok := c.topics[hash]
	return state, ok
}

// Merge merges the state of a remote topic into the local state.
// This implements the CRDT merge semantics: older log-age wins, ties broken by larger eviction counter.
func (c *CRDT) Merge(hash uint64, remoteLogAge int32, remoteEvictions uint32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	localState, ok := c.topics[hash]
	if !ok {
		// Local topic doesn't exist, create it with remote state
		// This shouldn't happen in normal operation
		return false
	}

	// CRDT merge: older log-age wins
	if remoteLogAge < localState.logAge {
		// Remote is older, we win - no change
		return false
	} else if remoteLogAge > localState.logAge {
		// Remote is newer, they win
		localState.logAge = remoteLogAge
		localState.evictions = remoteEvictions
		return true
	} else {
		// Same log-age, larger eviction counter wins
		if remoteEvictions > localState.evictions {
			localState.evictions = remoteEvictions
			return true
		}
		// We win or it's a tie
		return false
	}
}

// ComputeSubjectID computes the subject-ID for a topic based on its hash and evictions.
func (c *CRDT) ComputeSubjectID(hash uint64, evictions uint32) uint32 {
	if evictions >= EVICTIONS_PINNED_MIN {
		// This is a pinned topic
		return ^evictions
	}

	// Normal topic: use quadratic probing formula
	hashMod := hash % uint64(c.subjectIDModulus)
	evictMod := uint64(evictions) % uint64(c.subjectIDModulus)
	probe := (hashMod + (evictMod*evictMod)%uint64(c.subjectIDModulus)) % uint64(c.subjectIDModulus)

	return SubjectIDPinnedMax + 1 + uint32(probe)
}

// IncrementEvictions increments the eviction counter for a topic.
// This is called when a topic is evicted and recreated.
func (c *CRDT) IncrementEvictions(hash uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	state, ok := c.topics[hash]
	if !ok {
		return
	}

	state.evictions++
	state.logAge = LAGEMin
}

// UpdateLogAge updates the log-age for a topic.
func (c *CRDT) UpdateLogAge(hash uint64, logAge int32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	state, ok := c.topics[hash]
	if !ok {
		return
	}

	state.logAge = logAge
}

// RemoveTopic removes a topic from the CRDT state.
func (c *CRDT) RemoveTopic(hash uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.topics, hash)
}

// GossipData returns the gossip data for a topic.
func (c *CRDT) GossipData(hash uint64) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()

	state, ok := c.topics[hash]
	if !ok {
		return nil
	}

	// Format: [hash:8][logAge:4][evictions:4]
	data := make([]byte, 16)

	// Write hash (8 bytes, little-endian)
	for i := 0; i < 8; i++ {
		data[i] = byte(state.hash >> (i * 8))
	}

	// Write logAge (4 bytes, little-endian, signed)
	for i := 0; i < 4; i++ {
		data[8+i] = byte(uint32(state.logAge) >> (i * 8))
	}

	// Write evictions (4 bytes, little-endian)
	for i := 0; i < 4; i++ {
		data[12+i] = byte(state.evictions >> (i * 8))
	}

	return data
}

// ParseGossipData parses gossip data and updates the topic state.
func (c *CRDT) ParseGossipData(data []byte) (uint64, bool) {
	if len(data) < 16 {
		return 0, false
	}

	// Parse hash
	var hash uint64
	for i := 0; i < 8; i++ {
		hash |= uint64(data[i]) << (i * 8)
	}

	// Parse logAge
	var logAge int32
	for i := 0; i < 4; i++ {
		logAge |= int32(uint32(data[8+i]) << (i * 8))
	}

	// Parse evictions
	var evictions uint32
	for i := 0; i < 4; i++ {
		evictions |= uint32(data[12+i]) << (i * 8)
	}

	// Update the topic state
	return hash, c.Merge(hash, logAge, evictions)
}

// Validate checks if the CRDT state is valid.
func (c *CRDT) Validate() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for hash, state := range c.topics {
		// Check log-age bounds
		if state.logAge < LAGEMin || state.logAge > LAGEMax {
			return false
		}

		// Check pinned topic evictions
		if state.pinned && state.evictions < EVICTIONS_PINNED_MIN {
			return false
		}

		// Check subject-ID computation
		computed := c.ComputeSubjectID(hash, state.evictions)
		if state.pinned && computed != state.pinnedSubjectID {
			return false
		}
	}

	return true
}

// AddPinnedTopic adds a pinned topic with a specific subject-ID.
func (c *CRDT) AddPinnedTopic(name string, hash uint64, pinnedSubjectID uint32) *TopicState {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Create a new pinned topic state
	state := NewPinnedTopicState(name, hash, pinnedSubjectID)
	c.topics[hash] = state

	return state
}

// IsPinned checks if a topic is pinned.
func (c *CRDT) IsPinned(hash uint64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	state, ok := c.topics[hash]
	if !ok {
		return false
	}
	return state.pinned
}

// GetPinnedSubjectID returns the pinned subject-ID for a topic.
func (c *CRDT) GetPinnedSubjectID(hash uint64) (uint32, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	state, ok := c.topics[hash]
	if !ok || !state.pinned {
		return 0, false
	}
	return state.pinnedSubjectID, true
}

// MarkForGossip marks a topic for gossip dissemination.
func (c *CRDT) MarkForGossip(hash uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if we already have this topic marked
	for _, h := range c.pendingGossip {
		if h == hash {
			return
		}
	}
	
	c.pendingGossip = append(c.pendingGossip, hash)
}

// GetPendingGossip returns and clears the list of topics to gossip.
func (c *CRDT) GetPendingGossip() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	pending := c.pendingGossip
	c.pendingGossip = make([]uint64, 0)
	return pending
}

// UpdateFromGossip updates the local state from received gossip data.
// Returns true if any updates were made.
func (c *CRDT) UpdateFromGossip(data []byte) bool {
	hash, updated := c.ParseGossipData(data)
	if !updated {
		return false
	}
	
	// Mark this topic for re-gossip
	c.MarkForGossip(hash)
	
	return true
}

// GetAllTopicStates returns all topic states for gossip.
func (c *CRDT) GetAllTopicStates() []*TopicState {
	c.mu.RLock()
	defer c.mu.RUnlock()

	states := make([]*TopicState, 0, len(c.topics))
	for _, state := range c.topics {
		// Create a copy
		stateCopy := *state
		states = append(states, &stateCopy)
	}
	return states
}

// IncrementLogAge increments the log-age for all topics.
// This is called periodically to age out old topics.
func (c *CRDT) IncrementLogAge() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, state := range c.topics {
		if state.logAge < LAGEMax {
			state.logAge++
		}
	}
}

// Cleanup removes topics that have been evicted and not revived.
// This is called periodically to clean up stale topics.
func (c *CRDT) Cleanup() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for hash, state := range c.topics {
		// Check if this is a pinned topic
		if state.pinned {
			continue
		}
		
		// Check if the topic has been evicted and not revived
		// A topic is considered stale if it has a high log-age
		// and no recent activity
		if state.logAge > LAGEMax/2 && state.evictions > 0 {
			// Remove the topic
			delete(c.topics, hash)
		}
	}
}

// Reset resets the CRDT state (for testing).
func (c *CRDT) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.topics = make(map[uint64]*TopicState)
	c.pendingGossip = make([]uint64, 0)
	c.lastGossipTime = 0
}
