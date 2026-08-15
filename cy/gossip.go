package cy

import (
	"sync"
	"time"
)

// Gossip represents the gossip protocol state for a Cy instance.
// Gossip is used to propagate topic allocation information across the network.
type Gossip struct {
	mu sync.RWMutex

	// cy is the parent Cy instance.
	cy *Cy
	// period is the interval between gossip messages (in microseconds).
	period time.Duration

	// urgentDelayMax is the maximum delay for urgent gossip (in microseconds).
	urgentDelayMax time.Duration

	// broadcastRatio determines how often gossip is broadcast (1/n).
	broadcastRatio uint8

	// shardCount is the number of gossip shards.
	shardCount uint32

	// writers maps subject-IDs to gossip shard writers.
	writers map[uint32]*gossipWriter

	// readers maps subject-IDs to gossip shard readers.
	readers map[uint32]*gossipReader

	// pendingGossips tracks topics that need to be gossiped.
	pendingGossips []*Topic

	// lastGossipTime tracks when the last gossip was sent.
	lastGossipTime Microsecond

	// gossipCounter is incremented with each gossip sent.
	gossipCounter uint64
}

// gossipWriter represents a writer for gossip shard messages.
type gossipWriter struct {
	// subjectID is the subject-ID for this gossip shard.
	subjectID uint32
	// writer is the underlying subject writer.
	writer SubjectWriter
	// refcount tracks references.
	refcount int
}

// gossipReader represents a reader for gossip shard messages.
type gossipReader struct {
	// subjectID is the subject-ID for this gossip shard.
	subjectID uint32
	// reader is the underlying subject reader.
	reader SubjectReader
	// refcount tracks references.
	refcount int
}

// newGossip creates a new Gossip instance with default settings.
func newGossip(cy *Cy) *Gossip {
	return &Gossip{
		cy:             cy,
		period:        time.Duration(ACKBaselineDefaultTimeout) * time.Microsecond,
		urgentDelayMax: time.Duration(ACKBaselineDefaultTimeout/8) * time.Microsecond,
		broadcastRatio: 8,
		shardCount:    0, // Will be set based on subject-ID modulus
		writers:       make(map[uint32]*gossipWriter),
		readers:       make(map[uint32]*gossipReader),
		pendingGossips: make([]*Topic, 0),
	}
}

// SetPeriod sets the gossip period.
func (g *Gossip) SetPeriod(period time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.period = period
}

// Period returns the gossip period.
func (g *Gossip) Period() time.Duration {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.period
}

// SetShardCount sets the number of gossip shards.
func (g *Gossip) SetShardCount(count uint32) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.shardCount = count
}

// ShardCount returns the number of gossip shards.
func (g *Gossip) ShardCount() uint32 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.shardCount
}

// AddTopic adds a topic to the pending gossip list.
// Must be called with g.mu held.
func (g *Gossip) AddTopic(topic *Topic) {
	// Don't add if already in the list
	for _, t := range g.pendingGossips {
		if t == topic {
			return
		}
	}
	g.pendingGossips = append(g.pendingGossips, topic)
}

// RemoveTopic removes a topic from the pending gossip list.
func (g *Gossip) RemoveTopic(topic *Topic) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for i, t := range g.pendingGossips {
		if t == topic {
			g.pendingGossips = append(g.pendingGossips[:i], g.pendingGossips[i+1:]...)
			return
		}
	}
}

// HasPending returns true if there are pending gossips.
func (g *Gossip) HasPending() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.pendingGossips) > 0
}

// NextPending returns the next topic to gossip, or nil if none.
func (g *Gossip) NextPending() *Topic {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.pendingGossips) == 0 {
		return nil
	}

	// Return and remove the first topic
	topic := g.pendingGossips[0]
	g.pendingGossips = g.pendingGossips[1:]
	return topic
}

// SendGossip sends a gossip message for a topic.
func (g *Gossip) SendGossip(cy *Cy, topic *Topic) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.gossipCounter++
	g.lastGossipTime = cy.Now()

	// Create gossip data (CRDT state, post-header payload).
	data := topic.GossipData()
	if len(data) == 0 {
		return ErrArgument
	}

	// Determine which gossip shard to use
	shardID := g.getShardID(topic.hash)

	// Get or create the writer for this shard
	writer, err := g.getWriter(cy, shardID)
	if err != nil {
		return err
	}

	// Prepend the 24-byte Cy session header (C header_gossip).
	// Layout: [0]=type, [1]=0, [2]=incompat(0), [3]=lage,
	// [4:8]=evictions(0), [8:16]=hash, [16:24]=evictions(u32 LE), [23]=namelen(0).
	header := &Header{
		Type:      HeaderTypeGossip,
		Lage:      int8(topic.LogAge()),
		Hash:      topic.Hash(),
		Evictions: uint32(topic.Evictions()),
	}
	headed := PrependHeader(header, data)

	// Send the gossip message
	deadline := cy.Now() + Microsecond(g.period)
	return cy.platform.SubjectWriterSend(writer, deadline, PriorityLow, headed)
}

// getShardID computes the shard ID for a topic hash.
func (g *Gossip) getShardID(hash uint64) uint32 {
	if g.shardCount == 0 {
		return 0
	}
	return uint32(hash % uint64(g.shardCount))
}

// getWriter gets or creates a writer for a gossip shard.
func (g *Gossip) getWriter(cy *Cy, shardID uint32) (SubjectWriter, error) {
	subjectID := SubjectIDPinnedMax + 1 + shardID

	if writer, ok := g.writers[subjectID]; ok {
		return writer.writer, nil
	}

	// Create a new writer
	w, err := cy.platform.NewSubjectWriter(subjectID)
	if err != nil {
		return nil, err
	}

	gw := &gossipWriter{
		subjectID: subjectID,
		writer:    w,
		refcount: 1,
	}
	g.writers[subjectID] = gw

	return gw.writer, nil
}

// ReleaseWriter releases a gossip writer.
func (g *Gossip) ReleaseWriter(subjectID uint32) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if gw, ok := g.writers[subjectID]; ok {
		gw.refcount--
		if gw.refcount <= 0 {
			g.cy.platform.DestroySubjectWriter(gw.writer)
			delete(g.writers, subjectID)
		}
	}
}

// ProcessGossip processes an incoming gossip message payload (header already
// stripped by HandleMessage). Format: [hash:8][logAge:4][evictions:4] LE.
func (g *Gossip) ProcessGossip(data []byte, subjectID uint32) {
	if len(data) < 16 {
		return
	}

	var hash uint64
	for i := 0; i < 8; i++ {
		hash |= uint64(data[i]) << (i * 8)
	}

	var logAge int32
	for i := 0; i < 4; i++ {
		logAge |= int32(data[8+i]) << (i * 8)
	}

	var evictions uint32
	for i := 0; i < 4; i++ {
		evictions |= uint32(data[12+i]) << (i * 8)
	}

	// Update CRDT state
	if g.cy != nil && g.cy.crdt != nil {
		g.cy.crdt.UpdateFromGossip(data)
	}
}

// SchedulePeriodic schedules periodic gossip for a topic.
func (g *Gossip) SchedulePeriodic(cy *Cy, topic *Topic, now Microsecond) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Add to pending gossips
	g.AddTopic(topic)

	// Schedule the next gossip
	cy.olga.Schedule(int64(now)+int64(g.period), func() {
		g.sendPeriodicGossip(cy, topic)
	})
}

// sendPeriodicGossip sends periodic gossip for a topic.
func (g *Gossip) sendPeriodicGossip(cy *Cy, topic *Topic) {
	g.SendGossip(cy, topic)
	
	// Reschedule
	now := cy.Now()
	g.SchedulePeriodic(cy, topic, now)
}

// ScheduleUrgent schedules urgent gossip for a topic (immediately).
func (g *Gossip) ScheduleUrgent(cy *Cy, topic *Topic) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Add to pending gossips
	g.AddTopic(topic)

	// Schedule immediately with a small delay
	now := cy.Now()
	cy.olga.Schedule(int64(now)+int64(g.urgentDelayMax), func() {
		g.SendGossip(cy, topic)
	})
}

// Tick performs periodic gossip processing.
func (g *Gossip) Tick(cy *Cy) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Process pending gossips
	for g.HasPending() {
		topic := g.NextPending()
		if topic == nil {
			break
		}

		// Send gossip for this topic
		g.SendGossip(cy, topic)
	}
}
