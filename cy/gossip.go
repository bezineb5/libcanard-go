package cy

import (
	"sync"
	"time"
)

// gossipScope identifies how a gossip message was received, which governs
// suppression and auto-subscribe behavior. It mirrors the C gossip_scope_t enum.
type gossipScope int

const (
	gossipBroadcast gossipScope = iota // On the broadcast subject.
	gossipSharded                     // On a per-topic gossip shard subject.
	gossipUnicast                     // Unicast to us from a remote.
	gossipInline                      // Piggybacked in a data-frame header.
)

// Gossip represents the gossip protocol state for a Cy instance.
// Gossip is used to propagate topic-allocation (CRDT) information across the
// network, perform collision arbitration, and (optionally) auto-subscribe
// unknown topics learned from the wire name.
type Gossip struct {
	mu sync.RWMutex

	// cy is the parent Cy instance.
	cy *Cy
	// period is the baseline interval between periodic gossips (microseconds).
	period time.Duration

	// urgentDelayMax is the maximum delay for urgent gossip (microseconds).
	urgentDelayMax time.Duration

	// broadcastRatio: every Nth gossip (and the first N) is broadcast for
	// observability; the rest are sharded. Urgent gossips are always broadcast.
	broadcastRatio uint8

	// shardCount is the number of gossip shards (subject-IDs between the max valid
	// subject-ID and the broadcast subject-ID).
	shardCount uint32

	// writers maps gossip-shard subject-IDs to refcounted writers.
	writers map[uint32]*gossipWriter

	// pendingGossips tracks topics that need to be gossiped.
	pendingGossips []*Topic

	// lastGossipTime tracks when the last gossip was sent.
	lastGossipTime Microsecond

	// gossipCounter is incremented with each gossip sent (kept for diagnostics).
	gossipCounter uint64
}

// gossipWriter represents a writer for a gossip shard subject, refcounted so that
// topics sharing a shard subject share one platform writer.
type gossipWriter struct {
	subjectID uint32
	writer    SubjectWriter
	refcount  int
}

// newGossip creates a new Gossip instance with default settings.
func newGossip(cy *Cy) *Gossip {
	return &Gossip{
		cy:             cy,
		period:         time.Duration(GossipPeriod) * time.Microsecond,
		urgentDelayMax: time.Duration(GossipUrgentDelayMax) * time.Microsecond,
		broadcastRatio: GossipBroadcastRatio,
		shardCount:     0,
		writers:        make(map[uint32]*gossipWriter),
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

// SetBroadcastRatio sets the broadcast ratio (Nth gossip is broadcast).
func (g *Gossip) SetBroadcastRatio(ratio uint8) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if ratio == 0 {
		ratio = 1
	}
	g.broadcastRatio = ratio
}

// BroadcastRatio returns the broadcast ratio.
func (g *Gossip) BroadcastRatio() uint8 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.broadcastRatio
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
	topic := g.pendingGossips[0]
	g.pendingGossips = g.pendingGossips[1:]
	return topic
}

// shardSubjectFor computes the gossip shard subject-ID for a topic hash.
// Gossip shard subjects sit between the max valid subject-ID and the broadcast
// subject-ID, matching C topic_gossip_shard_subject_id():
//   subject_id = CY_SUBJECT_ID_MAX(modulus) + 1 + shard_index.
// The caller must hold g.mu.
func (c *Cy) shardSubjectFor(hash uint64) uint32 {
	shardCount := c.gossip.shardCount
	if shardCount == 0 {
		return c.broadcastSubjectID // degenerate: send on broadcast.
	}
	return SubjectIDMax(c.subjectIDModulus) + 1 + uint32(hash%uint64(shardCount))
}

// getWriter gets or creates a (refcounted) writer for a gossip shard subject.
// The caller must hold c.mu (g.mu is acquired internally).
func (g *Gossip) getWriter(cy *Cy, shardSubject uint32) (SubjectWriter, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if w, ok := g.writers[shardSubject]; ok {
		w.refcount++
		return w.writer, nil
	}
	w, err := cy.platform.NewSubjectWriter(shardSubject)
	if err != nil {
		return nil, err
	}
	g.writers[shardSubject] = &gossipWriter{subjectID: shardSubject, writer: w, refcount: 1}
	return w, nil
}

// ReleaseWriter decrements the refcount for a gossip shard writer and destroys it
// when it reaches zero.
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

// SendGossip sends a gossip message for a topic. The wire form is a 24-byte
// session header (carrying hash, log-age, evictions) followed by the topic name,
// matching C send_gossip_raw(). Urgent gossips (repair / initial allocation) are
// always broadcast; periodic gossips alternate between broadcast and the topic's
// gossip shard subject so background load is distributed.
func (g *Gossip) SendGossip(cy *Cy, topic *Topic, urgent bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := cy.Now()
	g.gossipCounter++
	g.lastGossipTime = now

	// Decide transport: broadcast subject or this topic's gossip shard subject.
	var writer SubjectWriter
	if urgent {
		writer = cy.broadWriter
	} else {
		// C computes the broadcast flag from the pre-increment counter, then increments.
		ratio := uint64(cy.gossipBroadcastRatio)
		broadcast := (topic.gossipCounter < ratio) || ((topic.gossipCounter%ratio) == 0)
		topic.gossipCounter++
		if broadcast {
			writer = cy.broadWriter
		} else {
			w, err := g.getWriter(cy, cy.shardSubjectFor(topic.hash))
			if err != nil {
				return err
			}
			writer = w
		}
	}
	if writer == nil {
		return ErrArgument
	}

	// Build the C-compatible wire form: 24-byte header + name.
	data := MarshalGossipMessage(int8(topic.Lage(now)), topic.Hash(), uint64(topic.Evictions()), topic.GossipName())

	deadline := now + Microsecond(g.period)
	return cy.platform.SubjectWriterSend(writer, deadline, PriorityLow, data)
}

// ProcessGossip handles a received gossip message. The 24-byte header has already
// been read and stripped by HandleMessage; lage/hash/evictions come from that
// header and name is the remaining payload (the topic name, if any). It mirrors
// C on_gossip().
func (g *Gossip) ProcessGossip(lage int8, hash, evictions uint64, name []byte, subjectID uint32, scope gossipScope, now Microsecond) {
	if lage < LAGEMin || lage > LAGEMax {
		return
	}
	g.cy.onGossip(now, hash, uint32(evictions), int32(lage), string(name), scope)
}

// onGossip is the central gossip convergence routine. It mirrors C on_gossip():
// it routes to a known-topic handler, an unknown-topic (collision) handler, or
// auto-subscribes an unknown topic that matches a local pattern subscription.
// The caller must hold c.mu.
func (c *Cy) onGossip(now Microsecond, hash uint64, evictions uint32, lage int32, name string, scope gossipScope) {
	mine := c.findTopicByHash(hash)
	if mine == nil && name != "" && (scope == gossipBroadcast || scope == gossipUnicast) {
		mine = c.topicSubscribeIfMatching(name, hash, evictions, int32(lage), now)
	}
	if mine != nil {
		c.onGossipKnownTopic(mine, evictions, int32(lage), scope, now)
	} else {
		c.onGossipUnknownTopic(hash, evictions, int32(lage), now)
	}
}

// onGossipKnownTopic handles gossip for a locally known topic (same hash).
// It mirrors C on_gossip_known_topic(): it arbitrates subject-ID divergence and,
// if the local topic loses, reallocates it to converge; otherwise it just merges
// the log-age and may suppress further gossip.
// The caller must hold c.mu.
func (c *Cy) onGossipKnownTopic(mine *Topic, evictions uint32, lage int32, scope gossipScope, now Microsecond) {
	// Treat any message as activity on the topic (keeps implicit topics alive).
	if mine.evictions != evictions {
		_, win := mine.UpdateState(now, lage, evictions)
		if win {
			// We win: keep our subject-ID and tell the remote to move.
			c.gossip.ScheduleUrgent(c, mine)
		} else {
			// We lose: adopt the remote's eviction counter (and thus its
			// subject-ID), converging both nodes.
			c.topicAllocate(mine, evictions, now)
			if mine.evictions == evictions {
				// Subject occupancy unchanged; no urgent repair needed.
				c.gossip.SchedulePeriodic(c, mine, true)
			}
		}
	} else {
		mine.MergeLageAt(now, lage)
		// Suppress gossip if we're already up to date and the gossip reached a
		// large audience (broadcast/sharded); inline and unicast gossips are only
		// seen by a small subset, so they never suppress others.
		suppress := (scope == gossipBroadcast || scope == gossipSharded) &&
			(mine.Lage(now) == lage)
		if suppress {
			c.gossip.SchedulePeriodic(c, mine, true)
		}
	}
	// Repair the subscription if it was broken (eager reader acquisition).
	c.resyncTopicReader(mine)
}

// onGossipUnknownTopic handles gossip for a topic we don't know by hash, but whose
// desired subject-ID we currently occupy with a different topic. It arbitrates the
// collision and, if we lose, re-evicts our topic. It mirrors C
// on_gossip_unknown_topic().
// The caller must hold c.mu.
func (c *Cy) onGossipUnknownTopic(hash uint64, evictions uint32, lage int32, now Microsecond) {
	subjectID := ComputeSubjectID(hash, evictions, c.subjectIDModulus)
	mine := c.topicsBySubjectID[subjectID]
	if mine == nil {
		return // We are not using this subject-ID; no collision.
	}
	win := c.leftWins(mine, now, lage, hash)
	if win {
		// Announce that we are using this subject-ID; the remote will move.
		c.gossip.ScheduleUrgent(c, mine)
	} else {
		// We lose the collision: re-evict our topic to a new subject-ID.
		c.topicAllocate(mine, mine.evictions+1, now)
	}
}

// topicSubscribeIfMatching auto-subscribes an unknown topic learned from gossip
// when a local pattern subscription matches its name. It mirrors C
// topic_subscribe_if_matching(). Returns the (possibly newly created) topic, or
// nil if there is no matching pattern. The caller must hold c.mu.
func (c *Cy) topicSubscribeIfMatching(name string, hash uint64, evictions uint32, lage int32, now Microsecond) *Topic {
	if len(c.patternMatcher.Match(name)) == 0 {
		return nil
	}
	// Already known? Then just converge it.
	if existing, ok := c.topicsByName[name]; ok {
		return existing
	}
	topic, err := newTopic(name, c.subjectIDModulus)
	if err != nil {
		return nil
	}
	topic.extent = DefaultTopicExtent
	c.topicsByName[name] = topic
	c.topicsByHash[topic.hash] = topic
	topic.SetOriginAt(now, lage, evictions)
	c.crdt.AddTopic(name, topic.hash)
	c.topicAllocate(topic, evictions, now)
	c.gossip.ScheduleUrgent(c, topic)
	return topic
}

// SchedulePeriodic schedules periodic gossip for a topic, optionally suppressed
// (when the node is up to date and another node is the dedicated gossiper).
func (g *Gossip) SchedulePeriodic(cy *Cy, topic *Topic, suppressed bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.AddTopic(topic)

	// Simple periodic reschedule with light dithering.
	dither := time.Duration(GossipPeriod) / GossipPeriodDitherRatio
	var delay time.Duration
	if suppressed {
		delay = g.period + dither
	} else {
		delay = g.period - dither
	}
	at := cy.Now() + Microsecond(delay)
	cy.olga.Schedule(int64(at), func() {
		g.SendGossip(cy, topic, false)
		g.SchedulePeriodic(cy, topic, false)
	})
}

// ScheduleUrgent schedules urgent (repair / initial-allocation) gossip for a
// topic, broadcast and reset to broadcast-ratio eligibility, matching C
// gossip_event_urgent.
func (g *Gossip) ScheduleUrgent(cy *Cy, topic *Topic) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.AddTopic(topic)
	topic.gossipCounter = 0 // Urgent gossips are always broadcast; reset eligibility.

	at := cy.Now() + Microsecond(g.urgentDelayMax)
	cy.olga.Schedule(int64(at), func() {
		g.SendGossip(cy, topic, true)
	})
}

// Tick performs periodic gossip processing.
func (g *Gossip) Tick(cy *Cy) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for g.HasPending() {
		topic := g.NextPending()
		if topic == nil {
			break
		}
		_ = g.SendGossip(cy, topic, false)
	}
}
