package cy

import (
	"encoding/binary"
	"sync"
	"unsafe"

	"github.com/opencyphal/cy-go/cavl"
	"github.com/opencyphal/cy-go/olga"
	"go.dw1.io/rapidhash"
)

// Cy is the main Cyphal instance.
// It manages topics, publishers, subscribers, and the event loop.
type Cy struct {
	// Platform is the underlying transport/platform layer.
	platform Platform

	// olga is the event loop scheduler.
	olga *olga.Scheduler

	// home is the local node name.
	home string
	// ns is the namespace prefix.
	ns string

	// remap contains name remapping configuration.
	// Keys are normalized from-prefixes, values are to-prefixes.
	remap map[string]string

	// topicsByName maps topic names to Topic instances.
	topicsByName map[string]*Topic
	// topicsByHash maps topic hashes to Topic instances.
	topicsByHash map[uint64]*Topic
	// topicsBySubjectID maps subject-IDs to Topic instances (non-pinned only).
	topicsBySubjectID map[uint32]*Topic

	// publishers maps topics to Publisher instances.
	publishers map[*Topic]*Publisher
	// subscribers maps topics to lists of Subscriber instances.
	subscribers map[*Topic][]*Subscriber

	// unicastExtent is the maximum size for incoming unicast messages.
	unicastExtent int

	// startedAt is the timestamp when the Cy instance was created.
	startedAt Microsecond

	// implicitTopicTimeout is how long implicit topics are kept after inactivity.
	implicitTopicTimeout Microsecond
	// ackBaselineTimeout is the default timeout for reliable message ACKs.
	ackBaselineTimeout Microsecond

	// gossip manages the gossip protocol.
	gossip *Gossip
	// rpc manages RPC and streaming.
	rpc *RPC
	// crdt manages CRDT-based topic allocation.
	crdt *CRDT
	// patternMatcher manages pattern matching for subscriptions.
	patternMatcher *PatternMatcher

	// subjectIDModulus is the subject-ID modulus for this network.
	subjectIDModulus uint32

	// broadcastSubjectID is the subject-ID reserved for broadcast transport
	// (broadcast gossips, scouts, and other protocol needs). It is the largest
	// subject-ID of the form 2^k-1 that is >= SubjectIDMax(modulus).
	broadcastSubjectID uint32
	// broadWriter is the subject writer for the broadcast subject; urgent and
	// periodic broadcast gossips are sent through it.
	broadWriter SubjectWriter
	// gossipBroadcastRatio: every Nth gossip (and multiples of N) is broadcast for
	// observability; the rest are sharded. Urgent gossips are always broadcast.
	gossipBroadcastRatio uint8

	// scouting tracks pattern subscriptions that still need to emit a scout
	// (broadcast request for matching gossips). Entries are removed once a scout
	// is sent successfully; remaining entries are retried by sendPendingScouts().
	scouting map[string]struct{}

	// prngState is the PRNG state for random number generation.
	prngState uint64

	// mu protects the Cy instance from concurrent access.
	mu sync.RWMutex
	// diagMu guards the diagnostics listener list (diags). It is intentionally
	// separate from mu so diagnostics dispatches do not deadlock with, and are
	// unaffected by, the main instance lock.
	diagMu sync.Mutex
	// diags is the singly-linked list of installed diagnostics listeners.
	diags *Diag

	// onMessage is the callback for received messages.
	onMessage func(lane Lane, subjectID *uint32, message MessageTS)
}

// New creates a new Cy instance with the specified platform and node name.
// The namespace and remapConfig are optional and can be empty strings.
func New(platform Platform, home, namespace, remapConfig string) (*Cy, error) {
	if platform == nil {
		return nil, ErrArgument
	}

	cy := &Cy{
		platform:             platform,
		olga:                 olga.New(),
		home:                 home,
		ns:                   namespace,
		remap:                make(map[string]string),
		topicsByName:         make(map[string]*Topic),
		topicsByHash:         make(map[uint64]*Topic),
		topicsBySubjectID:    make(map[uint32]*Topic),
		publishers:           make(map[*Topic]*Publisher),
		subscribers:          make(map[*Topic][]*Subscriber),
		unicastExtent:        0,
		startedAt:            0,
		implicitTopicTimeout: ImplicitTopicDefaultTimeout,
		ackBaselineTimeout:   ACKBaselineDefaultTimeout,
		gossip:               nil, // Will be initialized below
		rpc:                  nil, // Will be initialized below
		crdt:                 nil, // Will be initialized below
		patternMatcher:       NewPatternMatcher(),
		subjectIDModulus:     SubjectIDModulus16bit,
		prngState:            0,
	}

	// Set up the platform
	platform.SetCy(cy)

	// Parse remap configuration
	if remapConfig != "" {
		// TODO: Parse remap config string
		// Format: "from1=to1 from2=to2 ..."
	}

	// Set up the olga scheduler's time function
	cy.olga.SetNowFunc(func() int64 { return int64(cy.Now()) })

	// Initialize components that need the cy reference
	cy.rpc = newRPC(cy)
	cy.crdt = NewCRDT(cy, SubjectIDModulus16bit)
	cy.gossip = newGossip(cy)
	cy.scouting = make(map[string]struct{})

	// Set up the gossip/broadcast subject layout. The broadcast subject-ID and the
	// gossip shard count are derived from the modulus; they must be created before
	// any topics are allocated so gossip can target the right subjects.
	cy.subjectIDModulus = SubjectIDModulus16bit
	cy.broadcastSubjectID = BroadcastSubjectID(cy.subjectIDModulus)
	cy.gossipBroadcastRatio = GossipBroadcastRatio
	if err := cy.initBroadcastSubject(); err != nil {
		return nil, err
	}
	cy.gossip.SetShardCount(GossipShardCount(cy.subjectIDModulus))
	cy.gossip.SetBroadcastRatio(GossipBroadcastRatio)

	// Set up message callback
	cy.onMessage = func(lane Lane, subjectID *uint32, message MessageTS) {
		cy.HandleMessage(lane, subjectID, message)
	}

	// Set the platform's message callback.
	// Works for a bare *PlatformBase and for any platform that embeds it
	// (the SetOnMessage method is promoted), so embedding platforms are wired correctly.
	if s, ok := platform.(interface{ SetOnMessage(OnMessageCallback) }); ok {
		s.SetOnMessage(cy.onMessage)
	}

	return cy, nil
}

// Destroy cleans up the Cy instance.
// All publishers and subscribers must be destroyed before calling this.
func (c *Cy) Destroy() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Clean up all topics
	for _, topic := range c.topicsByName {
		c.destroyTopic(topic)
	}
	c.topicsByName = make(map[string]*Topic)
	c.topicsByHash = make(map[uint64]*Topic)
	c.topicsBySubjectID = make(map[uint32]*Topic)

	// Clean up publishers and subscribers
	c.publishers = make(map[*Topic]*Publisher)
	c.subscribers = make(map[*Topic][]*Subscriber)

	// Clean up gossip
	if c.gossip != nil {
		c.gossip = nil
	}

	// Clean up the broadcast subject writer
	if c.broadWriter != nil {
		c.platform.DestroySubjectWriter(c.broadWriter)
		c.broadWriter = nil
	}

	// Clean up RPC
	if c.rpc != nil {
		c.rpc = nil
	}

	// Clean up CRDT
	if c.crdt != nil {
		c.crdt = nil
	}

	// Clean up pattern matcher
	if c.patternMatcher != nil {
		c.patternMatcher = nil
	}

	// Stop the scheduler
	c.olga.Stop()

	// Clear the platform reference
	c.platform.SetCy(nil)
}

// destroyTopic destroys a topic and all its associated state.
func (c *Cy) destroyTopic(topic *Topic) {
	// Report destruction to diagnostics listeners first, mirroring C's
	// diag_topic_destroyed placement at the start of topic destruction.
	c.DiagTopicDestroyed(topic)

	// Remove from all maps
	delete(c.topicsByName, topic.name)
	delete(c.topicsByHash, topic.hash)
	if !topic.pinned {
		delete(c.topicsBySubjectID, topic.subjectID)
	}

	// Destroy all publishers on this topic
	if pub, ok := c.publishers[topic]; ok {
		pub.Destroy()
		delete(c.publishers, topic)
	}

	// Destroy all subscribers on this topic
	if subs, ok := c.subscribers[topic]; ok {
		for _, sub := range subs {
			sub.Destroy()
		}
		delete(c.subscribers, topic)
	}
}

// Now returns the current time from the platform.
func (c *Cy) Now() Microsecond {
	return c.platform.Now()
}

// Random returns a random 64-bit value from the platform.
func (c *Cy) Random() uint64 {
	return c.platform.Random()
}

// RPC returns the RPC manager.
func (c *Cy) RPC() *RPC {
	return c.rpc
}

// Gossip returns the gossip manager.
func (c *Cy) Gossip() *Gossip {
	return c.gossip
}

// CRDT returns the CRDT manager.
func (c *Cy) CRDT() *CRDT {
	return c.crdt
}

// BroadcastSubjectID returns the subject-ID reserved for broadcast transport
// (broadcast gossips, scouts, and other protocol needs).
func (c *Cy) BroadcastSubjectID() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.broadcastSubjectID
}

// Spin runs the event loop until the specified deadline.
// This is the main entry point for processing messages and timers.
func (c *Cy) Spin(deadline Microsecond) error {
	return c.SpinUntil(deadline)
}

// SpinUntil runs the event loop until the specified deadline.
// It processes incoming messages and executes scheduled tasks.
func (c *Cy) SpinUntil(deadline Microsecond) error {
	// Process scheduled tasks
	c.olga.RunUntil(int64(deadline))
	// olga.RunUntil doesn't return an error currently

	// Retry any scouts that failed to emit when their pattern subscription was created.
	c.sendPendingScouts()

	// Process platform events
	err := c.platform.Spin(deadline)
	if err != nil {
		return err
	}

	return OK
}

// HandleMessage handles an incoming message.
// This is called by platform implementations to deliver received messages.
func (c *Cy) HandleMessage(lane Lane, subjectID *uint32, message MessageTS) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if subjectID != nil {
		// Multicast message
		c.handleMulticastMessage(lane, *subjectID, message)
	} else {
		// Unicast message
		c.handleUnicastMessage(lane, message)
	}
}

// handleMulticastMessage handles a multicast/broadcast/shard message.
// It mirrors C cy_on_message: read the 24-byte session header, skip it, then
// dispatch on the message type. Data frames carry inline CRDT gossip (subject-ID
// consistency + collision arbitration); gossip frames drive consensus; scout
// frames trigger responses.
func (c *Cy) handleMulticastMessage(lane Lane, subjectID uint32, message MessageTS) {
	if message.Content == nil || message.Content.Size() < HeaderSize {
		return
	}
	// Read the header before skipping it.
	var hdr [HeaderSize]byte
	if message.Content.Read(0, HeaderSize, hdr[:]) != HeaderSize {
		return
	}
	msgType := HeaderType(hdr[0])
	lage := int8(hdr[3])
	// evictions is read per message type below (data: [4:8]; gossip: [16:20]).
	hash := binary.LittleEndian.Uint64(hdr[8:16])
	msgTag := binary.LittleEndian.Uint64(hdr[16:24])
	// C always skips the session header before dispatch.
	message.Content.Skip(HeaderSize)

	now := message.Timestamp // C uses the message arrival timestamp for CRDT merge/arbitration.
	broadcast := subjectID == c.broadcastSubjectID
	multicast := !broadcast && subjectID <= SubjectIDMax(c.subjectIDModulus)

	switch msgType {
	case HeaderTypeMsgBE, HeaderTypeMsgRel:
		if broadcast {
			return // Data frames are never carried on the broadcast subject.
		}
		reliable := msgType == HeaderTypeMsgRel
		// For data frames the eviction counter is carried at [4:8].
		evictions := binary.LittleEndian.Uint32(hdr[4:8])
		if multicast && ComputeSubjectID(hash, evictions, c.subjectIDModulus) != subjectID {
			// Inline CRDT gossip is inconsistent with the subject we received on;
			// the sender is malfunctioning. Drop the frame (C logs and bails).
			return
		}
		topic := c.findTopicByHash(hash)
		if topic != nil {
			// Inline CRDT gossip + deliver to subscribers.
			c.onGossipKnownTopic(topic, evictions, int32(lage), gossipInline, now)
			if subs, ok := c.subscribers[topic]; ok {
				for _, sub := range subs {
					sub.Deliver(message, lane, msgTag)
				}
			}
			for sub := range c.patternMatcher.Match(topic.name) {
				sub.Deliver(message, lane, msgTag)
			}
			_ = reliable
		} else {
			// We occupy this subject-ID for a different topic: collision arbitration.
			c.onGossipUnknownTopic(hash, evictions, int32(lage), now)
		}

	case HeaderTypeGossip:
		if multicast {
			return // Gossip is never carried on a normal multicast subject.
		}
		var scope gossipScope
		if broadcast {
			scope = gossipBroadcast
		} else {
			scope = gossipSharded
		}
		// For gossip frames the eviction counter is carried at [16:20] and the
		// trailing name length at byte 23 (the [16:24] trailer), not at [4:8].
		evictions := binary.LittleEndian.Uint32(hdr[16:20])
		// The header has been skipped; the remainder is the topic name.
		c.gossip.ProcessGossip(lage, hash, uint64(evictions), message.Content.Payload(), subjectID, scope, now)

	case HeaderTypeScout:
		// Scouts are always broadcast; they ask us to unicast gossips for any local
		// topics matching the carried pattern. (C on_scout.)
		pattern := string(message.Content.Payload())
		if pattern != "" {
			c.onScout(pattern, lane, now)
		}

	default:
		// Unknown/unsupported multicast type.
	}
}

// handleUnicastMessage handles an incoming unicast message.
// Mirrors C cy_on_message unicast branch: read type + tag from the 24-byte header,
// skip it, then dispatch on the type. The tag (header field at offset 16:24) is the
// correlation tag for ACK/NACK/response handling.
func (c *Cy) handleUnicastMessage(lane Lane, message MessageTS) {
	if message.Content == nil || message.Content.Size() < HeaderSize {
		return
	}
	// Read the type byte from offset 0, the topic hash from offset 8, and the tag
	// from offset 16 before skipping. ACK/NACK headers carry the topic hash at
	// [8:16] and the message tag at [16:24] (see header.go), which we use to
	// route the ACK to the correct publisher.
	var hdr [HeaderSize]byte
	if message.Content.Read(0, HeaderSize, hdr[:]) != HeaderSize {
		return
	}
	msgType := HeaderType(hdr[0])
	topicHash := binary.LittleEndian.Uint64(hdr[8:16])
	tag := binary.LittleEndian.Uint64(hdr[16:24])
	message.Content.Skip(HeaderSize)

	switch msgType {
	case HeaderTypeMsgAck, HeaderTypeMsgNack:
		c.handleACKMessage(lane, topicHash, tag, msgType == HeaderTypeMsgAck)
	case HeaderTypeRspBE, HeaderTypeRspRel:
		parsed, err := ParseResponseHeader(hdr[:])
		if err != nil {
			return
		}
		c.handleResponseMessage(lane, parsed.MessageTag, parsed.Seqno, parsed.Tag, parsed.Hash, parsed.Reliable, Response{
			RemoteID:  lane.ID,
			Seqno:     parsed.Seqno,
			Timestamp: c.Now(),
			Message:   &message,
		})
	case HeaderTypeRspAck, HeaderTypeRspNack:
		c.handleResponseAckMessage(lane, hdr, message)
	case HeaderTypeGossip:
		// A unicast gossip (e.g., a scout response) carries the topic name as the
		// payload and is processed with gossip_unicast scope so unknown topics are
		// auto-subscribed. (C on_gossip with unicast scope.)
		age := int8(hdr[3])
		hash := binary.LittleEndian.Uint64(hdr[8:16])
		evictions := binary.LittleEndian.Uint32(hdr[16:20])
		c.gossip.ProcessGossip(age, hash, uint64(evictions), message.Content.Payload(), 0, gossipUnicast, message.Timestamp)
	default:
		// Unicast carrying a non-unicast type is invalid; ignore.
	}
}

// sendScout broadcasts a scout frame for the given pattern, asking every peer to
// unicast gossips for any local topics matching the pattern. It mirrors C
// do_send_scout(). On success the pattern is removed from the pending-scout set.
// The caller must hold c.mu. It does not re-enter the lock (the platform send is
// lock-free with respect to c.mu).
func (c *Cy) sendScout(pattern string) error {
	if c.broadWriter == nil {
		return ErrArgument
	}
	now := c.Now()
	deadline := now + MEGA
	data := MarshalScout(pattern)
	if err := c.platform.SubjectWriterSend(c.broadWriter, deadline, PriorityNominal, data); err != nil {
		// Resubscription / scout broadcast failure during a consensus update. Keep
		// the pattern in the scouting set so it is retried; also surface it to
		// diagnostics listeners (C: ON_ASYNC_ERROR_IF after do_send_scout).
		c.DiagAsyncError(nil, errorToCyErr(err))
		return err
	}
	delete(c.scouting, pattern)
	return nil
}

// sendPendingScouts retries any scouts that previously failed to send. It is
// invoked from Spin() so pattern subscriptions that could not emit a scout at
// creation time eventually do. The caller must NOT hold c.mu.
func (c *Cy) sendPendingScouts() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for pattern := range c.scouting {
		_ = c.sendScout(pattern)
	}
}

// onScout handles an incoming scout: for each local topic whose name matches the
// scout's pattern, send a unicast gossip directly to the asking node so it can
// auto-subscribe the topic. It mirrors C on_scout(). The caller must hold c.mu.
func (c *Cy) onScout(pattern string, lane Lane, now Microsecond) {
	for name, topic := range c.topicsByName {
		if matched, _ := MatchPattern(pattern, name); matched {
			_ = c.gossip.sendGossipUnicast(c, topic, lane)
		}
	}
}

// handleResponseMessage correlates an incoming response with a pending/retained request, faithfully
// porting C's header_rsp_be/rel branch: deduplicate reliable responses and answer with a response
// ACK (fresh/known) or NACK (orphan/too-old) on the wire. Best-effort responses are delivered silently.
func (c *Cy) handleResponseMessage(lane Lane, messageTag, seqno uint64, tag byte, hash uint64, reliable bool, response Response) {
	c.rpc.mu.Lock()
	verdict := c.rpc.handleResponseCorrelation(messageTag, response.RemoteID, seqno, reliable, response)
	c.rpc.sweepRequestAcks(c.Now())
	c.rpc.mu.Unlock()

	if reliable && verdict != responseRxSilent {
		c.rpc.sendResponseAck(lane, messageTag, seqno, tag, hash, verdict == responseRxAck)
	}
}

// handleACKMessage routes an incoming message ACK/NACK (C header_msg_ack/nack) to
// the publisher responsible for the tagged topic. The topic hash carried in the
// ACK header identifies the publisher (C collapses per-node ACKs, but routing by
// topic keeps the Go model faithful to one-publisher-per-topic). Unknown topics
// or tags are ignored (orphaned/duplicate ACKs), matching C's trace path.
//
// NOTE: HandleMessage already holds c.mu (write) for the duration of message
// dispatch, so this function must NOT re-acquire c.mu (RWMutex is not
// re-entrant for a write-locked goroutine).
func (c *Cy) handleACKMessage(lane Lane, topicHash, tag uint64, positive bool) {
	topic, ok := c.topicsByHash[topicHash]
	if !ok {
		return
	}
	pub := c.publishers[topic]
	if pub == nil {
		return
	}

	if positive {
		pub.handleAck(lane.ID, tag)
	} else {
		pub.handleNack(lane.ID, tag)
	}
}

// handleResponseAckMessage handles an incoming response ACK/NACK (C header_rsp_ack/nack).
// Faithful port of the C header_rsp_ack/rsp_nack branch: look up the pending reliable
// response by respondKey(lane.id, message_tag, hash, seqno, tag) and, on a full match of
// the breadcrumb correlation fields, complete it (ACK -> OK, NACK -> ErrNACK). Unknown
// ACKs (duplicate or orphaned) are ignored, matching C's orphan-ACK trace path.
func (c *Cy) handleResponseAckMessage(lane Lane, hdr [HeaderSize]byte, message MessageTS) {
	parsed, err := ParseResponseHeader(hdr[:])
	if err != nil {
		return
	}
	positive := parsed.Type == HeaderTypeRspAck
	r := c.rpc
	key := respondKey(lane.ID, parsed.MessageTag, parsed.Hash, parsed.Seqno, parsed.Tag)
	r.mu.RLock()
	fut, ok := r.respondFutures[key]
	r.mu.RUnlock()
	if !ok {
		return // duplicate or orphaned ACK; nothing pending.
	}
	// Full match check to guard against key collisions (mirrors C's match guard).
	if fut.breadcrumb == nil ||
		fut.breadcrumb.RemoteID != lane.ID ||
		fut.breadcrumb.MessageTag != parsed.MessageTag ||
		fut.hash != parsed.Hash ||
		fut.seqno != parsed.Seqno ||
		fut.tag != parsed.Tag {
		return
	}
	fut.onAck(positive)
}

// handleRequestMessage is unused under the C wire model (requests arrive via the
// subscriber path / unicast). Retained for API completeness.
func (c *Cy) handleRequestMessage(message MessageTS) {
}
func (c *Cy) Advertise(topicName string) (*Publisher, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Resolve the topic name (apply namespace and remapping)
	resolvedName := c.resolveTopicName(topicName)

	// Find or create the topic
	topic, err := c.findOrCreateTopic(resolvedName, DefaultTopicExtent)
	if err != nil {
		return nil, err
	}

	// Check if there's already a publisher for this topic
	if pub, ok := c.publishers[topic]; ok {
		return pub, nil
	}

	// Create a new publisher
	pub := NewPublisher(c, topic)
	c.publishers[topic] = pub

	return pub, nil
}

// Subscribe creates a new subscriber for the specified topic.
// The extent is the maximum message size to accept.
// If the topicName contains wildcards (* or >), it creates a pattern subscription.
// Returns a Subscriber that will be completed when messages arrive.
func (c *Cy) Subscribe(topicName string, extent int) (*Subscriber, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subscribeLocked(topicName, extent, false)
}

// subscribeLocked resolves a subscription. It assumes c.mu is already held by the
// caller; this lets the auto-subscribe (gossip) path subscribe from within the
// handle-message critical section without re-entering the lock.
func (c *Cy) subscribeLocked(topicName string, extent int, ordered bool) (*Subscriber, error) {
	// Resolve the topic name
	resolvedName := c.resolveTopicName(topicName)

	// Check if this is a pattern subscription
	if IsPattern(resolvedName) {
		// Pattern subscribers are inherently unordered; an ordered request on a
		// pattern name still yields a regular pattern subscriber (matches the
		// pre-existing SubscribeOrdered behavior).
		sub := NewPatternSubscriber(c, resolvedName, extent)

		// Add to pattern matcher
		firstForPattern := !c.patternMatcher.HasPattern(resolvedName)
		c.patternMatcher.AddPattern(resolvedName, sub)

		// A freshly created pattern subscription scouts the network for matching
		// topics: we broadcast a scout and peers respond with unicast gossips
		// (C sets needs_scouting and calls do_send_scout on root creation).
		if firstForPattern {
			c.scouting[resolvedName] = struct{}{}
			c.sendScout(resolvedName)
		}

		return sub, nil
	}

	// Find or create the topic
	topic, err := c.findOrCreateTopic(resolvedName, extent)
	if err != nil {
		return nil, err
	}
	// Create a new subscriber
	var sub *Subscriber
	if ordered {
		sub = NewOrderedSubscriber(c, topic, extent)
	} else {
		sub = NewSubscriber(c, topic, extent)
	}
	c.subscribers[topic] = append(c.subscribers[topic], sub)

	return sub, nil
}

// SubscribeOrdered creates a new ordered subscriber for the specified topic.
// Ordered subscribers receive messages in the exact order they were sent.
func (c *Cy) SubscribeOrdered(topicName string, extent int) (*Subscriber, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subscribeLocked(topicName, extent, true)
}

// resolveTopicName applies namespace and remapping to a topic name.
func (c *Cy) resolveTopicName(name string) string {
	// Apply namespace prefix if the name doesn't start with /
	if c.ns != "" && len(name) > 0 && name[0] != '/' {
		name = c.ns + "/" + name
	}

	// Apply remapping
	if remapped, ok := c.remap[name]; ok {
		name = remapped
	}

	return name
}

// findOrCreateTopic finds a topic by name or creates it if it doesn't exist.
// extent is the maximum message size for the topic (per subscription) and is applied
// before the platform reader is created. The topic's subject-ID is then allocated
// (and possibly re-allocated on collision) by topicAllocate. The caller must hold
// c.mu.
func (c *Cy) findOrCreateTopic(name string, extent int) (*Topic, error) {
	// Check if the topic already exists
	if topic, ok := c.topicsByName[name]; ok {
		return topic, nil
	}
	// Create a new topic
	topic, err := newTopic(name, c.subjectIDModulus)
	if err != nil {
		return nil, err
	}
	topic.extent = extent

	// Add to CRDT (canonical allocation state). Pinned topics keep their
	// eviction encoding (UINT32_MAX - subject_id); only non-pinned topics adopt
	// the CRDT's zero eviction counter.
	state := c.crdt.AddTopic(name, topic.hash)
	if !topic.pinned {
		topic.evictions = state.evictions
	} else {
		// Pinned topics keep the eviction encoding UINT32_MAX - subject_id, which
		// is what lets topicAllocate() recognize and preserve them. The CRDT state
		// carries zero evictions (it has no notion of pinning).
		topic.evictions = ^topic.pinnedSubjectID
	}

	// A freshly created topic is observed at 'now', so its log-age is LAGEMin and its
	// tsOrigin is now. (Auto-subscribed topics instead inherit the remote's age.)
	now := c.Now()
	topic.SetOriginAt(now, state.logAge, topic.evictions)

	// Add to the name and hash indexes. The subject-ID index entry is managed by
	// topicAllocate (which may also re-index on collision).
	c.topicsByName[name] = topic
	c.topicsByHash[topic.hash] = topic

	// Allocate (and, if necessary, re-allocate on collision) the subject-ID.
	c.topicAllocate(topic, topic.evictions, now)

	// Schedule gossip for this topic so the network learns our allocation.
	c.gossip.ScheduleUrgent(c, topic)

	// Report the new topic to diagnostics listeners (after its allocation is
	// committed, mirroring C's diag_topic_created placement).
	c.DiagTopicCreated(topic)

	return topic, nil
}

// findTopicBySubjectID finds a topic by its subject-ID.
func (c *Cy) findTopicBySubjectID(subjectID uint32) *Topic {
	// Check pinned topics first (they can share subject-IDs)
	// For now, just check the map
	return c.topicsBySubjectID[subjectID]
}

// FindTopicByHash finds a topic by its hash.
func (c *Cy) FindTopicByHash(hash uint64) *Topic {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.topicsByHash[hash]
}

// FindTopic finds a topic by its name.
func (c *Cy) FindTopic(name string) *Topic {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.topicsByName[name]
}

// FindOrCreateTopic finds a topic by name or creates it if it does not exist.
// It mirrors the C cy_find_topic_or_create() entry point.
func (c *Cy) FindOrCreateTopic(name string, extent int) (*Topic, error) {
	return c.findOrCreateTopic(name, extent)
}

// SubjectIDModulus returns the configured subject-ID modulus.
func (c *Cy) SubjectIDModulus() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.subjectIDModulus
}

// TopicsBySubjectID returns a snapshot of the live subject-ID -> topic index.
// Pinned topics are not included (they are not indexed by subject-ID).
func (c *Cy) TopicsBySubjectID() map[uint32]*Topic {
	c.mu.RLock()
	defer c.mu.RUnlock()
	snap := make(map[uint32]*Topic, len(c.topicsBySubjectID))
	for k, v := range c.topicsBySubjectID {
		snap[k] = v
	}
	return snap
}

// PendingScouts returns the set of pattern subscriptions that still need to emit
// a scout (their broadcast scout has not yet been sent successfully).
func (c *Cy) PendingScouts() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.scouting))
	for p := range c.scouting {
		out = append(out, p)
	}
	return out
}

// SetSubjectIDModulus sets the subject-ID modulus for the network.
// The modulus is validated: it must be a prime number congruent to 3 mod 4 and at
// least SubjectIDModulus16bit, otherwise the quadratic-probing subject-ID
// function would not cover all residues and fast eviction reconstruction would be
// impossible. On success the broadcast subject and gossip shard layout are
// recomputed and the broadcast writer is recreated.
func (c *Cy) SetSubjectIDModulus(modulus uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !IsValidSubjectIDModulus(modulus) {
		return ErrArgument
	}

	c.subjectIDModulus = modulus
	c.crdt.SetSubjectIDModulus(modulus)
	c.broadcastSubjectID = BroadcastSubjectID(modulus)
	if err := c.initBroadcastSubject(); err != nil {
		return err
	}
	c.gossip.SetShardCount(GossipShardCount(modulus))
	return nil
}

// initBroadcastSubject (re)creates the broadcast subject writer used for urgent
// and periodic broadcast gossips. It mirrors the cy->broad_writer setup in C
// cy_new(). The previous writer, if any, is destroyed first.
func (c *Cy) initBroadcastSubject() error {
	if c.broadWriter != nil {
		c.platform.DestroySubjectWriter(c.broadWriter)
		c.broadWriter = nil
	}
	// A reader on the broadcast subject is intentionally not created: in this port
	// the broadcast scope is detected by subject-ID value in handleMulticastMessage,
	// and the platform delivers by subject-ID regardless.
	w, err := c.platform.NewSubjectWriter(c.broadcastSubjectID)
	if err != nil {
		return err
	}
	c.broadWriter = w
	return nil
}

// findTopicByHash finds a topic by its hash (write lock not held by caller).
func (c *Cy) findTopicByHash(hash uint64) *Topic {
	return c.topicsByHash[hash]
}

// leftWins is the conflict comparator used only on subject-ID allocation
// conflicts. Per the model, the OLDER topic wins; ties are broken by the smaller
// hash. It mirrors C left_wins().
func (c *Cy) leftWins(left *Topic, now Microsecond, rLage int32, rHash uint64) bool {
	lLage := left.Lage(now)
	if lLage != rLage {
		return lLage > rLage
	}
	return left.hash < rHash
}

// topicAllocate is the consensus reallocation operator. Given a topic and a
// desired eviction counter, it (re)computes the topic's subject-ID, arbitrates
// collisions against other local topics, and recursively re-evicts the loser.
// It mirrors C topic_allocate() and the AllocateTopic(t, topics) TLA+ operator.
// The caller must hold c.mu.
func (c *Cy) topicAllocate(topic *Topic, newEvictions uint32, now Microsecond) {
	modulus := c.subjectIDModulus

	// We cannot mutate the eviction counter while the topic is indexed by subject-ID,
	// so remove it first. (No-op if it is not currently indexed, e.g. a brand-new topic.)
	if !topic.pinned {
		delete(c.topicsBySubjectID, topic.subjectID)
	}

	// Pinned topics are not indexed by subject-ID (multiple may share one), so there
	// is no collision to arbitrate; just adopt the eviction counter and re-sync.
	if isPinned(newEvictions) {
		if topic.reader != nil {
			c.platform.DestroySubjectReader(topic.reader)
			topic.reader = nil
		}
		topic.evictions = newEvictions
		topic.pinned = true
		topic.subjectID = ^newEvictions
		c.resyncTopicReader(topic)
		c.gossip.ScheduleUrgent(c, topic)
		c.DiagTopicReallocated(topic, topic.subjectID, topic.evictions)
		return
	}

	newSid := ComputeSubjectID(topic.hash, newEvictions, modulus)
	that := c.topicsBySubjectID[newSid] // current occupant of the desired subject-ID
	if that != nil {
		// A topic can never collide with itself.
		if that == topic {
			that = nil
		}
	}
	sameSubject := newSid == topic.subjectID
	victory := (that == nil) || c.leftWins(topic, now, that.Lage(now), that.hash)

	if victory {
		// Release the old reader only if we are actually leaving the subject.
		if !sameSubject && topic.reader != nil && topic.reader.SubjectID() == topic.subjectID {
			c.platform.DestroySubjectReader(topic.reader)
			topic.reader = nil
		}
		// The loser's transport state is not preserved (Go recreates writers lazily
		// on send); we just evict it from the index so we can claim the slot.
		if that != nil {
			delete(c.topicsBySubjectID, newSid)
		}
		topic.evictions = newEvictions
		topic.pinned = false
		topic.subjectID = newSid
		c.topicsBySubjectID[newSid] = topic
		c.resyncTopicReader(topic)
		c.gossip.ScheduleUrgent(c, topic)
		c.DiagTopicReallocated(topic, topic.subjectID, topic.evictions)

		// Recursively re-evict the defeated topic onto a new subject-ID.
		if that != nil {
			if that.reader != nil {
				c.platform.DestroySubjectReader(that.reader)
				that.reader = nil
			}
			c.topicAllocate(that, that.evictions+1, now)
		}
	} else {
		// Tail-recurse: re-evict self until it wins a free subject-ID.
		c.topicAllocate(topic, newEvictions+1, now)
	}
}

// resyncTopicReader ensures a subject reader exists for the topic at its current
// subject-ID. It is a no-op if the reader already matches. The caller must hold
// c.mu.
func (c *Cy) resyncTopicReader(topic *Topic) {
	if topic.reader != nil && topic.reader.SubjectID() == topic.subjectID {
		return
	}
	if topic.reader != nil {
		c.platform.DestroySubjectReader(topic.reader)
		topic.reader = nil
	}
	r, err := c.platform.NewSubjectReader(topic.subjectID, topic.extent)
	if err == nil {
		topic.reader = r
	}
}

// hashString computes a hash for a string using rapidhash.
func hashString(s string) uint64 {
	return rapidhash.Hash([]byte(s))
}

// hashBytes computes a hash for a byte slice using rapidhash.
func hashBytes(b []byte) uint64 {
	return rapidhash.Hash(b)
}

// Platform returns the underlying platform.
func (c *Cy) Platform() Platform {
	return c.platform
}

// SetUnicastExtent sets the maximum extent for incoming unicast messages.
func (c *Cy) SetUnicastExtent(extent int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unicastExtent = extent
	c.platform.SetUnicastExtent(extent)
}

// UnicastExtent returns the current unicast extent.
func (c *Cy) UnicastExtent() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.unicastExtent
}

// DefaultPlatform is a simple default platform implementation for testing.
// It doesn't actually send or receive any messages.
type DefaultPlatform struct {
	PlatformBase
	nowValue Microsecond
}

// NewDefaultPlatform creates a new default platform.
func NewDefaultPlatform() *DefaultPlatform {
	return &DefaultPlatform{
		PlatformBase: PlatformBase{},
		nowValue:     0,
	}
}

// Now returns the mock time value.
func (p *DefaultPlatform) Now() Microsecond {
	return p.nowValue
}

// SetNow sets the mock time value.
func (p *DefaultPlatform) SetNow(now Microsecond) {
	p.nowValue = now
}

// IncrementNow increments the mock time value.
func (p *DefaultPlatform) IncrementNow(delta Microsecond) {
	p.nowValue += delta
}

// Test that the package compiles
var _ = cavl.New[string, int](func(a, b string) int { return 1 })
var _ = olga.New()
var _ = rapidhash.Hash([]byte("test"))

// Ensure unsafe.Pointer is used (for the linter)
var _ unsafe.Pointer
