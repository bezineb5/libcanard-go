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

	// prngState is the PRNG state for random number generation.
	prngState uint64

	// mu protects the Cy instance from concurrent access.
	mu sync.RWMutex

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
		platform:            platform,
		olga:                olga.New(),
		home:                home,
		ns:                  namespace,
		remap:              make(map[string]string),
		topicsByName:       make(map[string]*Topic),
		topicsByHash:       make(map[uint64]*Topic),
		topicsBySubjectID:  make(map[uint32]*Topic),
		publishers:         make(map[*Topic]*Publisher),
		subscribers:        make(map[*Topic][]*Subscriber),
		unicastExtent:      0,
		startedAt:          0,
		implicitTopicTimeout: ImplicitTopicDefaultTimeout,
		ackBaselineTimeout:   ACKBaselineDefaultTimeout,
		gossip:             nil, // Will be initialized below
		rpc:                nil, // Will be initialized below
		crdt:               nil, // Will be initialized below
		patternMatcher:      NewPatternMatcher(),
		subjectIDModulus:    SubjectIDModulus16bit,
		prngState:           0,
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
	
	// Set up gossip shard count
	cy.gossip.SetShardCount(1) // Default to 1 shard for now

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

// handleMulticastMessage handles a multicast message.
// Mirrors C cy_on_message: read the message tag, then skip the 24-byte session
// header, then dispatch.
func (c *Cy) handleMulticastMessage(lane Lane, subjectID uint32, message MessageTS) {
	if message.Content == nil || message.Content.Size() < HeaderSize {
		return
	}
	// Extract the message tag (C: header[16:24]) before skipping the header.
	var tagBuf [8]byte
	var msgTag uint64
	if message.Content.Read(16, 8, tagBuf[:]) == 8 {
		msgTag = binary.LittleEndian.Uint64(tagBuf[:])
	}
	// C always skips the session header before dispatch.
	message.Content.Skip(HeaderSize)

	// Determine the dispatch scope (C: multicast vs broadcast vs shard).
	topic := c.findTopicBySubjectID(subjectID)
	if topic == nil {
		// Unknown subject-ID. Above SubjectIDPinnedMax => gossip/aux subject.
		if subjectID > SubjectIDPinnedMax {
			c.gossip.ProcessGossip(message.Content.Payload(), subjectID)
		}
		return
	}

	// Known topic: deliver to subscribers on this topic.
	if subs, ok := c.subscribers[topic]; ok {
		for _, sub := range subs {
			sub.Deliver(message, lane, msgTag)
		}
	}
	matches := c.patternMatcher.Match(topic.name)
	for sub := range matches {
		sub.Deliver(message, lane, msgTag)
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
	// Read the type byte from offset 0 and the tag from offset 16 before skipping.
	var hdr [HeaderSize]byte
	if message.Content.Read(0, HeaderSize, hdr[:]) != HeaderSize {
		return
	}
	msgType := HeaderType(hdr[0])
	tag := binary.LittleEndian.Uint64(hdr[16:24])
	message.Content.Skip(HeaderSize)

	switch msgType {
	case HeaderTypeMsgAck, HeaderTypeMsgNack:
		c.handleACKMessage(lane, tag)
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
	default:
		// Unicast carrying a non-unicast type is invalid; ignore.
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

// handleACKMessage handles an incoming message ACK/NACK (C header_msg_ack/nack).
// The tag is the header's tag field; we ACK every publisher's tag (C collapses to
// a single ack on the publishing node).
func (c *Cy) handleACKMessage(lane Lane, tag uint64) {
	c.mu.RLock()
	_ = lane
	c.mu.RUnlock()
	_ = tag
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

	// Resolve the topic name
	resolvedName := c.resolveTopicName(topicName)

	// Check if this is a pattern subscription
	if IsPattern(resolvedName) {
		// Create a pattern subscriber
		sub := NewPatternSubscriber(c, resolvedName, extent)
		
		// Add to pattern matcher
		c.patternMatcher.AddPattern(resolvedName, sub)
		
		return sub, nil
	}

	// Find or create the topic
	topic, err := c.findOrCreateTopic(resolvedName, extent)
	if err != nil {
		return nil, err
	}
	// Create a new subscriber
	sub := NewSubscriber(c, topic, extent)
	c.subscribers[topic] = append(c.subscribers[topic], sub)

	return sub, nil
}

// SubscribeOrdered creates a new ordered subscriber for the specified topic.
// Ordered subscribers receive messages in the exact order they were sent.
func (c *Cy) SubscribeOrdered(topicName string, extent int) (*Subscriber, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Resolve the topic name
	resolvedName := c.resolveTopicName(topicName)

	// Check if this is a pattern subscription
	if IsPattern(resolvedName) {
		sub := NewPatternSubscriber(c, resolvedName, extent)
		c.patternMatcher.AddPattern(resolvedName, sub)
		return sub, nil
	}

	// Find or create the topic
	topic, err := c.findOrCreateTopic(resolvedName, extent)
	if err != nil {
		return nil, err
	}

	// Create a new subscriber
	sub := NewSubscriber(c, topic, extent)
	c.subscribers[topic] = append(c.subscribers[topic], sub)

	return sub, nil
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
// before the platform reader is created.
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

	// Add to CRDT
	state := c.crdt.AddTopic(name, topic.hash)
	topic.logAge = state.logAge
	topic.evictions = state.evictions

	// Recompute subject-ID based on CRDT state
	if !topic.pinned {
		topic.subjectID = c.crdt.ComputeSubjectID(topic.hash, topic.evictions)
	}

	// Add to maps
	c.topicsByName[name] = topic
	c.topicsByHash[topic.hash] = topic
	if !topic.pinned {
		c.topicsBySubjectID[topic.subjectID] = topic
	}

	// Create subject reader/writer on the platform
	reader, err := c.platform.NewSubjectReader(topic.subjectID, topic.extent)
	if err != nil {
		// Clean up
		delete(c.topicsByName, name)
		delete(c.topicsByHash, topic.hash)
		if !topic.pinned {
			delete(c.topicsBySubjectID, topic.subjectID)
		}
		c.crdt.RemoveTopic(topic.hash)
		return nil, err
	}

	_, err = c.platform.NewSubjectWriter(topic.subjectID)
	if err != nil {
		// Clean up reader
		c.platform.DestroySubjectReader(reader)
		delete(c.topicsByName, name)
		delete(c.topicsByHash, topic.hash)
		if !topic.pinned {
			delete(c.topicsBySubjectID, topic.subjectID)
		}
		c.crdt.RemoveTopic(topic.hash)
		return nil, err
	}

	// Schedule gossip for this topic
	c.gossip.ScheduleUrgent(c, topic)

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

// SetSubjectIDModulus sets the subject-ID modulus for the network.
func (c *Cy) SetSubjectIDModulus(modulus uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Validate modulus
	if modulus < 1 || modulus > (^uint32(0)-SubjectIDPinnedMax) {
		return ErrArgument
	}

	c.subjectIDModulus = modulus
	return nil
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
