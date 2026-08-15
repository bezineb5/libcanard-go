package cy

import (
	"sync"
)

// Publisher represents a publisher on a specific topic.
// Publishers are used to send messages to subscribers of the topic.
type Publisher struct {
	// cy is the owning Cy instance.
	cy *Cy
	// topic is the topic this publisher is publishing to.
	topic *Topic
	// priority is the priority of messages published by this publisher.
	priority Priority
	// ackTimeout is the timeout for reliable message acknowledgments.
	ackTimeout Microsecond

	// For reliable publishers
	// reliable manages reliable delivery.
	reliable *ReliableDelivery

	// For request/response
	// nextRequestTag is the next tag to use for requests.
	nextRequestTag uint64
	// pendingRequests is a map of message tags to pending request futures.
	pendingRequests map[uint64]*RequestFuture

	// For client publishers (expecting responses)
	// responseExtent is the maximum size of response messages.
	responseExtent int

	// Tag generation state (C: topic->pub_tag_baseline + topic->pub_seqno).
	msgTagBaseline uint64
	msgSeqno       uint64

	// mu protects the publisher state.
	mu sync.RWMutex

	// destroyed indicates whether the publisher has been destroyed.
	destroyed bool
}

// Association tracks the state of a remote subscriber for reliable delivery.
type Association struct {
	// remoteID is the unique identifier of the remote node.
	remoteID uint64
	// lastAckTag is the last message tag that was acknowledged.
	lastAckTag uint64
	// lastNackTag is the last message tag that was negatively acknowledged.
	lastNackTag uint64
	// lastActivity is the timestamp of the last activity from this remote.
	lastActivity Microsecond
}

// NewPublisher creates a new publisher for the specified topic.
func NewPublisher(cy *Cy, topic *Topic) *Publisher {
	pub := &Publisher{
		cy:              cy,
		topic:           topic,
		priority:        PriorityNominal,
		ackTimeout:      ACKBaselineDefaultTimeout,
		nextRequestTag:  0,
		pendingRequests: make(map[uint64]*RequestFuture),
		responseExtent:  0,
		// C seeds the baseline from the platform/PRNG; we use a non-zero
		// deterministic baseline derived from the topic hash so tags are
		// unique across reboots as required by the protocol.
		msgTagBaseline: topic.Hash() | 1,
	}

	// Initialize reliable delivery
	pub.reliable = newReliableDelivery(pub)

	return pub
}

// NewClientPublisher creates a new publisher that expects responses.
// The responseExtent is the maximum size of response messages.
func NewClientPublisher(cy *Cy, topic *Topic, responseExtent int) *Publisher {
	pub := NewPublisher(cy, topic)
	pub.responseExtent = responseExtent
	// C cy_advertise_client grows the node's incoming unicast extent so large
	// reliable responses fit the reassembly buffer. Use the max across all
	// client publishers so the largest requirement wins.
	if needed := responseExtent + HeaderSize; needed > cy.UnicastExtent() {
		cy.SetUnicastExtent(needed)
	}
	return pub
}

// mapSendError converts a transport-layer error (builtin error, frequently a
// cy.Error boxed as error) into the cy.Error domain for future completion.
func mapSendError(err error) Error {
	if err == nil {
		return OK
	}
	if e, ok := err.(Error); ok {
		return e
	}
	return ErrMedia
}

// Topic returns the topic this publisher is publishing to.
func (p *Publisher) Topic() *Topic {
	return p.topic
}

// Priority returns the priority of this publisher.
func (p *Publisher) Priority() Priority {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.priority
}

// SetPriority sets the priority of this publisher.
func (p *Publisher) SetPriority(priority Priority) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.priority = priority
}

// AckTimeout returns the acknowledgment timeout for reliable messages.
func (p *Publisher) AckTimeout() Microsecond {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.ackTimeout
}

// SetAckTimeout sets the acknowledgment timeout for reliable messages.
func (p *Publisher) SetAckTimeout(timeout Microsecond) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ackTimeout = timeout
}

// Destroy destroys the publisher.
// All futures created by this publisher must be destroyed beforehand.
func (p *Publisher) Destroy() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.destroyed {
		return
	}

	p.destroyed = true

	// Cancel all pending reliable messages
	p.reliable.Cancel()
}

// buildWire prepends the 24-byte Cy session header and returns the headed
// payload together with the wire tag (publisher sequence tag). It takes the
// publisher lock so sequence numbering and tag stay consistent between the
// cached wire bytes and the tag used for ACK correlation. headerType selects
// the wire message type (best-effort vs reliable). Mirrors C do_publish_impl.
func (p *Publisher) buildWire(data []byte, headerType HeaderType) (uint64, []byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.destroyed {
		return 0, nil, ErrArgument
	}
	p.msgSeqno++
	tag := p.msgTagBaseline + p.msgSeqno
	header := NewHeader(headerType, int8(p.topic.Lage(p.cy.Now())), uint32(p.topic.Evictions()), p.topic.Hash(), tag)
	wire := PrependHeader(header, data)
	return tag, wire, nil
}

// sendWire transmits already-headed payload onto the multicast subject for this
// publisher's topic. The header is reused verbatim on every retransmit so the
// wire tag is stable for ACK correlation.
func (p *Publisher) sendWire(deadline Microsecond, wire []byte) error {
	writer, err := p.cy.platform.NewSubjectWriter(p.topic.subjectID)
	if err != nil {
		return err
	}
	defer p.cy.platform.DestroySubjectWriter(writer)
	return p.cy.platform.SubjectWriterSend(writer, deadline, p.priority, wire)
}

// Publish sends a best-effort (non-reliable) message.
// The deadline is the absolute time by which the message must be sent.
func (p *Publisher) Publish(deadline Microsecond, data []byte) error {
	_, wire, err := p.buildWire(data, HeaderTypeMsgBE)
	if err != nil {
		return err
	}
	return p.sendWire(deadline, wire)
}

// PublishReliable sends a reliable message.
// The message will be retransmitted until acknowledged by all subscribers or until the deadline is reached.
// Returns a future that will be completed when the message is delivered or the deadline is reached.
func (p *Publisher) PublishReliable(deadline Microsecond, data []byte) *PublicationFuture {
	p.mu.RLock()
	if p.destroyed {
		p.mu.RUnlock()
		return nil
	}
	p.mu.RUnlock()

	return p.reliable.Publish(deadline, data)
}

// handleAck handles an acknowledgment for a reliable message.
// This is called by the Cy instance when an ACK message is received.
func (p *Publisher) handleAck(remoteID, tag uint64) {
	p.reliable.HandleAck(remoteID, tag)
}

// handleNack handles a negative acknowledgment for a reliable message.
// This is called by the Cy instance when a NACK message is received.
func (p *Publisher) handleNack(remoteID, tag uint64) {
	p.reliable.HandleNack(remoteID, tag)
}

// Request sends a request message and waits for responses.
// The deliveryDeadline is the deadline for delivering the request to subscribers.
// The responseTimeout is the timeout for waiting for the first response.
// Returns a future that will receive responses.
func (p *Publisher) Request(deliveryDeadline, responseTimeout Microsecond, data []byte) *RequestFuture {
	p.mu.RLock()
	if p.destroyed {
		p.mu.RUnlock()
		return nil
	}
	p.mu.RUnlock()
	// Delegate to RPC
	return p.cy.RPC().Request(p, deliveryDeadline, responseTimeout, data)
}

// AddAssociation adds a new association for a remote subscriber.
func (p *Publisher) AddAssociation(remoteID uint64) {
	p.reliable.AddAssociation(remoteID)
}

// RemoveAssociation removes an association for a remote subscriber.
func (p *Publisher) RemoveAssociation(remoteID uint64) {
	p.reliable.RemoveAssociation(remoteID)
}

// GetAssociation returns the association for a remote subscriber.
func (p *Publisher) GetAssociation(remoteID uint64) *Association {
	return p.reliable.GetAssociation(remoteID)
}

// AssociationCount returns the number of associations.
func (p *Publisher) AssociationCount() int {
	return p.reliable.AssociationCount()
}

// IsClient returns true if this publisher expects responses.
func (p *Publisher) IsClient() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.responseExtent > 0
}

// ResponseExtent returns the maximum size of response messages.
func (p *Publisher) ResponseExtent() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.responseExtent
}
