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
		cy:             cy,
		topic:          topic,
		priority:       PriorityNominal,
		ackTimeout:     ACKBaselineDefaultTimeout,
		nextRequestTag: 0,
		pendingRequests: make(map[uint64]*RequestFuture),
		responseExtent: 0,
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
	return pub
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

// Publish sends a best-effort (non-reliable) message.
// The deadline is the absolute time by which the message must be sent.
func (p *Publisher) Publish(deadline Microsecond, data []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.destroyed {
		return ErrArgument
	}

	// Create a message
	msg := NewMessage(data)
	defer msg.RefcountDec()

	// Send via the platform
	writer, err := p.cy.platform.NewSubjectWriter(p.topic.subjectID)
	if err != nil {
		return err
	}
	defer p.cy.platform.DestroySubjectWriter(writer)

	return p.cy.platform.SubjectWriterSend(writer, deadline, p.priority, data)
}

// PublishReliable sends a reliable message.
// The message will be retransmitted until acknowledged by all subscribers or until the deadline.
// Returns a future that will be completed when the message is delivered or the deadline is reached.
func (p *Publisher) PublishReliable(deadline Microsecond, data []byte) *PublicationFuture {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.destroyed {
		return nil
	}

	// Delegate to reliable delivery
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

// sendRequest sends a request message as a protocol message.
func (p *Publisher) sendRequest(tag uint64, deadline Microsecond, data []byte) {
	// Create a request message
	request := &RequestMessage{
		Header: ProtocolHeader{
			MessageType: uint8(ProtocolMessageRequest),
		},
		Tag:          tag,
		SourceNodeID: 0, // Would be the local node ID
		ServiceID:    uint32(p.topic.subjectID),
		RequestID:    uint32(tag), // Use tag as request ID for now
	}
	
	// Marshal the request header
	requestHeader := request.MarshalBinary()
	
	// Combine header + payload
	payload := append(requestHeader, data...)
	
	// Send as a regular message (the protocol header will be parsed by receivers)
	p.Publish(deadline, payload)
}

// handleRequestTimeout handles a timeout for a request.
func (p *Publisher) handleRequestTimeout(tag uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Find the future
	future, ok := p.pendingRequests[tag]
	if !ok {
		return
	}

	// Check if we've received any responses
	if future.ResponseCount() == 0 {
		// No responses received, mark as liveness error
		future.complete(ErrLiveness)
	} else {
		// We have responses, but no more are expected
		// The future remains done with OK
		future.complete(OK)
	}
	
	// Clean up
	delete(p.pendingRequests, tag)
}

// handleResponse handles a response to a request.
func (p *Publisher) handleResponse(tag uint64, response Response) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Find the future for this tag
	// In a real implementation, we'd have a map of request tags to futures
	// Check in pending requests
	if reqFuture, ok := p.pendingRequests[tag]; ok {
		reqFuture.AddResponse(response)
		return
	}
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
