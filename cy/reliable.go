package cy

import (
	"sync"
	"time"
)

// ReliableDelivery manages reliable message delivery for publishers.
// It tracks associations (remote subscribers) and handles ACK/NACK messages.
type ReliableDelivery struct {
	mu sync.RWMutex

	// publisher is the parent publisher.
	publisher *Publisher

	// associations maps remote node IDs to their association state.
	associations map[uint64]*Association

	// pending maps message tags to pending reliable messages.
	pending map[uint64]*reliableMessage

	// nextTag is the next message tag to use.
	nextTag uint64

	// ackTimeout is the timeout for waiting for acknowledgments.
	ackTimeout Microsecond

	// retryCount tracks the number of retries for each message.
	maxRetries int
}

// reliableMessage represents a message being sent reliably.
type reliableMessage struct {
	// tag is the unique message tag.
	tag uint64
	// data is the message payload.
	data []byte
	// deadline is when the message must be delivered by.
	deadline Microsecond
	// future is the future for this message.
	future *PublicationFuture
	// sentAt tracks when the message was first sent.
	sentAt Microsecond
	// retryCount tracks how many times this message has been retried.
	retryCount int
	// ackedBy tracks which remotes have acknowledged.
	ackedBy map[uint64]bool
}

// newReliableDelivery creates a new ReliableDelivery instance.
func newReliableDelivery(pub *Publisher) *ReliableDelivery {
	return &ReliableDelivery{
		publisher:    pub,
		associations: make(map[uint64]*Association),
		pending:     make(map[uint64]*reliableMessage),
		nextTag:     0,
		ackTimeout:  pub.ackTimeout,
		maxRetries:  3,
	}
}

// SetAckTimeout sets the acknowledgment timeout.
func (rd *ReliableDelivery) SetAckTimeout(timeout Microsecond) {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	rd.ackTimeout = timeout
}

// AckTimeout returns the acknowledgment timeout.
func (rd *ReliableDelivery) AckTimeout() Microsecond {
	rd.mu.RLock()
	defer rd.mu.RUnlock()
	return rd.ackTimeout
}

// Publish sends a reliable message.
func (rd *ReliableDelivery) Publish(deadline Microsecond, data []byte) *PublicationFuture {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	// Generate a new tag
	tag := rd.nextTag
	rd.nextTag++

	// Create the future
	// Total count is the number of associations + 1 (for multicast)
	totalCount := len(rd.associations) + 1
	if totalCount == 0 {
		totalCount = 1
	}

	future := NewPublicationFuture(tag, totalCount)

	// Create the reliable message
	rmsg := &reliableMessage{
		tag:      tag,
		data:     data,
		deadline: deadline,
		future:   future,
		sentAt:   rd.publisher.cy.Now(),
		ackedBy:  make(map[uint64]bool),
	}

	rd.pending[tag] = rmsg

	// Send the initial message
	rd.sendMessage(rmsg)

	// Schedule timeout
	cy := rd.publisher.cy
	cy.olga.Schedule(int64(deadline), func() {
		rd.handleTimeout(tag)
	})

	return future
}

// sendMessage sends a reliable message with the tag encoded in the header.
func (rd *ReliableDelivery) sendMessage(rmsg *reliableMessage) {
	pub := rd.publisher
	
	// Create a header with the message tag
	// SourceNodeID would typically come from the platform or Cy instance
	// For now, we use 0 as a placeholder
	header := NewHeader(
		rmsg.tag,
		0, // sequence number (not used for reliable)
		pub.cy.Now(),
		pub.priority,
		0, // SourceNodeID - placeholder, would be set from platform
	)
	
	// Prepend the header to the data
	payloadWithHeader := PrependHeader(header, rmsg.data)
	
	// If we have associations, we might send unicast
	// Otherwise, send multicast
	if len(rd.associations) > 0 {
		// For now, just send multicast
		// In a real implementation, we'd send unicast to each association
		pub.Publish(rmsg.deadline, payloadWithHeader)
	} else {
		pub.Publish(rmsg.deadline, payloadWithHeader)
	}
}

// handleTimeout handles a timeout for a reliable message.
func (rd *ReliableDelivery) handleTimeout(tag uint64) {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	rmsg, ok := rd.pending[tag]
	if !ok {
		return
	}

	// Check if we've exceeded max retries
	if rmsg.retryCount >= rd.maxRetries {
		rmsg.future.complete(ErrDelivery)
		delete(rd.pending, tag)
		return
	}

	// Resend the message
	rmsg.retryCount++
	rmsg.sentAt = rd.publisher.cy.Now()
	
	// Update the deadline for the next attempt
	// Use exponential backoff
	backoff := time.Duration(1<<rmsg.retryCount) * time.Millisecond
	rmsg.deadline = rd.publisher.cy.Now() + Microsecond(backoff)
	
	rd.sendMessage(rmsg)

	// Reschedule timeout
	cy := rd.publisher.cy
	cy.olga.Schedule(int64(rmsg.deadline), func() {
		rd.handleTimeout(tag)
	})
}

// HandleAck handles an acknowledgment from a remote.
func (rd *ReliableDelivery) HandleAck(remoteID, tag uint64) {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	rmsg, ok := rd.pending[tag]
	if !ok {
		return
	}

	// Mark this remote as having acknowledged
	if !rmsg.ackedBy[remoteID] {
		rmsg.ackedBy[remoteID] = true
		rmsg.future.Ack()
	}

	// Check if all remotes have acknowledged
	if len(rmsg.ackedBy) >= rmsg.future.TotalCount() {
		rmsg.future.complete(OK)
		delete(rd.pending, tag)
	}
}

// HandleNack handles a negative acknowledgment from a remote.
func (rd *ReliableDelivery) HandleNack(remoteID, tag uint64) {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	rmsg, ok := rd.pending[tag]
	if !ok {
		return
	}

	rmsg.future.Nack()
}

// AddAssociation adds a new association for a remote subscriber.
func (rd *ReliableDelivery) AddAssociation(remoteID uint64) {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	if _, ok := rd.associations[remoteID]; !ok {
		rd.associations[remoteID] = &Association{
			remoteID:      remoteID,
			lastAckTag:   0,
			lastNackTag:  0,
			lastActivity: rd.publisher.cy.Now(),
		}
	}
}

// RemoveAssociation removes an association.
func (rd *ReliableDelivery) RemoveAssociation(remoteID uint64) {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	delete(rd.associations, remoteID)
}

// GetAssociation returns an association by remote ID.
func (rd *ReliableDelivery) GetAssociation(remoteID uint64) *Association {
	rd.mu.RLock()
	defer rd.mu.RUnlock()

	return rd.associations[remoteID]
}

// AssociationCount returns the number of associations.
func (rd *ReliableDelivery) AssociationCount() int {
	rd.mu.RLock()
	defer rd.mu.RUnlock()

	return len(rd.associations)
}

// Cancel cancels all pending reliable messages.
func (rd *ReliableDelivery) Cancel() {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	for tag, rmsg := range rd.pending {
		rmsg.future.complete(ErrNACK)
		delete(rd.pending, tag)
	}
	
	rd.associations = make(map[uint64]*Association)
}

// Cleanup removes expired associations and pending messages.
func (rd *ReliableDelivery) Cleanup(now Microsecond) {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	// Remove stale associations
	for remoteID, assoc := range rd.associations {
		// Session lifetime check
		if now-assoc.lastActivity > SessionLifetime {
			delete(rd.associations, remoteID)
		}
	}

	// Remove expired pending messages
	for tag, rmsg := range rd.pending {
		if now-rmsg.sentAt > rd.ackTimeout*Microsecond(rmsg.retryCount+1) {
			rmsg.future.complete(ErrDelivery)
			delete(rd.pending, tag)
		}
	}
}
