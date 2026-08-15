package cy

import (
	"sync"
)

// ReliableDelivery manages reliable message delivery for publishers.
// It tracks associations (remote subscribers) and handles ACK/NACK messages.
//
// Faithful port of the C reliable-publish contract:
//   - A publication is acknowledged on the first ACK from ANY remote.
//   - A publication completes with OK once it is acknowledged and every known
//     subscriber association present at publish time has explicitly ACKed it.
//   - A publisher with zero known associations completes with OK on the first
//     ACK (e.g. a late subscriber that joined after publish).
//   - NACKs are treated as gaps to be repaired by the retransmission cycle; they
//     never complete or fail the future.
//   - Retransmission uses a deadline-driven doubling backoff (ack_timeout *= 2),
//     surfacing transient ErrLag, until the publish deadline elapses, after
//     which completion materializes as OK (fully ACKed) or ErrDelivery.
type ReliableDelivery struct {
	mu sync.Mutex

	// publisher is the parent publisher.
	publisher *Publisher

	// associations maps remote node IDs to their association state.
	associations map[uint64]*Association

	// pending maps message tags to pending reliable messages.
	pending map[uint64]*reliableMessage

	// ackTimeout is the current (doubling) retransmission timeout.
	ackTimeout Microsecond
}

// reliableMessage represents a message being sent reliably.
type reliableMessage struct {
	// tag is the wire message tag (== publisher sequence tag).
	tag uint64
	// wire is the fully headed payload, reused verbatim on every retransmit.
	wire []byte
	// deadline is the absolute time by which the message must be delivered.
	deadline Microsecond
	// ackTimeout is the current retransmission timeout for this message.
	ackTimeout Microsecond
	// future is the publication future for this message.
	future *PublicationFuture
	// sentAt tracks when the message was last sent (for lag diagnostics).
	sentAt Microsecond
	// retryCount tracks how many times this message has been retransmitted.
	retryCount int
	// remaining is the set of known association remote-IDs still expected to ACK.
	remaining map[uint64]bool
	// acknowledged is set true on the first ACK from any remote.
	acknowledged bool
}

// newReliableDelivery creates a new ReliableDelivery instance.
func newReliableDelivery(pub *Publisher) *ReliableDelivery {
	return &ReliableDelivery{
		publisher:    pub,
		associations: make(map[uint64]*Association),
		pending:      make(map[uint64]*reliableMessage),
		ackTimeout:   pub.ackTimeout,
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
	rd.mu.Lock()
	defer rd.mu.Unlock()
	return rd.ackTimeout
}

// Publish sends a reliable message. It returns a future that completes with OK
// once the message is acknowledged by every known subscriber (or, with zero
// known subscribers, by the first ACK), or ErrDelivery if the deadline elapses
// before that happens.
func (rd *ReliableDelivery) Publish(deadline Microsecond, data []byte) *PublicationFuture {
	pub := rd.publisher

	// Build the wire bytes (24-byte header + payload) and the wire tag under the
	// publisher lock so sequence numbering and tag stay consistent. publishImpl
	// internally takes pub.mu for the sequence number.
	tag, wire, err := pub.buildWire(data, HeaderTypeMsgRel)
	if err != nil {
		return NewFailedPublicationFuture(mapSendError(err))
	}

	rd.mu.Lock()

	// Snapshot the known associations at publish time. These are the remotes we
	// wait on; an ACK from an unknown remote still confirms/completes the
	// publication but is not counted in remaining.
	remaining := make(map[uint64]bool, len(rd.associations))
	for remoteID := range rd.associations {
		remaining[remoteID] = true
	}

	future := NewPublicationFuture(tag, len(remaining))
	rmsg := &reliableMessage{
		tag:          tag,
		wire:         wire,
		deadline:     deadline,
		ackTimeout:   rd.ackTimeout,
		future:       future,
		sentAt:       pub.cy.Now(),
		retryCount:   0,
		remaining:    remaining,
		acknowledged: false,
	}
	rd.pending[tag] = rmsg

	if err := rd.send(rmsg); err != nil {
		delete(rd.pending, tag)
		rd.mu.Unlock()
		return NewFailedPublicationFuture(mapSendError(err))
	}

	cy := pub.cy
	rd.armTimeout(rmsg)
	rd.mu.Unlock()

	// Schedule the final materialization at the publish deadline.
	cy.olga.Schedule(int64(deadline), func() {
		rd.materialize(rmsg.tag)
	})
	return future
}

// armTimeout reschedules the doubling-backoff retransmission timeout for rmsg.
// Caller holds rd.mu.
func (rd *ReliableDelivery) armTimeout(rmsg *reliableMessage) {
	cy := rd.publisher.cy
	next := cy.Now() + rmsg.ackTimeout
	if next > rmsg.deadline {
		next = rmsg.deadline
	}
	cy.olga.Schedule(int64(next), func() {
		rd.handleTimeout(rmsg.tag)
	})
}

// send transmits rmsg over the wire. It chooses unicast to the single remaining
// remote when exactly one association is outstanding (C assoc_knockout switch),
// otherwise multicast via the cached wire bytes. Caller holds rd.mu; this
// releases rd.mu across the actual send because the platform lock and the
// publisher lock must be acquired (avoiding lock-order inversion with Publish).
func (rd *ReliableDelivery) send(rmsg *reliableMessage) error {
	pub := rd.publisher
	if len(rmsg.remaining) == 1 {
		var remoteID uint64
		for rid := range rmsg.remaining {
			remoteID = rid
		}
		rd.mu.Unlock()
		lane := Lane{ID: remoteID, Priority: pub.priority}
		err := pub.cy.platform.Unicast(lane, rmsg.deadline, rmsg.wire)
		rd.mu.Lock()
		return err
	}
	rd.mu.Unlock()
	err := pub.sendWire(rmsg.deadline, rmsg.wire)
	rd.mu.Lock()
	return err
}

// handleTimeout retransmits an unacknowledged message with a doubling backoff.
func (rd *ReliableDelivery) handleTimeout(tag uint64) {
	rd.mu.Lock()

	rmsg, ok := rd.pending[tag]
	if !ok {
		rd.mu.Unlock()
		return
	}

	// Double the per-message timeout (C ack_timeout *= 2) and surface transient
	// scheduler lag. Stop retransmitting once the publish deadline has passed;
	// the deadline materialization handles final completion.
	if rd.publisher.cy.Now() >= rmsg.deadline {
		rd.mu.Unlock()
		return
	}
	rmsg.ackTimeout *= 2
	rmsg.retryCount++
	rmsg.sentAt = rd.publisher.cy.Now()
	rmsg.future.updateError(ErrLag)

	if err := rd.send(rmsg); err != nil {
		// Send failure: treat like a missed deadline at materialization.
		rd.mu.Unlock()
		return
	}
	rd.armTimeout(rmsg)
	rd.mu.Unlock()
}

// materialize finalizes the publication at the publish deadline: OK if fully
// acknowledged by every known association, otherwise ErrDelivery.
func (rd *ReliableDelivery) materialize(tag uint64) {
	rd.mu.Lock()
	rmsg, ok := rd.pending[tag]
	if !ok {
		rd.mu.Unlock()
		return
	}
	delete(rd.pending, tag)
	rd.mu.Unlock()

	if rmsg.acknowledged && len(rmsg.remaining) == 0 {
		rmsg.future.complete(OK)
	} else {
		rmsg.future.complete(ErrDelivery)
	}
}

// HandleAck handles an acknowledgment from a remote. A known association (one of
// the remotes expected at publish time) is knocked out of the remaining set and
// counts toward completion; an ACK from any remote (including an unknown one)
// confirms the publication. Completion (OK) happens once the publication is
// acknowledged and every known association has ACKed.
func (rd *ReliableDelivery) HandleAck(remoteID, tag uint64) {
	rd.mu.Lock()
	rmsg, ok := rd.pending[tag]
	if !ok {
		rd.mu.Unlock()
		return
	}

	rmsg.acknowledged = true
	_, known := rmsg.remaining[remoteID]
	if known {
		delete(rmsg.remaining, remoteID)
	}
	rd.mu.Unlock()

	// Drop the rd.mu lock before touching the future (its completion may invoke
	// the user callback). The future methods are themselves idempotent and
	// self-locking, so no further synchronization is needed here.
	if known {
		rmsg.future.Ack() // increments ackedCount, sets acknowledged, completes if fully ACKed
	} else {
		rmsg.future.Acknowledge() // sets acknowledged, completes if already fully ACKed
	}
}

// HandleNack handles a negative acknowledgment from a remote. Faithful to C:
// a NACK marks a gap to be repaired by retransmission; the association stays in
// remaining, and the future is neither completed nor failed.
func (rd *ReliableDelivery) HandleNack(remoteID, tag uint64) {
	rd.mu.Lock()
	defer rd.mu.Unlock()
	_ = remoteID
	_ = tag
}

// AddAssociation adds a new association for a remote subscriber.
func (rd *ReliableDelivery) AddAssociation(remoteID uint64) {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	if _, ok := rd.associations[remoteID]; !ok {
		rd.associations[remoteID] = &Association{
			remoteID:     remoteID,
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

// GetAssociation returns the association by remote ID.
func (rd *ReliableDelivery) GetAssociation(remoteID uint64) *Association {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	return rd.associations[remoteID]
}

// AssociationCount returns the number of associations.
func (rd *ReliableDelivery) AssociationCount() int {
	rd.mu.Lock()
	defer rd.mu.Unlock()

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

// Cleanup removes expired associations. Pending-message expiry is handled by the
// deadline materialization scheduled in Publish; this keeps the association
// soft-state bounded.
func (rd *ReliableDelivery) Cleanup(now Microsecond) {
	rd.mu.Lock()
	defer rd.mu.Unlock()

	for remoteID, assoc := range rd.associations {
		if now-assoc.lastActivity > SessionLifetime {
			delete(rd.associations, remoteID)
		}
	}
}
