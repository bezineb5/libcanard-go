package cy

import (
	"sync"

	"github.com/opencyphal/cy-go/olga"
)

// Reliable-delivery constants, faithful to cy.c.
const (
	// sessionCounterMaxBackwardLag is the maximum number of tags a sequence may
	// move backwards (wrapping) before it is treated as a restart and forces a
	// resequence. Mirrors C SESSION_COUNTER_MAX_BACKWARD_LAG.
	sessionCounterMaxBackwardLag = uint64(100000)
	// reorderingCapacity bounds the per-(remote,topic) reordering window.
	reorderingCapacity = 16
	// dedupHistory is the size (in bits) of the per-remote reliable-dedup sliding
	// bitmap. Must be a multiple of 64. Mirrors C DEDUP_HISTORY.
	dedupHistory = 512
	// int64Max is the largest representable int64, used to detect wrapped-around
	// (negative) linearized tags. Mirrors C INT64_MAX.
	int64Max = uint64(9223372036854775807)
)

// ===========================================================================
// Reliable message (publisher-side) delivery with ACK/NACK and retransmission.
// ===========================================================================

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

// ===========================================================================
// Reliable-message deduplication to mitigate ACK loss (subscriber side).
//
// An instance is kept per remote node that publishes reliable messages on a given
// topic. It is a sliding window of the most recently received message tags
// (relative to the last accepted tag), used to suppress duplicate deliveries that
// occur when an acknowledgement is lost. Faithful port of C dedup_t.
// ===========================================================================

// dedupState is the per-remote sliding bitmap of accepted reliable tags.
type dedupState struct {
	remoteID     uint64
	tag          uint64 // Most recent (frontier) accepted tag.
	lastActiveAt Microsecond
	bitmap       [dedupHistory / 64]uint64
}

// check reports whether tag was already accepted within the current window.
func (d *dedupState) check(tag uint64) bool {
	rev := d.tag - tag
	if rev < dedupHistory {
		return (d.bitmap[rev/64] & (1 << (rev % 64))) != 0
	}
	return false
}

// commit records tag as accepted, shifting the window forward if needed.
func (d *dedupState) commit(tag uint64) {
	rev := d.tag - tag
	if rev < dedupHistory {
		d.bitmap[rev/64] |= 1 << (rev % 64)
		return
	}
	fwd := tag - d.tag
	if fwd >= dedupHistory {
		// The new tag is far ahead: the entire window is obsolete.
		d.bitmap = [dedupHistory / 64]uint64{}
	} else {
		bitmapShift(d.bitmap[:], dedupHistory, int64(fwd))
	}
	d.tag = tag
	d.bitmap[0] |= 1
}

// bitmapShift shifts a bitmap of bitCount bits by shiftAmount positions. A
// positive shift moves bits toward higher indices (dropping the lowest), a
// negative shift moves them toward lower indices. Faithful port of C bitmap_shift.
func bitmapShift(bitmap []uint64, bitCount int, shiftAmount int64) {
	if len(bitmap) == 0 || bitCount <= 0 || shiftAmount == 0 {
		return
	}
	words := (bitCount + 63) / 64
	tail := bitCount % 64
	if tail > 0 {
		bitmap[words-1] &= (1 << uint(tail)) - 1
	}
	var shiftMag uint64
	if shiftAmount >= 0 {
		shiftMag = uint64(shiftAmount)
	} else {
		shiftMag = uint64(-(shiftAmount + 1)) + 1
	}
	if shiftMag >= uint64(bitCount) {
		for i := 0; i < words; i++ {
			bitmap[i] = 0
		}
		return
	}
	wholeWords := int(shiftMag / 64)
	partBits := int(shiftMag % 64)
	if shiftAmount > 0 { // Left shift.
		if wholeWords > 0 {
			for i := words - 1; i >= 0; i-- {
				if i >= wholeWords {
					bitmap[i] = bitmap[i-wholeWords]
				} else {
					bitmap[i] = 0
				}
			}
		}
		if partBits > 0 {
			for i := words - 1; i >= 0; i-- {
				carry := uint64(0)
				if i > 0 {
					carry = bitmap[i-1] >> uint(64-partBits)
				}
				bitmap[i] = (bitmap[i] << uint(partBits)) | carry
			}
		}
	} else { // Right shift.
		if wholeWords > 0 {
			for i := 0; i < words; i++ {
				if i+wholeWords < words {
					bitmap[i] = bitmap[i+wholeWords]
				} else {
					bitmap[i] = 0
				}
			}
		}
		if partBits > 0 {
			for i := 0; i < words; i++ {
				carry := uint64(0)
				if i+1 < words {
					carry = bitmap[i+1] << uint(64-partBits)
				}
				bitmap[i] = (bitmap[i] >> uint(partBits)) | carry
			}
		}
	}
	if tail > 0 {
		bitmap[words-1] &= (1 << uint(tail)) - 1
	}
}

// ===========================================================================
// Message reordering for ordered subscribers.
//
// One reorderingState is used per (remote node & topic) per subscription, to
// enforce strictly-increasing order of message tags (modulo 2**64) from each
// remote. Missing messages are waited for for up to the reordering window, after
// which the gap is closed by advancing the expected tag; late arrivals are then
// dropped. Faithful port of C reordering_t / reordering_slot_t.
// ===========================================================================

// reorderingKey identifies a reordering instance by (remote node, topic hash).
type reorderingKey struct {
	remoteID  uint64
	topicHash uint64
}

// reorderingSlot is an interned (not-yet-ejected) message pending in-order delivery.
type reorderingSlot struct {
	linTag   uint64
	priority Priority
	message  MessageTS
}

// reorderingState is a per-(remote,topic) in-order delivery buffer.
type reorderingState struct {
	subscriber *Subscriber
	remoteID   uint64
	topicHash  uint64
	unicastCtx [24]byte

	tagBaseline       uint64 // First seen tag; subtract from incoming tags for linearization.
	lastEjectedLinTag uint64 // Linearized tag last delivered to the application.
	slots             map[uint64]*reorderingSlot
	windowTask        *olga.Task
	lastActiveAt      Microsecond
}

// resequence resets the window baseline around tag so that tag maps to the
// middle of the window. Used when the sequence appears to have restarted.
func (r *reorderingState) resequence(tag uint64) {
	r.tagBaseline = tag - (reorderingCapacity / 2)
	r.lastEjectedLinTag = 0
}

// minSlot returns the interned slot with the smallest linearized tag (or nil).
func (r *reorderingState) minSlot() *reorderingSlot {
	var best *reorderingSlot
	for _, s := range r.slots {
		if best == nil || s.linTag < best.linTag {
			best = s
		}
	}
	return best
}

// eject delivers a single interned slot in order and advances the window.
func (r *reorderingState) eject(slot *reorderingSlot) {
	delete(r.slots, slot.linTag)
	r.lastEjectedLinTag = slot.linTag
	arrival := &Arrival{
		Message: slot.message,
		Breadcrumb: Breadcrumb{
			Cy:         r.subscriber.cy,
			Priority:   slot.priority,
			RemoteID:   r.remoteID,
			TopicHash:  r.topicHash,
			MessageTag: slot.linTag + r.tagBaseline,
			UnicastCtx: r.unicastCtx,
		},
	}
	r.subscriber.notify(arrival)
}

// scan ejects in-order messages. When forceFirst is true, it also force-ejects
// the head slot even if a gap remains (used on window expiration). It keeps the
// window timer armed against the current head-of-line slot.
func (r *reorderingState) scan(forceFirst bool) {
	for {
		slot := r.minSlot()
		if slot == nil {
			if r.windowTask != nil {
				r.subscriber.cy.olga.Cancel(r.windowTask)
				r.windowTask = nil
			}
			break
		}
		if forceFirst || slot.linTag == r.lastEjectedLinTag+1 {
			forceFirst = false
			r.eject(slot)
		} else {
			deadline := int64(slot.message.Timestamp + r.subscriber.reorderingWindow)
			if r.windowTask != nil {
				r.subscriber.cy.olga.Cancel(r.windowTask)
			}
			r.windowTask = r.subscriber.cy.olga.Schedule(deadline, r.onWindowExpiration)
			break
		}
	}
}

// onWindowExpiration is invoked when the reordering window for the head-of-line
// gap closes: force-eject whatever is pending so late gaps do not stall delivery.
func (r *reorderingState) onWindowExpiration() {
	r.scan(true)
}

// push decides whether message (for the given tag) can be ejected now or must be
// interned. It returns true if the message is accepted (ejected or interned) and
// should be acknowledged; false if it is a late drop and must not be acknowledged.
func (r *reorderingState) push(tag uint64, priority Priority, message MessageTS) bool {
	linTag := tag - r.tagBaseline

	// Late arrival or duplicate: the gap is already closed, cannot accept.
	if linTag <= r.lastEjectedLinTag {
		return false
	}

	// Negative (wrapped) linearized tag => the sequence moved backwards.
	if linTag > int64Max {
		backwardDistance := r.lastEjectedLinTag - linTag // wrapping subtraction.
		if backwardDistance <= sessionCounterMaxBackwardLag {
			return false
		}
		r.ejectAll(false)
		r.resequence(tag)
		linTag = tag - r.tagBaseline
	}

	// If too far ahead, force-eject old messages to slide the window right.
	for len(r.slots) > 0 && linTag > (r.lastEjectedLinTag+reorderingCapacity) {
		r.scan(true)
	}
	if r.subscriber.disposed {
		return false
	}

	// The next expected message: eject immediately (fast path).
	if linTag == r.lastEjectedLinTag+1 {
		r.lastEjectedLinTag = linTag
		delivered := r.subscriber.notify(&Arrival{
			Message: message,
			Breadcrumb: Breadcrumb{
				Cy:         r.subscriber.cy,
				Priority:   priority,
				RemoteID:   r.remoteID,
				TopicHash:  r.topicHash,
				MessageTag: tag,
				UnicastCtx: r.unicastCtx,
			},
		})
		r.scan(false)
		return delivered
	}

	// Still too far ahead: treat as a restart and resequence.
	if linTag > (r.lastEjectedLinTag + reorderingCapacity) {
		r.resequence(tag)
		linTag = tag - r.tagBaseline
	}

	// Intern the message within the reordering window.
	if _, exists := r.slots[linTag]; exists {
		// Already interned with this tag: a duplicate. Accept for reliability
		// semantics (idempotent drop for the application).
		return true
	}
	r.slots[linTag] = &reorderingSlot{
		linTag:   linTag,
		priority: priority,
		message:  message,
	}
	// Re-arm the window timer against the new head-of-line slot.
	if first := r.minSlot(); first != nil {
		deadline := int64(first.message.Timestamp + r.subscriber.reorderingWindow)
		if r.windowTask != nil {
			r.subscriber.cy.olga.Cancel(r.windowTask)
		}
		r.windowTask = r.subscriber.cy.olga.Schedule(deadline, r.onWindowExpiration)
	}
	return true
}

// ejectAll delivers (or silences) every interned message and leaves the state idle.
func (r *reorderingState) ejectAll(silenced bool) {
	for len(r.slots) > 0 {
		slot := r.minSlot()
		if slot == nil {
			break
		}
		if silenced {
			delete(r.slots, slot.linTag)
		} else {
			r.eject(slot)
		}
	}
	if r.windowTask != nil {
		r.subscriber.cy.olga.Cancel(r.windowTask)
		r.windowTask = nil
	}
}

// destroy tears down the reordering state, optionally silenced.
func (r *reorderingState) destroy(silenced bool) {
	r.ejectAll(silenced)
	r.subscriber.removeReordering(reorderingKey{r.remoteID, r.topicHash})
}
