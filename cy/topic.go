package cy

import (
	"fmt"
	"strings"

	"go.dw1.io/rapidhash"
)

// Topic represents a Cyphal topic.
// Topics are identified by their name and are assigned a subject-ID through
// a distributed CRDT-based allocation protocol.
type Topic struct {
	// name is the topic name (e.g., "sensors/temperature").
	name string
	// hash is the hash of the topic name.
	hash uint64
	// subjectID is the subject-ID assigned to this topic.
	subjectID uint32
	// pinned indicates whether this topic has a pinned subject-ID.
	pinned bool
	// pinnedSubjectID is the explicitly pinned subject-ID (if pinned).
	pinnedSubjectID uint32

	// CRDT state for topic allocation
	// tsOrigin is the wall-clock approximation of when this topic was first seen
	// on the network (microseconds). The wire log-age is always derived from it
	// via log2(now - tsOrigin), mirroring the C topic->ts_origin field. A freshly
	// created topic has tsOrigin == now, yielding a log-age of LAGEMin (-1).
	tsOrigin Microsecond
	// logAge is the cached wire log-age (log2 of seconds since tsOrigin). It is
	// recomputed from tsOrigin whenever the topic is (re)allocated or merged, and
	// read for the wire at send time via Lage(now).
	logAge int32
	// evictions is the number of times this topic has been evicted and recreated.
	evictions uint32

	// transport handles managed by the consensus layer on reallocation.
	reader       SubjectReader
	readerExtent int

	// gossipCounter counts gossips sent for this topic; the first few (and every
	// Nth) are broadcast for observability, the rest are sharded. Reset to zero on
	// urgent (repair) gossip. Mirrors C cy_topic_t.gossip_counter.
	gossipCounter uint64

	// extent is the maximum message size for this topic.
	extent int

	// refcount is the number of references to this topic.
	refcount int

	// dedup holds per-remote reliable-deduplication state. It is only consulted
	// for reliable (MsgRel) transfers, where ACK loss can cause duplicates.
	dedup map[uint64]*dedupState
}

// newTopic creates a new topic with the specified name. The name is expected to be
// pin-free (resolution strips any pin); the explicit pin argument (PinNone for a
// non-pinned topic, or a value <= SubjectIDPinnedMax) selects the pinned subject-ID.
// It mirrors C topic_new(), which receives an already-resolved name and eviction state.
func newTopic(name string, modulus uint32, pin uint16) (*Topic, error) {
	if name == "" {
		return nil, ErrName
	}

	// Normalize and charset-validate the name.
	name, ok := normalizeName(name)
	if !ok {
		return nil, ErrName
	}

	// Compute hash
	hash := HashString(name)

	topic := &Topic{
		name:     name,
		hash:     hash,
		tsOrigin: 0,
		logAge:   LAGEMin,
		extent:   0,
		refcount: 1,
		dedup:    make(map[uint64]*dedupState),
	}

	// Pinned topics encode the subject-ID in the eviction counter as
	// UINT32_MAX - subject_id, placing it in the reserved pinned range
	// [EVICTIONS_PINNED_MIN, UINT32_MAX]; this is what lets is_pinned() detect them
	// and keeps the hash reflecting the name. findOrCreateTopic re-syncs evictions
	// from the CRDT afterwards (pinned topics keep this encoding).
	if pin <= SubjectIDPinnedMax {
		topic.pinned = true
		topic.pinnedSubjectID = uint32(pin)
		topic.subjectID = uint32(pin)
		topic.evictions = ^uint32(pin)
	} else {
		topic.subjectID = ComputeSubjectID(hash, 0, modulus)
	}

	return topic, nil
}

// ParsePinnedTopic checks if a topic name has a pinned subject-ID.
// Pinned topics have the format "name#1234" where 1234 is the subject-ID.
// NOTE: Prefer resolving names via Cy.Resolve/Cy.Resolve, which strips and reports
// the pin explicitly; this helper remains for direct name inspection.
func ParsePinnedTopic(name string) (bool, uint32) {
	// Find the last '#' in the name
	idx := strings.LastIndex(name, "#")
	if idx == -1 {
		return false, 0
	}

	// Extract the subject-ID part
	subjectIDStr := name[idx+1:]
	
	// Check if it's a valid number
	var subjectID uint64
	_, err := fmt.Sscanf(subjectIDStr, "%d", &subjectID)
	if err != nil || subjectID > SubjectIDPinnedMax {
		// Not a valid pinned topic
		return false, 0
	}

	// Check for leading zeros (not allowed)
	if len(subjectIDStr) > 1 && subjectIDStr[0] == '0' {
		return false, 0
	}

	// The actual topic name is the part before the '#'
	// But we need to check if it's empty
	topicName := name[:idx]
	if topicName == "" {
		return false, 0
	}

	return true, uint32(subjectID)
}

// isPinned reports whether an eviction counter is in the pinned range
// [EVICTIONS_PINNED_MIN, UINT32_MAX]. A topic is pinned when its eviction counter
// is in this reserved range; the pinned subject-ID is then UINT32_MAX - evictions.
// This mirrors the C is_pinned() helper.
func isPinned(evictions uint32) bool {
	return evictions >= EVICTIONS_PINNED_MIN
}

// ComputeSubjectID computes the subject-ID for a topic based on its hash and evictions.
// This implements the quadratic probing formula from the Cyphal specification.
func ComputeSubjectID(hash uint64, evictions uint32, modulus uint32) uint32 {
	// For pinned topics, the subject-ID is explicitly set
	// For normal topics, use the CRDT formula:
	// subject_id = CY_SUBJECT_ID_PINNED_MAX + 1
	//              + (((hash mod modulus) + ((evictions mod modulus)^2 mod modulus)) mod modulus)
	
	hashMod := hash % uint64(modulus)
	evictMod := uint64(evictions) % uint64(modulus)
	probe := (hashMod + (evictMod*evictMod)%uint64(modulus)) % uint64(modulus)
	
	return SubjectIDPinnedMax + 1 + uint32(probe)
}

// HashString computes a hash for a string using rapidhash.
func HashString(s string) uint64 {
	return rapidhash.Hash([]byte(s))
}

// Name returns the topic name.
func (t *Topic) Name() string {
	return t.name
}

// Hash returns the topic hash.
func (t *Topic) Hash() uint64 {
	return t.hash
}

// SubjectID returns the topic's subject-ID.
func (t *Topic) SubjectID() uint32 {
	return t.subjectID
}

// Pinned returns whether this topic has a pinned subject-ID.
func (t *Topic) Pinned() bool {
	return t.pinned
}

// PinnedSubjectID returns the explicitly pinned subject-ID, or 0 if not pinned.
func (t *Topic) PinnedSubjectID() uint32 {
	return t.pinnedSubjectID
}

// Evictions returns the eviction count of the topic.
func (t *Topic) Evictions() uint32 {
	return t.evictions
}

// Extent returns the maximum message size for this topic.
func (t *Topic) Extent() int {
	return t.extent
}

// SetExtent sets the maximum message size for this topic.
func (t *Topic) SetExtent(extent int) {
	t.extent = extent
}

// Refcount returns the number of references to this topic.
func (t *Topic) Refcount() int {
	return t.refcount
}

// IncRef increases the reference count.
func (t *Topic) IncRef() {
	t.refcount++
}

// DecRef decreases the reference count.
func (t *Topic) DecRef() {
	if t.refcount > 0 {
		t.refcount--
	}
}

// LogAge returns the cached wire log-age of the topic.
// The authoritative value is derived from tsOrigin via Lage(now); this accessor
// returns the last cached computation (refreshed on allocation and merge).
func (t *Topic) LogAge() int32 {
	return t.logAge
}

// Lage computes the current wire log-age from tsOrigin: log2((now - ts_origin)/1s).
// A freshly created topic (tsOrigin == now) yields LAGEMin. It mirrors C topic_lage().
func (t *Topic) Lage(now Microsecond) int32 {
	age := now - t.tsOrigin
	if age < 0 {
		age = 0
	}
	return int32(Log2Floor(uint64(age) / MEGA))
}

// SetOriginAt sets tsOrigin and refreshes the cached log-age from an explicit
// (lage, evictions) pair, mirroring the C topic_lage / topic_new initialization.
func (t *Topic) SetOriginAt(now Microsecond, lage int32, evictions uint32) {
	t.evictions = evictions
	t.tsOrigin = now - LageToUS(int(lage))
	t.logAge = t.Lage(now)
}

// MergeLageAt merges a remote log-age into the topic by shifting tsOrigin into
// the past if the remote topic is older. This is the CRDT merge operator on the
// log-age, mirroring C topic_merge_lage().
func (t *Topic) MergeLageAt(now Microsecond, rLage int32) {
	otherOrigin := now - LageToUS(int(rLage))
	if otherOrigin < t.tsOrigin {
		t.tsOrigin = otherOrigin
	}
	t.logAge = t.Lage(now)
}

// UpdateState is the known-topic divergence merge used by the consensus layer.
// It returns whether the local log-age was refreshed (the merge always pulls the
// remote age into tsOrigin) and whether the local topic wins arbitration over the
// remote (older topic wins; ties broken by larger eviction counter). When the
// local topic loses, the caller is expected to reallocate it to adopt the
// remote's eviction counter (and therefore its subject-ID), converging both.
// It mirrors the divergence branch of C on_gossip_known_topic().
func (t *Topic) UpdateState(now Microsecond, otherLogAge int32, otherEvictions uint32) (merged bool, win bool) {
	mineLage := t.Lage(now)
	t.MergeLageAt(now, otherLogAge)
	if t.evictions != otherEvictions {
		return true, (mineLage > otherLogAge) || ((mineLage == otherLogAge) && (t.evictions > otherEvictions))
	}
	return true, false
}

// State returns the current CRDT state of the topic.
func (t *Topic) State() (logAge int32, evictions uint32) {
	return t.logAge, t.evictions
}

// SetState sets the CRDT state of the topic.
func (t *Topic) SetState(logAge int32, evictions uint32) {
	t.logAge = logAge
	t.evictions = evictions
}

	// GossipName returns the topic name carried in gossip messages so receivers can
// auto-subscribe unknown topics (C sends the name for this purpose).
func (t *Topic) GossipName() string {
	return t.name
}

// ---------------------------------------------------------------------------
// Reliable-message deduplication (per remote node).
// ---------------------------------------------------------------------------

// dedupFind returns the per-remote dedup state, dropping it first if stale.
func (t *Topic) dedupFind(remoteID uint64, now Microsecond) *dedupState {
	d, ok := t.dedup[remoteID]
	if !ok {
		return nil
	}
	if now-d.lastActiveAt > SessionLifetime {
		delete(t.dedup, remoteID)
		return nil
	}
	return d
}

// dedupFindOrCreate returns the per-remote dedup state, creating it if missing
// or stale. The new state's frontier tag is set to tag.
func (t *Topic) dedupFindOrCreate(remoteID, tag uint64, now Microsecond) *dedupState {
	if d := t.dedupFind(remoteID, now); d != nil {
		return d
	}
	d := &dedupState{remoteID: remoteID, tag: tag, lastActiveAt: now}
	t.dedup[remoteID] = d
	return d
}

// dedupCheck reports whether tag was already accepted from remoteID.
func (t *Topic) dedupCheck(remoteID, tag uint64, now Microsecond) bool {
	d := t.dedupFind(remoteID, now)
	if d == nil {
		return false
	}
	return d.check(tag)
}

// dedupCommit records tag as accepted from remoteID (creating state if needed).
func (t *Topic) dedupCommit(remoteID, tag uint64, now Microsecond) {
	d := t.dedupFindOrCreate(remoteID, tag, now)
	d.commit(tag)
	d.lastActiveAt = now
}

// String returns a string representation of the topic.
func (t *Topic) String() string {
	if t == nil {
		return "<nil>"
	}
	pinnedStr := ""
	if t.pinned {
		pinnedStr = fmt.Sprintf(" (pinned:%d)", t.pinnedSubjectID)
	}
	return fmt.Sprintf("Topic{name:%q, hash:0x%016x, subjectID:%d%s}",
		t.name, t.hash, t.subjectID, pinnedStr)
}
