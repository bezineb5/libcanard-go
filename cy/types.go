// Package cy implements Cyphal v1.1 - a robust decentralized pub/sub protocol
// with tunable reliability and service discovery.
//
// This package provides the core session layer that is platform- and transport-agnostic.
// Transport-specific implementations are in subpackages (can, udp).
package cy

import "math/bits"

// Error codes returned by Cy functions.
// These match the C library error codes for compatibility.
type Error int

const (
	// OK indicates successful operation.
	OK Error = iota
	// Error code 1 is omitted intentionally (reserved)
	_ Error = iota
	// ErrArgument indicates an invalid argument was passed.
	ErrArgument
	// ErrMemory indicates a memory allocation failure.
	ErrMemory
	// ErrCapacity indicates a resource capacity was exceeded.
	ErrCapacity
	// ErrName indicates an invalid topic name.
	ErrName
	// ErrMedia indicates a generic low-level network error.
	ErrMedia
	// ErrLag indicates strong scheduler lag was detected.
	ErrLag
	// ErrDelivery indicates a reliable message was not acknowledged.
	ErrDelivery
	// ErrLiveness indicates a message or response did not arrive on time.
	ErrLiveness
	// ErrNACK indicates the message was explicitly rejected by the remote.
	ErrNACK
)

// Error implements the error interface.
func (e Error) Error() string {
	switch e {
	case OK:
		return "OK"
	case ErrArgument:
		return "invalid argument"
	case ErrMemory:
		return "memory allocation failed"
	case ErrCapacity:
		return "capacity exceeded"
	case ErrName:
		return "invalid name"
	case ErrMedia:
		return "media error"
	case ErrLag:
		return "scheduler lag"
	case ErrDelivery:
		return "delivery failed"
	case ErrLiveness:
		return "liveness timeout"
	case ErrNACK:
		return "explicitly rejected"
	default:
		return "unknown error"
	}
}

// Priority levels for message transmission.
// Higher priority messages are transmitted first.
type Priority uint8

const (
	// PriorityExceptional is the highest priority, for exceptional/emergency messages.
	PriorityExceptional Priority = iota
	// PriorityImmediate is for immediate action required.
	PriorityImmediate
	// PriorityFast is for fast updates.
	PriorityFast
	// PriorityHigh is for high priority messages.
	PriorityHigh
	// PriorityNominal is the default priority for normal messages.
	PriorityNominal
	// PriorityLow is for low priority messages.
	PriorityLow
	// PrioritySlow is for slow/background updates.
	PrioritySlow
	// PriorityOptional is the lowest priority, for optional data.
	PriorityOptional
)

// PriorityCount is the total number of priority levels.
const PriorityCount = 8

// Microsecond is a monotonic microsecond timestamp.
// It is signed to permit arithmetic in the past.
type Microsecond int64

// Time scale constants (microseconds), matching cy.c.
const (
	// KILO is one thousand.
	KILO = 1000
	// MEGA is one million microseconds (one second).
	MEGA = 1000000
)

// Gossip timing/configuration constants, matching cy.c defaults.
const (
	// GossipPeriod is the baseline interval between periodic gossips (5 seconds).
	GossipPeriod = 5 * MEGA
	// GossipUrgentDelayMax bounds how long an urgent gossip is delayed (10 ms).
	GossipUrgentDelayMax = 10 * KILO
	// GossipBroadcastRatio: every Nth gossip (and multiples of N) is broadcast for
	// observability; the rest are sharded (and urgent gossips are always broadcast).
	GossipBroadcastRatio = 10
	// GossipPeriodDitherRatio controls the random jitter added to the gossip period.
	GossipPeriodDitherRatio = 8
)

// Log2Floor returns floor(log2(x)). It returns -1 when x == 0, mirroring the C
// log2_floor() helper (which feeds contiguous comparisons for log-age).
func Log2Floor(x uint64) int8 {
	return int8(63 - bits.LeadingZeros64(x))
}

// LageToUS converts a log-age (log2 of seconds) into microseconds. Negative
// values (which represent a freshly created topic) are treated as zero, mirroring
// the C lage_to_us() helper and log2_floor() continuity.
func LageToUS(lage int) Microsecond {
	if lage < 0 {
		return 0
	}
	clamped := lage
	if clamped > LAGEMax {
		clamped = LAGEMax
	}
	return Microsecond(1)<<uint(clamped) * MEGA
}

// IsPrimeU32 reports whether n is a prime number. It mirrors the C is_prime_u32().
func IsPrimeU32(n uint32) bool {
	if n <= 2 {
		return n == 2
	}
	if n&1 == 0 {
		return false
	}
	for d := uint32(3); d <= n/d; d += 2 {
		if n%d == 0 {
			return false
		}
	}
	return true
}

// IsValidSubjectIDModulus reports whether a subject-ID modulus is acceptable for
// the network. It mirrors the C is_valid_subject_id_modulus() precondition:
//   - modulus >= SubjectIDModulus16bit (smallest usable value),
//   - modulus <= UINT32_MAX - SubjectIDPinnedMax,
//   - modulus is prime (quadratic probing requires a prime modulus to cover all
//     residues), and
//   - modulus mod 4 == 3 (enables fast reconstruction of the eviction count).
// It additionally guarantees that at least one gossip shard fits between the max
// valid subject-ID and the broadcast subject-ID.
func IsValidSubjectIDModulus(modulus uint32) bool {
	if modulus < SubjectIDModulus16bit ||
		modulus > (^uint32(0)-SubjectIDPinnedMax) ||
		!IsPrimeU32(modulus) ||
		modulus%4 != 3 {
		return false
	}
	maxSubjectID := SubjectIDMax(modulus)
	broadcastSubjectID := BroadcastSubjectID(modulus)
	shardMinSubjectID := maxSubjectID + 1
	if broadcastSubjectID <= shardMinSubjectID {
		return false
	}
	return (broadcastSubjectID - shardMinSubjectID) < modulus
}

// BroadcastSubjectID computes the subject-ID reserved for broadcast transport
// (broadcast gossips, scouts, and other protocol needs). It is the largest
// subject-ID of the form 2^k-1 that is >= the max valid subject-ID, matching the
// C computation in cy_new().
func BroadcastSubjectID(modulus uint32) uint32 {
	maxSubjectID := SubjectIDMax(modulus)
	return (uint32(1) << (uint8(Log2Floor(uint64(maxSubjectID))+1))) - 1
}

// GossipShardCount computes how many gossip shard subject-IDs fit between the max
// valid subject-ID and the broadcast subject-ID. It matches the C computation:
// broadcast_subject_id - (CY_SUBJECT_ID_MAX(modulus) + 1).
func GossipShardCount(modulus uint32) uint32 {
	return BroadcastSubjectID(modulus) - (SubjectIDMax(modulus) + 1)
}
const (
	// SubjectIDPinnedMax is the maximum subject-ID for pinned topics.
	// Pinned topics are identified by evictions >= (UINT32_MAX - SubjectIDPinnedMax).
	// The pinned subject-ID is UINT32_MAX - evictions.
	SubjectIDPinnedMax = 0x1FFF
)

// SubjectIDMax computes the maximum subject-ID for a given modulus.
// This notably excludes the broadcast subject and gossip shards.
func SubjectIDMax(modulus uint32) uint32 {
	return SubjectIDPinnedMax + modulus
}

// Subject-ID modulus constants from cy_platform.h
const (
	// SubjectIDModulus16bit is suitable for all Cyphal transports.
	SubjectIDModulus16bit = 57203
	// SubjectIDModulus23bit is incompatible with Cyphal/CAN.
	SubjectIDModulus23bit = 8378431
	// SubjectIDModulus32bit is incompatible with Cyphal/CAN and Cyphal/UDPv4.
	SubjectIDModulus32bit = 4294954663
)

// AuxSubjectExtent is the maximum size of messages exchanged by the session layer itself.
// Currently this includes only gossip and scout messages.
const AuxSubjectExtent = 476

// BigBang is the initial timestamp value (minimum int64).
const BigBang = -9223372036854775808 // INT64_MIN

// LAGE (Log-Age) constants
const (
	// LAGEMin is the log-age of a newly created topic.
	LAGEMin = -1
	// LAGEMax is the maximum log-age (log2 of seconds).
	// 2^35 seconds is a little over one millennium.
	LAGEMax = 35
)

// EVICTIONS_PINNED_MIN is the eviction counter threshold for pinned topics.
// A topic is pinned when evictions >= EVICTIONS_PINNED_MIN.
// The pinned subject-ID is (UINT32_MAX - evictions), in [0, SubjectIDPinnedMax].
// Normal eviction counters never approach this range in practice.
const EVICTIONS_PINNED_MIN = ^uint32(SubjectIDPinnedMax) // 0xFFFFE000

// Default timeouts (in microseconds)
const (
	// ImplicitTopicDefaultTimeout is how long an implicit topic (created from pattern subscription)
	// is kept after it's been idle.
	ImplicitTopicDefaultTimeout = 600 * 1000000 // 600 seconds

	// ACKBaselineDefaultTimeout is the default timeout for reliable message acknowledgments.
	ACKBaselineDefaultTimeout = 16 * 1000 // 16 ms

	// ACKTXTimeout is how long pending ACK transfers wait in TX buffer if not transmitted.
	ACKTXTimeout = 1000000 // 1 second

	// DefaultTopicExtent is the receive extent used when a topic is created without an
	// explicit subscriber extent (e.g. via Advertise). It must be large enough for the
	// libcanard reassembly buffer; a zero extent would make ingest panic.
	DefaultTopicExtent = 256

	// SessionLifetime is how long soft states associated with remotes are retained.
	SessionLifetime = 60 * 1000000 // 60 seconds

	// SessionCounterMaxBackwardLag is the largest backward monotonic-counter jump
	// still treated as delayed traffic from the current session.
	SessionCounterMaxBackwardLag = 100000
)

// UserContext is an opaque user context enabling the application to share data with callbacks.
// It is intended to be passed by value.
// The size is chosen to match most small closures, which is helpful when interfacing with
// Rust/C++ lambdas. This size must be the same for all translation units.
const UserContextPtrCount = 2

type UserContext struct {
	Ptr [UserContextPtrCount]interface{}
}

// EmptyUserContext returns an empty user context.
func EmptyUserContext() UserContext {
	return UserContext{}
}
