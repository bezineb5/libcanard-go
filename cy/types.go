// Package cy implements Cyphal v1.1 - a robust decentralized pub/sub protocol
// with tunable reliability and service discovery.
//
// This package provides the core session layer that is platform- and transport-agnostic.
// Transport-specific implementations are in subpackages (can, udp).
package cy

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

// Constants from cy.h
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
