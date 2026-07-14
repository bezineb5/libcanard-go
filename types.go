// Package libcanard is a Go port of https://github.com/OpenCyphal/libcanard,
// a compact implementation of the Cyphal/CAN transport for high-integrity real-time embedded systems.
//
// The library offers a non-blocking, callback-based API. It is not safe for concurrent use; if used in a
// concurrent environment it is the responsibility of the application to provide adequate synchronization.
//
// The original library is distributed under the terms of the MIT License. Copyright (c) OpenCyphal.
package libcanard


import "unsafe"

// ---------------------------------------------------------------------------------------------------------------------
// Versioning.
// ---------------------------------------------------------------------------------------------------------------------

const (
	VersionMajor = 5
	VersionMinor = 0

	CyphalVersionMajor = 1
	CyphalVersionMinor = 1
)

// IfaceCount is the maximum number of local redundant network interfaces supported by a single instance.
// The library supports at least 1 and at most 8. Changing this value requires recompilation.
const IfaceCount = 1

// ---------------------------------------------------------------------------------------------------------------------
// Parameter ranges (inclusive; lower bound is zero for all).
// ---------------------------------------------------------------------------------------------------------------------

const (
	SubjectIDMax    = 0xFFFF // Applies to Cyphal v1.1 and UAVCAN v0/DroneCAN message data type IDs.
	SubjectIDMax13b = 8191   // Cyphal v1.0 supports only 13-bit subject-IDs.
	ServiceIDMax    = 511    // Applies to Cyphal, all versions. In v0 this is narrower.
	NodeIDMax       = 127    //
	NodeIDCapacity  = NodeIDMax + 1
	TransferIDBits  = 5
	TransferIDModulo = 1 << TransferIDBits
	TransferIDMax   = TransferIDModulo - 1

	// NodeIDAnonymous is used only with Cyphal v1.0 and legacy v0 protocols to indicate anonymous messages.
	NodeIDAnonymous = 0xFF

	// DefaultTransferIDTimeoutUs is the recommended transfer-ID timeout given in the Cyphal Specification.
	DefaultTransferIDTimeoutUs = 2000000

	MTUClassic = 8 //
	MTUFD      = 64

	KindCount = 7 //
	PrioCount = 8 //
	PrioBits  = 3 //
)

// IfaceBitmapAll is the set of all interface bits for the configured IfaceCount.
const IfaceBitmapAll = (1 << IfaceCount) - 1

// ---------------------------------------------------------------------------------------------------------------------
// Transfer kinds.
// ---------------------------------------------------------------------------------------------------------------------

// Kind identifies the kind of a transfer. Distinct message kinds use separate ID spaces.
type Kind int

const (
	KindMessage16b Kind = 0 // 16-bit subject-ID message introduced in Cyphal v1.1. Isolated subject-ID space.
	KindMessage13b Kind = 1 // 13-bit subject-ID message originally defined in Cyphal v1.0.
	KindResponse   Kind = 2 // Cyphal v1 RPC-service response.
	KindRequest    Kind = 3 // Cyphal v1 RPC-service request.
	KindV0Message  Kind = 4 // Legacy DroneCAN/UAVCAN v0 message.
	KindV0Response Kind = 5 // Legacy DroneCAN/UAVCAN v0 service response.
	KindV0Request  Kind = 6 // Legacy DroneCAN/UAVCAN v0 service request.
)

// kindVersion returns 1 for Cyphal v1.x kinds, 0 for legacy v0 kinds.
func kindVersion(k Kind) uint8 {
	if k < KindV0Message {
		return 1
	}
	return 0
}

// ---------------------------------------------------------------------------------------------------------------------
// Priorities.
// ---------------------------------------------------------------------------------------------------------------------

// Prio is the transfer priority level.
type Prio uint8

const (
	PrioExceptional Prio = 0
	PrioImmediate   Prio = 1
	PrioFast        Prio = 2
	PrioHigh        Prio = 3
	PrioNominal     Prio = 4 // Nominal priority level should be the default.
	PrioLow         Prio = 5
	PrioSlow        Prio = 6
	PrioOptional    Prio = 7
)

// ---------------------------------------------------------------------------------------------------------------------
// Memory resources.
//
// Each resource is used for allocating memory for a specific purpose so applications can fine-tune memory use.
// Ordinary applications can use the same resource for everything.
//
// In this Go port the variable-size byte buffers are allocated through the provided Mem resources (so an O(1)
// allocator such as o1heap can be plugged in). The fixed-size internal objects (transfer/session/subscription
// descriptors) are allocated through the Go runtime because they contain pointers the Go garbage collector must
// track.
// ---------------------------------------------------------------------------------------------------------------------

// MemVTable is the allocator interface for a single memory resource.
type MemVTable struct {
	Free  func(m Mem, size int, ptr unsafe.Pointer)
	Alloc func(m Mem, size int) unsafe.Pointer
}

// Mem models a memory resource for allocating a particular kind of objects used by the library.
type Mem struct {
	VTable  *MemVTable
	Context unsafe.Pointer
}

// MemSet groups the per-purpose memory resources used by the library.
type MemSet struct {
	TXTransfer Mem // TX transfer objects, fixed-size, one per enqueued transfer.
	TXFrame    Mem // One per enqueued frame, at least one per multi-frame transfer, size MTU+overhead.
	RXSession  Mem // Remote-associated sessions per subscriber, fixed-size.
	RXPayload  Mem // Variable-size, max size approx. extent+sizeof(rx_slot_t).
	RXFilters  Mem // For Filter[filter_count] temporary storage. Not needed if filters not used.
}

var defaultMemVTable = MemVTable{
	Alloc: func(m Mem, size int) unsafe.Pointer {
		// Use Go's allocator - size is the type size, not byte count
		// We'll use a byte slice for variable-size allocations
		b := make([]byte, size)
		return unsafe.Pointer(&b[0])
	},
	Free: func(m Mem, size int, ptr unsafe.Pointer) {},
}

// NewDefaultMemSet returns a memory set that uses the Go runtime for all allocations.
func NewDefaultMemSet() MemSet {
	mk := func() Mem { return Mem{VTable: &defaultMemVTable} }
	return MemSet{
		TXTransfer: mk(),
		TXFrame:    mk(),
		RXSession:  mk(),
		RXPayload:  mk(),
		RXFilters:  mk(),
	}
}

// memAlloc allocates size bytes from the given memory resource.
func memAlloc(m Mem, size int) unsafe.Pointer { return m.VTable.Alloc(m, size) }

// memFree releases memory previously allocated with memAlloc.
func memFree(m Mem, size int, ptr unsafe.Pointer) {
	if ptr != nil {
		m.VTable.Free(m, size, ptr)
	}
}

func memValid(m Mem) bool {
	return m.VTable != nil && m.VTable.Alloc != nil && m.VTable.Free != nil
}

// ---------------------------------------------------------------------------------------------------------------------
// Bytes, payloads and chains.
// ---------------------------------------------------------------------------------------------------------------------

// Bytes represents a view of a contiguous byte buffer.
type Bytes struct {
	Size int
	Data unsafe.Pointer
}

// BytesMut is a mutable byte buffer (used for payload ownership transfer).
type BytesMut struct {
	Size int
	Data unsafe.Pointer
}

// BytesChain is a scattered (potentially non-contiguous) byte sequence. In this Go port payloads are normally
// contiguous, but the chain type is retained for API compatibility with the C original.
type BytesChain struct {
	Bytes Bytes
	Next  *BytesChain
}

// Payload represents a received transfer payload.
//
// The application shall access the useful payload through View and use Origin only for lifetime management: for
// multi-frame transfers, View points into dynamically allocated RX payload storage and Origin owns it; for
// single-frame transfers, View points into the CAN frame data buffer passed via IngestFrame and Origin is empty
// (Data == nil). The lifetime of View ends upon return from the OnMessage callback, so the application must copy the
// data if it needs to outlive the callback.
type Payload struct {
	View   Bytes
	Origin BytesMut
}

// ---------------------------------------------------------------------------------------------------------------------
// Filters.
// ---------------------------------------------------------------------------------------------------------------------

// Filter matches extended CAN IDs on data frames (no std/rtr). Bits above 29 are always zero.
type Filter struct {
	ExtendedCANID uint32
	ExtendedMask  uint32
}

// ---------------------------------------------------------------------------------------------------------------------
// Intrusive AVL tree node and intrusive list node.
// ---------------------------------------------------------------------------------------------------------------------

// cavlNode is an intrusive AVL tree node (compatible with the CAVL2 abstraction used by the original library).
type cavlNode struct {
	height int8
	up    *cavlNode
	lr    [2]*cavlNode
	bf    int8
	owner any // Pointer to the owning Go object (the CAVL2_TO_OWNER equivalent).
}

// cavlListed is an intrusive doubly-linked list node.
type cavlListed struct {
	next  *cavlListed
	prev  *cavlListed
	owner any
}

// cavlList is a doubly-linked list with head and tail pointers.
type cavlList struct {
	head *cavlListed
	tail *cavlListed
}

// ---------------------------------------------------------------------------------------------------------------------
// Subscription and vtable types.
// ---------------------------------------------------------------------------------------------------------------------

// SubscriptionVTable carries the callbacks invoked by the library for a subscription.
type SubscriptionVTable struct {
	// OnMessage is invoked when a new message is received on a subscription. The timestamp is the arrival timestamp
	// of the first frame of the transfer. The callback may unsubscribe itself; it must not otherwise reenter the
	// library (no Ingest/Publish/Poll).
	OnMessage func(self *Subscription, timestamp int64, priority Prio, sourceNodeID uint8, transferID uint8, payload Payload)
}

// Subscription instances must not be moved while in use. Each subscription is indexed by its port-ID inside the
// canard instance, and in turn contains a tree of sessions indexed by remote node-ID.
type Subscription struct {
	indexPortID cavlNode // Tree node indexed by port-ID within the owner instance.

	TransferIDTimeout int64
	Extent            int
	PortID            uint16
	CRCSeed           uint16
	Kind              Kind

	Owner    *Canard
	Sessions *cavlNode
	VTable   *SubscriptionVTable

	UserContext any
}

// VTable carries the callbacks the library invokes to interact with the platform.
type VTable struct {
	// Now returns the current monotonic time in microseconds. Must be a non-negative non-decreasing value.
	Now func(self *Canard) int64

	// TX submits one CAN frame for transmission via the specified interface. It returns true if the frame was
	// accepted for transmission, false if there is no free mailbox (try again later). The callback must not mutate
	// the TX pipeline (no Publish/Cancel/Free/etc).
	TX func(self *Canard, userContext any, deadline int64, ifaceIndex uint8, fd bool, extendedCANID uint32, canData []byte) bool

	// Filter reconfigures the acceptance filters of the CAN controller hardware. It returns true on success. This
	// function may be nil if filtering is unsupported. It is only invoked from Poll.
	Filter func(self *Canard, filterCount int, filters []Filter) bool
}

// ---------------------------------------------------------------------------------------------------------------------
// The main instance object.
// ---------------------------------------------------------------------------------------------------------------------

type canardTX struct {
	FD            bool
	queueCapacity int
	queueSize     int
	seqno         uint64
	pending       [IfaceCount]*cavlNode
	deadline      *cavlNode
	agewise       cavlList
}

type canardRX struct {
	subscriptions          [KindCount]*cavlNode
	listSessionByAnimation cavlList
	filterCount            int
	filtersDirty           bool
}

type canardErr struct {
	OOM          uint64 // Out of memory; a transfer could have been lost.
	TXCapacity   uint64 // A transfer could not be enqueued due to queue capacity limit.
	TXSacrifice  uint64 // An old pending transfer had to be sacrificed to make room for a new transfer.
	TXExpiration uint64 // A transfer had to be dequeued due to deadline expiration.
	RXFrame      uint64 // A received frame was malformed and thus dropped.
	RXTransfer   uint64 // A transfer could not be reassembled correctly.
	Collision    uint64 // Number of times the local node-ID was changed to repair a collision.
}

// Canard is the main instance object.
type Canard struct {
	NodeIDOccupancyBitmap [2]uint64

	// NodeID is the local node-ID. By default it is allocated automatically, with occupancy/collision tracking.
	NodeID uint8

	// IfaceBitmap is the bitmap of interfaces available on this node; the enqueue interface bitmap is ANDed with it.
	IfaceBitmap uint8

	tx canardTX
	rx canardRX
	Err canardErr

	Mem       MemSet
	PRNGState uint64

	VTable *VTable

	UserContext any
}
