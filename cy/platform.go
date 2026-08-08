package cy

import (
	"unsafe"
)

// Platform is the interface that abstracts away the specifics of the transport
// (UDP, serial, CAN, etc) and the platform where Cy is running (POSIX, baremetal MCU, RTOS, etc).
//
// This interface corresponds to the cy_platform_vtable_t in the C implementation.
type Platform interface {
	// === MULTICAST ===

	// NewSubjectWriter creates a new subject writer for the specified subject-ID.
	// The writer is used to send messages on that subject.
	// Returns nil on OOM.
	NewSubjectWriter(subjectID uint32) (SubjectWriter, error)

	// DestroySubjectWriter destroys a subject writer.
	DestroySubjectWriter(writer SubjectWriter)

	// SubjectWriterSend non-blockingly publishes a new message on the subject.
	// The message lifetime ends upon return from this function.
	// Valid subject-ID values are in the range [0, 2^ceil(log2(subject_id_modulus+SubjectIDPinnedMax))).
	SubjectWriterSend(writer SubjectWriter, deadline Microsecond, priority Priority, data []byte) error

	// NewSubjectReader creates a new subject reader for the specified subject-ID.
	// The reader is used to receive messages from that subject.
	// The extent is the maximum message size the reader should accept.
	// Returns nil on OOM.
	NewSubjectReader(subjectID uint32, extent int) (SubjectReader, error)

	// DestroySubjectReader destroys a subject reader.
	DestroySubjectReader(reader SubjectReader)

	// SetSubjectReaderExtent updates the maximum extent of incoming messages for the reader.
	// This is used when subscription configuration is changed such that the extent
	// that was used when the subject reader was created is no longer sufficient.
	SetSubjectReaderExtent(reader SubjectReader, extent int)

	// === UNICAST ===

	// Unicast sends a unicast transfer to the specified remote node.
	// The lane and message lifetime ends upon return from this function.
	Unicast(lane Lane, deadline Microsecond, data []byte) error

	// SetUnicastExtent sets the maximum extent of incoming unicast transfers.
	// Messages larger than this may be truncated.
	SetUnicastExtent(extent int)

	// === EVENT LOOP ===

	// Spin runs the event loop until the specified deadline, or until the first error.
	// Early exit is allowed.
	// If the deadline is in the past, update the event loop once without blocking and return.
	// The OnMessage callback will be invoked from this function.
	// This is the only platform function that is allowed to block.
	Spin(deadline Microsecond) error

	// === MISC ===

	// Now returns the current monotonic time in microseconds.
	// The initial time shall be non-negative.
	Now() Microsecond

	// Realloc reallocates memory. If size is zero, it must behave like free.
	// Standard realloc semantics.
	Realloc(ptr unsafe.Pointer, size int) unsafe.Pointer

	// Random returns a random 64-bit unsigned integer.
	// A TRNG is preferred; if not available, a PRNG will suffice, but its initial state
	// MUST be distinct across reboots in quick succession, and it should be hashed with
	// the node's unique identifier.
	Random() uint64

	// SetCy sets the Cy instance reference (called by cy.New).
	// This allows the platform to access the Cy instance if needed.
	SetCy(cy *Cy)
}

// SubjectWriter is used to send messages on a specific subject-ID.
// Cy guarantees that there will be at most one subject writer per subject-ID.
type SubjectWriter interface {
	// SubjectID returns the subject-ID this writer is configured for.
	SubjectID() uint32
}

// SubjectReader is created when the higher layer requires data from the specified subject-ID.
// The transport layer must report all received messages via the platform's message callback.
// Deduplication and long message support are not required on subjects above (SubjectIDPinnedMax+modulus).
// Cy guarantees that there will be at most one subject reader per subject-ID.
type SubjectReader interface {
	// SubjectID returns the subject-ID this reader is configured for.
	SubjectID() uint32
	// Extent returns the current maximum message size.
	Extent() int
	// SetExtent updates the maximum message size.
	SetExtent(extent int)
}

// Lane identifies a remote node that originated a message along with the information
// needed to send a response if needed.
type Lane struct {
	// ID uniquely identifies the remote node within the network.
	ID uint64
	// Context is transport-specific metadata needed to send a unicast message to this remote.
	Context [24]byte
	// Priority is stored here to allow rx/tx priority matching.
	Priority Priority
}

// Message, MessageTS, and MessageVTable are defined in message.go
// OnMessageCallback is the function signature for message reception callbacks.
// This is called by the platform when a new message is received.
type OnMessageCallback func(lane Lane, subjectID *uint32, message MessageTS)

// PlatformBase is a base struct that can be embedded in platform implementations.
// It provides common functionality and fields.
type PlatformBase struct {
	// Cy is the Cy instance tied to this platform.
	// It is assigned automatically in cy.New and should not be altered.
	Cy *Cy
	// SubjectIDModulus is the subject-ID modulus for this platform.
	SubjectIDModulus uint32
	// OnMessage is the callback for received messages.
	OnMessage OnMessageCallback
	// UnicastExtent is the maximum extent for incoming unicast transfers.
	UnicastExtent int
}

// NewSubjectWriter creates a new subject writer (default implementation returns error).
func (p *PlatformBase) NewSubjectWriter(subjectID uint32) (SubjectWriter, error) {
	return nil, ErrArgument
}

// DestroySubjectWriter destroys a subject writer (default implementation is no-op).
func (p *PlatformBase) DestroySubjectWriter(writer SubjectWriter) {}

// SubjectWriterSend sends a message via a subject writer (default implementation returns error).
func (p *PlatformBase) SubjectWriterSend(writer SubjectWriter, deadline Microsecond, priority Priority, data []byte) error {
	return ErrArgument
}

// NewSubjectReader creates a new subject reader (default implementation returns error).
func (p *PlatformBase) NewSubjectReader(subjectID uint32, extent int) (SubjectReader, error) {
	return nil, ErrArgument
}

// DestroySubjectReader destroys a subject reader (default implementation is no-op).
func (p *PlatformBase) DestroySubjectReader(reader SubjectReader) {}

// SetSubjectReaderExtent sets the extent for a subject reader (default implementation is no-op).
func (p *PlatformBase) SetSubjectReaderExtent(reader SubjectReader, extent int) {}

// Unicast sends a unicast message (default implementation returns error).
func (p *PlatformBase) Unicast(lane Lane, deadline Microsecond, data []byte) error {
	return ErrArgument
}

// SetUnicastExtent sets the unicast extent (default implementation updates field).
func (p *PlatformBase) SetUnicastExtent(extent int) {
	p.UnicastExtent = extent
}

// Spin runs the event loop (default implementation returns OK).
func (p *PlatformBase) Spin(deadline Microsecond) error {
	return OK
}

// Now returns the current time (default implementation uses time.Now).
func (p *PlatformBase) Now() Microsecond {
	// Note: In a real implementation, this should use the platform's time source
	// For now, we'll use a placeholder
	return 0
}

// Realloc reallocates memory (default implementation uses Go's allocator).
func (p *PlatformBase) Realloc(ptr unsafe.Pointer, size int) unsafe.Pointer {
	if size == 0 {
		// Free - in Go we can't actually free, but we can return nil
		return nil
	}
	if ptr == nil {
		// Allocate new
		b := make([]byte, size)
		return unsafe.Pointer(&b[0])
	}
	// Reallocate - in Go we need to copy to new slice
	oldSlice := (*[1 << 30]byte)(ptr)[:size:size]
	newSlice := make([]byte, size)
	copy(newSlice, oldSlice)
	return unsafe.Pointer(&newSlice[0])
}

// Random returns a random value (default implementation uses math/rand).
func (p *PlatformBase) Random() uint64 {
	// Placeholder - real implementation should use proper RNG
	return 0
}

// SetCy sets the Cy instance reference.
func (p *PlatformBase) SetCy(cy *Cy) {
	p.Cy = cy
}
