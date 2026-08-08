package cy

import (
	"sync"
	"unsafe"
)

// MessageVTable defines the platform-specific implementation of Message.
// This corresponds to cy_message_vtable_t in the C implementation.
type MessageVTable struct {
	// Skip is used to skip the session-layer header after receiving the message.
	// All subsequent reads must add this offset to the requested offset.
	// The effect is incremental if invoked more than once.
	Skip func(msg *Message, offset int)

	// Read adds the skip offset to the requested offset and adjusts the size accordingly.
	// The implementation must limit the size if the requested range exceeds the available message size.
	// Returns the number of bytes copied to the output buffer.
	Read func(msg *Message, offset, size int, destination []byte) int

	// GetSize returns the size of the message sans the skip offset.
	GetSize func(msg *Message) int

	// Destroy invalidates the message instance.
	// Cy invokes this when the refcount drops to zero.
	Destroy func(msg *Message)
}

// Message is an abstract buffer handle with a platform-specific implementation.
// It allows the platform layer to eliminate data copying until/unless explicitly requested.
// Some transport libraries (e.g., libudpard) store the data in a set of segments obtained
// directly from the NIC.
type Message struct {
	// Refcount is the reference count for the message.
	Refcount int
	// VTable is the platform-specific implementation.
	VTable *MessageVTable
	// payloadData is platform-specific data.
	payloadData interface{}
	
	// For messages that store data directly
	data []byte
	
	// skipOffset is the offset to skip (for session-layer header).
	skipOffset int
}

// SetData sets the message data.
func (m *Message) SetData(data []byte) {
	m.data = append(m.data[:0], data...)
}

// GetData returns the message data as a byte slice.
// The returned slice is a copy, so modifying it won't affect the message.
func (m *Message) GetData() []byte {
	if m.data == nil {
		return nil
	}
	result := make([]byte, len(m.data))
	copy(result, m.data)
	return result
}

// NewMessage creates a new message with the specified data.
func NewMessage(data []byte) *Message {
	return &Message{
		Refcount: 1,
		VTable:   &defaultMessageVTable,
		data:    data,
	}
}

// NewMessageWithVTable creates a new message with a custom vtable.
func NewMessageWithVTable(vtable *MessageVTable, data []byte) *Message {
	return &Message{
		Refcount: 1,
		VTable:   vtable,
		data:    data,
	}
}

// defaultMessageVTable is the default vtable for simple messages.
var defaultMessageVTable MessageVTable = MessageVTable{
	Skip: func(msg *Message, offset int) {
		msg.skipOffset += offset
	},
	Read: func(msg *Message, offset, size int, destination []byte) int {
		// Adjust offset and size for skip
		offset += msg.skipOffset
		
		// Check bounds
		if offset < 0 {
			offset = 0
		}
		
		available := len(msg.data) - offset
		if size > available {
			size = available
		}
		
		if size <= 0 {
			return 0
		}
		
		// Copy data
		copy(destination, msg.data[offset:offset+size])
		return size
	},
	GetSize: func(msg *Message) int {
		return len(msg.data) - msg.skipOffset
	},
	Destroy: func(msg *Message) {
		// Clear the data to allow GC
		msg.data = nil
	},
}

// Size returns the size of the message in bytes.
// This is the only way to access the received message size.
func (m *Message) Size() int {
	if m.VTable != nil && m.VTable.GetSize != nil {
		return m.VTable.GetSize(m)
	}
	return len(m.data) - m.skipOffset
}

// Read gathers size bytes of data located at offset bytes from payload origin
// into the provided contiguous buffer.
// The function returns the number of bytes copied.
// If the requested range exceeds the available message size, only the available bytes are copied.
func (m *Message) Read(offset, size int, destination []byte) int {
	if m.VTable != nil && m.VTable.Read != nil {
		return m.VTable.Read(m, offset, size, destination)
	}
	
	// Default implementation
	offset += m.skipOffset
	
	// Check bounds
	if offset < 0 {
		offset = 0
	}
	
	available := len(m.data) - offset
	if size > available {
		size = available
	}
	
	if size <= 0 {
		return 0
	}
	
	// Copy data
	if len(destination) < size {
		size = len(destination)
	}
	
	copy(destination, m.data[offset:offset+size])
	return size
}

// Skip skips the specified number of bytes at the beginning of the message.
// This is used to skip the session-layer header.
func (m *Message) Skip(offset int) {
	if m.VTable != nil && m.VTable.Skip != nil {
		m.VTable.Skip(m, offset)
	} else {
		m.skipOffset += offset
	}
}

// Payload returns the message payload as a byte slice.
// Note: This may not be safe for all message implementations.
func (m *Message) Payload() []byte {
	if m.skipOffset > 0 {
		return m.data[m.skipOffset:]
	}
	return m.data
}

// RefcountInc increases the reference count of the message.
// No effect if the message pointer is nil.
func (m *Message) RefcountInc() {
	if m != nil {
		m.Refcount++
	}
}

// RefcountDec decreases the reference count of the message.
// If the reference count reaches zero, the message is destroyed.
// No effect if the message pointer is nil.
func (m *Message) RefcountDec() {
	if m == nil {
		return
	}
	
	m.Refcount--
	if m.Refcount <= 0 {
		m.destroy()
	}
}

// destroy destroys the message.
func (m *Message) destroy() {
	if m.VTable != nil && m.VTable.Destroy != nil {
		m.VTable.Destroy(m)
	}
	// Clear data
	m.data = nil
	m.payloadData = nil
}

// MessageTS is a message with an associated non-negative arrival timestamp.
// The timestamp is carried over from the low-level NIC driver.
type MessageTS struct {
	Timestamp Microsecond
	Content   *Message
}

// NewMessageTS creates a new message with timestamp.
func NewMessageTS(timestamp Microsecond, content *Message) *MessageTS {
	return &MessageTS{
		Timestamp: timestamp,
		Content:   content,
	}
}

// MessageSize returns the size of the message content.
func (m *MessageTS) MessageSize() int {
	if m.Content == nil {
		return 0
	}
	return m.Content.Size()
}

// ReadMessage reads data from the message content.
func (m *MessageTS) ReadMessage(offset, size int, destination []byte) int {
	if m.Content == nil {
		return 0
	}
	return m.Content.Read(offset, size, destination)
}

// Bytes represents an immutable borrowed buffer, optionally fragmented.
// The last entry has next==nil.
type Bytes struct {
	Size int
	Data unsafe.Pointer
	Next *Bytes
}

// NewBytes creates a new Bytes from a byte slice.
func NewBytes(data []byte) *Bytes {
	if len(data) == 0 {
		return &Bytes{Size: 0, Data: nil, Next: nil}
	}
	return &Bytes{
		Size: len(data),
		Data: unsafe.Pointer(&data[0]),
		Next: nil,
	}
}

// NewBytesChain creates a chain of Bytes from multiple slices.
func NewBytesChain(slices ...[]byte) *Bytes {
	if len(slices) == 0 {
		return nil
	}
	
	var head, current *Bytes
	for _, s := range slices {
		b := NewBytes(s)
		if head == nil {
			head = b
			current = b
		} else {
			current.Next = b
			current = b
		}
	}
	
	return head
}

// TotalSize returns the total size of the bytes chain.
func (b *Bytes) TotalSize() int {
	if b == nil {
		return 0
	}
	total := b.Size
	for next := b.Next; next != nil; next = next.Next {
		total += next.Size
	}
	return total
}

// ToSlice copies the bytes chain to a single slice.
func (b *Bytes) ToSlice() []byte {
	if b == nil {
		return nil
	}
	
	size := b.TotalSize()
	result := make([]byte, size)
	
	offset := 0
	for current := b; current != nil; current = current.Next {
		if current.Size > 0 && current.Data != nil {
			data := (*[1 << 30]byte)(current.Data)[:current.Size:current.Size]
			copy(result[offset:], data)
			offset += current.Size
		}
	}
	
	return result
}

// MessagePool is a pool of Message objects for reuse.
var MessagePool = sync.Pool{
	New: func() interface{} {
		return &Message{
			Refcount: 0,
			VTable:   &defaultMessageVTable,
			data:    make([]byte, 0, 1024),
		}
	},
}

// AcquireMessage gets a message from the pool.
func AcquireMessage() *Message {
	return MessagePool.Get().(*Message)
}

// ReleaseMessage returns a message to the pool.
func ReleaseMessage(m *Message) {
	if m == nil {
		return
	}
	
	// Reset the message
	m.Refcount = 0
	m.skipOffset = 0
	m.data = m.data[:0]
	m.payloadData = nil
	
	MessagePool.Put(m)
}

// MessageTSPool is a pool of MessageTS objects.
var MessageTSPool = sync.Pool{
	New: func() interface{} {
		return &MessageTS{}
	},
}

// AcquireMessageTS gets a MessageTS from the pool.
func AcquireMessageTS() *MessageTS {
	return MessageTSPool.Get().(*MessageTS)
}

// ReleaseMessageTS returns a MessageTS to the pool.
func ReleaseMessageTS(m *MessageTS) {
	if m == nil {
		return
	}
	
	// Release the content message
	if m.Content != nil {
		ReleaseMessage(m.Content)
		m.Content = nil
	}
	
	m.Timestamp = 0
	MessageTSPool.Put(m)
}
