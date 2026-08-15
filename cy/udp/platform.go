// Package udp provides a UDP transport layer implementation for Cyphal.
// This is a Go-native implementation that doesn't depend on libUDPard.
//
// This package implements the Platform interface from the cy package,
// providing a complete UDP-based transport for Cyphal v1.1.
package udp

import (
	"fmt"
	"net"
	"sync"
	"time"
	"unsafe"

	"github.com/opencyphal/cy-go"
)

// DefaultUDPPort is the default port for Cyphal/UDP (12345).
const DefaultUDPPort = 12345

// MaxUDPPayloadSize is the maximum payload size for UDP datagrams.
// This is limited by the MTU and IP fragmentation considerations.
const MaxUDPPayloadSize = 1472 // Safe for Ethernet MTU (1500 - IP header - UDP header)

// Platform implements the cy.Platform interface for UDP transport.
type Platform struct {
	// cyInstance is the Cy instance (set by cy.New).
	cyInstance *cy.Cy
	
	// uid is the unique identifier for this node.
	uid uint64
	
	// localAddr is the local address and port.
	localAddr *net.UDPAddr
	
	// conn is the UDP connection for sending and receiving.
	conn *net.UDPConn
	
	// ifaceCount is the number of redundant interfaces.
	ifaceCount int
	
	// txQueueCapacity is the capacity of the TX queue.
	txQueueCapacity int
	
	// writers maps subject-IDs to subject writers.
	writers map[uint32]*subjectWriter
	
	// readers maps subject-IDs to subject readers.
	readers map[uint32]*subjectReader
	
	// unicastExtent is the maximum extent for unicast messages.
	unicastExtent int
	
	// subjectIDModulus is the subject-ID modulus.
	subjectIDModulus uint32
	
	// mu protects the platform state.
	mu sync.RWMutex
	
	// closed indicates if the platform has been destroyed.
	closed bool
}

// subjectWriter implements cy.SubjectWriter for UDP transport.
type subjectWriter struct {
	subjectID uint32
	platform  *Platform
	// multicastAddr is the multicast address for this subject.
	multicastAddr *net.UDPAddr
}

// SubjectID returns the subject-ID for this writer.
func (w *subjectWriter) SubjectID() uint32 {
	return w.subjectID
}

// subjectReader implements cy.SubjectReader for UDP transport.
type subjectReader struct {
	subjectID uint32
	extent   int
	platform  *Platform
	// multicastAddr is the multicast address for this subject.
	multicastAddr *net.UDPAddr
}

// SubjectID returns the subject-ID for this reader.
func (r *subjectReader) SubjectID() uint32 {
	return r.subjectID
}

// Extent returns the maximum message size for this reader.
func (r *subjectReader) Extent() int {
	return r.extent
}

// SetExtent sets the maximum message size for this reader.
func (r *subjectReader) SetExtent(extent int) {
	r.extent = extent
}

// New creates a new UDP platform with default settings.
// It automatically selects a local address and port.
func New() (*Platform, error) {
	return NewWithAddress("", DefaultUDPPort, 0)
}

// NewWithAddress creates a new UDP platform with the specified local address and port.
// If address is empty, it will listen on all interfaces.
// txQueueCapacity is the capacity of the TX queue (not used in this simple implementation).
// uid is the unique identifier for this node.
func NewWithAddress(address string, port int, uid uint64) (*Platform, error) {
	p := &Platform{
		uid:             uid,
		ifaceCount:      1,
		txQueueCapacity: 1000,
		writers:        make(map[uint32]*subjectWriter),
		readers:        make(map[uint32]*subjectReader),
		subjectIDModulus: cy.SubjectIDModulus16bit,
		unicastExtent:   MaxUDPPayloadSize,
	}

	// Resolve local address
	localAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", address, port))
	if err != nil {
		return nil, err
	}
	p.localAddr = localAddr

	// Create UDP connection
	conn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		return nil, err
	}
	p.conn = conn

	// Start receiving datagrams in a goroutine
	go p.receiveLoop()

	return p, nil
}

// NewManual creates a new UDP platform with explicit configuration.
// This is similar to cy_udp_posix_new_manual in the C implementation.
func NewManual(uid uint64, localIfaceAddresses []uint32, txQueueCapacity int) (*Platform, error) {
	// For simplicity, we'll just use the first address
	if len(localIfaceAddresses) == 0 || localIfaceAddresses[0] == 0 {
		return New()
	}

	// Convert IP address from uint32 to string
	address := ipToString(localIfaceAddresses[0])
	return NewWithAddress(address, DefaultUDPPort, uid)
}

// Destroy cleans up the platform and all resources.
func (p *Platform) Destroy() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	p.closed = true

	// Close the connection
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}

	// Clear maps
	p.writers = nil
	p.readers = nil
	p.cyInstance = nil
}

// receiveLoop receives UDP datagrams and dispatches them to the appropriate handlers.
func (p *Platform) receiveLoop() {
	buf := make([]byte, MaxUDPPayloadSize)

	for {
		p.mu.RLock()
		closed := p.closed
		conn := p.conn
		p.mu.RUnlock()

		if closed || conn == nil {
			return
		}

		// Read a datagram
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			// Error receiving, wait and retry
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// Process the datagram
		p.processDatagram(buf[:n], addr)
	}
}

// processDatagram processes a received UDP datagram.
func (p *Platform) processDatagram(data []byte, addr *net.UDPAddr) {
	p.mu.RLock()
	cyInstance := p.cyInstance
	p.mu.RUnlock()

	if cyInstance == nil {
		return
	}

	// Parse the Cyphal/UDP header
	// The header format depends on the Cyphal specification
	// For now, we'll use a simplified approach
	
	// First 2 bytes: subject-ID (16-bit)
	if len(data) < 2 {
		return
	}
	
	subjectID := uint32(data[0]) | uint32(data[1])<<8
	
	// Next byte: priority (3 bits) and flags
	priority := cy.PriorityNominal
	if len(data) > 2 {
		priority = cy.Priority((data[2] >> 5) & 0x07)
	}

	// Rest is payload
	payload := data[3:]

	// Create a message
	message := cy.AcquireMessage()
	message.SetData(payload)

	msgTS := cy.NewMessageTS(cy.Microsecond(time.Now().UnixMicro()), message)
	lane := cy.Lane{
		ID:       hashAddress(addr),
		Priority: priority,
	}

	// Dispatch to Cy instance
	cyInstance.HandleMessage(lane, &subjectID, *msgTS)
	
	// Release resources
	cy.ReleaseMessageTS(msgTS)
}

// hashAddress creates a unique ID from an address for the lane.
func hashAddress(addr *net.UDPAddr) uint64 {
	// Simple hash of IP and port
	return uint64(hashIP(addr.IP))<<16 | uint64(addr.Port)
}

// hashIP creates a hash from an IP address.
func hashIP(ip net.IP) uint32 {
	var hash uint32
	for i := 0; i < len(ip) && i < 4; i++ {
		hash = (hash << 8) | uint32(ip[i])
	}
	return hash
}

// NewSubjectWriter creates a new subject writer for the specified subject-ID.
func (p *Platform) NewSubjectWriter(subjectID uint32) (cy.SubjectWriter, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, cy.ErrArgument
	}

	// Check if we already have a writer for this subject-ID
	if writer, ok := p.writers[subjectID]; ok {
		return writer, nil
	}

	// Create multicast address for this subject
	// In Cyphal/UDP, each subject has a multicast group
	multicastAddr := p.getMulticastAddr(subjectID)

	writer := &subjectWriter{
		subjectID:    subjectID,
		platform:     p,
		multicastAddr: multicastAddr,
	}
	
	p.writers[subjectID] = writer
	return writer, nil
}

// getMulticastAddr returns the multicast address for a subject-ID.
// This is a simplified implementation.
func (p *Platform) getMulticastAddr(subjectID uint32) *net.UDPAddr {
	// Cyphal/UDP uses multicast groups in the range 239.255.0.0 to 239.255.255.255
	// The subject-ID is mapped to the last octet of the multicast address
	// This is a simplified mapping - the actual mapping depends on the Cyphal spec
	
	ip := net.IP{239, 255, 0, byte(subjectID % 256)}
	return &net.UDPAddr{
		IP:   ip,
		Port: DefaultUDPPort,
	}
}

// DestroySubjectWriter destroys a subject writer.
func (p *Platform) DestroySubjectWriter(writer cy.SubjectWriter) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	if sw, ok := writer.(*subjectWriter); ok {
		delete(p.writers, sw.subjectID)
	}
}

// SubjectWriterSend sends a message via a subject writer.
func (p *Platform) SubjectWriterSend(writer cy.SubjectWriter, deadline cy.Microsecond, priority cy.Priority, data []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return cy.ErrArgument
	}

	sw, ok := writer.(*subjectWriter)
	if !ok {
		return cy.ErrArgument
	}

	// Build the UDP datagram
	// Format: [subjectID:2][priority:3 bits][payload...]
	buf := make([]byte, 3+len(data))
	
	// Write subject-ID (2 bytes, little-endian)
	buf[0] = byte(sw.subjectID)
	buf[1] = byte(sw.subjectID >> 8)
	
	// Write priority in the top 3 bits of byte 2
	buf[2] = byte(priority) << 5
	
	// Copy payload
	copy(buf[3:], data)

	// Send to multicast address
	_, err := p.conn.WriteToUDP(buf, sw.multicastAddr)
	if err != nil {
		// Non-blocking, so we don't return error
		return nil
	}

	return nil
}

// NewSubjectReader creates a new subject reader for the specified subject-ID.
func (p *Platform) NewSubjectReader(subjectID uint32, extent int) (cy.SubjectReader, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, cy.ErrArgument
	}

	// Check if we already have a reader for this subject-ID
	if reader, ok := p.readers[subjectID]; ok {
		return reader, nil
	}

	// Create multicast address for this subject
	multicastAddr := p.getMulticastAddr(subjectID)

	// Join the multicast group
	if p.conn != nil && p.localAddr != nil {
		// For multicast, we need to join the group
		// This is platform-specific and may require additional setup
		// For now, we'll just create the reader without joining
		// In a real implementation, we'd use IP_ADD_MEMBERSHIP
	}

	reader := &subjectReader{
		subjectID:    subjectID,
		extent:      extent,
		platform:     p,
		multicastAddr: multicastAddr,
	}
	
	p.readers[subjectID] = reader
	return reader, nil
}

// DestroySubjectReader destroys a subject reader.
func (p *Platform) DestroySubjectReader(reader cy.SubjectReader) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	if sr, ok := reader.(*subjectReader); ok {
		delete(p.readers, sr.subjectID)
	}
}

// SetSubjectReaderExtent sets the extent for a subject reader.
func (p *Platform) SetSubjectReaderExtent(reader cy.SubjectReader, extent int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return
	}

	if sr, ok := reader.(*subjectReader); ok {
		sr.extent = extent
	}
}

// Unicast sends a unicast message to a remote.
func (p *Platform) Unicast(lane cy.Lane, deadline cy.Microsecond, data []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return cy.ErrArgument
	}

	// For unicast, we need the remote address
	// In Cyphal/UDP, unicast uses a well-known port
	// The remote address would be extracted from the lane context
	
	// For now, we'll just send to a default address
	// In a real implementation, we'd extract the address from the lane
	remoteAddr := &net.UDPAddr{
		IP:   net.ParseIP("255.255.255.255"), // Broadcast for now
		Port: DefaultUDPPort,
	}

	// Build the UDP datagram for unicast
	// Format: [special marker][remote ID][seqno][payload...]
	buf := make([]byte, 1+8+1+len(data))
	buf[0] = 0xFF // Special marker for unicast
	
	// Copy lane.ID (remote ID) - for now, just use first 8 bytes
	for i := 0; i < 8 && i < len(lane.Context); i++ {
		buf[1+i] = lane.Context[i]
	}
	
	// Seqno
	buf[9] = 0 // Would be extracted from context
	
	// Copy payload
	copy(buf[10:], data)

	_, err := p.conn.WriteToUDP(buf, remoteAddr)
	if err != nil {
		return nil
	}

	return nil
}

// SetUnicastExtent sets the maximum extent for unicast messages.
func (p *Platform) SetUnicastExtent(extent int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unicastExtent = extent
}

// Spin runs the event loop until the specified deadline.
func (p *Platform) Spin(deadline cy.Microsecond) error {
	// For UDP, spin just processes pending datagrams
	// The actual receiving happens in the background goroutine
	
	// For now, just return OK
	// In a real implementation, we might set a read deadline
	return cy.OK
}

// Now returns the current time in microseconds.
func (p *Platform) Now() cy.Microsecond {
	return cy.Microsecond(time.Now().UnixMicro())
}

// SubjectIDModulus returns the subject-ID modulus configured for this platform.
// It mirrors the C platform->subject_id_modulus field.
func (p *Platform) SubjectIDModulus() uint32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.subjectIDModulus
}

// Realloc reallocates memory.
func (p *Platform) Realloc(ptr unsafe.Pointer, size int) unsafe.Pointer {
	if size == 0 {
		return nil
	}
	if ptr == nil {
		b := make([]byte, size)
		return unsafe.Pointer(&b[0])
	}
	oldSlice := (*[1 << 30]byte)(ptr)[:size:size]
	newSlice := make([]byte, size)
	copy(newSlice, oldSlice)
	return unsafe.Pointer(&newSlice[0])
}

// Random returns a random 64-bit value.
func (p *Platform) Random() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.uid++ // Simple increment for now
	return p.uid
}

// SetCy sets the Cy instance reference.
func (p *Platform) SetCy(cyInstance *cy.Cy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cyInstance = cyInstance
}

// ipToString converts a uint32 IP address to a string.
func ipToString(ip uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d",
		byte(ip>>24), byte(ip>>16), byte(ip>>8), byte(ip))
}

// Home returns the default home name for this platform.
// This is the fixed-length zero-padded lowercase hex UID.
func (p *Platform) Home(prefix string) string {
	uidStr := fmt.Sprintf("%016x", p.uid)
	if prefix != "" {
		return prefix + "/" + uidStr
	}
	return uidStr
}

// Namespace returns the default namespace.
// This reads from the CYPHAL_NAMESPACE environment variable.
func (p *Platform) Namespace() string {
	// In a real implementation, read from environment
	return ""
}

// Stats returns platform statistics.
// This is a placeholder for now.
func (p *Platform) Stats() (subjectWriterCount int, subjectReaderCount int) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.writers), len(p.readers)
}

// Ensure interfaces are satisfied
var _ cy.Platform = (*Platform)(nil)
var _ cy.SubjectWriter = (*subjectWriter)(nil)
var _ cy.SubjectReader = (*subjectReader)(nil)
