// Package udp provides a Cyphal/UDP transport platform for the cy-go stack.
//
// It is a faithful Go port of the C cy_udp_posix plugin. All UDP framing,
// reassembly, deduplication and session management are implemented here so that
// the wire format is bit-for-bit compatible with the reference libudpard-based
// implementation used by the C cy_can / cy_udp_posix plugins.
//
// Wire format (libudpard, see cyphal_udp_header.dsdl):
//
//	All Cyphal/UDP traffic uses UDP port 9382. The multicast group for a subject
//	is 239.0.0.0 (0xEF000000) | (subject_id & 0x7FFFFF). Each transfer is split
//	into one or more UDP datagrams; every datagram carries a 32-byte header with
//	a CRC32C checksum, followed by the frame payload. Reassembly, deduplication
//	and return-path (unicast endpoint) discovery are performed per remote UID.
package udp

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	mrand "math/rand/v2"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/opencyphal/cy-go"
)

// Cyphal/UDP constants (mirroring libudpard).
const (
	// udpPort is the well-known Cyphal/UDP port (UDP_PORT in libudpard).
	udpPort = 9382

	// ipv4McastPrefix is 239.0.0.0 (IPv4_MCAST_PREFIX). The subject multicast
	// group is ipv4McastPrefix | (subject_id & IPv4_SUBJECT_ID_MAX).
	ipv4McastPrefix = 0xEF000000

	// headerSizeBytes is the fixed Cyphal/UDP header size (HEADER_SIZE_BYTES).
	headerSizeBytes = 32
	// headerVersion is the current header version (HEADER_VERSION = 2).
	headerVersion = 2
	// crcSizeBytes is the size of each CRC32C field.
	crcSizeBytes = 4

	// mtuDefault is the default maximum datagram payload size (UDPARD_MTU_DEFAULT).
	mtuDefault = 1400
	// mtuMin is the minimum transfer payload size (UDPARD_MTU_MIN).
	mtuMin = 476

	// ifaceCountMax is the maximum number of redundant network interfaces.
	ifaceCountMax = 3

	// ipv4SubjectIDMax limits subject-IDs on IPv4 (UDPARD_IPv4_SUBJECT_ID_MAX).
	ipv4SubjectIDMax = 0x7FFFFF

	// subjectIDModulus23bit is the subject-ID modulus for the IPv4 transport
	// (CY_SUBJECT_ID_MODULUS_23bit). Subjects above the modulus are treated as
	// "stateless" (broadcast / gossip shard) subjects.
	subjectIDModulus23bit = 8378431

	// maxDatagram is the largest datagram we accept on RX (header + MTU + slack).
	maxDatagram = headerSizeBytes + mtuDefault + 256
)

// crcResidue is crc32c over the 32-byte header of a well-formed frame. The
// header CRC field is computed over the first 28 bytes; verifying the full
// 32-byte CRC yields this constant residue (CRC_RESIDUE_AFTER_OUTPUT_XOR).
const crcResidue = 0xB798B438 ^ 0xFFFFFFFF // 0x48674BC7

// crcTable is the reflected CRC32C (Castagnoli) table, copied verbatim from
// libudpard so the checksum is byte-identical to the reference implementation.
var crcTable = [256]uint32{
	0x00000000, 0xF26B8303, 0xE13B70F7, 0x1350F3F4, 0xC79A971F, 0x35F1141C, 0x26A1E7E8, 0xD4CA64EB,
	0x8AD958CF, 0x78B2DBCC, 0x6BE22838, 0x9989AB3B, 0x4D43CFD0, 0xBF284CD3, 0xAC78BF27, 0x5E133C24,
	0x105EC76F, 0xE235446C, 0xF165B798, 0x030E349B, 0xD7C45070, 0x25AFD373, 0x36FF2087, 0xC494A384,
	0x9A879FA0, 0x68EC1CA3, 0x7BBCEF57, 0x89D76C54, 0x5D1D08BF, 0xAF768BBC, 0xBC267848, 0x4E4DFB4B,
	0x20BD8EDE, 0xD2D60DDD, 0xC186FE29, 0x33ED7D2A, 0xE72719C1, 0x154C9AC2, 0x061C6936, 0xF477EA35,
	0xAA64D611, 0x580F5512, 0x4B5FA6E6, 0xB93425E5, 0x6DFE410E, 0x9F95C20D, 0x8CC531F9, 0x7EAEB2FA,
	0x30E349B1, 0xC288CAB2, 0xD1D83946, 0x23B3BA45, 0xF779DEAE, 0x05125DAD, 0x1642AE59, 0xE4292D5A,
	0xBA3A117E, 0x4851927D, 0x5B016189, 0xA96AE28A, 0x7DA08661, 0x8FCB0562, 0x9C9BF696, 0x6EF07595,
	0x417B1DBC, 0xB3109EBF, 0xA0406D4B, 0x522BEE48, 0x86E18AA3, 0x748A09A0, 0x67DAFA54, 0x95B17957,
	0xCBA24573, 0x39C9C670, 0x2A993584, 0xD8F2B687, 0x0C38D26C, 0xFE53516F, 0xED03A29B, 0x1F682198,
	0x5125DAD3, 0xA34E59D0, 0xB01EAA24, 0x42752927, 0x96BF4DCC, 0x64D4CECF, 0x77843D3B, 0x85EFBE38,
	0xDBFC821C, 0x2997011F, 0x3AC7F2EB, 0xC8AC71E8, 0x1C661503, 0xEE0D9600, 0xFD5D65F4, 0x0F36E6F7,
	0x61C69362, 0x93AD1061, 0x80FDE395, 0x72966096, 0xA65C047D, 0x5437877E, 0x4767748A, 0xB50CF789,
	0xEB1FCBAD, 0x197448AE, 0x0A24BB5A, 0xF84F3859, 0x2C855CB2, 0xDEEEDFB1, 0xCDBE2C45, 0x3FD5AF46,
	0x7198540D, 0x83F3D70E, 0x90A324FA, 0x62C8A7F9, 0xB602C312, 0x44694011, 0x5739B3E5, 0xA55230E6,
	0xFB410CC2, 0x092A8FC1, 0x1A7A7C35, 0xE811FF36, 0x3CDB9BDD, 0xCEB018DE, 0xDDE0EB2A, 0x2F8B6829,
	0x82F63B78, 0x709DB87B, 0x63CD4B8F, 0x91A6C88C, 0x456CAC67, 0xB7072F64, 0xA457DC90, 0x563C5F93,
	0x082F63B7, 0xFA44E0B4, 0xE9141340, 0x1B7F9043, 0xCFB5F4A8, 0x3DDE77AB, 0x2E8E845F, 0xDCE5075C,
	0x92A8FC17, 0x60C37F14, 0x73938CE0, 0x81F80FE3, 0x55326B08, 0xA759E80B, 0xB4091BFF, 0x466298FC,
	0x1871A4D8, 0xEA1A27DB, 0xF94AD42F, 0x0B21572C, 0xDFEB33C7, 0x2D80B0C4, 0x3ED04330, 0xCCBBC033,
	0xA24BB5A6, 0x502036A5, 0x4370C551, 0xB11B4652, 0x65D122B9, 0x97BAA1BA, 0x84EA524E, 0x7681D14D,
	0x2892ED69, 0xDAF96E6A, 0xC9A99D9E, 0x3BC21E9D, 0xEF087A76, 0x1D63F975, 0x0E330A81, 0xFC588982,
	0xB21572C9, 0x407EF1CA, 0x532E023E, 0xA145813D, 0x758FE5D6, 0x87E466D5, 0x94B49521, 0x66DF1622,
	0x38CC2A06, 0xCAA7A905, 0xD9F75AF1, 0x2B9CD9F2, 0xFF56BD19, 0x0D3D3E1A, 0x1E6DCDEE, 0xEC064EED,
	0xC38D26C4, 0x31E6A5C7, 0x22B65633, 0xD0DDD530, 0x0417B1DB, 0xF67C32D8, 0xE52CC12C, 0x1747422F,
	0x49547E0B, 0xBB3FFD08, 0xA86F0EFC, 0x5A048DFF, 0x8ECEE914, 0x7CA56A17, 0x6FF599E3, 0x9D9E1AE0,
	0xD3D3E1AB, 0x21B862A8, 0x32E8915C, 0xC083125F, 0x144976B4, 0xE622F5B7, 0xF5720643, 0x07198540,
	0x590AB964, 0xAB613A67, 0xB831C993, 0x4A5A4A90, 0x9E902E7B, 0x6CFBAD78, 0x7FAB5E8C, 0x8DC0DD8F,
	0xE330A81A, 0x115B2B19, 0x020BD8ED, 0xF0605BEE, 0x24AA3F05, 0xD6C1BC06, 0xC5914FF2, 0x37FACCF1,
	0x69E9F0D5, 0x9B8273D6, 0x88D28022, 0x7AB90321, 0xAE7367CA, 0x5C18E4C9, 0x4F48173D, 0xBD23943E,
	0xF36E6F75, 0x0105EC76, 0x12551F82, 0xE03E9C81, 0x34F4F86A, 0xC69F7B69, 0xD5CF889D, 0x27A40B9E,
	0x79B737BA, 0x8BDCB4B9, 0x988C474D, 0x6AE7C44E, 0xBE2DA0A5, 0x4C4623A6, 0x5F16D052, 0xAD7D5351,
}

// crc32c computes the full CRC32C (init 0xFFFFFFFF, reflected table, output XOR
// 0xFFFFFFFF). It matches libudpard's crc_full exactly.
func crc32c(data []byte) uint32 {
	crc := uint32(0xFFFFFFFF)
	for _, b := range data {
		crc = (crc >> 8) ^ crcTable[(b^byte(crc))&0xFF]
	}
	return crc ^ 0xFFFFFFFF
}

// udpHeader is the decoded Cyphal/UDP frame header.
type udpHeader struct {
	priority     uint8  // 0..7
	transferID   uint64 // 48-bit
	senderUID    uint64
	offset       uint32
	transferSize uint32
	prefixCRC    uint32
}

// serializeHeader writes a 32-byte Cyphal/UDP header into buf (len must be 32).
// prefixCRC is the CRC32C of the transfer payload prefix ending at this frame.
func serializeHeader(buf []byte, h udpHeader, prefixCRC uint32) {
	buf[0] = byte(headerVersion) | (h.priority << 5)
	buf[1] = 0                                                          // reserved (void5) + incompatibility (must be 0)
	binary.LittleEndian.PutUint64(buf[2:], h.transferID&0xFFFFFFFFFFFF) // 48-bit transfer-ID
	binary.LittleEndian.PutUint64(buf[8:], h.senderUID)                 // 64-bit sender UID
	binary.LittleEndian.PutUint32(buf[16:], h.offset)                   // 32-bit offset
	binary.LittleEndian.PutUint32(buf[20:], h.transferSize)
	binary.LittleEndian.PutUint32(buf[24:], prefixCRC)
	binary.LittleEndian.PutUint32(buf[28:], crc32c(buf[0:headerSizeBytes-crcSizeBytes]))
}

// deserializeHeader decodes a datagram into a header and payload. Returns
// ok=false if the datagram is too small, the header CRC is invalid, the version
// or incompatibility bits are wrong, or the frame size is inconsistent.
func deserializeHeader(data []byte) (h udpHeader, payload []byte, ok bool) {
	if len(data) < headerSizeBytes {
		return udpHeader{}, nil, false
	}
	if crc32c(data[0:headerSizeBytes]) != crcResidue {
		return udpHeader{}, nil, false
	}
	head := data[0]
	if (head & 0x1F) != headerVersion {
		return udpHeader{}, nil, false
	}
	if data[1]>>5 != 0 { // incompatibility bits must be zero
		return udpHeader{}, nil, false
	}
	h.priority = (head >> 5) & 0x07
	h.transferID = binary.LittleEndian.Uint64(data[2:]) & 0xFFFFFFFFFFFF // 48-bit
	h.senderUID = binary.LittleEndian.Uint64(data[8:])
	h.offset = binary.LittleEndian.Uint32(data[16:])
	h.transferSize = binary.LittleEndian.Uint32(data[20:])
	h.prefixCRC = binary.LittleEndian.Uint32(data[24:])
	payload = data[headerSizeBytes:]
	if uint64(h.offset)+uint64(len(payload)) > uint64(h.transferSize) {
		return udpHeader{}, nil, false
	}
	if h.offset == 0 && crc32c(payload) != h.prefixCRC {
		return udpHeader{}, nil, false
	}
	return h, payload, true
}

// endpoint is a UDP/IPv4 address (mirrors udpard_udpip_ep_t, 6 bytes).
type endpoint struct {
	ip   uint32
	port uint16
}

func (e endpoint) valid() bool { return e.ip != 0 && e.port != 0 }

func ipToU32(ip net.IP) uint32 {
	v4 := ip.To4()
	if v4 == nil {
		return 0
	}
	return binary.BigEndian.Uint32(v4) // network byte order
}

func u32ToIP(ip uint32) net.IP {
	b := make(net.IP, 4)
	binary.BigEndian.PutUint32(b, ip)
	return b
}

// subjectEndpoint returns the multicast UDP endpoint for a subject-ID.
func subjectEndpoint(subjectID uint32) *net.UDPAddr {
	v := uint32(ipv4McastPrefix) | (subjectID & ipv4SubjectIDMax)
	return &net.UDPAddr{IP: u32ToIP(v), Port: udpPort}
}

// Lane.Context (24 bytes) carries up to ifaceCountMax endpoints (6 bytes each:
// uint32 IP + uint16 port, little-endian). This is exactly the layout used by
// the C plugin (udpard_udpip_ep_t endpoints[UDPARD_IFACE_COUNT_MAX]).
func encodeEndpoints(src endpoint, ifindex int) (ctx [24]byte) {
	if ifindex < 0 || ifindex >= ifaceCountMax {
		return ctx
	}
	off := ifindex * 6
	binary.LittleEndian.PutUint32(ctx[off:], src.ip)
	binary.LittleEndian.PutUint16(ctx[off+4:], src.port)
	return ctx
}

func decodeEndpoints(ctx [24]byte) [ifaceCountMax]endpoint {
	var eps [ifaceCountMax]endpoint
	for i := 0; i < ifaceCountMax; i++ {
		eps[i].ip = binary.LittleEndian.Uint32(ctx[i*6:])
		eps[i].port = binary.LittleEndian.Uint16(ctx[i*6+4:])
	}
	return eps
}

// ----------------------------------------------------------------------------
// Subject writer / reader
// ----------------------------------------------------------------------------

// subjectWriter implements cy.SubjectWriter for the UDP transport.
type subjectWriter struct {
	subjectID      uint32
	nextTransferID uint64
}

// SubjectID returns the subject-ID for this writer.
func (w *subjectWriter) SubjectID() uint32 { return w.subjectID }

// subjectReader implements cy.SubjectReader for the UDP transport.
type subjectReader struct {
	subjectID uint32
	port      *rxPort
	rxConns   []rxConn // multicast sockets registered for RX
}

// SubjectID returns the subject-ID for this reader.
func (r *subjectReader) SubjectID() uint32 { return r.subjectID }

// Extent returns the maximum message size for this reader.
func (r *subjectReader) Extent() int { return r.port.extent }

// SetExtent sets the maximum message size for this reader.
func (r *subjectReader) SetExtent(extent int) { r.port.setExtent(extent) }

// ----------------------------------------------------------------------------
// RX reassembly / deduplication
// ----------------------------------------------------------------------------

// rxConn is a registered receive socket mapped to an RX port.
type rxConn struct {
	conn    *net.UDPConn
	port    *rxPort
	subject *uint32 // nil for the unicast (return-path) port
	ifindex int
}

// rxSlot holds the in-progress reassembly state for a single transfer.
type rxSlot struct {
	tid      uint64
	total    uint64
	received uint64
	frags    map[uint32][]byte // offset -> payload
}

// rxSession is a per-remote-UID stateful reassembly/dedup context.
type rxSession struct {
	inited  bool
	lastTID uint64
	slot    *rxSlot
}

// rxPort performs reassembly and deduplication for one subscription (subject or
// unicast). Stateless ports (broadcast / gossip shard subjects) use the cheap
// history[2] fingerprint filter; all other ports maintain per-UID sessions with
// transfer-ID ordering and multi-frame reassembly.
type rxPort struct {
	mu       sync.Mutex
	stateful bool
	extent   int
	// Stateless dedup fingerprints (transfer_id ^ sender_uid).
	history [2]uint64
	// Stateful sessions keyed by sender UID.
	sessions map[uint64]*rxSession
}

func newRXPort(stateful bool, extent int) *rxPort {
	return &rxPort{
		stateful: stateful,
		extent:   extent,
		sessions: make(map[uint64]*rxSession),
	}
}

func (p *rxPort) setExtent(extent int) {
	p.mu.Lock()
	p.extent = extent
	p.mu.Unlock()
}

// tidGreater reports whether a is strictly ahead of b in 48-bit space (tolerant
// to wrap-around).
func tidGreater(a, b uint64) bool {
	diff := (a - b) & 0xFFFFFFFFFFFF
	return diff != 0 && diff < (uint64(1)<<47)
}

// accept parses and reassembles a frame, delivering complete transfers via the
// platform's OnMessage callback.
func (p *rxPort) accept(plat *Platform, h udpHeader, payload []byte, src endpoint, ifindex int, subject *uint32) {
	if p.stateful {
		p.acceptStateful(plat, h, payload, src, ifindex, subject)
	} else {
		p.acceptStateless(plat, h, payload, src, ifindex, subject)
	}
}

func (p *rxPort) acceptStateless(plat *Platform, h udpHeader, payload []byte, src endpoint, ifindex int, subject *uint32) {
	p.mu.Lock()
	fp := h.transferID ^ h.senderUID
	if fp == p.history[0] || fp == p.history[1] {
		p.mu.Unlock()
		return // duplicate
	}
	p.history[1] = p.history[0]
	p.history[0] = fp
	required := p.extent
	if int(h.transferSize) < required {
		required = int(h.transferSize)
	}
	// Stateless subscriptions only accept single-frame transfers; the first
	// frame must cover the configured extent prefix.
	full := h.offset == 0 && len(payload) >= required
	if !full {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	plat.deliver(h, payload, src, ifindex, subject)
}

func (p *rxPort) acceptStateful(plat *Platform, h udpHeader, payload []byte, src endpoint, ifindex int, subject *uint32) {
	p.mu.Lock()
	session := p.sessions[h.senderUID]
	if session == nil {
		session = &rxSession{}
		p.sessions[h.senderUID] = session
	}
	if !session.inited {
		session.inited = true
		session.lastTID = h.transferID - 1 // accept the very first transfer
	}
	if session.slot == nil || session.slot.tid != h.transferID {
		if !tidGreater(h.transferID, session.lastTID) {
			p.mu.Unlock()
			return // old or already-delivered transfer
		}
		session.slot = &rxSlot{tid: h.transferID, total: uint64(h.transferSize), frags: make(map[uint32][]byte)}
	}
	slot := session.slot
	if _, exists := slot.frags[h.offset]; !exists {
		slot.frags[h.offset] = append([]byte(nil), payload...)
		slot.received += uint64(len(payload))
	}
	if slot.received >= slot.total {
		out := reassemble(slot.frags, slot.total)
		session.lastTID = h.transferID
		session.slot = nil
		p.mu.Unlock()
		plat.deliver(h, out, src, ifindex, subject)
		return
	}
	p.mu.Unlock()
}

// reassemble concatenates the fragments in offset order. Offsets are guaranteed
// non-overlapping; only fully-reassembled transfers reach this point.
func reassemble(frags map[uint32][]byte, total uint64) []byte {
	out := make([]byte, 0, total)
	offsets := make([]uint32, 0, len(frags))
	for off := range frags {
		offsets = append(offsets, off)
	}
	// Simple insertion sort (tiny lists).
	for i := 1; i < len(offsets); i++ {
		for j := i; j > 0 && offsets[j-1] > offsets[j]; j-- {
			offsets[j-1], offsets[j] = offsets[j], offsets[j-1]
		}
	}
	for _, off := range offsets {
		out = append(out, frags[off]...)
	}
	return out
}

// ----------------------------------------------------------------------------
// Platform
// ----------------------------------------------------------------------------

// Platform implements cy.Platform for the Cyphal/UDP transport. It embeds
// cy.PlatformBase (which provides the OnMessage callback wiring, SetCy, etc.).
type Platform struct {
	cy.PlatformBase

	mu                  sync.RWMutex
	uid                 uint64
	rng                 *mrand.Rand // per-platform PRNG seeded with crypto entropy
	rngMu               sync.Mutex
	closed              bool
	subjectIDModulusVal uint32

	// ifaces is the set of local interface IPv4 addresses. A nil/empty slice
	// means "all interfaces" (a single logical interface with an unspecified
	// local address); otherwise there is one redundant interface per entry.
	ifaces   []net.IP
	localIPs []uint32       // all local IPv4 addresses (for self-receive filtering)
	txSocks  []*net.UDPConn // per-interface unicast socket (TX + unicast RX)
	rxConns  []rxConn       // all registered receive sockets (unicast + multicast)

	readers map[uint32]*subjectReader
	writers map[uint32]*subjectWriter

	unicast    *rxPort // single stateful unicast (return-path) port
	unicastTID atomic.Uint64
	rxRunning  atomic.Int32 // ensures a single RX goroutine per platform
}

// New creates a UDP platform bound to all interfaces with an ephemeral local
// port. A semi-random UID is generated when uid is zero.
// PlatformUDP is the Cyphal/UDP platform interface. It extends cy.Platform with the
// UDP-specific operations (local endpoint, namespace, and live transport stats).
// udp.New, udp.NewWithAddress, and udp.NewManual all return a PlatformUDP, which is
// therefore assignable to cy.Platform wherever only the base transport API is needed.
type PlatformUDP interface {
	cy.Platform

	// Home returns the local endpoint address string for the given prefix
	// (e.g. "" or "udp"). It is UDP-specific.
	Home(prefix string) string

	// Namespace returns the platform's UDP multicast namespace.
	Namespace() string

	// Stats returns the number of live subject writers and readers.
	Stats() (subjectWriterCount int, subjectReaderCount int)
}

func New() (PlatformUDP, error) {
	return NewWithAddress("", 0, 0)
}

// NewWithAddress creates a UDP platform. An empty address listens on all
// interfaces; port 0 selects an ephemeral local port. The UID is generated
// randomly when zero.
func NewWithAddress(address string, port int, uid uint64) (PlatformUDP, error) {
	localIPs, _ := localIPv4Addrs(nil)
	return newPlatform(uid, nil, localIPs, port, address)
}

// NewManual creates a UDP platform with explicit configuration, mirroring
// cy_udp_posix_new_manual. Unused (zero) interface addresses are ignored; an
// empty slice listens on all interfaces. txQueueCapacity is accepted for API
// compatibility (the Go implementation does not require a fixed queue size).
func NewManual(uid uint64, localIfaceAddresses []uint32, txQueueCapacity int) (PlatformUDP, error) {
	var ifaces []net.IP
	for _, a := range localIfaceAddresses {
		if a != 0 {
			ifaces = append(ifaces, u32ToIP(a))
		}
	}
	localIPs, _ := localIPv4Addrs(ifaces)
	return newPlatform(uid, ifaces, localIPs, 0, "")
}

func newPlatform(uid uint64, ifaces []net.IP, localIPs []uint32, port int, address string) (*Platform, error) {
	if len(ifaces) > ifaceCountMax {
		ifaces = ifaces[:ifaceCountMax]
	}
	p := &Platform{
		uid:                 uid,
		ifaces:              ifaces,
		localIPs:            localIPs,
		subjectIDModulusVal: subjectIDModulus23bit,
		readers:             make(map[uint32]*subjectReader),
		writers:             make(map[uint32]*subjectWriter),
		unicast:             newRXPort(true, mtuDefault),
	}
	// Seed the per-platform PRNG with cryptographic entropy so transfer-ID
	// seeds and Random() are distinct across reboots (mirrors the C requirement).
	var seed [16]byte
	if _, err := crand.Read(seed[:]); err != nil {
		return nil, fmt.Errorf("cy/udp: failed to seed PRNG: %w", err)
	}
	p.rng = mrand.New(mrand.NewPCG(binary.LittleEndian.Uint64(seed[0:8]), binary.LittleEndian.Uint64(seed[8:16])))
	if p.uid == 0 {
		var u [8]byte
		if _, err := crand.Read(u[:]); err != nil {
			return nil, fmt.Errorf("cy/udp: failed to generate UID: %w", err)
		}
		p.uid = binary.LittleEndian.Uint64(u[:])
	}

	// One unicast TX/RX socket per interface (or a single "all interfaces"
	// socket when none are specified). These also receive unicast transfers.
	n := len(ifaces)
	if n == 0 {
		n = 1
	}
	localAddr := &net.UDPAddr{Port: port}
	if address != "" {
		localAddr.IP = net.ParseIP(address)
	}
	for i := 0; i < n; i++ {
		la := *localAddr
		if n > 1 {
			la.IP = ifaces[i]
		}
		conn, err := net.ListenUDP("udp", &la)
		if err != nil {
			p.destroySocks()
			return nil, fmt.Errorf("cy/udp: failed to open unicast socket: %w", err)
		}
		p.txSocks = append(p.txSocks, conn)
		p.rxConns = append(p.rxConns, rxConn{conn: conn, port: p.unicast, subject: nil, ifindex: i})
	}
	return p, nil
}

func (p *Platform) destroySocks() {
	for _, c := range p.txSocks {
		if c != nil {
			_ = c.Close()
		}
	}
	for _, rc := range p.rxConns {
		if rc.conn != nil {
			_ = rc.conn.Close()
		}
	}
}

// Destroy releases all platform resources. It is safe to call multiple times.
func (p *Platform) Destroy() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	txSocks := p.txSocks
	rxConns := p.rxConns
	p.txSocks = nil
	p.rxConns = nil
	p.readers = nil
	p.writers = nil
	p.mu.Unlock()

	for _, c := range txSocks {
		if c != nil {
			_ = c.Close()
		}
	}
	for _, rc := range rxConns {
		if rc.conn != nil {
			_ = rc.conn.Close()
		}
	}
}

// ensureRX launches the per-platform background datagram reader if not already
// running. There is exactly one reader goroutine per Platform instance.
func (p *Platform) ensureRX() {
	if p.rxRunning.CompareAndSwap(0, 1) {
		go p.rxLoop()
	}
}

// rxLoop continuously reads datagrams from all registered sockets and feeds them
// to the matching RX port. It runs until the platform is destroyed.
func (p *Platform) rxLoop() {
	buf := make([]byte, maxDatagram)
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			p.rxRunning.Store(0)
			return
		}
		conns := append([]rxConn(nil), p.rxConns...)
		p.mu.Unlock()

		got := false
		for _, rc := range conns {
			if rc.conn == nil {
				continue
			}
			_ = rc.conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
			n, addr, err := rc.conn.ReadFromUDP(buf)
			if err == nil && n > 0 {
				p.handleDatagram(buf[:n], addr, rc)
				got = true
			}
		}
		if !got {
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (p *Platform) handleDatagram(data []byte, srcAddr *net.UDPAddr, rc rxConn) {
	h, payload, ok := deserializeHeader(data)
	if !ok {
		return // malformed / CRC failure: drop (matches C behavior)
	}
	src := endpoint{ip: ipToU32(srcAddr.IP), port: uint16(srcAddr.Port)}
	if p.isSelf(src) {
		return // ignore our own looped-back frames
	}
	rc.port.accept(p, h, payload, src, rc.ifindex, rc.subject)
}

// isSelf reports whether the source endpoint belongs to one of our local TX
// sockets (avoid re-ingesting our own multicast transmissions).
func (p *Platform) isSelf(src endpoint) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.txSocks {
		if c == nil {
			continue
		}
		la, ok := c.LocalAddr().(*net.UDPAddr)
		if ok && la.Port == int(src.port) && containsU32(p.localIPs, ipToU32(la.IP)) {
			return true
		}
	}
	return false
}

// deliver builds a cy arrival and invokes the OnMessage callback wired by cy.New.
func (p *Platform) deliver(h udpHeader, payload []byte, src endpoint, ifindex int, subject *uint32) {
	ts := p.Now()
	lane := cy.Lane{
		ID:       h.senderUID,
		Priority: cy.Priority(h.priority),
		Context:  encodeEndpoints(src, ifindex),
	}
	msg := *cy.NewMessageTS(ts, cy.NewMessage(payload))
	p.OnMessage(lane, subject, msg)
}

// ----------------------------------------------------------------------------
// Platform interface methods
// ----------------------------------------------------------------------------

// Now returns the current time in microseconds.
func (p *Platform) Now() cy.Microsecond {
	return cy.Microsecond(time.Now().UnixMicro())
}

// Random returns a pseudo-random 64-bit value from a per-platform PRNG seeded
// with cryptographic entropy.
func (p *Platform) Random() uint64 {
	p.rngMu.Lock()
	v := p.rng.Uint64()
	p.rngMu.Unlock()
	return v
}

// SubjectIDModulus returns the subject-ID modulus for the IPv4 transport.
func (p *Platform) SubjectIDModulus() uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.subjectIDModulusVal
}

// SetUnicastExtent sets the maximum extent for incoming unicast transfers.
func (p *Platform) SetUnicastExtent(extent int) {
	p.mu.Lock()
	p.UnicastExtent = extent
	p.mu.Unlock()
	if p.unicast != nil {
		p.unicast.setExtent(extent)
	}
}

// Spin runs the event loop until the specified deadline. The background reader
// goroutine performs the actual IO and delivers arrivals via OnMessage; Spin
// blocks here so the caller (cy.SpinUntil) can then drain the scheduler.
func (p *Platform) Spin(deadline cy.Microsecond) error {
	p.ensureRX()
	for {
		now := p.Now()
		if now >= deadline {
			return nil
		}
		rem := deadline - now
		if rem > 1<<40 {
			rem = 1 << 40
		}
		// Sleep up to half the remaining time, then re-check (avoids busy-spin).
		d := time.Duration(rem) * time.Microsecond / 2
		if d > 50*time.Millisecond {
			d = 50 * time.Millisecond
		}
		time.Sleep(d)
		if p.Now() >= deadline {
			return nil
		}
	}
}

// NewSubjectWriter creates (or returns an existing) subject writer.
func (p *Platform) NewSubjectWriter(subjectID uint32) (cy.SubjectWriter, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, cy.ErrArgument
	}
	if w, ok := p.writers[subjectID]; ok {
		return w, nil
	}
	w := &subjectWriter{subjectID: subjectID, nextTransferID: p.Random()}
	p.writers[subjectID] = w
	return w, nil
}

// DestroySubjectWriter releases a subject writer.
func (p *Platform) DestroySubjectWriter(writer cy.SubjectWriter) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if w, ok := writer.(*subjectWriter); ok {
		delete(p.writers, w.subjectID)
	}
}

// SubjectWriterSend publishes data on the subject, fragmenting it into MTU-sized
// Cyphal/UDP datagrams and transmitting them to the subject multicast group on
// all interfaces.
func (p *Platform) SubjectWriterSend(writer cy.SubjectWriter, deadline cy.Microsecond, priority cy.Priority, data []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return cy.ErrArgument
	}
	w, ok := writer.(*subjectWriter)
	if !ok {
		return cy.ErrArgument
	}
	meta := udpHeader{
		priority:     uint8(priority) & 0x07,
		transferID:   atomic.AddUint64(&w.nextTransferID, 1) - 1,
		senderUID:    p.uid,
		transferSize: uint32(len(data)),
	}
	p.sendTransfer(meta, data, subjectEndpoint(w.subjectID), p.txSocks)
	return nil
}

// Unicast sends a unicast transfer to the remote node described by lane.Context,
// which carries the discovered return-path endpoints.
func (p *Platform) Unicast(lane cy.Lane, deadline cy.Microsecond, data []byte) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return cy.ErrArgument
	}
	meta := udpHeader{
		priority:     uint8(lane.Priority) & 0x07,
		transferID:   p.unicastTID.Add(1) - 1,
		senderUID:    p.uid,
		transferSize: uint32(len(data)),
	}
	eps := decodeEndpoints(lane.Context)
	// Send to each valid per-interface endpoint via its dedicated socket.
	for i := 0; i < ifaceCountMax && i < len(p.txSocks); i++ {
		if !eps[i].valid() {
			continue
		}
		addr := &net.UDPAddr{IP: u32ToIP(eps[i].ip), Port: int(eps[i].port)}
		if p.txSocks[i] != nil {
			p.sendTransfer(meta, data, addr, p.txSocks[i:i+1])
		}
	}
	return nil
}

// sendTransfer fragments the payload into MTU-sized frames and writes each
// datagram to dst via the supplied sockets (one per redundant interface).
func (p *Platform) sendTransfer(meta udpHeader, data []byte, dst *net.UDPAddr, socks []*net.UDPConn) {
	mtu := mtuDefault
	n := len(data)
	offset := 0
	for offset < n {
		chunk := n - offset
		if chunk > mtu {
			chunk = mtu
		}
		prefix := crc32c(data[:offset+chunk])
		var buf [headerSizeBytes]byte
		frame := meta
		frame.offset = uint32(offset)
		serializeHeader(buf[:], frame, prefix)
		datagram := append(buf[:], data[offset:offset+chunk]...)
		for _, sock := range socks {
			if sock == nil {
				continue
			}
			_, _ = sock.WriteToUDP(datagram, dst)
		}
		offset += chunk
	}
}

// NewSubjectReader creates (or returns an existing) subject reader and joins the
// subject multicast group on every interface. Subjects above the modulus are
// stateless (broadcast / gossip shard) subscriptions.
func (p *Platform) NewSubjectReader(subjectID uint32, extent int) (cy.SubjectReader, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, cy.ErrArgument
	}
	if r, ok := p.readers[subjectID]; ok {
		r.port.setExtent(extent)
		p.mu.Unlock()
		return r, nil
	}
	stateful := subjectID <= p.subjectIDModulusVal
	r := &subjectReader{subjectID: subjectID, port: newRXPort(stateful, extent)}
	group := subjectEndpoint(subjectID)
	// Join the multicast group on every interface.
	n := len(p.txSocks)
	for i := 0; i < n; i++ {
		var ifi *net.Interface
		if n > 1 {
			if iface, err := ifaceForIP(p.ifaces[i]); err == nil {
				ifi = iface
			}
		}
		conn, err := net.ListenMulticastUDP("udp", ifi, &net.UDPAddr{IP: group.IP, Port: udpPort})
		if err != nil {
			// Best-effort: if the group cannot be joined (e.g. no multicast
			// capable interface), the reader is created but inert.
			continue
		}
		rc := rxConn{conn: conn, port: r.port, subject: &r.subjectID, ifindex: i}
		r.rxConns = append(r.rxConns, rc)
		p.rxConns = append(p.rxConns, rc)
	}
	p.readers[subjectID] = r
	p.mu.Unlock()

	p.ensureRX()
	return r, nil
}

// DestroySubjectReader releases a subject reader and leaves the multicast group.
func (p *Platform) DestroySubjectReader(reader cy.SubjectReader) {
	p.mu.Lock()
	if r, ok := reader.(*subjectReader); ok {
		delete(p.readers, r.subjectID)
		// Remove this reader's multicast sockets from the RX set and close them.
		kept := p.rxConns[:0]
		for _, rc := range p.rxConns {
			if rc.port == r.port {
				if rc.conn != nil {
					_ = rc.conn.Close()
				}
				continue
			}
			kept = append(kept, rc)
		}
		p.rxConns = kept
	}
	p.mu.Unlock()
}

// SetSubjectReaderExtent updates the maximum extent for a subject reader.
func (p *Platform) SetSubjectReaderExtent(reader cy.SubjectReader, extent int) {
	if r, ok := reader.(*subjectReader); ok {
		r.port.setExtent(extent)
	}
}

// Realloc reallocates memory using Go's allocator. It tracks buffer sizes so
// that contents are preserved on growth, matching the documented semantics.
func (p *Platform) Realloc(ptr unsafe.Pointer, size int) unsafe.Pointer {
	if size == 0 {
		if ptr != nil {
			p.freeAlloc(ptr)
		}
		return nil
	}
	if ptr == nil {
		b := make([]byte, size)
		p.recordAlloc(unsafe.Pointer(&b[0]), size)
		return unsafe.Pointer(&b[0])
	}
	old := p.lookupAlloc(ptr)
	b := make([]byte, size)
	if old != nil {
		copy(b, old)
	}
	p.recordAlloc(unsafe.Pointer(&b[0]), size)
	p.freeAlloc(ptr)
	return unsafe.Pointer(&b[0])
}

// ----------------------------------------------------------------------------
// Realloc size tracking
// ----------------------------------------------------------------------------

var (
	allocMu sync.Mutex
	allocs  = map[uintptr]int{}
)

func (p *Platform) recordAlloc(ptr unsafe.Pointer, size int) {
	allocMu.Lock()
	allocs[uintptr(ptr)] = size
	allocMu.Unlock()
}

func (p *Platform) lookupAlloc(ptr unsafe.Pointer) []byte {
	allocMu.Lock()
	defer allocMu.Unlock()
	n, ok := allocs[uintptr(ptr)]
	if !ok {
		return nil
	}
	return (*[1 << 30]byte)(ptr)[:n:n]
}

func (p *Platform) freeAlloc(ptr unsafe.Pointer) {
	allocMu.Lock()
	delete(allocs, uintptr(ptr))
	allocMu.Unlock()
}

// ----------------------------------------------------------------------------
// Helpers / compatibility surface
// ----------------------------------------------------------------------------

func containsU32(s []uint32, v uint32) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// localIPv4Addrs returns the set of local IPv4 addresses to use for self-receive
// filtering. When ifaces is non-empty it is those addresses; otherwise it is all
// host IPv4 addresses.
func localIPv4Addrs(ifaces []net.IP) ([]uint32, error) {
	if len(ifaces) > 0 {
		out := make([]uint32, 0, len(ifaces))
		for _, ip := range ifaces {
			if v := ipToU32(ip); v != 0 {
				out = append(out, v)
			}
		}
		return out, nil
	}
	out := []uint32{}
	ifacesAll, err := net.Interfaces()
	if err != nil {
		return out, err
	}
	for i := range ifacesAll {
		addrs, err := ifacesAll[i].Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var ipn net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ipn = v.IP
			case *net.IPAddr:
				ipn = v.IP
			}
			if v := ipToU32(ipn); v != 0 {
				out = append(out, v)
			}
		}
	}
	return out, nil
}

func ifaceForIP(ip net.IP) (*net.Interface, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for i := range ifaces {
		addrs, err := ifaces[i].Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var ipn net.IP
			switch v := a.(type) {
			case *net.IPNet:
				ipn = v.IP
			case *net.IPAddr:
				ipn = v.IP
			}
			if ipn != nil && ipn.Equal(ip) {
				return &ifaces[i], nil
			}
		}
	}
	return nil, fmt.Errorf("no interface for %v", ip)
}

// Home returns the default home name for this platform: the fixed-length,
// zero-padded lowercase hex UID (optionally prefixed). Mirrors
// cy_udp_posix_home.
func (p *Platform) Home(prefix string) string {
	uidStr := fmt.Sprintf("%016x", p.uid)
	if prefix != "" {
		return prefix + "/" + uidStr
	}
	return uidStr
}

// Namespace returns the default namespace (reads CYPHAL_NAMESPACE). Mirrors
// cy_udp_posix_namespace.
func (p *Platform) Namespace() string {
	return os.Getenv("CYPHAL_NAMESPACE")
}

// Stats returns the current subject writer / reader counts.
func (p *Platform) Stats() (subjectWriterCount int, subjectReaderCount int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.writers), len(p.readers)
}

// Ensure the subject writer/reader types satisfy the cy interfaces.
var _ cy.SubjectWriter = (*subjectWriter)(nil)
var _ cy.SubjectReader = (*subjectReader)(nil)
