package udp

import (
	"encoding/binary"
	"hash/crc32"
	"net"
	"testing"

	"github.com/opencyphal/cy-go"
)

// buildFrame is an INDEPENDENT implementation of the Cyphal/UDP frame layout,
// following cyphal_udp_header.dsdl and libudpard exactly. It uses the standard
// library's CRC32C (Castagnoli) rather than the package's embedded table, so it
// cross-checks both the table and the parser.
func buildFrame(tid uint64, uid uint64, priority uint8, offset, transferSize uint32, prefixPayload []byte) []byte {
	const version = 2
	buf := make([]byte, headerSizeBytes)
	buf[0] = byte(version) | (priority << 5)
	buf[1] = 0
	binary.LittleEndian.PutUint64(buf[2:], tid&0xFFFFFFFFFFFF) // 48-bit
	binary.LittleEndian.PutUint64(buf[8:], uid)                // 64-bit
	binary.LittleEndian.PutUint32(buf[16:], offset)
	binary.LittleEndian.PutUint32(buf[20:], transferSize)
	cast := crc32.MakeTable(crc32.Castagnoli)
	binary.LittleEndian.PutUint32(buf[24:], crc32.Checksum(prefixPayload, cast))
	binary.LittleEndian.PutUint32(buf[28:], crc32.Checksum(buf[0:headerSizeBytes-crcSizeBytes], cast))
	return buf
}

// capturedDelivery records one OnMessage invocation.
type capturedDelivery struct {
	lane    cy.Lane
	subject *uint32
	msg     cy.MessageTS
}

// newTestPlatform builds a minimal Platform with a capturing OnMessage, without
// opening any sockets (so it never touches the network). Deliveries are recorded
// synchronously in the returned slice (the tests drive RX inline, single-threaded).
func newTestPlatform(t *testing.T) (*Platform, *[]capturedDelivery) {
	t.Helper()
	var captured []capturedDelivery
	p := &Platform{
		PlatformBase: cy.PlatformBase{OnMessage: func(l cy.Lane, s *uint32, m cy.MessageTS) {
			captured = append(captured, capturedDelivery{lane: l, subject: s, msg: m})
		}},
	}
	return p, &captured
}

func TestCRC32CTableMatchesCastagnoli(t *testing.T) {
	cast := crc32.MakeTable(crc32.Castagnoli)
	for i := 0; i < 256; i++ {
		if crcTable[i] != cast[i] {
			t.Fatalf("crcTable[%d] = %#x, want %#x (Castagnoli)", i, crcTable[i], cast[i])
		}
	}
	// Cross-check the running function against the stdlib on random-ish data.
	data := []byte("Cyphal/UDP header checksum cross-check vector")
	if crc32c(data) != crc32.Checksum(data, cast) {
		t.Fatalf("crc32c != Castagnoli for reference vector")
	}
	// The header residue constant must equal Castagnoli over a valid 32-byte header.
	h := buildFrame(1, 2, 3, 0, 4, []byte{0xAA, 0xBB, 0xCC, 0xDD})
	if got := crc32c(h); got != crcResidue {
		t.Fatalf("crc32c(valid header) = %#x, want residue %#x", got, crcResidue)
	}
}

func TestSubjectEndpoint(t *testing.T) {
	const sub = 0x1234
	ep := subjectEndpoint(sub)
	want := u32ToIP(uint32(ipv4McastPrefix) | (sub & ipv4SubjectIDMax))
	if !ep.IP.Equal(want) {
		t.Fatalf("group IP = %v, want %v", ep.IP, want)
	}
	if ep.Port != udpPort {
		t.Fatalf("port = %d, want %d", ep.Port, udpPort)
	}
	if got := uint32(ipv4McastPrefix) | (sub & ipv4SubjectIDMax); got != 0xEF001234 {
		t.Fatalf("group value = %#x, want 0xEF001234", got)
	}
}

func TestSerializeMatchesSpec(t *testing.T) {
	// The package serializer must produce byte-identical output to the spec.
	h := udpHeader{priority: 4, transferID: 0x123456789ABC, senderUID: 0xDEADBEEFCAFEF00D, offset: 0, transferSize: 4}
	payload := []byte{0x01, 0x02, 0x03, 0x04}
	var buf [headerSizeBytes]byte
	serializeHeader(buf[:], h, crc32c(payload))
	want := buildFrame(h.transferID, h.senderUID, h.priority, 0, 4, payload)
	if len(buf) != len(want) {
		t.Fatalf("header length = %d, want %d", len(buf), len(want))
	}
	for i := range buf {
		if buf[i] != want[i] {
			t.Fatalf("byte %d = %#x, want %#x\nfull=% x\nwant=% x", i, buf[i], want[i], buf[:], want)
		}
	}
}

func TestDeserializeMatchesSpec(t *testing.T) {
	payload := []byte{0x10, 0x20, 0x30, 0x40}
	frame := append(buildFrame(0x123456789ABC, 0xCAFEF00D, 5, 0, uint32(len(payload)), payload), payload...)
	got, pl, ok := deserializeHeader(frame)
	if !ok {
		t.Fatal("valid frame rejected")
	}
	if got.priority != 5 || got.transferID != 0x123456789ABC || got.senderUID != 0xCAFEF00D {
		t.Fatalf("decoded header = %+v, want priority=5 tid=0x123456789ABC uid=0xCAFEF00D", got)
	}
	if got.transferSize != uint32(len(payload)) || got.offset != 0 {
		t.Fatalf("decoded offset/size = %d/%d", got.offset, got.transferSize)
	}
	if string(pl) != string(payload) {
		t.Fatalf("payload = % x, want % x", pl, payload)
	}

	// Corrupt the header CRC: deserialize must reject.
	bad := append([]byte(nil), frame...)
	bad[28] ^= 0xFF
	if _, _, ok := deserializeHeader(bad); ok {
		t.Fatal("corrupted header CRC was accepted")
	}
	// Corrupt the body: the first-frame prefix CRC check must reject.
	bad2 := append([]byte(nil), frame...)
	bad2[headerSizeBytes] ^= 0xFF
	if _, _, ok := deserializeHeader(bad2); ok {
		t.Fatal("corrupted first-frame payload CRC was accepted")
	}
}

func TestEndpointCtxRoundTrip(t *testing.T) {
	ep := endpoint{ip: 0x0A000001, port: 1234}
	ctx := encodeEndpoints(ep, 1)
	got := decodeEndpoints(ctx)
	if got[1] != ep {
		t.Fatalf("endpoint at ifindex 1 = %+v, want %+v", got[1], ep)
	}
	if got[0].valid() || got[2].valid() {
		t.Fatal("other iface slots should be zero")
	}
	// Pathological ifindex is ignored.
	if encodeEndpoints(ep, 9) != [24]byte{} {
		t.Fatal("out-of-range ifindex should produce zero ctx")
	}
}

// TestPlatformParsesCFrame feeds a hand-built C-format frame through the
// platform's real RX path (handleDatagram -> deserializeHeader -> reassembly ->
// deliver) and checks the delivered payload, sender UID, priority, and return
// path carried in Lane.Context. Because buildFrame uses the standard-library
// CRC32C and the platform uses its embedded table, this also proves the two
// CRC implementations agree (which is what makes interop with C possible).
func TestPlatformParsesCFrame(t *testing.T) {
	p, captured := newTestPlatform(t)

	subjectID := uint32(0x1234)
	src := endpoint{ip: 0x7F000001, port: 9999} // arbitrary, not a local TX port
	hdr := udpHeader{priority: 2, transferID: 0xABCDEF123456, senderUID: 0x1122334455667788, offset: 0, transferSize: 4}
	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	frame := append(buildFrame(hdr.transferID, hdr.senderUID, hdr.priority, 0, hdr.transferSize, payload), payload...)

	rc := rxConn{port: newRXPort(true, 64), subject: &subjectID, ifindex: 0}
	p.handleDatagram(frame, &net.UDPAddr{IP: u32ToIP(src.ip), Port: int(src.port)}, rc)

	if len(*captured) != 1 {
		t.Fatalf("deliveries = %d, want 1", len(*captured))
	}
	d := (*captured)[0]
	if string(d.msg.Content.Payload()) != string(payload) {
		t.Fatalf("payload = % x, want % x", d.msg.Content.Payload(), payload)
	}
	// Lane must carry the sender UID and a return-path context.
	if d.lane.ID != hdr.senderUID {
		t.Fatalf("lane.ID = %#x, want %#x", d.lane.ID, hdr.senderUID)
	}
	if d.lane.Priority != cy.Priority(hdr.priority) {
		t.Fatalf("lane.Priority = %v, want %v", d.lane.Priority, cy.Priority(hdr.priority))
	}
	eps := decodeEndpoints(d.lane.Context)
	if eps[0] != src {
		t.Fatalf("lane.Context endpoint = %+v, want %+v", eps[0], src)
	}
}

func TestStatefulReassembly(t *testing.T) {
	p, captured := newTestPlatform(t)
	subjectID := uint32(0x100)
	port := newRXPort(true, 256)
	src := endpoint{ip: 0x7F000001, port: 9999}

	// Two fragments of one transfer (tid=7, uid=0x42).
	tid := uint64(7)
	uid := uint64(0x42)
	full := []byte("the quick brown fox jumps over the lazy dog")
	const mtu = 20
	var off uint32
	for off < uint32(len(full)) {
		chunk := full[off:]
		if len(chunk) > mtu {
			chunk = chunk[:mtu]
		}
		hdr := udpHeader{priority: 4, transferID: tid, senderUID: uid, offset: off, transferSize: uint32(len(full))}
		port.accept(p, hdr, chunk, src, 0, &subjectID)
		off += uint32(len(chunk))
	}
	if len(*captured) != 1 {
		t.Fatalf("reassembled deliveries = %d, want 1", len(*captured))
	}
	if string((*captured)[0].msg.Content.Payload()) != string(full) {
		t.Fatalf("reassembled payload mismatch:\n got %q\nwant %q", (*captured)[0].msg.Content.Payload(), full)
	}

	// Redelivering the same transfer-ID must be dropped (dedup).
	hdr := udpHeader{priority: 4, transferID: tid, senderUID: uid, offset: 0, transferSize: uint32(len(full))}
	port.accept(p, hdr, full[:mtu], src, 0, &subjectID)
	if len(*captured) != 1 {
		t.Fatalf("after dedup deliveries = %d, want 1", len(*captured))
	}
}

func TestStatelessDedup(t *testing.T) {
	p, captured := newTestPlatform(t)
	subjectID := uint32(0x900000) // above the modulus -> stateless
	port := newRXPort(false, 64)
	src := endpoint{ip: 0x7F000001, port: 9999}

	// Stateless ports accept single-frame transfers only; duplicates dropped.
	fpPayload := []byte{0x01, 0x02}
	hdr := udpHeader{priority: 4, transferID: 100, senderUID: 1, offset: 0, transferSize: uint32(len(fpPayload))}
	port.accept(p, hdr, fpPayload, src, 0, &subjectID)
	port.accept(p, hdr, fpPayload, src, 0, &subjectID) // duplicate fingerprint
	if len(*captured) != 1 {
		t.Fatalf("stateless deliveries = %d, want 1 (duplicate dropped)", len(*captured))
	}

	// A different transfer-ID is delivered.
	hdr2 := udpHeader{priority: 4, transferID: 101, senderUID: 1, offset: 0, transferSize: uint32(len(fpPayload))}
	port.accept(p, hdr2, fpPayload, src, 0, &subjectID)
	if len(*captured) != 2 {
		t.Fatalf("stateless deliveries = %d, want 2", len(*captured))
	}

	// A multi-frame (offset != 0) transfer is rejected by the stateless port.
	hdr3 := udpHeader{priority: 4, transferID: 102, senderUID: 1, offset: 5, transferSize: 10}
	port.accept(p, hdr3, []byte{0x03, 0x04, 0x05}, src, 0, &subjectID)
	if len(*captured) != 2 {
		t.Fatalf("stateless deliveries = %d, want 2 (multi-frame rejected)", len(*captured))
	}
}
