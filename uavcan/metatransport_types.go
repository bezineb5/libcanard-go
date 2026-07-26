package uavcan

// =============================================================================
// uavcan.metatransport namespace - Meta-transport tunneling types
// =============================================================================

// can namespace types

// Manifestation represents the manifestation of a CAN frame.
type CanManifestation uint8

const (
	// CanManifestationClassic indicates a classic CAN frame.
	CanManifestationClassic CanManifestation = iota
	// CanManifestationFD indicates a CAN FD frame.
	CanManifestationFD
)

// Frame represents a CAN frame for tunneling.
// Fixed Subject ID: 71
type CanFrame struct {
	// The manifestation (classic or FD).
	Manifestation CanManifestation

	// The arbitration ID (11-bit for classic, 29-bit for extended).
	ArbitrationId uint32

	// True if the frame is extended (29-bit ID).
	IsExtended bool

	// True if the frame is a remote transmission request (RTR).
	IsRtr bool

	// True if the frame has error state indicator (ESI).
	IsEsi bool

	// The data length code (DLC).
	Dlc uint8

	// The data bytes.
	// Maximum: 8 bytes for classic CAN, 64 bytes for CAN FD.
	Data []uint8

	// Timestamp in microseconds (optional).
	Timestamp SynchronizedTimestamp
}

// BaseArbitrationID represents a base arbitration ID without flags.
type CanBaseArbitrationID uint32

// ArbitrationID represents a full arbitration ID with flags.
type CanArbitrationID struct {
	Value      uint32
	IsExtended bool
}

// ExtendedArbitrationID represents an extended arbitration ID.
type CanExtendedArbitrationID uint32

// RTR represents a remote transmission request frame.
type CanRTR struct {
	ArbitrationId uint32
	IsExtended    bool
}

// DataClassic represents a classic CAN data frame.
type CanDataClassic struct {
	ArbitrationId uint32
	IsExtended    bool
	Dlc           uint8
	Data          [8]uint8
}

// DataFD represents a CAN FD data frame.
type CanDataFD struct {
	ArbitrationId uint32
	IsExtended    bool
	Dlc           uint8
	Data          [64]uint8
}

// Error represents a CAN error frame.
type CanError struct {
	// Error code (vendor-specific).
	Code uint8

	// Reserved for future use.
	_ [7]uint8
}

// ethernet namespace types

// EtherType represents the Ethernet frame type.
type EthernetEtherType uint16

// Frame represents an Ethernet frame for tunneling.
type EthernetFrame struct {
	// The EtherType.
	EtherType EthernetEtherType

	// The source MAC address.
	SourceMac [6]uint8

	// The destination MAC address.
	DestinationMac [6]uint8

	// The payload.
	Payload []uint8
}

// serial namespace types

// Fragment represents a fragment of serial data for tunneling.
// Fixed Subject ID: 2050
type SerialFragment struct {
	// The sequence number of this fragment.
	SequenceNumber uint8

	// The total number of fragments in this message.
	TotalFragments uint8

	// The index of this fragment (0-based).
	FragmentIndex uint8

	// Reserved for future use.
	_ [1]uint8

	// The data bytes.
	Data []uint8
}

// udp namespace types

// Endpoint represents a UDP endpoint for tunneling.
type MetatransportUdpEndpoint struct {
	// The IP address (4 bytes for IPv4, 16 bytes for IPv6).
	Address [16]uint8

	// The port number.
	Port uint16

	// Reserved for future use.
	_ [2]uint8
}

// Frame represents a UDP frame for tunneling.
// Fixed Subject ID: 10244
type MetatransportUdpFrame struct {
	// The source endpoint.
	SourceEndpoint MetatransportUdpEndpoint

	// The destination endpoint.
	DestinationEndpoint MetatransportUdpEndpoint

	// The payload.
	Payload []uint8
}
