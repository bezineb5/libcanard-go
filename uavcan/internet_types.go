package uavcan

// =============================================================================
// uavcan.internet namespace - Internet/LAN forwarding interface
// =============================================================================

// udp namespace types

// HandleIncomingPacketRequest is used to handle incoming UDP packets.
// Fixed Service ID: 500
type InternetUdpHandleIncomingPacketRequest struct {
	// The source endpoint (IP address and port).
	SourceEndpoint InternetUdpEndpoint

	// The destination endpoint (IP address and port).
	DestinationEndpoint InternetUdpEndpoint

	// The UDP payload.
	Payload []uint8
}

// HandleIncomingPacketResponse is the response to a HandleIncomingPacket request.
type InternetUdpHandleIncomingPacketResponse struct {
	// The result of the operation.
	// 0 = Success, non-zero = Error
	Status uint8

	// Reserved for future use.
	_ [7]uint8
}

// OutgoingPacket is used to send outgoing UDP packets.
// Fixed Subject ID: 8174
type InternetUdpOutgoingPacket struct {
	// The source endpoint (IP address and port).
	SourceEndpoint InternetUdpEndpoint

	// The destination endpoint (IP address and port).
	DestinationEndpoint InternetUdpEndpoint

	// The UDP payload.
	Payload []uint8
}

// Endpoint represents a UDP endpoint (IP address and port).
type InternetUdpEndpoint struct {
	// The IP address (4 bytes for IPv4, 16 bytes for IPv6).
	// The first byte indicates the address family:
	// 0 = IPv4, 1 = IPv6
	Address [16]uint8

	// The port number.
	Port uint16

	// Reserved for future use.
	_ [2]uint8
}
