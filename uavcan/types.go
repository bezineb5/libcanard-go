package uavcan

// =============================================================================
// uavcan.node namespace - Node management types
// =============================================================================

// Health represents the abstract health status of a node.
// Part of uavcan.node.Heartbeat message.
type Health uint8

const (
	// HealthNominal indicates that the node is functioning correctly.
	HealthNominal Health = iota
	// HealthAdvisory indicates a non-critical issue that may affect performance.
	HealthAdvisory
	// HealthCaution indicates a potentially degrading situation.
	HealthCaution
	// HealthWarning indicates a serious issue that requires attention.
	HealthWarning
	// HealthError indicates a critical failure.
	HealthError
)

// Mode represents the abstract operating mode of a node.
// Part of uavcan.node.Heartbeat message.
type Mode uint8

const (
	// ModeUninitialized indicates the node is starting up.
	ModeUninitialized Mode = iota
	// ModeInitialization indicates the node is initializing.
	ModeInitialization
	// ModeMaintenance indicates the node is in maintenance mode.
	ModeMaintenance
	// ModeSoftwareUpdate indicates the node is performing a software update.
	ModeSoftwareUpdate
	// ModeOffline indicates the node is intentionally offline.
	ModeOffline
	// ModeStandby indicates the node is ready but not active.
	ModeStandby
	// ModeActive indicates the node is operating normally.
	ModeActive
	// ModeFault indicates the node has encountered a fault.
	ModeFault
)

// Version represents a version number with major and minor components.
// Used in various node information messages.
type Version struct {
	Major uint8
	Minor uint8
}

// Heartbeat is the mandatory node heartbeat message.
// Fixed Subject ID: 7509
// All Cyphal nodes that have a node-ID are required to publish this message
// periodically to its fixed subject.
type Heartbeat struct {
	// The uptime seconds counter should never overflow. The counter will reach
	// the upper limit in ~136 years, upon which time it should stay at 0xFFFFFFFF
	// until the node is restarted.
	Uptime uint32

	// The abstract health status of this node.
	Health Health

	// The abstract operating mode of the publishing node.
	// This field indicates the general level of readiness.
	Mode Mode

	// Optional, vendor-specific node status code, e.g. a fault code or a status bitmask.
	VendorSpecificStatusCode uint8
}

// GetInfoRequest is the request type for the GetInfo service.
// Fixed Service ID: 430
type GetInfoRequest struct{}

// NodeStatus represents additional node status information.
type NodeStatus uint8

const (
	NodeStatusVendorSpecific NodeStatus = iota
	NodeStatusRestartRequired
)

// GetInfoResponse is the response type for the GetInfo service.
// Contains comprehensive information about a node.
type GetInfoResponse struct {
	// The Cyphal protocol version implemented on this node, both major and minor.
	// Not to be changed while the node is running.
	ProtocolVersion Version

	// The hardware version. The correct hardware version shall be reported at all times,
	// excepting software-only nodes, in which case it should be set to zeros.
	HardwareVersion Version

	// The software version. Shall not be changed while the node is running.
	SoftwareVersion Version

	// A version control system (VCS) revision number or hash.
	// For example, this field can be used for reporting the short git commit hash.
	// Set to zero if not used.
	SoftwareVcsRevisionId uint64

	// The unique-ID (UID) is a 128-bit long sequence that is likely to be globally unique per node.
	// The vendor shall ensure that the probability of a collision with any other node UID globally
	// is negligibly low. UID is defined once per hardware unit and should never be changed.
	// All zeros is not a valid UID.
	UniqueId [16]uint8

	// Human-readable non-empty ASCII node name.
	// Allowed characters are: a-z (lowercase ASCII letters) 0-9 (decimal digits) . (dot) - (dash) _ (underscore).
	// Node name is a reversed Internet domain name, e.g. "com.manufacturer.project.product".
	// Maximum length: 50 bytes
	NameLength uint8
	Name       [50]uint8 // Variable-length, but max 50 bytes

	// The value of an arbitrary hash function applied to the software image.
	// This field can be used to detect whether the software or firmware running on the node
	// is an exact same version as a certain specific revision.
	// Optional field - may be empty.
	SoftwareImageCrcLength uint8
	SoftwareImageCrc      [8]uint8 // Optional CRC-64

	// The certificate of authenticity (COA) of the node, 222 bytes max, optional.
	// This field can be used for reporting digital signatures.
	CertificateOfAuthenticityLength uint8
	CertificateOfAuthenticity      [222]uint8 // Variable-length, but max 222 bytes
}

// ExecuteCommandRequest is the request type for the ExecuteCommand service.
// Fixed Service ID: 431
type ExecuteCommandRequest struct {
	// The command to execute. Standard commands are defined below.
	// Values 0-127 are reserved for standard commands.
	// Values 128-255 are available for vendor-specific commands.
	Command uint8

	// Optional parameter for the command. Interpretation is command-specific.
	Parameter uint8
}

// Standard command codes for ExecuteCommand
// These are uint8 values used in the Command field of ExecuteCommandRequest.
const (
	CommandBeginSoftwareUpdate uint8 = iota
	CommandJumpToBootloader
	CommandReconnect
	CommandFactoryReset
	CommandStoreConfiguration
	CommandResetConfiguration
	CommandRestart
	CommandPowerOff
)

// ExecuteCommandResponse is the response type for the ExecuteCommand service.
type ExecuteCommandResponse struct {
	// Status code indicating the result of the command execution.
	// 0 = Success, non-zero = Error (vendor-specific)
	Status uint8
}

// =============================================================================
// uavcan.node.port namespace - Port monitoring types
// =============================================================================

// SubjectID represents a subject identifier.
type SubjectID uint16

// ServiceID represents a service identifier.
type ServiceID uint16

// PortID represents a generic port identifier (either subject or service).
type PortID struct {
	IsService bool
	ID        uint16
}

// =============================================================================
// uavcan.time namespace - Time synchronization types
// =============================================================================

// SynchronizedTimestamp represents a timestamp that is synchronized across the network.
// This is used as a field type in many sample messages.
// The timestamp is in microseconds since the Unix epoch (1970-01-01 00:00:00 UTC).
type SynchronizedTimestamp uint64

// Synchronization is used for network-wide time synchronization.
// Fixed Subject ID: 7168
type Synchronization struct {
	// Timestamp of the synchronization message at the master (in microseconds).
	Timestamp SynchronizedTimestamp

	// Estimated error of the master's clock (in microseconds).
	// This is the standard deviation of the clock error.
	MasterClockErrorUs uint32
}

// GetSynchronizationMasterInfoRequest is the request for time master info.
type GetSynchronizationMasterInfoRequest struct{}

// TimeSystem represents the time system in use.
type TimeSystem uint8

const (
	TimeSystemTAI TimeSystem = iota // International Atomic Time
	TimeSystemUTC                    // Coordinated Universal Time
	TimeSystemUT1                    // Universal Time 1
	TimeSystemGPS                   // GPS Time
	TimeSystemLocal                 // Local time (not recommended for distributed systems)
)

// TAIInfo contains information about TAI (International Atomic Time).
type TAIInfo struct {
	// Number of leap seconds between TAI and UTC.
	LeapSeconds uint8
}

// GetSynchronizationMasterInfoResponse contains information about the time master.
type GetSynchronizationMasterInfoResponse struct {
	// The time system in use.
	TimeSystem TimeSystem

	// TAI-specific information (if TimeSystem is TAI).
	TaiInfo TAIInfo

	// The node ID of the current time synchronization master.
	MasterNodeId uint8

	// The unique ID of the current time synchronization master.
	MasterUniqueId [16]uint8

	// Reserved for future use.
	_ [7]uint8
}

// =============================================================================
// uavcan.primitive namespace - Primitive types
// =============================================================================

// Empty represents an empty value (0 bytes).
type Empty struct{}

// String represents a dynamically sized UTF-8 string.
// For use with the dsdl package, use fixed-size byte arrays for known sizes.
type String struct {
	Value []byte
}

// Unstructured represents a raw block of bytes with no defined structure.
type Unstructured struct {
	Value []byte
}

// =============================================================================
// uavcan.primitive.scalar namespace - Scalar primitive types
// =============================================================================

// Bit represents a single bit value.
type Bit bool

// Integer8 represents a signed 8-bit integer.
type Integer8 int8

// Integer16 represents a signed 16-bit integer.
type Integer16 int16

// Integer32 represents a signed 32-bit integer.
type Integer32 int32

// Integer64 represents a signed 64-bit integer.
type Integer64 int64

// Natural8 represents an unsigned 8-bit integer.
type Natural8 uint8

// Natural16 represents an unsigned 16-bit integer.
type Natural16 uint16

// Natural32 represents an unsigned 32-bit integer.
type Natural32 uint32

// Natural64 represents an unsigned 64-bit integer.
type Natural64 uint64

// Real32 represents a 32-bit floating point number (IEEE 754 binary32).
type Real32 float32

// Real64 represents a 64-bit floating point number (IEEE 754 binary64).
type Real64 float64

// =============================================================================
// uavcan.primitive.array namespace - Array primitive types
// =============================================================================

// MaxArraySize is the maximum size for primitive arrays (255 bytes serialized).
const MaxArraySize = 255

// BitArray represents a variable-size array of bits.
type BitArray []Bit

// Integer8Array represents a variable-size array of signed 8-bit integers.
type Integer8Array []Integer8

// Integer16Array represents a variable-size array of signed 16-bit integers.
type Integer16Array []Integer16

// Integer32Array represents a variable-size array of signed 32-bit integers.
type Integer32Array []Integer32

// Integer64Array represents a variable-size array of signed 64-bit integers.
type Integer64Array []Integer64

// Natural8Array represents a variable-size array of unsigned 8-bit integers.
type Natural8Array []Natural8

// Natural16Array represents a variable-size array of unsigned 16-bit integers.
type Natural16Array []Natural16

// Natural32Array represents a variable-size array of unsigned 32-bit integers.
type Natural32Array []Natural32

// Natural64Array represents a variable-size array of unsigned 64-bit integers.
type Natural64Array []Natural64

// Real32Array represents a variable-size array of 32-bit floating point numbers.
type Real32Array []Real32

// Real64Array represents a variable-size array of 64-bit floating point numbers.
type Real64Array []Real64
