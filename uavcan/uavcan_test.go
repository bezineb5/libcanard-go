package uavcan

import (
	"testing"

	"github.com/bezineb5/libcanard-go/dsdl"
)

func TestHeartbeatMarshalUnmarshal(t *testing.T) {
	hb := Heartbeat{
		Uptime:                 12345678,
		Health:                 HealthNominal,
		Mode:                   ModeActive,
		VendorSpecificStatusCode: 0,
	}

	// Marshal
	data, err := dsdl.MarshalV2(hb)
	if err != nil {
		t.Fatalf("Failed to marshal Heartbeat: %v", err)
	}

	// Expected size: 4 (uptime) + 1 (health) + 1 (mode) + 1 (status) = 7 bytes
	// Note: No padding in V2 - fields are packed sequentially
	if len(data) != 7 {
		t.Errorf("Expected 7 bytes, got %d", len(data))
	}

	// Unmarshal
	var hb2 Heartbeat
	err = dsdl.UnmarshalV2(data, &hb2)
	if err != nil {
		t.Fatalf("Failed to unmarshal Heartbeat: %v", err)
	}

	// Verify
	if hb2.Uptime != hb.Uptime {
		t.Errorf("Uptime mismatch: expected %d, got %d", hb.Uptime, hb2.Uptime)
	}
	if hb2.Health != hb.Health {
		t.Errorf("Health mismatch: expected %d, got %d", hb.Health, hb2.Health)
	}
	if hb2.Mode != hb.Mode {
		t.Errorf("Mode mismatch: expected %d, got %d", hb.Mode, hb2.Mode)
	}
	if hb2.VendorSpecificStatusCode != hb.VendorSpecificStatusCode {
		t.Errorf("VendorSpecificStatusCode mismatch: expected %d, got %d",
			hb.VendorSpecificStatusCode, hb2.VendorSpecificStatusCode)
	}
}

func TestVersionMarshalUnmarshal(t *testing.T) {
	v := Version{
		Major: 1,
		Minor: 2,
	}

	data, err := dsdl.MarshalV2(v)
	if err != nil {
		t.Fatalf("Failed to marshal Version: %v", err)
	}

	// Expected size: 1 + 1 = 2 bytes
	if len(data) != 2 {
		t.Errorf("Expected 2 bytes, got %d", len(data))
	}

	var v2 Version
	err = dsdl.UnmarshalV2(data, &v2)
	if err != nil {
		t.Fatalf("Failed to unmarshal Version: %v", err)
	}

	if v2.Major != v.Major {
		t.Errorf("Major mismatch: expected %d, got %d", v.Major, v2.Major)
	}
	if v2.Minor != v.Minor {
		t.Errorf("Minor mismatch: expected %d, got %d", v.Minor, v2.Minor)
	}
}

func TestSynchronizationMarshalUnmarshal(t *testing.T) {
	sync := Synchronization{
		Timestamp:          1234567890123456,
		MasterClockErrorUs: 100,
	}

	data, err := dsdl.MarshalV2(sync)
	if err != nil {
		t.Fatalf("Failed to marshal Synchronization: %v", err)
	}

	// Expected size: 8 (timestamp) + 4 (error) = 12 bytes
	if len(data) != 12 {
		t.Errorf("Expected 12 bytes, got %d", len(data))
	}

	var sync2 Synchronization
	err = dsdl.UnmarshalV2(data, &sync2)
	if err != nil {
		t.Fatalf("Failed to unmarshal Synchronization: %v", err)
	}

	if sync2.Timestamp != sync.Timestamp {
		t.Errorf("Timestamp mismatch: expected %d, got %d", sync.Timestamp, sync2.Timestamp)
	}
	if sync2.MasterClockErrorUs != sync.MasterClockErrorUs {
		t.Errorf("MasterClockErrorUs mismatch: expected %d, got %d",
			sync.MasterClockErrorUs, sync2.MasterClockErrorUs)
	}
}

func TestVelocityVector3MarshalUnmarshal(t *testing.T) {
	vv := VelocityVector3{
		X: 1.5,
		Y: 2.0,
		Z: 0.5,
	}

	data, err := dsdl.MarshalV2(vv)
	if err != nil {
		t.Fatalf("Failed to marshal VelocityVector3: %v", err)
	}

	// Expected size: 4 + 4 + 4 = 12 bytes
	if len(data) != 12 {
		t.Errorf("Expected 12 bytes, got %d", len(data))
	}

	var vv2 VelocityVector3
	err = dsdl.UnmarshalV2(data, &vv2)
	if err != nil {
		t.Fatalf("Failed to unmarshal VelocityVector3: %v", err)
	}

	if vv2.X != vv.X {
		t.Errorf("X mismatch: expected %f, got %f", vv.X, vv2.X)
	}
	if vv2.Y != vv.Y {
		t.Errorf("Y mismatch: expected %f, got %f", vv.Y, vv2.Y)
	}
	if vv2.Z != vv.Z {
		t.Errorf("Z mismatch: expected %f, got %f", vv.Z, vv2.Z)
	}
}

func TestAngleQuaternionMarshalUnmarshal(t *testing.T) {
	q := AngleQuaternion{
		W: 0.7071,
		X: 0.0,
		Y: 0.7071,
		Z: 0.0,
	}

	data, err := dsdl.MarshalV2(q)
	if err != nil {
		t.Fatalf("Failed to marshal AngleQuaternion: %v", err)
	}

	// Expected size: 4 * 4 = 16 bytes
	if len(data) != 16 {
		t.Errorf("Expected 16 bytes, got %d", len(data))
	}

	var q2 AngleQuaternion
	err = dsdl.UnmarshalV2(data, &q2)
	if err != nil {
		t.Fatalf("Failed to unmarshal AngleQuaternion: %v", err)
	}

	if q2.W != q.W {
		t.Errorf("W mismatch: expected %f, got %f", q.W, q2.W)
	}
	if q2.X != q.X {
		t.Errorf("X mismatch: expected %f, got %f", q.X, q2.X)
	}
	if q2.Y != q.Y {
		t.Errorf("Y mismatch: expected %f, got %f", q.Y, q2.Y)
	}
	if q2.Z != q.Z {
		t.Errorf("Z mismatch: expected %f, got %f", q.Z, q2.Z)
	}
}

func TestExecuteCommandRequestMarshalUnmarshal(t *testing.T) {
	req := ExecuteCommandRequest{
		Command:   CommandRestart,
		Parameter: 0,
	}

	data, err := dsdl.MarshalV2(req)
	if err != nil {
		t.Fatalf("Failed to marshal ExecuteCommandRequest: %v", err)
	}

	// Expected size: 1 + 1 = 2 bytes
	if len(data) != 2 {
		t.Errorf("Expected 2 bytes, got %d", len(data))
	}

	var req2 ExecuteCommandRequest
	err = dsdl.UnmarshalV2(data, &req2)
	if err != nil {
		t.Fatalf("Failed to unmarshal ExecuteCommandRequest: %v", err)
	}

	if req2.Command != req.Command {
		t.Errorf("Command mismatch: expected %d, got %d", req.Command, req2.Command)
	}
	if req2.Parameter != req.Parameter {
		t.Errorf("Parameter mismatch: expected %d, got %d", req.Parameter, req2.Parameter)
	}
}

func TestPathMarshalUnmarshal(t *testing.T) {
	path := Path{
		Length: 5,
	}
	// Set the first 5 bytes of Value
	copy(path.Value[:5], []byte{'/', 'h', 'e', 'l', 'l'})

	data, err := dsdl.MarshalV2(path)
	if err != nil {
		t.Fatalf("Failed to marshal Path: %v", err)
	}

	// Expected size: 1 (length) + 100 (value) = 101 bytes
	if len(data) != 101 {
		t.Errorf("Expected 101 bytes, got %d", len(data))
	}

	var path2 Path
	err = dsdl.UnmarshalV2(data, &path2)
	if err != nil {
		t.Fatalf("Failed to unmarshal Path: %v", err)
	}

	if path2.Length != path.Length {
		t.Errorf("Length mismatch: expected %d, got %d", path.Length, path2.Length)
	}
}

func TestRegisterNameMarshalUnmarshal(t *testing.T) {
	name := RegisterName{
		Length: 9,
	}
	// Set the first 9 bytes of Value
	copy(name.Value[:9], []byte{'t', 'e', 's', 't', '_', 'n', 'a', 'm', 'e'})

	data, err := dsdl.MarshalV2(name)
	if err != nil {
		t.Fatalf("Failed to marshal RegisterName: %v", err)
	}

	// Expected size: 1 (length) + 255 (value) = 256 bytes
	if len(data) != 256 {
		t.Errorf("Expected 256 bytes, got %d", len(data))
	}

	var name2 RegisterName
	err = dsdl.UnmarshalV2(data, &name2)
	if err != nil {
		t.Fatalf("Failed to unmarshal RegisterName: %v", err)
	}

	if name2.Length != name.Length {
		t.Errorf("Length mismatch: expected %d, got %d", name.Length, name2.Length)
	}
}

func TestNodeIDAllocationDataMarshalUnmarshal(t *testing.T) {
	dataMsg := NodeIDAllocationData{
		AllocatedNodeId: 42,
		UniqueId:        [16]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	}

	data, err := dsdl.MarshalV2(dataMsg)
	if err != nil {
		t.Fatalf("Failed to marshal NodeIDAllocationData: %v", err)
	}

	// Expected size: 1 (node ID) + 16 (unique ID) = 17 bytes
	// Note: The padding field was removed, so no extra bytes
	if len(data) != 17 {
		t.Errorf("Expected 17 bytes, got %d", len(data))
	}

	var dataMsg2 NodeIDAllocationData
	err = dsdl.UnmarshalV2(data, &dataMsg2)
	if err != nil {
		t.Fatalf("Failed to unmarshal NodeIDAllocationData: %v", err)
	}

	if dataMsg2.AllocatedNodeId != dataMsg.AllocatedNodeId {
		t.Errorf("AllocatedNodeId mismatch: expected %d, got %d",
			dataMsg.AllocatedNodeId, dataMsg2.AllocatedNodeId)
	}
	for i := range dataMsg.UniqueId {
		if dataMsg2.UniqueId[i] != dataMsg.UniqueId[i] {
			t.Errorf("UniqueId[%d] mismatch: expected %d, got %d",
				i, dataMsg.UniqueId[i], dataMsg2.UniqueId[i])
		}
	}
}

func TestDiagnosticRecordMarshalUnmarshal(t *testing.T) {
	record := DiagnosticRecord{
		Severity:    DiagnosticSeverityWarning,
		Timestamp:   1234567890123456,
		SourceNodeId: 42,
		TextLength:  11,
	}
	// Set the first 11 bytes of Text
	copy(record.Text[:11], []byte{'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd'})

	data, err := dsdl.MarshalV2(record)
	if err != nil {
		t.Fatalf("Failed to marshal DiagnosticRecord: %v", err)
	}

	// Expected size: 1 (severity) + 8 (timestamp) + 1 (node ID) + 1 (text length) + 255 (text) = 266 bytes
	if len(data) != 266 {
		t.Errorf("Expected 266 bytes, got %d", len(data))
	}

	var record2 DiagnosticRecord
	err = dsdl.UnmarshalV2(data, &record2)
	if err != nil {
		t.Fatalf("Failed to unmarshal DiagnosticRecord: %v", err)
	}

	if record2.Severity != record.Severity {
		t.Errorf("Severity mismatch: expected %d, got %d", record.Severity, record2.Severity)
	}
	if record2.Timestamp != record.Timestamp {
		t.Errorf("Timestamp mismatch: expected %d, got %d", record.Timestamp, record2.Timestamp)
	}
	if record2.SourceNodeId != record.SourceNodeId {
		t.Errorf("SourceNodeId mismatch: expected %d, got %d",
			record.SourceNodeId, record2.SourceNodeId)
	}
	if record2.TextLength != record.TextLength {
		t.Errorf("TextLength mismatch: expected %d, got %d",
			record.TextLength, record2.TextLength)
	}
}

// Test that the standard types are correctly defined
func TestStandardTypesExist(t *testing.T) {
	// This test just verifies that all the standard types compile
	// and can be instantiated

	// Node types
	_ = Heartbeat{}
	_ = Version{}
	_ = GetInfoRequest{}
	_ = ExecuteCommandRequest{}
	_ = ExecuteCommandResponse{}

	// Time types
	_ = SynchronizedTimestamp(0)
	_ = Synchronization{}

	// SI unit scalar types
	_ = AccelerationScalar(0)
	_ = AngleScalar(0)
	_ = VelocityScalar(0)
	_ = TemperatureScalar(0)

	// SI unit vector types
	_ = VelocityVector3{}
	_ = AccelerationVector3{}
	_ = AngleQuaternion{}

	// SI unit sample types
	_ = VelocityScalarSample{}
	_ = VelocityVector3Sample{}

	// Primitive types
	_ = Empty{}
	_ = Integer8(0)
	_ = Integer16(0)
	_ = Natural8(0)
	_ = Real32(0)

	// File types
	_ = Path{}
	_ = FileError(0)

	// Register types
	_ = RegisterName{}
	_ = RegisterValue{}

	// PnP types
	_ = NodeIDAllocationData{}

	// Diagnostic types
	_ = DiagnosticRecord{}
}

// Test that constants are defined correctly
func TestConstants(t *testing.T) {
	// Health constants
	if HealthNominal != 0 {
		t.Errorf("HealthNominal should be 0, got %d", HealthNominal)
	}
	if HealthError != 4 {
		t.Errorf("HealthError should be 4, got %d", HealthError)
	}

	// Mode constants
	if ModeUninitialized != 0 {
		t.Errorf("ModeUninitialized should be 0, got %d", ModeUninitialized)
	}
	if ModeActive != 6 {
		t.Errorf("ModeActive should be 6, got %d", ModeActive)
	}

	// Command constants
	if CommandBeginSoftwareUpdate != 0 {
		t.Errorf("CommandBeginSoftwareUpdate should be 0, got %d", CommandBeginSoftwareUpdate)
	}
	if CommandPowerOff != 7 {
		t.Errorf("CommandPowerOff should be 7, got %d", CommandPowerOff)
	}
}
