package uavcan

// =============================================================================
// uavcan.si.unit namespace - SI Unit types for physical quantities
// =============================================================================
//
// These types provide enhanced type safety by using explicitly typed alternatives
// instead of raw scalar types. For example, use VelocityScalar instead of float32
// for velocity values.
//
// All quantities use SI units unless otherwise specified in the type name.
// All values are encoded in little-endian format (Cyphal standard).

// ----------------------------------------------------------------------------
// Scalar types (single values)
// ----------------------------------------------------------------------------

// AccelerationScalar represents acceleration in meters per second squared [m/s²].
type AccelerationScalar float32

// AngleScalar represents an angle in radians [rad].
type AngleScalar float32

// AngularAccelerationScalar represents angular acceleration in radians per second squared [rad/s²].
type AngularAccelerationScalar float32

// AngularVelocityScalar represents angular velocity in radians per second [rad/s].
type AngularVelocityScalar float32

// DurationScalar represents a time duration in seconds [s].
type DurationScalar float32

// WideDurationScalar represents a time duration in seconds [s] with higher precision.
type WideDurationScalar float64

// ElectricChargeScalar represents electric charge in coulombs [C].
type ElectricChargeScalar float32

// ElectricCurrentScalar represents electric current in amperes [A].
type ElectricCurrentScalar float32

// EnergyScalar represents energy in joules [J].
type EnergyScalar float32

// ForceScalar represents force in newtons [N].
type ForceScalar float32

// FrequencyScalar represents frequency in hertz [Hz].
type FrequencyScalar float32

// LengthScalar represents length in meters [m].
type LengthScalar float32

// WideLengthScalar represents length in meters [m] with higher precision.
type WideLengthScalar float64

// LuminanceScalar represents luminance in candela per square meter [cd/m²].
type LuminanceScalar float32

// MagneticFieldStrengthScalar represents magnetic field strength in amperes per meter [A/m].
type MagneticFieldStrengthScalar float32

// MagneticFluxDensityScalar represents magnetic flux density in teslas [T].
type MagneticFluxDensityScalar float32

// MassScalar represents mass in kilograms [kg].
type MassScalar float32

// PowerScalar represents power in watts [W].
type PowerScalar float32

// PressureScalar represents pressure in pascals [Pa].
type PressureScalar float32

// TemperatureScalar represents temperature in kelvin [K].
type TemperatureScalar float32

// TorqueScalar represents torque in newton-meters [N·m].
type TorqueScalar float32

// VelocityScalar represents velocity in meters per second [m/s].
type VelocityScalar float32

// VoltageScalar represents voltage in volts [V].
type VoltageScalar float32

// VolumeScalar represents volume in cubic meters [m³].
type VolumeScalar float32

// VolumetricFlowRateScalar represents volumetric flow rate in cubic meters per second [m³/s].
type VolumetricFlowRateScalar float32

// ----------------------------------------------------------------------------
// Vector3 types (3D vectors)
// ----------------------------------------------------------------------------

// AccelerationVector3 represents 3D acceleration in meters per second squared [m/s²].
// Layout: X, Y, Z (each float32, 12 bytes total)
type AccelerationVector3 struct {
	X AccelerationScalar
	Y AccelerationScalar
	Z AccelerationScalar
}

// AngularAccelerationVector3 represents 3D angular acceleration in radians per second squared [rad/s²].
type AngularAccelerationVector3 struct {
	X AngularAccelerationScalar
	Y AngularAccelerationScalar
	Z AngularAccelerationScalar
}

// AngularVelocityVector3 represents 3D angular velocity in radians per second [rad/s].
type AngularVelocityVector3 struct {
	X AngularVelocityScalar
	Y AngularVelocityScalar
	Z AngularVelocityScalar
}

// ForceVector3 represents 3D force in newtons [N].
type ForceVector3 struct {
	X ForceScalar
	Y ForceScalar
	Z ForceScalar
}

// LengthVector3 represents 3D length in meters [m].
type LengthVector3 struct {
	X LengthScalar
	Y LengthScalar
	Z LengthScalar
}

// WideLengthVector3 represents 3D length in meters [m] with higher precision.
type WideLengthVector3 struct {
	X WideLengthScalar
	Y WideLengthScalar
	Z WideLengthScalar
}

// MagneticFieldStrengthVector3 represents 3D magnetic field strength in amperes per meter [A/m].
type MagneticFieldStrengthVector3 struct {
	X MagneticFieldStrengthScalar
	Y MagneticFieldStrengthScalar
	Z MagneticFieldStrengthScalar
}

// MagneticFluxDensityVector3 represents 3D magnetic flux density in teslas [T].
type MagneticFluxDensityVector3 struct {
	X MagneticFluxDensityScalar
	Y MagneticFluxDensityScalar
	Z MagneticFluxDensityScalar
}

// TorqueVector3 represents 3D torque in newton-meters [N·m].
type TorqueVector3 struct {
	X TorqueScalar
	Y TorqueScalar
	Z TorqueScalar
}

// VelocityVector3 represents 3D velocity in meters per second [m/s].
type VelocityVector3 struct {
	X VelocityScalar
	Y VelocityScalar
	Z VelocityScalar
}

// ----------------------------------------------------------------------------
// Quaternion type
// ----------------------------------------------------------------------------

// AngleQuaternion represents a rotation quaternion (W, X, Y, Z).
// The quaternion elements are ordered as: W, X, Y, Z.
// This is a right-handed quaternion representation.
type AngleQuaternion struct {
	W AngleScalar
	X AngleScalar
	Y AngleScalar
	Z AngleScalar
}

// =============================================================================
// uavcan.si.sample namespace - Timestamped SI sample types
// =============================================================================
//
// Each sample type contains a timestamp field of type SynchronizedTimestamp.
// Every emitted message should be timestamped to allow subscribers to identify
// which messages relate to the same event or instant.

// AccelerationScalarSample represents a timestamped scalar acceleration sample.
type AccelerationScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    AccelerationScalar
}

// AccelerationVector3Sample represents a timestamped 3D acceleration sample.
type AccelerationVector3Sample struct {
	Timestamp SynchronizedTimestamp
	Value    AccelerationVector3
}

// AngleScalarSample represents a timestamped scalar angle sample.
type AngleScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    AngleScalar
}

// AngleQuaternionSample represents a timestamped quaternion sample.
type AngleQuaternionSample struct {
	Timestamp SynchronizedTimestamp
	Value    AngleQuaternion
}

// AngularAccelerationScalarSample represents a timestamped scalar angular acceleration sample.
type AngularAccelerationScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    AngularAccelerationScalar
}

// AngularAccelerationVector3Sample represents a timestamped 3D angular acceleration sample.
type AngularAccelerationVector3Sample struct {
	Timestamp SynchronizedTimestamp
	Value    AngularAccelerationVector3
}

// AngularVelocityScalarSample represents a timestamped scalar angular velocity sample.
type AngularVelocityScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    AngularVelocityScalar
}

// AngularVelocityVector3Sample represents a timestamped 3D angular velocity sample.
type AngularVelocityVector3Sample struct {
	Timestamp SynchronizedTimestamp
	Value    AngularVelocityVector3
}

// DurationScalarSample represents a timestamped scalar duration sample.
type DurationScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    DurationScalar
}

// WideDurationScalarSample represents a timestamped wide scalar duration sample.
type WideDurationScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    WideDurationScalar
}

// ElectricChargeScalarSample represents a timestamped scalar electric charge sample.
type ElectricChargeScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    ElectricChargeScalar
}

// ElectricCurrentScalarSample represents a timestamped scalar electric current sample.
type ElectricCurrentScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    ElectricCurrentScalar
}

// EnergyScalarSample represents a timestamped scalar energy sample.
type EnergyScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    EnergyScalar
}

// ForceScalarSample represents a timestamped scalar force sample.
type ForceScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    ForceScalar
}

// ForceVector3Sample represents a timestamped 3D force sample.
type ForceVector3Sample struct {
	Timestamp SynchronizedTimestamp
	Value    ForceVector3
}

// FrequencyScalarSample represents a timestamped scalar frequency sample.
type FrequencyScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    FrequencyScalar
}

// LengthScalarSample represents a timestamped scalar length sample.
type LengthScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    LengthScalar
}

// LengthVector3Sample represents a timestamped 3D length sample.
type LengthVector3Sample struct {
	Timestamp SynchronizedTimestamp
	Value    LengthVector3
}

// WideLengthScalarSample represents a timestamped wide scalar length sample.
type WideLengthScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    WideLengthScalar
}

// WideLengthVector3Sample represents a timestamped 3D wide length sample.
type WideLengthVector3Sample struct {
	Timestamp SynchronizedTimestamp
	Value    WideLengthVector3
}

// LuminanceScalarSample represents a timestamped scalar luminance sample.
type LuminanceScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    LuminanceScalar
}

// MagneticFieldStrengthScalarSample represents a timestamped scalar magnetic field strength sample.
type MagneticFieldStrengthScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    MagneticFieldStrengthScalar
}

// MagneticFieldStrengthVector3Sample represents a timestamped 3D magnetic field strength sample.
type MagneticFieldStrengthVector3Sample struct {
	Timestamp SynchronizedTimestamp
	Value    MagneticFieldStrengthVector3
}

// MagneticFluxDensityScalarSample represents a timestamped scalar magnetic flux density sample.
type MagneticFluxDensityScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    MagneticFluxDensityScalar
}

// MagneticFluxDensityVector3Sample represents a timestamped 3D magnetic flux density sample.
type MagneticFluxDensityVector3Sample struct {
	Timestamp SynchronizedTimestamp
	Value    MagneticFluxDensityVector3
}

// MassScalarSample represents a timestamped scalar mass sample.
type MassScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    MassScalar
}

// PowerScalarSample represents a timestamped scalar power sample.
type PowerScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    PowerScalar
}

// PressureScalarSample represents a timestamped scalar pressure sample.
type PressureScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    PressureScalar
}

// TemperatureScalarSample represents a timestamped scalar temperature sample.
type TemperatureScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    TemperatureScalar
}

// TorqueScalarSample represents a timestamped scalar torque sample.
type TorqueScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    TorqueScalar
}

// TorqueVector3Sample represents a timestamped 3D torque sample.
type TorqueVector3Sample struct {
	Timestamp SynchronizedTimestamp
	Value    TorqueVector3
}

// VelocityScalarSample represents a timestamped scalar velocity sample.
type VelocityScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    VelocityScalar
}

// VelocityVector3Sample represents a timestamped 3D velocity sample.
type VelocityVector3Sample struct {
	Timestamp SynchronizedTimestamp
	Value    VelocityVector3
}

// VoltageScalarSample represents a timestamped scalar voltage sample.
type VoltageScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    VoltageScalar
}

// VolumeScalarSample represents a timestamped scalar volume sample.
type VolumeScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    VolumeScalar
}

// VolumetricFlowRateScalarSample represents a timestamped scalar volumetric flow rate sample.
type VolumetricFlowRateScalarSample struct {
	Timestamp SynchronizedTimestamp
	Value    VolumetricFlowRateScalar
}
