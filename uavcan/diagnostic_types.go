package uavcan

// =============================================================================
// uavcan.diagnostic namespace - Diagnostics and event logging
// =============================================================================

// Severity represents the severity level of a diagnostic message.
type DiagnosticSeverity uint8

const (
	// DiagnosticSeverityDebug indicates a debug message.
	DiagnosticSeverityDebug DiagnosticSeverity = iota
	// DiagnosticSeverityInfo indicates an informational message.
	DiagnosticSeverityInfo
	// DiagnosticSeverityWarning indicates a warning.
	DiagnosticSeverityWarning
	// DiagnosticSeverityError indicates an error.
	DiagnosticSeverityError
	// DiagnosticSeverityCritical indicates a critical error.
	DiagnosticSeverityCritical
)

// Record represents a diagnostic record or event log entry.
// This message is designed to facilitate emission of human-readable diagnostic
// messages and event logging, both for real-time display and for long-term storage.
type DiagnosticRecord struct {
	// The severity of the diagnostic message.
	Severity DiagnosticSeverity

	// The timestamp when the diagnostic event occurred.
	Timestamp SynchronizedTimestamp

	// The source node-ID (0 if the message is published by an anonymous node).
	SourceNodeId uint8

	// The text of the diagnostic message (UTF-8 encoded).
	// Maximum length: 255 bytes.
	TextLength uint8
	Text       [255]uint8
}
