package uavcan

// =============================================================================
// uavcan.file namespace - Remote file system interface
// =============================================================================

// Error represents an error code for file operations.
type FileError uint8

const (
	// ErrorSuccess indicates the operation completed successfully.
	FileErrorSuccess FileError = iota
	// ErrorInternal indicates an internal error.
	FileErrorInternal
	// ErrorInvalidArgument indicates an invalid argument was provided.
	FileErrorInvalidArgument
	// ErrorInvalidPath indicates the path is invalid.
	FileErrorInvalidPath
	// ErrorPathTooLong indicates the path is too long.
	FileErrorPathTooLong
	// ErrorNotFound indicates the file or directory was not found.
	FileErrorNotFound
	// ErrorAlreadyExists indicates a file or directory already exists.
	FileErrorAlreadyExists
	// ErrorNotADirectory indicates a directory was expected but a file was found.
	FileErrorNotADirectory
	// ErrorNotAFile indicates a file was expected but a directory was found.
	FileErrorNotAFile
	// ErrorDirectoryNotEmpty indicates a directory is not empty.
	FileErrorDirectoryNotEmpty
	// ErrorAccessDenied indicates access was denied.
	FileErrorAccessDenied
	// ErrorStorageFull indicates the storage is full.
	FileErrorStorageFull
	// ErrorUnsupported indicates the operation is not supported.
	FileErrorUnsupported
	// ErrorTimeout indicates the operation timed out.
	FileErrorTimeout
	// ErrorIO indicates an I/O error occurred.
	FileErrorIO
)

// Path represents a file system path.
// Maximum length: 100 bytes for the path string.
type Path struct {
	Length uint8
	Value  [100]uint8 // UTF-8 encoded path string
}

// GetInfoRequest is the request for file/directory information.
// Fixed Service ID: 405
type FileGetInfoRequest struct {
	Path Path
}

// FileType represents the type of a file system entry.
type FileType uint8

const (
	// FileTypeFile indicates a regular file.
	FileTypeFile FileType = iota
	// FileTypeDirectory indicates a directory.
	FileTypeDirectory
	// FileTypeSpecial indicates a special file (device, pipe, etc.).
	FileTypeSpecial
)

// GetInfoResponse contains information about a file or directory.
type FileGetInfoResponse struct {
	// The type of the file system entry.
	Type FileType

	// The size of the file in bytes (for directories, this may be 0 or implementation-defined).
	Size uint64

	// The modification time in microseconds since Unix epoch.
	ModificationTime SynchronizedTimestamp

	// Reserved for future use.
	_ [4]uint8
}

// ListRequest is the request to list directory contents.
// Fixed Service ID: 406
type FileListRequest struct {
	Path Path
}

// ListResponse contains a list of directory entries.
type FileListResponse struct {
	// The path that was listed.
	Path Path

	// The number of entries in the directory.
	EntryCount uint8

	// Reserved for future use.
	_ [3]uint8

	// The list of entries (variable-length).
	// Each entry consists of: FileType (1 byte) + Path (variable length)
	Entries []uint8
}

// ReadRequest is the request to read from a file.
// Fixed Service ID: 408
type FileReadRequest struct {
	Path    Path
	Offset  uint64 // Byte offset to read from
	MaxSize uint32 // Maximum number of bytes to read
}

// ReadResponse contains data read from a file.
type FileReadResponse struct {
	// The path that was read from.
	Path Path

	// The offset in the file where the read started.
	Offset uint64

	// The number of bytes read.
	Size uint32

	// The data read from the file.
	// Maximum size is transport-dependent but guaranteed to be at least 255 bytes.
	Data []uint8
}

// WriteRequest is the request to write to a file.
// Fixed Service ID: 409
type FileWriteRequest struct {
	Path    Path
	Offset  uint64 // Byte offset to write to
	Data    []uint8 // Data to write
}

// WriteResponse contains the result of a write operation.
type FileWriteResponse struct {
	// The path that was written to.
	Path Path

	// The offset in the file where the write started.
	Offset uint64

	// The number of bytes written.
	Size uint32
}

// ModifyRequest is the request to modify file metadata or perform special operations.
// Fixed Service ID: 407
type FileModifyRequest struct {
	Path Path

	// The operation to perform.
	Operation FileModifyOperation

	// Operation-specific parameters.
	Parameter []uint8
}

// FileModifyOperation represents the operation to perform in a Modify request.
type FileModifyOperation uint8

const (
	// FileModifyOperationTruncate truncates the file to the specified size.
	FileModifyOperationTruncate FileModifyOperation = iota
	// FileModifyOperationCreate creates a new file or directory.
	FileModifyOperationCreate
	// FileModifyOperationDelete deletes a file or directory.
	FileModifyOperationDelete
	// FileModifyOperationRename renames a file or directory.
	FileModifyOperationRename
	// FileModifyOperationMove moves a file or directory.
	FileModifyOperationMove
)

// ModifyResponse contains the result of a modify operation.
type FileModifyResponse struct {
	// The path that was modified.
	Path Path

	// The result of the operation.
	Result FileError
}
