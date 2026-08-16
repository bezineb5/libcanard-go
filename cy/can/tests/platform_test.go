package tests

import (
	"testing"

	"github.com/opencyphal/cy-go"
	"github.com/opencyphal/cy-go/can"
)

// asCAN resolves the concrete CAN platform from the cy.Platform returned by can.New.
func asCAN(p cy.Platform) *can.Platform { return p.(*can.Platform) }

// TestNew tests the New function with various inputs.
func TestNew(t *testing.T) {
	tests := []struct {
		name       string
		ifaceName  string
		txQueueCap int
		filterCap  int
		prngSeed   uint64
		wantErr    bool
	}{
		{
			name:       "empty interface name",
			ifaceName:  "",
			txQueueCap: 1000,
			filterCap:  4,
			prngSeed:   0,
			wantErr:    true,
		},
		{
			name:       "valid parameters",
			ifaceName:  "vcan0", // Use virtual CAN for testing
			txQueueCap: 1000,
			filterCap:  4,
			prngSeed:   0xC0DEC0DEC0DEC0DE,
			wantErr:    false, // Will fail if vcan0 doesn't exist, but that's OK
		},
		{
			name:       "non-existent interface",
			ifaceName:  "nonexistent",
			txQueueCap: 1000,
			filterCap:  4,
			prngSeed:   0,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			platform, err := can.New(tt.ifaceName, tt.txQueueCap, tt.filterCap, tt.prngSeed)
			
			if tt.wantErr {
				if err == nil {
					// If we expected an error but didn't get one, check if it's because
					// the interface doesn't exist (which is also an error)
					if platform != nil {
						asCAN(platform).Destroy()
						t.Error("Expected error, got nil")
					}
				}
				return
			}
			
			if err != nil {
				// Some tests may fail if CAN interface doesn't exist
				// That's acceptable for this test
				t.Logf("Skipping test (CAN interface not available): %v", err)
				return
			}
			
			if platform == nil {
				t.Error("Expected platform, got nil")
				return
			}
			
			// Clean up
			asCAN(platform).Destroy()
		})
	}
}

// TestNewSubjectWriter tests creating subject writers.
func TestNewSubjectWriter(t *testing.T) {
	// Skip if CAN interface not available
	platform, err := can.New("vcan0", 1000, 4, 0)
	if err != nil {
		t.Skip("CAN interface not available, skipping test")
		return
	}
	defer asCAN(platform).Destroy()

	// Create a subject writer
	writer, err := platform.NewSubjectWriter(123)
	if err != nil {
		t.Errorf("Failed to create subject writer: %v", err)
		return
	}

	if writer == nil {
		t.Error("Expected writer, got nil")
		return
	}

	if writer.SubjectID() != 123 {
		t.Errorf("Expected SubjectID 123, got %d", writer.SubjectID())
	}
}

// TestDestroySubjectWriter tests destroying subject writers.
func TestDestroySubjectWriter(t *testing.T) {
	// Skip if CAN interface not available
	platform, err := can.New("vcan0", 1000, 4, 0)
	if err != nil {
		t.Skip("CAN interface not available, skipping test")
		return
	}
	defer asCAN(platform).Destroy()

	// Create and destroy a subject writer
	writer, err := platform.NewSubjectWriter(123)
	if err != nil {
		t.Fatalf("Failed to create subject writer: %v", err)
	}

	// Destroy it
	platform.DestroySubjectWriter(writer)

	// Should not panic or error
}

// TestNewSubjectReader tests creating subject readers.
func TestNewSubjectReader(t *testing.T) {
	// Skip if CAN interface not available
	platform, err := can.New("vcan0", 1000, 4, 0)
	if err != nil {
		t.Skip("CAN interface not available, skipping test")
		return
	}
	defer asCAN(platform).Destroy()

	// Create a subject reader
	reader, err := platform.NewSubjectReader(123, 256)
	if err != nil {
		t.Errorf("Failed to create subject reader: %v", err)
		return
	}

	if reader == nil {
		t.Error("Expected reader, got nil")
		return
	}

	if reader.SubjectID() != 123 {
		t.Errorf("Expected SubjectID 123, got %d", reader.SubjectID())
	}

	if reader.Extent() != 256 {
		t.Errorf("Expected Extent 256, got %d", reader.Extent())
	}
}

// TestSetSubjectReaderExtent tests setting reader extent.
func TestSetSubjectReaderExtent(t *testing.T) {
	// Skip if CAN interface not available
	platform, err := can.New("vcan0", 1000, 4, 0)
	if err != nil {
		t.Skip("CAN interface not available, skipping test")
		return
	}
	defer asCAN(platform).Destroy()

	// Create a subject reader
	reader, err := platform.NewSubjectReader(123, 256)
	if err != nil {
		t.Fatalf("Failed to create subject reader: %v", err)
	}

	// Set new extent
	platform.SetSubjectReaderExtent(reader, 512)

	if reader.Extent() != 512 {
		t.Errorf("Expected Extent 512, got %d", reader.Extent())
	}
}

// TestNow tests the Now function.
func TestNow(t *testing.T) {
	// Skip if CAN interface not available
	platform, err := can.New("vcan0", 1000, 4, 0)
	if err != nil {
		t.Skip("CAN interface not available, skipping test")
		return
	}
	defer asCAN(platform).Destroy()

	// Get current time
	now := platform.Now()
	
	// Should be a positive value (microseconds since epoch)
	if now <= 0 {
		t.Errorf("Expected positive time, got %d", now)
	}
}

// TestRandom tests the Random function.
func TestRandom(t *testing.T) {
	// Skip if CAN interface not available
	platform, err := can.New("vcan0", 1000, 4, 0xDEADBEEF)
	if err != nil {
		t.Skip("CAN interface not available, skipping test")
		return
	}
	defer asCAN(platform).Destroy()

	// Get random value
	rand1 := platform.Random()
	rand2 := platform.Random()

	// Should be non-zero (with seed)
	if rand1 == 0 && rand2 == 0 {
		t.Error("Expected non-zero random values")
	}
}

// TestRealloc tests the Realloc function.
func TestRealloc(t *testing.T) {
	// Skip if CAN interface not available
	platform, err := can.New("vcan0", 1000, 4, 0)
	if err != nil {
		t.Skip("CAN interface not available, skipping test")
		return
	}
	defer asCAN(platform).Destroy()

	// Allocate some memory
	ptr1 := platform.Realloc(nil, 100)
	if ptr1 == nil {
		t.Error("Expected non-nil pointer")
		return
	}

	// Reallocate to larger size
	ptr2 := platform.Realloc(ptr1, 200)
	if ptr2 == nil {
		t.Error("Expected non-nil pointer after realloc")
	}

	// Realloc with zero size should return nil
	ptr3 := platform.Realloc(ptr2, 0)
	if ptr3 != nil {
		t.Error("Expected nil for zero size")
	}
}

// TestDestroy tests the Destroy function.
func TestDestroy(t *testing.T) {
	// Skip if CAN interface not available
	platform, err := can.New("vcan0", 1000, 4, 0)
	if err != nil {
		t.Skip("CAN interface not available, skipping test")
		return
	}

	// Destroy should not panic
	asCAN(platform).Destroy()
	
	// Destroy again should not panic
	asCAN(platform).Destroy()
}

// TestMultipleWriters tests creating multiple writers.
func TestMultipleWriters(t *testing.T) {
	// Skip if CAN interface not available
	platform, err := can.New("vcan0", 1000, 4, 0)
	if err != nil {
		t.Skip("CAN interface not available, skipping test")
		return
	}
	defer asCAN(platform).Destroy()

	// Create multiple writers
	writers := make([]interface{}, 10)
	for i := 0; i < 10; i++ {
		writer, err := platform.NewSubjectWriter(uint32(i))
		if err != nil {
			t.Fatalf("Failed to create writer %d: %v", i, err)
		}
		writers[i] = writer
	}

	// All should be created successfully
	if len(writers) != 10 {
		t.Errorf("Expected 10 writers, got %d", len(writers))
	}
}

// TestMultipleReaders tests creating multiple readers.
func TestMultipleReaders(t *testing.T) {
	// Skip if CAN interface not available
	platform, err := can.New("vcan0", 1000, 4, 0)
	if err != nil {
		t.Skip("CAN interface not available, skipping test")
		return
	}
	defer asCAN(platform).Destroy()

	// Create multiple readers
	readers := make([]interface{}, 10)
	for i := 0; i < 10; i++ {
		reader, err := platform.NewSubjectReader(uint32(i), 256)
		if err != nil {
			t.Fatalf("Failed to create reader %d: %v", i, err)
		}
		readers[i] = reader
	}

	// All should be created successfully
	if len(readers) != 10 {
		t.Errorf("Expected 10 readers, got %d", len(readers))
	}
}

// TestSpin tests the Spin function.
func TestSpin(t *testing.T) {
	// Skip if CAN interface not available
	platform, err := can.New("vcan0", 1000, 4, 0)
	if err != nil {
		t.Skip("CAN interface not available, skipping test")
		return
	}
	defer asCAN(platform).Destroy()

	// Spin should not panic or block indefinitely
	err = platform.Spin(platform.Now() + 1000) // Spin for 1ms
	if err != nil {
		t.Errorf("Spin returned error: %v", err)
	}
}

// TestSetCy tests setting the Cy instance.
func TestSetCy(t *testing.T) {
	// Skip if CAN interface not available
	platform, err := can.New("vcan0", 1000, 4, 0)
	if err != nil {
		t.Skip("CAN interface not available, skipping test")
		return
	}
	defer asCAN(platform).Destroy()

	// SetCy should not panic with nil
	platform.SetCy(nil)
}
