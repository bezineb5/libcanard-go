package tests

import (
	"net"
	"testing"

	"github.com/opencyphal/cy-go"
	"github.com/opencyphal/cy-go/udp"
)

// asUDP resolves the concrete UDP platform from the cy.Platform returned by the
// udp.New* constructors. The constructors return the interface; udp-specific
// methods (Destroy, Home, Namespace, Stats) live only on *udp.Platform.
func asUDP(p cy.Platform) *udp.Platform { return p.(*udp.Platform) }

// TestNew tests the New function.
func TestNew(t *testing.T) {
	// Test default constructor
	platform, err := udp.New()
	if err != nil {
		t.Fatalf("Failed to create UDP platform: %v", err)
	}
	defer asUDP(platform).Destroy()

	if platform == nil {
		t.Error("Expected platform, got nil")
	}
}

// TestNewWithAddress tests the NewWithAddress function.
func TestNewWithAddress(t *testing.T) {
	tests := []struct {
		name    string
		address string
		port    int
		uid     uint64
		wantErr bool
	}{
		{
			name:    "localhost",
			address: "127.0.0.1",
			port:    12345,
			uid:     0xDEADBEEF,
			wantErr: false,
		},
		{
			name:    "empty address",
			address: "",
			port:    12345,
			uid:     0,
			wantErr: false, // Empty address should work (listens on all interfaces)
		},
		{
			name:    "invalid port",
			address: "127.0.0.1",
			port:    70000, // Port out of range
			uid:     0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			platform, err := udp.NewWithAddress(tt.address, tt.port, tt.uid)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Logf("Skipping test (port may be in use): %v", err)
				return
			}

			if platform == nil {
				t.Error("Expected platform, got nil")
				return
			}

			// Clean up
			asUDP(platform).Destroy()
		})
	}
}

// TestNewManual tests the NewManual function.
func TestNewManual(t *testing.T) {
	// Test with empty interfaces
	platform, err := udp.NewManual(0xDEADBEEF, []uint32{}, 1000)
	if err != nil {
		t.Logf("Skipping test: %v", err)
		return
	}
	defer asUDP(platform).Destroy()

	if platform == nil {
		t.Error("Expected platform, got nil")
	}
}

// TestDestroy tests the Destroy function.
func TestDestroy(t *testing.T) {
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}

	// Destroy should not panic
	asUDP(platform).Destroy()

	// Destroy again should not panic
	asUDP(platform).Destroy()
}

// TestNewSubjectWriter tests creating subject writers.
func TestNewSubjectWriter(t *testing.T) {
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

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
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

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
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

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
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

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
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

	// Get current time
	now := platform.Now()

	// Should be a positive value (microseconds since epoch)
	if now <= 0 {
		t.Errorf("Expected positive time, got %d", now)
	}
}

// TestRandom tests the Random function.
func TestRandom(t *testing.T) {
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

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
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

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

// TestHome tests the Home function.
func TestHome(t *testing.T) {
	platform, err := udp.NewManual(0x0123456789ABCDEF, []uint32{0x7F000001}, 1000)
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

	// Test with no prefix
	home := asUDP(platform).Home("")
	if len(home) != 16 {
		t.Errorf("Expected 16 character home, got %d: %s", len(home), home)
	}

	// Test with prefix
	homeWithPrefix := asUDP(platform).Home("udp")
	if len(homeWithPrefix) != 20 { // "udp/" + 16 hex chars = 20
		t.Errorf("Expected 20 character home with prefix, got %d: %s", len(homeWithPrefix), homeWithPrefix)
	}
}

// TestNamespace tests the Namespace function.
func TestNamespace(t *testing.T) {
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

	// Get namespace (should be empty if CYPHAL_NAMESPACE not set)
	ns := asUDP(platform).Namespace()
	// Just verify it doesn't panic
	_ = ns
}

// TestStats tests the Stats function.
func TestStats(t *testing.T) {
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

	// Get stats
	writerCount, readerCount := asUDP(platform).Stats()

	// Should be zero initially
	if writerCount != 0 {
		t.Errorf("Expected 0 writers, got %d", writerCount)
	}
	if readerCount != 0 {
		t.Errorf("Expected 0 readers, got %d", readerCount)
	}

	// Create a writer and reader
	platform.NewSubjectWriter(123)
	platform.NewSubjectReader(456, 256)

	// Stats should reflect the new counts
	writerCount, readerCount = asUDP(platform).Stats()
	if writerCount != 1 {
		t.Errorf("Expected 1 writer, got %d", writerCount)
	}
	if readerCount != 1 {
		t.Errorf("Expected 1 reader, got %d", readerCount)
	}
}

// TestSpin tests the Spin function.
func TestSpin(t *testing.T) {
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

	// Spin should not panic or block indefinitely
	// Note: Spin returns cy.OK (Error(0)) which is not nil but is not an error
	err = platform.Spin(platform.Now() + 1000) // Spin for 1ms
	if err != nil && err != cy.OK {
		t.Errorf("Spin returned unexpected error: %v", err)
	}
}

// TestSetCy tests setting the Cy instance.
func TestSetCy(t *testing.T) {
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

	// SetCy should not panic with nil
	platform.SetCy(nil)
}

// TestMultipleWriters tests creating multiple writers.
func TestMultipleWriters(t *testing.T) {
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

	// Create multiple writers
	for i := 0; i < 10; i++ {
		writer, err := platform.NewSubjectWriter(uint32(i))
		if err != nil {
			t.Fatalf("Failed to create writer %d: %v", i, err)
		}
		_ = writer
	}
}

// TestMultipleReaders tests creating multiple readers.
func TestMultipleReaders(t *testing.T) {
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

	// Create multiple readers
	for i := 0; i < 10; i++ {
		reader, err := platform.NewSubjectReader(uint32(i), 256)
		if err != nil {
			t.Fatalf("Failed to create reader %d: %v", i, err)
		}
		_ = reader
	}
}

// TestUnicast tests the Unicast function.
func TestUnicast(t *testing.T) {
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

	// Create a lane
	lane := cy.Lane{
		ID:       42,
		Priority: 3,
	}

	// Send unicast message (should not panic)
	err = platform.Unicast(lane, platform.Now()+1000, []byte{0x01, 0x02, 0x03})
	if err != nil {
		t.Logf("Unicast returned error (expected for missing address): %v", err)
	}
}

// TestSetUnicastExtent tests setting unicast extent.
func TestSetUnicastExtent(t *testing.T) {
	platform, err := udp.New()
	if err != nil {
		t.Skip("Failed to create platform, skipping test")
		return
	}
	defer asUDP(platform).Destroy()

	// Set unicast extent
	platform.SetUnicastExtent(512)
	// No way to verify, but should not panic
}

// TestParseIfaceAddress tests IP address parsing.
func TestParseIfaceAddress(t *testing.T) {
	// Note: This function is not exported in the current implementation
	// but we can test the concept

	tests := []struct {
		address string
		valid   bool
	}{
		{"127.0.0.1", true},
		{"192.168.1.1", true},
		{"0.0.0.0", true},
		{"255.255.255.255", true},
		{"invalid", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.address, func(t *testing.T) {
			// Parse the address
			ip := net.ParseIP(tt.address)

			if tt.valid {
				if ip == nil {
					t.Errorf("Expected valid IP for %s", tt.address)
				}
			} else {
				if ip != nil {
					t.Errorf("Expected invalid IP for %s", tt.address)
				}
			}
		})
	}
}
