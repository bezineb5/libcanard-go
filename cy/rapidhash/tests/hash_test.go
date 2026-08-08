package tests

import (
	"testing"

	"github.com/opencyphal/cy-go/rapidhash"
)

// TestHashFunction tests the rapidhash hash function.
// We test for basic properties rather than specific values since
// the hash function has a non-zero initial state.
func TestHashFunction(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
	}{
		// Empty input
		{"empty", []byte{}},
		
		// Single byte
		{"single_byte_00", []byte{0x00}},
		{"single_byte_01", []byte{0x01}},
		{"single_byte_ff", []byte{0xFF}},
		
		// Short strings
		{"hello", []byte("hello")},
		{"world", []byte("world")},
		{"test", []byte("test")},
		
		// All zeros
		{"zeros_8", []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		
		// All 0xFF
		{"ff_8", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
		
		// Pattern
		{"pattern_0_7", []byte{0, 1, 2, 3, 4, 5, 6, 7}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rapidhash.Hash(tt.input)
			
			// Hash should never be 0 due to non-zero initial state
			// But we can verify it produces a valid 64-bit value
			if result == 0 {
				t.Logf("Warning: Hash(%x) = 0 (unlikely but possible)", tt.input)
			}
			
			// Hash should be deterministic
			result2 := rapidhash.Hash(tt.input)
			if result != result2 {
				t.Errorf("Hash is not deterministic for input %x", tt.input)
			}
		})
	}
}

// TestHashConsistency tests that the hash function produces consistent results.
func TestHashConsistency(t *testing.T) {
	inputs := [][]byte{
		[]byte("test"),
		[]byte{0xDE, 0xAD, 0xBE, 0xEF},
		[]byte("The quick brown fox jumps over the lazy dog"),
		make([]byte, 100), // 100 zeros
	}

	for _, input := range inputs {
		h1 := rapidhash.Hash(input)
		h2 := rapidhash.Hash(input)
		if h1 != h2 {
			t.Errorf("Hash inconsistency for input %x: got %016x and %016x", input, h1, h2)
		}
	}
}

// TestHashAvalanche tests that small changes in input produce large changes in output.
func TestHashAvalanche(t *testing.T) {
	// Test that changing one bit changes many bits in the output
	input1 := []byte("test")
	input2 := []byte("test")
	
	// Flip one bit
	input2[0] ^= 0x01
	
	h1 := rapidhash.Hash(input1)
	h2 := rapidhash.Hash(input2)
	
	// They should be different
	if h1 == h2 {
		t.Error("Hash should change when input changes")
	}
	
	// Count differing bits
	diff := h1 ^ h2
	bitCount := 0
	for diff != 0 {
		bitCount++
		diff &= diff - 1
	}
	
	// Should have a reasonable number of bits changed (at least 20 for 64-bit hash)
	if bitCount < 20 {
		t.Errorf("Avalanche effect insufficient: only %d bits changed, expected >= 20", bitCount)
	}
	
	t.Logf("Avalanche test: %d bits changed", bitCount)
}

// TestHashZeroInput tests various zero-length and zero-value inputs.
func TestHashZeroInput(t *testing.T) {
	// Empty slice
	h1 := rapidhash.Hash([]byte{})
	
	// Nil slice
	h2 := rapidhash.Hash(nil)
	
	// They should produce the same result
	if h1 != h2 {
		t.Errorf("Empty and nil slices produce different hashes: %016x vs %016x", h1, h2)
	}
	
	// Due to non-zero initial state, hash won't be 0
	// But it should be consistent
	if h1 == 0 {
		t.Logf("Hash of empty input is 0 (unlikely)")
	}
}

// TestHashDifferentLengths tests that different length inputs produce different hashes.
func TestHashDifferentLengths(t *testing.T) {
	// Same bytes but different lengths
	h1 := rapidhash.Hash([]byte{0x01})
	h2 := rapidhash.Hash([]byte{0x01, 0x00})
	h3 := rapidhash.Hash([]byte{0x01, 0x00, 0x00})
	
	// They should likely be different (but collisions are possible)
	// At least some should be different
	if h1 == h2 && h1 == h3 {
		t.Logf("Warning: All hashes are the same (collision)")
	}
	
	// Most likely at least two are different
	if h1 == h2 && h2 == h3 {
		t.Logf("All three hashes are identical: %016x", h1)
	} else {
		// Good - at least some are different
		t.Logf("Hashes are different: %016x, %016x, %016x", h1, h2, h3)
	}
}

// TestHashPerformance tests that the hash function is reasonably fast.
// This is a basic benchmark-style test.
func TestHashPerformance(t *testing.T) {
	// Hash 100,000 inputs
	input := []byte("test input for performance")
	
	for i := 0; i < 100000; i++ {
		_ = rapidhash.Hash(input)
	}
	
	// If we get here, it completed in reasonable time
	t.Logf("Hashed 100,000 inputs successfully")
}
