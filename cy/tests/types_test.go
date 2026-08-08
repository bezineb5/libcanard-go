package tests

import (
	"testing"

	"github.com/opencyphal/cy-go"
)

// TestPriority tests the Priority type.
func TestPriority(t *testing.T) {
	// Test priority ordering
	priorities := []cy.Priority{
		cy.PriorityExceptional,
		cy.PriorityImmediate,
		cy.PriorityFast,
		cy.PriorityHigh,
		cy.PriorityNominal,
		cy.PriorityLow,
		cy.PrioritySlow,
		cy.PriorityOptional,
	}
	
	// Verify ordering
	for i := 0; i < len(priorities)-1; i++ {
		if priorities[i] >= priorities[i+1] {
			t.Errorf("Priority ordering incorrect: %d >= %d", priorities[i], priorities[i+1])
		}
	}
	
	// Test that values match expected
	if cy.PriorityExceptional != 0 {
		t.Errorf("PriorityExceptional should be 0, got %d", cy.PriorityExceptional)
	}
	if cy.PriorityOptional != 7 {
		t.Errorf("PriorityOptional should be 7, got %d", cy.PriorityOptional)
	}
}

// TestMicrosecond tests the Microsecond type.
func TestMicrosecond(t *testing.T) {
	var m cy.Microsecond
	
	// Test zero value
	if m != 0 {
		t.Errorf("Zero Microsecond should be 0, got %d", m)
	}
	
	// Test positive value
	m = cy.Microsecond(1234567)
	if m != 1234567 {
		t.Errorf("Expected 1234567, got %d", m)
	}
	
	// Test negative value (for time differences)
	m = cy.Microsecond(-1000)
	if m != -1000 {
		t.Errorf("Expected -1000, got %d", m)
	}
	
	// Test arithmetic
	m1 := cy.Microsecond(1000)
	m2 := cy.Microsecond(2000)
	if m1+m2 != 3000 {
		t.Errorf("Addition failed: %d + %d != 3000", m1, m2)
	}
	
	if m2-m1 != 1000 {
		t.Errorf("Subtraction failed: %d - %d != 1000", m2, m1)
	}
}

// TestSubjectIDModulus tests the subject ID modulus constants.
func TestSubjectIDModulus(t *testing.T) {
	// Just verify the constants are non-zero
	if cy.SubjectIDModulus16bit == 0 {
		t.Error("SubjectIDModulus16bit should not be zero")
	}
	
	if cy.SubjectIDModulus32bit == 0 {
		t.Error("SubjectIDModulus32bit should not be zero")
	}
}

// TestLane tests the Lane type.
func TestLane(t *testing.T) {
	// Test zero value
	var lane cy.Lane
	if lane.ID != 0 {
		t.Errorf("Zero Lane ID should be 0, got %d", lane.ID)
	}
	// Priority is a uint8, so zero value is 0, not Nominal
	// This is expected behavior
	
	// Test with values
	lane = cy.Lane{
		ID:       42,
		Priority: cy.PriorityHigh,
		Context:  [24]byte{0xDE, 0xAD, 0xBE, 0xEF},
	}
	
	if lane.ID != 42 {
		t.Errorf("Expected ID 42, got %d", lane.ID)
	}
	if lane.Priority != cy.PriorityHigh {
		t.Errorf("Expected Priority High, got %d", lane.Priority)
	}
	// Context is a [24]byte array, so len is always 24
	if len(lane.Context) != 24 {
		t.Errorf("Expected Context length 24, got %d", len(lane.Context))
	}
}

// TestError tests the Error type.
func TestError(t *testing.T) {
	// Test OK
	if cy.OK != 0 {
		t.Errorf("cy.OK should be 0, got %d", cy.OK)
	}
	
	// Test error values - these are the actual error codes in the cy package
	errors := []cy.Error{
		cy.ErrArgument,
		cy.ErrMemory,
		cy.ErrCapacity,
		cy.ErrName,
		cy.ErrMedia,
		cy.ErrLag,
		cy.ErrDelivery,
		cy.ErrLiveness,
		cy.ErrNACK,
	}
	
	for i, err := range errors {
		if err == 0 {
			t.Errorf("Error %d should not be zero", i)
		}
	}
	
	// Test error messages
	if cy.ErrArgument.Error() == "" {
		t.Error("ErrArgument should have an error message")
	}
}

// TestUserContext tests the UserContext type.
func TestUserContext(t *testing.T) {
	var ctx cy.UserContext
	
	// Test zero value
	if len(ctx.Ptr) != cy.UserContextPtrCount {
		t.Errorf("UserContext.Ptr should have length %d, got %d", 
			cy.UserContextPtrCount, len(ctx.Ptr))
	}
	
	// Test with a value
	ctx.Ptr[0] = "test value"
	if ctx.Ptr[0] != "test value" {
		t.Error("UserContext.Ptr[0] should be settable")
	}
}

// TestTimeConversions tests time conversion utilities.
func TestTimeConversions(t *testing.T) {
	// Test Microsecond to int64 conversion
	ms := cy.Microsecond(1000000) // 1 second
	
	// Should be able to convert to int64
	i := int64(ms)
	if i != 1000000 {
		t.Errorf("Expected 1000000, got %d", i)
	}
	
	// Test from int64
	ms2 := cy.Microsecond(i)
	if ms2 != ms {
		t.Errorf("Expected %d, got %d", ms, ms2)
	}
}
