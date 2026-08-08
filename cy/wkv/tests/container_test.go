package tests

import (
	"testing"

	"github.com/opencyphal/cy-go/wkv"
)

// TestContainerInsertAndGet tests basic insertion and retrieval.
func TestContainerInsertAndGet(t *testing.T) {
	c := wkv.New()
	
	// Insert a simple key-value pair
	key := "test.key"
	value := "test.value"
	
	c.Set(key, value)
	
	// Retrieve the value
	retrieved, ok := c.Get(key)
	if !ok {
		t.Error("Failed to retrieve inserted value")
		return
	}
	
	if retrieved != value {
		t.Errorf("Expected %s, got %v", value, retrieved)
	}
}

// TestContainerInsertPattern tests pattern matching insertion.
func TestContainerInsertPattern(t *testing.T) {
	c := wkv.New()
	
	// Insert with wildcard pattern
	pattern := "sensor.*.temperature"
	value := "sensor_pattern"
	
	c.Set(pattern, value)
	
	// Route should match specific keys
	matchingKeys := []string{
		"sensor.front.temperature",
		"sensor.rear.temperature",
		"sensor.left.temperature",
	}
	
	for _, key := range matchingKeys {
		results := c.Route(key)
		if len(results) == 0 {
			t.Errorf("Failed to match key %s with pattern %s", key, pattern)
			continue
		}
		
		found := false
		for _, r := range results {
			if r.Value == value {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Value %s not found in route results for key %s", value, key)
		}
	}
	
	// Should not match non-matching keys
	nonMatchingKeys := []string{
		"sensor.temperature",
		"temperature.sensor.front",
		"sensor.front.humidity",
	}
	
	for _, key := range nonMatchingKeys {
		results := c.Route(key)
		for _, r := range results {
			if r.Value == value {
				t.Errorf("Unexpectedly matched key %s with pattern %s", key, pattern)
				break
			}
		}
	}
}

// TestContainerInsertMultiSegmentWildcard tests the > wildcard.
func TestContainerInsertMultiSegmentWildcard(t *testing.T) {
	c := wkv.New()
	
	// Insert with multi-segment wildcard
	pattern := "sensor.>"
	value := "all_sensor_data"
	
	c.Set(pattern, value)
	
	// Should match any key starting with sensor.
	matchingKeys := []string{
		"sensor",
		"sensor.front",
		"sensor.front.temperature",
		"sensor.front.left.temperature",
	}
	
	for _, key := range matchingKeys {
		results := c.Route(key)
		if len(results) == 0 {
			t.Errorf("Failed to match key %s with pattern %s", key, pattern)
			continue
		}
		
		found := false
		for _, r := range results {
			if r.Value == value {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Value %s not found in route results for key %s", value, key)
		}
	}
}

// TestContainerMultiplePatterns tests multiple patterns.
func TestContainerMultiplePatterns(t *testing.T) {
	c := wkv.New()
	
	// Insert multiple patterns
	c.Set("sensor.*.temperature", "temperature_pattern")
	c.Set("sensor.*.humidity", "humidity_pattern")
	c.Set("actuator.>", "actuator_pattern")
	
	// Test matching
	tests := []struct {
		key      string
		expected string
	}{
		{"sensor.front.temperature", "temperature_pattern"},
		{"sensor.rear.temperature", "temperature_pattern"},
		{"sensor.front.humidity", "humidity_pattern"},
		{"actuator.motor", "actuator_pattern"},
		{"actuator.motor.speed", "actuator_pattern"},
	}
	
	for _, tt := range tests {
		results := c.Route(tt.key)
		if len(results) == 0 {
			t.Errorf("No results for key %s", tt.key)
			continue
		}
		
		found := false
		for _, r := range results {
			if r.Value == tt.expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected to find %s for key %s", tt.expected, tt.key)
		}
	}
}

// TestContainerRemove tests removing a key.
func TestContainerRemove(t *testing.T) {
	c := wkv.New()
	
	key := "test.key"
	value := "test.value"
	
	c.Set(key, value)
	
	// Verify it exists
	if _, ok := c.Get(key); !ok {
		t.Error("Failed to insert value")
		return
	}
	
	// Remove it
	c.Delete(key)
	
	// Verify it's gone
	if _, ok := c.Get(key); ok {
		t.Error("Failed to remove value")
	}
}

// TestContainerClear tests clearing the container.
func TestContainerClear(t *testing.T) {
	c := wkv.New()
	
	// Insert many values
	for i := 0; i < 100; i++ {
		key := "key" + string(rune(i%26))
		c.Set(key, "value")
	}
	
	// Clear
	c.Clear()
	
	// Verify all are gone
	if c.Len() != 0 {
		t.Errorf("Expected 0 entries after clear, got %d", c.Len())
	}
}

// TestContainerSize tests size tracking.
func TestContainerSize(t *testing.T) {
	c := wkv.New()
	
	if c.Len() != 0 {
		t.Errorf("New container should have size 0, got %d", c.Len())
	}
	
	// Insert 10 values
	for i := 0; i < 10; i++ {
		key := "key" + string(rune(i%26))
		c.Set(key, "value")
	}
	
	if c.Len() != 10 {
		t.Errorf("Expected size 10, got %d", c.Len())
	}
	
	// Remove 5 values
	for i := 0; i < 5; i++ {
		key := "key" + string(rune(i%26))
		c.Delete(key)
	}
	
	if c.Len() != 5 {
		t.Errorf("Expected size 5 after removals, got %d", c.Len())
	}
}

// TestContainerKeys tests the Keys method.
func TestContainerKeys(t *testing.T) {
	c := wkv.New()
	
	// Insert values
	keys := []string{"banana", "apple", "cherry", "date"}
	for _, k := range keys {
		c.Set(k, "value")
	}
	
	// Get all keys
	allKeys := c.Keys()
	
	if len(allKeys) != len(keys) {
		t.Errorf("Expected %d keys, got %d", len(keys), len(allKeys))
		return
	}
	
	// Check that all inserted keys are present
	keySet := make(map[string]bool)
	for _, k := range allKeys {
		keySet[k] = true
	}
	
	for _, k := range keys {
		if !keySet[k] {
			t.Errorf("Key %s not found in Keys()", k)
		}
	}
}

// TestContainerContains tests the Contains method.
func TestContainerContains(t *testing.T) {
	c := wkv.New()
	
	c.Set("test.key", "value")
	
	if !c.Contains("test.key") {
		t.Error("Contains should return true for existing key")
	}
	
	if c.Contains("nonexistent") {
		t.Error("Contains should return false for nonexistent key")
	}
}

// TestContainerWildcardSpecificity tests that more specific patterns match first.
func TestContainerWildcardSpecificity(t *testing.T) {
	c := wkv.New()
	
	// Insert patterns with different specificity
	c.Set("sensor.*.temperature", "wildcard_pattern")
	c.Set("sensor.front.temperature", "specific_pattern")
	
	// Specific pattern should match first
	results := c.Route("sensor.front.temperature")
	
	if len(results) == 0 {
		t.Error("No results for key")
		return
	}
	
	// The first result should be the most specific
	if results[0].Value != "specific_pattern" {
		t.Errorf("Expected specific_pattern as first result, got %v", results[0].Value)
	}
}

// TestContainerEmptyKey tests handling of empty keys.
func TestContainerEmptyKey(t *testing.T) {
	c := wkv.New()
	
	c.Set("", "empty_value")
	
	retrieved, ok := c.Get("")
	if !ok {
		t.Error("Failed to retrieve empty key")
		return
	}
	
	if retrieved != "empty_value" {
		t.Errorf("Expected empty_value, got %v", retrieved)
	}
}

// TestContainerDotSeparatedKeys tests keys with dots.
func TestContainerDotSeparatedKeys(t *testing.T) {
	c := wkv.New()
	
	// Insert with various dot-separated keys
	keys := []string{"a", "a.b", "a.b.c", "a.b.c.d"}
	
	for _, key := range keys {
		c.Set(key, key+"_value")
	}
	
	// Verify all can be retrieved
	for _, key := range keys {
		retrieved, ok := c.Get(key)
		if !ok {
			t.Errorf("Failed to retrieve key %s", key)
			continue
		}
		if retrieved != key+"_value" {
			t.Errorf("Expected %s_value, got %v", key, retrieved)
		}
	}
}

// TestContainerPatternEdgeCases tests edge cases for pattern matching.
func TestContainerPatternEdgeCases(t *testing.T) {
	tests := []struct {
		pattern  string
		key      string
		shouldMatch bool
	}{
		// Single segment patterns
		{"a", "a", true},
		{"a", "b", false},
		{"a", "a.b", false},
		
		// Single wildcard
		{"*", "a", true},
		{"*", "a.b", false},
		
		// Multi-segment wildcard
		{">", "a", true},
		{">", "a.b", true},
		{">", "a.b.c", true},
		
		// Mixed patterns
		{"a.*", "a.b", true},
		{"a.*", "a.b.c", false},
		{"a.>", "a.b", true},
		{"a.>", "a.b.c", true},
		{"a.*.c", "a.b.c", true},
		{"a.*.c", "a.b.d", false},
		
		// Consecutive wildcards
		{"*.*", "a.b", true},
		
		// Leading/trailing dots
		{".a", ".a", true},
		{"a.", "a.", true},
	}
	
	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.key, func(t *testing.T) {
			c := wkv.New()
			c.Set(tt.pattern, "matched")
			
			results := c.Route(tt.key)
			
			matched := false
			for _, r := range results {
				if r.Value == "matched" {
					matched = true
					break
				}
			}
			
			if matched != tt.shouldMatch {
				t.Errorf("Pattern %q vs key %q: expected match=%v, got match=%v", 
					tt.pattern, tt.key, tt.shouldMatch, matched)
			}
		})
	}
}

// TestContainerMatch tests the Match method.
// Match finds all values whose keys match the given pattern.
func TestContainerMatch(t *testing.T) {
	c := wkv.New()
	
	// Set literal keys
	c.Set("sensor.front.temperature", "value1")
	c.Set("sensor.rear.temperature", "value2")
	c.Set("actuator.motor", "value3")
	
	// Match keys against pattern
	matches := c.Match("sensor.*.temperature")
	
	if len(matches) != 2 {
		t.Errorf("Expected 2 matches, got %d", len(matches))
	}
}

// TestContainerMatchKey tests the MatchKey method.
// MatchKey finds all literal keys that match a pattern.
func TestContainerMatchKey(t *testing.T) {
	c := wkv.New()
	
	c.Set("sensor.front.temperature", "value1")
	c.Set("sensor.rear.temperature", "value2")
	c.Set("actuator.motor", "value3")
	
	// Match keys against pattern
	matches := c.MatchKey("sensor.*.temperature")
	
	if len(matches) != 2 {
		t.Errorf("Expected 2 matches, got %d", len(matches))
	}
}

// TestContainerPatternCount tests the PatternCount method.
func TestContainerPatternCount(t *testing.T) {
	c := wkv.New()
	
	if c.PatternCount() != 0 {
		t.Errorf("New container should have 0 patterns, got %d", c.PatternCount())
	}
	
	c.Set("pattern.*", "value1")
	c.Set("pattern.>", "value2")
	c.Set("exact.key", "value3")
	
	// Only patterns with wildcards should count
	if c.PatternCount() != 2 {
		t.Errorf("Expected 2 patterns, got %d", c.PatternCount())
	}
}

// TestContainerMatchWithSubstitutions tests matching with substitutions.
// This tests pattern matching against literal keys.
func TestContainerMatchWithSubstitutions(t *testing.T) {
	c := wkv.New()
	
	// Set literal keys
	c.Set("sensor.front.temperature", "value1")
	c.Set("sensor.rear.temperature", "value2")
	
	// Match keys with pattern
	results := c.MatchKeyWithSubstitutions("sensor.*.temperature")
	
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
	
	// Check first result
	if results[0].Value != "value1" && results[0].Value != "value2" {
		t.Errorf("Expected value1 or value2, got %v", results[0].Value)
	}
	
	// Check substitutions - should have "front" or "rear"
	if len(results[0].Substitutions) != 1 {
		t.Errorf("Expected 1 substitution, got %d", len(results[0].Substitutions))
	}
}
