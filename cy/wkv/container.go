// Package wkv provides a key-value container with fast pattern matching and key routing.
// This is a Go port of the wild_key_value.h C library used by the cy library.
//
// The container supports wildcard patterns:
//   - `*` matches a single name segment
//   - `>` matches zero or more name segments
//
// It is optimized for routing keys to matching patterns efficiently.
package wkv

import (
	"strings"
	"sync"
)

// Str is a string type that can be used as a key.
// It wraps a string and provides efficient comparison.
type Str struct {
	str string
	len int
}

// NewStr creates a new Str from a string.
func NewStr(s string) Str {
	return Str{str: s, len: len(s)}
}

// String returns the underlying string.
func (s Str) String() string {
	return s.str
}

// Len returns the length of the string.
func (s Str) Len() int {
	return s.len
}

// Equal checks if two Str values are equal.
func (s Str) Equal(other Str) bool {
	return s.str == other.str
}

// Less checks if s is less than other.
func (s Str) Less(other Str) bool {
	return s.str < other.str
}

// Container is a key-value container with pattern matching support.
type Container struct {
	mu sync.RWMutex
	
	// data maps literal keys to values
	data map[Str]interface{}
	
	// patterns contains pattern entries for wildcard matching
	patterns []patternEntry
	
	// keys is a slice of all literal keys (for iteration)
	keys []Str
}

type patternEntry struct {
	pattern   string
	value    interface{}
	segments []patternSegment
}

type patternSegment struct {
	literal   string
	wildcard  bool
	multi     bool // true for '>' (matches zero or more)
}

// New creates a new Container.
func New() *Container {
	return &Container{
		data:    make(map[Str]interface{}),
		patterns: []patternEntry{},
		keys:    []Str{},
	}
}

// Set sets a value for a literal key.
// If the key already exists, the value is replaced.
func (c *Container) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	str := NewStr(key)
	
	// Check if this is a pattern (contains * or >)
	if isPattern(key) {
		// Add to patterns
		entry := patternEntry{
			pattern:   key,
			value:    value,
			segments: parsePattern(key),
		}
		c.patterns = append(c.patterns, entry)
		return
	}
	
	// Add to literal data
	if _, exists := c.data[str]; !exists {
		c.keys = append(c.keys, str)
	}
	c.data[str] = value
}

// Get retrieves a value by literal key.
// Returns the value and true if found, or nil and false if not found.
func (c *Container) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	str := NewStr(key)
	value, ok := c.data[str]
	return value, ok
}

// GetStr retrieves a value by Str key.
func (c *Container) GetStr(key Str) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	value, ok := c.data[key]
	return value, ok
}

// Delete removes a literal key.
func (c *Container) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	str := NewStr(key)
	delete(c.data, str)
	
	// Remove from keys slice
	for i, k := range c.keys {
		if k.Equal(str) {
			c.keys = append(c.keys[:i], c.keys[i+1:]...)
			break
		}
	}
}

// DeleteStr removes a Str key.
func (c *Container) DeleteStr(key Str) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.data, key)
	
	// Remove from keys slice
	for i, k := range c.keys {
		if k.Equal(key) {
			c.keys = append(c.keys[:i], c.keys[i+1:]...)
			break
		}
	}
}

// Contains checks if a literal key exists.
func (c *Container) Contains(key string) bool {
	_, ok := c.Get(key)
	return ok
}

// ContainsStr checks if a Str key exists.
func (c *Container) ContainsStr(key Str) bool {
	_, ok := c.GetStr(key)
	return ok
}

// Match finds all values that match a pattern.
// The pattern can contain `*` (single segment) and `>` (multi-segment) wildcards.
func (c *Container) Match(pattern string) []interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	results := []interface{}{}
	
	// Check literal keys first
	for key, value := range c.data {
		if matchPattern(pattern, key.String()) {
			results = append(results, value)
		}
	}
	
	// Check patterns
	for _, entry := range c.patterns {
		if matchPattern(pattern, entry.pattern) {
			results = append(results, entry.value)
		}
	}
	
	return results
}

// MatchKey finds all values whose keys match a pattern.
// This is the reverse of Match - it finds literal keys that match a pattern.
func (c *Container) MatchKey(pattern string) []interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	results := []interface{}{}
	
	for key, value := range c.data {
		if matchPattern(pattern, key.String()) {
			results = append(results, value)
		}
	}
	
	return results
}

// MatchKeyWithSubstitutions finds all values whose keys match a pattern
// and returns the substitutions for each match.
func (c *Container) MatchKeyWithSubstitutions(pattern string) []MatchResult {
	c.mu.RLock()
	defer c.mu.RUnlock()

	results := []MatchResult{}
	
	for key, value := range c.data {
		if matched, substitutions := matchPatternWithSubstitutions(pattern, key.String()); matched {
			results = append(results, MatchResult{
				Key:          key.String(),
				Value:       value,
				Substitutions: substitutions,
			})
		}
	}
	
	return results
}

// MatchResult contains the result of a pattern match.
type MatchResult struct {
	Key          string
	Value       interface{}
	Substitutions []string
}

// Route routes a key to all matching patterns.
// This is used for pattern subscriptions - when a new topic is created,
// we route its name against all pattern subscriptions.
func (c *Container) Route(key string) []RouteResult {
	c.mu.RLock()
	defer c.mu.RUnlock()

	results := []RouteResult{}
	str := NewStr(key)
	
	// First check literal match
	if value, ok := c.data[str]; ok {
		results = append(results, RouteResult{
			Pattern:      key,
			Value:        value,
			Substitutions: []string{},
		})
	}
	
	// Then check patterns
	for _, entry := range c.patterns {
		if matched, substitutions := matchPatternWithSubstitutions(entry.pattern, key); matched {
			results = append(results, RouteResult{
				Pattern:      entry.pattern,
				Value:        entry.value,
				Substitutions: substitutions,
			})
		}
	}
	
	return results
}

// RouteResult contains the result of routing a key to a pattern.
type RouteResult struct {
	Pattern      string
	Value        interface{}
	Substitutions []string
}

// Keys returns all literal keys in the container.
func (c *Container) Keys() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]string, len(c.keys))
	for i, k := range c.keys {
		keys[i] = k.String()
	}
	return keys
}

// Len returns the number of literal key-value pairs.
func (c *Container) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}

// PatternCount returns the number of pattern entries.
func (c *Container) PatternCount() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.patterns)
}

// Clear removes all entries.
func (c *Container) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.data = make(map[Str]interface{})
	c.patterns = []patternEntry{}
	c.keys = []Str{}
}

// isPattern checks if a string contains pattern wildcards.
func isPattern(s string) bool {
	return strings.ContainsAny(s, "*>")
}

// parsePattern parses a pattern string into segments.
// Patterns use '.' as the segment separator (e.g., "sensor.*.temperature").
func parsePattern(pattern string) []patternSegment {
	segments := []patternSegment{}
	
	// Split by '.' and process each segment
	parts := strings.Split(pattern, ".")
	for _, part := range parts {
		if part == "*" {
			segments = append(segments, patternSegment{
				wildcard: true,
				multi:    false,
			})
		} else if part == ">" {
			segments = append(segments, patternSegment{
				wildcard: true,
				multi:    true,
			})
		} else {
			segments = append(segments, patternSegment{
				literal:  part,
				wildcard: false,
				multi:    false,
			})
		}
	}
	
	return segments
}

// matchPattern checks if a key matches a pattern.
func matchPattern(pattern, key string) bool {
	matched, _ := matchPatternWithSubstitutions(pattern, key)
	return matched
}

// matchPatternWithSubstitutions checks if a key matches a pattern
// and returns the substitutions if it does.
func matchPatternWithSubstitutions(pattern, key string) (bool, []string) {
	// Normalize both strings
	pattern = normalizeName(pattern)
	key = normalizeName(key)
	
	// Split into segments using '.' as separator
	patternSegs := strings.Split(pattern, ".")
	keySegs := strings.Split(key, ".")
	
	return matchSegmentsWithSubstitutions(patternSegs, keySegs)
}

// normalizeName normalizes a topic/pattern name.
// For Cyphal, names use '.' as the separator.
func normalizeName(name string) string {
	// Remove leading and trailing dots
	name = strings.Trim(name, ".")
	
	// Collapse consecutive dots
	var parts []string
	for _, part := range strings.Split(name, ".") {
		if part != "" {
			parts = append(parts, part)
		}
	}
	
	return strings.Join(parts, ".")
}

// matchSegmentsWithSubstitutions matches pattern segments against key segments.
func matchSegmentsWithSubstitutions(pattern, key []string) (bool, []string) {
	substitutions := []string{}
	i, j := 0, 0
	
	for i < len(pattern) && j < len(key) {
		p := pattern[i]
		k := key[j]
		
		if p == "*" {
			// Single-segment wildcard
			substitutions = append(substitutions, k)
			i++
			j++
		} else if p == ">" {
			// Multi-segment wildcard - matches zero or more segments
			// Collect all remaining key segments
			remaining := []string{}
			for _, seg := range key[j:] {
				remaining = append(remaining, seg)
			}
			substitutions = append(substitutions, strings.Join(remaining, "/"))
			i++
			j = len(key) // Consume all remaining
		} else if p == k {
			// Literal match
			i++
			j++
		} else {
			return false, nil
		}
	}
	
	// Check if we've consumed all pattern segments
	if i < len(pattern) {
		// Pattern has more segments
		// Check if remaining are all multi-wildcards
		for _, p := range pattern[i:] {
			if p != ">" {
				return false, nil
			}
		}
		// All remaining are multi-wildcards, which is OK
		// Add empty substitutions for each
		for range pattern[i:] {
			substitutions = append(substitutions, "")
		}
	}
	
	// Check if we've consumed all key segments
	if j < len(key) {
		// Key has more segments
		// Only OK if pattern ends with multi-wildcard
		if len(pattern) == 0 || pattern[len(pattern)-1] != ">" {
			return false, nil
		}
		// Pattern ends with multi-wildcard, which can match remaining segments
		// But we've already consumed them above
	}
	
	return true, substitutions
}

// Iterate iterates over all literal key-value pairs.
func (c *Container) Iterate(fn func(key string, value interface{})) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for k, v := range c.data {
		fn(k.String(), v)
	}
}

// IteratePatterns iterates over all pattern entries.
func (c *Container) IteratePatterns(fn func(pattern string, value interface{})) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, entry := range c.patterns {
		fn(entry.pattern, entry.value)
	}
}
