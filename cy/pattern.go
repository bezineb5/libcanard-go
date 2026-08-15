package cy

import (
	"strings"
)

// PatternMatcher handles pattern matching for subscriptions.
// It allows subscribers to use wildcard patterns like "sensors/*/temperature" or "sensors/>".
type PatternMatcher struct {
	// patterns maps pattern strings to lists of subscribers.
	patterns map[string][]*Subscriber
	
	// compiledPatterns caches compiled patterns.
	compiledPatterns map[string][]PatternSegment
}

// PatternSegment represents a segment of a pattern.
type PatternSegment struct {
	// Literal is the literal string (empty if wildcard).
	Literal string
	// Wildcard is true if this is a wildcard segment.
	Wildcard bool
	// Multi is true if this is a multi-segment wildcard (>).
	Multi bool
}

// NewPatternMatcher creates a new PatternMatcher.
func NewPatternMatcher() *PatternMatcher {
	return &PatternMatcher{
		patterns:        make(map[string][]*Subscriber),
		compiledPatterns: make(map[string][]PatternSegment),
	}
}

// AddPattern adds a pattern subscription.
func (pm *PatternMatcher) AddPattern(pattern string, sub *Subscriber) {
	pm.patterns[pattern] = append(pm.patterns[pattern], sub)
}

// HasPattern reports whether at least one subscriber is registered for the pattern.
func (pm *PatternMatcher) HasPattern(pattern string) bool {
	return len(pm.patterns[pattern]) > 0
}

// RemovePattern removes a pattern subscription.
func (pm *PatternMatcher) RemovePattern(pattern string, sub *Subscriber) {
	if subs, ok := pm.patterns[pattern]; ok {
		for i, s := range subs {
			if s == sub {
				pm.patterns[pattern] = append(subs[:i], subs[i+1:]...)
				return
			}
		}
	}
}

// RemoveSubscriber removes all pattern subscriptions for a subscriber.
func (pm *PatternMatcher) RemoveSubscriber(sub *Subscriber) {
	for pattern, subs := range pm.patterns {
		for i, s := range subs {
			if s == sub {
				pm.patterns[pattern] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
	}
}

// Match matches a topic name against all patterns and returns matching subscribers.
// Returns a map of subscribers to their pattern substitutions.
func (pm *PatternMatcher) Match(topicName string) map[*Subscriber][]string {
	results := make(map[*Subscriber][]string)

	for pattern, subs := range pm.patterns {
		if matched, substitutions := MatchPattern(pattern, topicName); matched {
			for _, sub := range subs {
				results[sub] = substitutions
			}
		}
	}

	return results
}

// MatchPattern checks if a topic name matches a pattern.
// Returns true and substitutions if it matches, false otherwise.
func MatchPattern(pattern, topic string) (bool, []string) {
	// Normalize both names
	pattern = NormalizeTopicName(pattern)
	topic = NormalizeTopicName(topic)

	// Split into segments
	patternSegs := strings.Split(pattern, "/")
	topicSegs := strings.Split(topic, "/")

	return MatchSegments(patternSegs, topicSegs)
}

// MatchSegments matches pattern segments against topic segments, returning the
// per-wildcard substitutions in ordinal order (one entry per '*' or '>' wildcard;
// a '>' substitution joins its matched segments with '/'). The multi-segment
// wildcard '>' is matched with backtracking so that trailing literals (e.g.
// "a/>/c") are honored rather than consuming all remaining segments greedily.
func MatchSegments(pattern, topic []string) (bool, []string) {
	var subs []string
	if matchSegments(pattern, topic, 0, 0, &subs) {
		return true, subs
	}
	return false, nil
}

func matchSegments(p, t []string, pi, ti int, subs *[]string) bool {
	for pi < len(p) {
		seg := p[pi]
		switch seg {
		case "*":
			if ti >= len(t) {
				return false
			}
			*subs = append(*subs, t[ti])
			pi++
			ti++
		case ">":
			maxK := len(t) - ti
			for k := 0; k <= maxK; k++ {
				saved := len(*subs)
				if k > 0 {
					*subs = append(*subs, strings.Join(t[ti:ti+k], "/"))
				} else {
					*subs = append(*subs, "")
				}
				if matchSegments(p, t, pi+1, ti+k, subs) {
					return true
				}
				*subs = (*subs)[:saved]
			}
			return false
		default:
			if ti >= len(t) || seg != t[ti] {
				return false
			}
			pi++
			ti++
		}
	}
	return ti == len(t)
}

// NormalizeTopicName normalizes a topic name by removing leading/trailing
// slashes and collapsing consecutive slashes.
func NormalizeTopicName(name string) string {
	// Remove leading and trailing slashes
	name = strings.Trim(name, "/")

	// Collapse consecutive slashes
	var parts []string
	for _, part := range strings.Split(name, "/") {
		if part != "" {
			parts = append(parts, part)
		}
	}

	return strings.Join(parts, "/")
}

// IsPattern returns true if the given string is a pattern (contains wildcards).
func IsPattern(name string) bool {
	return strings.ContainsAny(name, "*>")
}

// CompilePattern compiles a pattern string into segments for efficient matching.
func CompilePattern(pattern string) []PatternSegment {
	segments := []PatternSegment{}

	// Split by '/' and process each segment
	parts := strings.Split(NormalizeTopicName(pattern), "/")
	for _, part := range parts {
		if part == "*" {
			segments = append(segments, PatternSegment{
				Wildcard: true,
				Multi:    false,
			})
		} else if part == ">" {
			segments = append(segments, PatternSegment{
				Wildcard: true,
				Multi:    true,
			})
		} else {
			segments = append(segments, PatternSegment{
				Literal:  part,
				Wildcard: false,
				Multi:    false,
			})
		}
	}

	return segments
}

// Substitution represents a substitution in a pattern match.
type Substitution struct {
	// Ordinal is the index of the wildcard in the pattern.
	Ordinal int
	// Str is the string that matched the wildcard.
	Str string
}

// NewSubstitution creates a new substitution.
func NewSubstitution(ordinal int, str string) Substitution {
	return Substitution{
		Ordinal: ordinal,
		Str:     str,
	}
}

// String returns a string representation of the substitution.
func (s Substitution) String() string {
	return s.Str
}
