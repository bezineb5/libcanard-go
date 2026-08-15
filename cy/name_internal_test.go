package cy

import (
	"strings"
	"testing"
)

// TestNameNormalize verifies separator collapsing and charset validation.
func TestNameNormalize(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantOk  bool
	}{
		{"foo/bar", "foo/bar", true},
		{"foo//bar", "foo/bar", true},   // duplicate slash collapses
		{"/foo/", "foo", true},          // leading/trailing slash trimmed
		{"foo///bar", "foo/bar", true},  // multiple duplicates collapse
		{"", "", true},                  // empty is charset-valid (length 0); resolution rejects it separately
		{"foo bar", "", false},          // space (char 32) is invalid
		{"foo\x7fbar", "", false},       // DEL (127) is invalid
		{"a*b", "a*b", true},            // wildcard char is a valid char
		{"a>b", "a>b", true},            // multi-wildcard char is valid
		{"~home", "~home", true},        // home char is valid
		{"foo#5", "foo#5", true},        // pin char is a valid char
	}
	for _, c := range cases {
		got, ok := normalizeName(c.in)
		if ok != c.wantOk || got != c.want {
			t.Errorf("normalizeName(%q) = (%q,%v), want (%q,%v)", c.in, got, ok, c.want, c.wantOk)
		}
	}
}

// TestNameConsumePinSuffix verifies pin parsing and its rejection rules.
func TestNameConsumePinSuffix(t *testing.T) {
	cases := []struct {
		in      string
		want    string
		wantPin uint16
	}{
		{"foo", "foo", PinNone},
		{"foo#5", "foo", 5},
		{"foo#0", "foo", 0},
		{"foo#1234", "foo", 1234},
		{"foo#8191", "foo", SubjectIDPinnedMax}, // max valid pin
		{"foo#8192", "foo#8192", PinNone},      // exceeds pinned max -> name unchanged, no pin
		{"foo#05", "foo#05", PinNone},           // leading zero rejected
		{"foo#", "foo#", PinNone},               // bare '#' -> no pin
		{"foo#1a", "foo#1a", PinNone},           // non-digit after # -> no pin
		{"foo#1#2", "foo#1", 2},                 // only trailing #digits consumed
	}
	for _, c := range cases {
		got, pin := nameConsumePinSuffix(c.in)
		if got != c.want || pin != c.wantPin {
			t.Errorf("nameConsumePinSuffix(%q) = (%q,%d), want (%q,%d)", c.in, got, pin, c.want, c.wantPin)
		}
	}
}

// TestNameResolvePure exercises NameResolve free of a Cy instance.
func TestNameResolvePure(t *testing.T) {
	// Relative name with namespace, no home, no remap.
	r := NameResolve(nil, "sensors/temp", "ns", "home")
	if !r.Ok || r.Name != "ns/sensors/temp" || r.Pin != PinNone || !r.Verbatim {
		t.Errorf("relative+ns: %+v", r)
	}

	// Absolute name overrides namespace, but its leading '/' is normalized away.
	r = NameResolve(nil, "/abs/path", "ns", "home")
	if !r.Ok || r.Name != "abs/path" {
		t.Errorf("absolute: %+v", r)
	}

	// Homeful expansion.
	r = NameResolve(nil, "~/x", "ns", "node")
	if !r.Ok || r.Name != "node/x" {
		t.Errorf("homeful: %+v", r)
	}

	// Pin stripping.
	r = NameResolve(nil, "foo#7", "ns", "node")
	if !r.Ok || r.Name != "ns/foo" || r.Pin != 7 {
		t.Errorf("pin: %+v", r)
	}

	// Pinned pattern is rejected.
	r = NameResolve(nil, "foo/*#7", "ns", "node")
	if r.Ok {
		t.Errorf("pinned pattern should be rejected: %+v", r)
	}

	// Remap applied BEFORE namespace/home expansion (rule 'to' can be absolute).
	// An absolute 'to' has its leading '/' normalized away, which (by preventing
	// namespace prepending) yields the bare name "remapped", matching C.
	remap := map[string]string{"foo": "/remapped"}
	r = NameResolve(remap, "foo", "ns", "node")
	if !r.Ok || r.Name != "remapped" {
		t.Errorf("remap absolute to: %+v", r)
	}

	// Remap key is normalized: "foo//bar" input matches a "foo/bar" rule.
	remap = map[string]string{"foo/bar": "baz"}
	r = NameResolve(remap, "foo//bar", "ns", "node")
	if !r.Ok || r.Name != "ns/baz" {
		t.Errorf("remap normalized key: %+v", r)
	}

	// A matched rule discards the user's pin and substitutes the rule's pin.
	remap = map[string]string{"foo": "bar#3"}
	r = NameResolve(remap, "foo#9", "ns", "node")
	if !r.Ok || r.Name != "ns/bar" || r.Pin != 3 {
		t.Errorf("rule pin wins: %+v", r)
	}

	// No rule match keeps the user's pin.
	remap = map[string]string{"other": "x"}
	r = NameResolve(remap, "foo#7", "ns", "node")
	if !r.Ok || r.Name != "ns/foo" || r.Pin != 7 {
		t.Errorf("user pin retained: %+v", r)
	}

	// Over-length name exceeds CY_TOPIC_NAME_MAX.
	r = NameResolve(nil, strings.Repeat("a", TopicNameMax+1), "ns", "node")
	if r.Ok {
		t.Errorf("over-length name should be rejected")
	}

	// Empty name is invalid.
	if NameResolve(nil, "", "ns", "node").Ok {
		t.Errorf("empty name should be rejected")
	}
}

// TestNameResolveViaCy exercises remap/namespace/home through a real Cy instance,
// including the Remap/Unremap API, and verifies namespace-from-config parsing.
func TestNameResolveViaCy(t *testing.T) {
	cy, _ := newTestCy(t) // home="n", ns="", no remap
	cy.ns = "svc"          // inject a namespace for the test

	r := cy.Resolve("temp")
	if !r.Ok || r.Name != "svc/temp" {
		t.Errorf("ns expansion via Cy: %+v", r)
	}

	// Remap API: install a rule and confirm it applies; the stored 'to' is verbatim.
	if err := cy.Remap("temp", "~/hot"); err != nil {
		t.Fatalf("Remap failed: %v", err)
	}
	r = cy.Resolve("temp")
	if !r.Ok || r.Name != "n/hot" {
		t.Errorf("remap+homeful to via Cy: %+v", r)
	}

	// Unremap removes the rule.
	if err := cy.Unremap("temp"); err != nil {
		t.Fatalf("Unremap failed: %v", err)
	}
	r = cy.Resolve("temp")
	if !r.Ok || r.Name != "svc/temp" {
		t.Errorf("after unremap: %+v", r)
	}

	// Invalid 'from' is rejected.
	if err := cy.Remap("", "x"); err != ErrArgument {
		t.Errorf("empty from should be ErrArgument, got %v", err)
	}
	// Pinned pattern 'to' is rejected.
	if err := cy.Remap("a", "b/*#1"); err != ErrName {
		t.Errorf("pinned-pattern to should be ErrName, got %v", err)
	}
}

// TestNamespaceFromConfig verifies that a leading "=ns" token in the remap config
// supplies the namespace when the explicit namespace argument is empty.
func TestNamespaceFromConfig(t *testing.T) {
	plat := &testPlatform{nowValue: 10 * MEGA}
	cy, err := New(plat, "node", "", "=cfgns from=to")
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if cy.ns != "cfgns" {
		t.Errorf("namespace from config = %q, want cfgns", cy.ns)
	}
	r := cy.Resolve("temp")
	if !r.Ok || r.Name != "cfgns/temp" {
		t.Errorf("resolve with config ns: %+v", r)
	}
	// The "from=to" pair should also have been installed (and gets the namespace).
	r = cy.Resolve("from")
	if !r.Ok || r.Name != "cfgns/to" {
		t.Errorf("config remap pair: %+v", r)
	}
}

// TestPinnedTopicCreation verifies that a pinned subject-ID requested via the name
// suffix is applied to the created topic.
func TestPinnedTopicCreation(t *testing.T) {
	cy, _ := newTestCy(t)
	sub, err := cy.Subscribe("sensors/temp#5", 64)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	topic := sub.Topic()
	if topic == nil {
		t.Fatalf("subscriber has no topic")
	}
	if !topic.Pinned() {
		t.Errorf("expected pinned topic")
	}
	if topic.SubjectID() != 5 {
		t.Errorf("expected subject-ID 5, got %d", topic.SubjectID())
	}
	if topic.PinnedSubjectID() != 5 {
		t.Errorf("expected pinned subject-ID 5, got %d", topic.PinnedSubjectID())
	}
	// The topic is indexed under its canonical (pin-free) name.
	if got := cy.FindTopic("sensors/temp#5"); got != topic {
		t.Errorf("FindTopic via pin suffix should resolve to the same topic")
	}
	if got := cy.FindTopic("sensors/temp"); got != topic {
		t.Errorf("FindTopic via canonical name should resolve to the same topic")
	}
}
