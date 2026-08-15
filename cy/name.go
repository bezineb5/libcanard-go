package cy

import (
	"strings"
)

// Name-resolution constants and helpers. This is a faithful port of the C
// cy_name_* family (cy.c ~5180-5441) and the associated cy_resolved_t model.
//
// Topic names are sequences of printable ASCII characters (33..126). The special
// characters are:
//   - '/'  (NameSep)        path separator; duplicated / leading / trailing are collapsed.
//   - '~'  (NameHome)       home prefix; a name beginning with '~' is expanded against the
//                           node's home name (e.g. "~/foo" -> "<home>/foo").
//   - '#'  (NamePinPrefix)  pin suffix; a trailing "#<digits>" encodes an explicit pinned
//                           subject-ID (e.g. "foo#5"). Leading zeros are rejected.
//   - '*'  (NameOne) and '>' (NameAny) are substitution tokens: any name containing them is
//                           a pattern (non-verbatim) and may match many concrete topics.

const (
	// NameSep is the path separator.
	NameSep = '/'
	// NameHome is the home prefix character.
	NameHome = '~'
	// NameOne is the single-segment wildcard.
	NameOne = '*'
	// NameAny is the multi-segment wildcard.
	NameAny = '>'
	// NamePinPrefix introduces a pin expression.
	NamePinPrefix = '#'

	// TopicNameMax is the maximum length of a resolved topic name (CY_TOPIC_NAME_MAX).
	TopicNameMax = 200

	// PinNone is the sentinel pin value meaning "not pinned" (UINT16_MAX).
	PinNone = 0xFFFF
)

// isNameChar reports whether c is a valid name character (printable ASCII, 33..126).
func isNameChar(c byte) bool {
	return c >= 33 && c <= 126
}

// nameNormalizedLen returns the length of the normalized form of name, or -1 if
// the name contains any invalid character. Normalization collapses duplicate,
// leading, and trailing separators but otherwise preserves all valid characters
// (including the pin prefix and wildcard tokens). A returned length of 0 means the
// name normalizes to empty (e.g. "//" or ""). Mirrors C name_normalized_len().
func nameNormalizedLen(name string) int {
	out := 0
	pendingSep := false
	for i := 0; i < len(name); i++ {
		c := name[i]
		if !isNameChar(c) {
			return -1
		}
		if c == NameSep {
			pendingSep = out > 0
			continue
		}
		if pendingSep {
			pendingSep = false
			out++
		}
		out++
	}
	return out
}

// normalizeName returns the normalized form of name (charset-validated, with
// separators collapsed) and whether the input was valid. Mirrors C name_normalize().
func normalizeName(name string) (string, bool) {
	if nameNormalizedLen(name) < 0 {
		return "", false
	}
	out := make([]byte, 0, len(name))
	pendingSep := false
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c == NameSep {
			pendingSep = len(out) > 0
			continue
		}
		if pendingSep {
			out = append(out, NameSep)
			pendingSep = false
		}
		out = append(out, c)
	}
	return string(out), true
}

// nameConsumePinSuffix parses and strips a trailing "#<digits>" pin expression,
// returning the shortened name and the pin value (PinNone if absent/invalid).
// Mirrors C name_consume_pin_suffix(): a bare '#', a leading zero, a value
// exceeding SubjectIDPinnedMax, or any non-digit before the '#' all mean "no pin".
func nameConsumePinSuffix(name string) (string, uint16) {
	pin := uint16(PinNone)
	hashPos := len(name)
	for i := len(name); i > 0; i-- {
		ch := name[i-1]
		if ch == NamePinPrefix {
			hashPos = i - 1
			break
		}
		if ch < '0' || ch > '9' {
			return name, pin
		}
	}
	if hashPos >= len(name) {
		return name, pin // No '#' found.
	}
	nDigits := len(name) - hashPos - 1
	if nDigits == 0 {
		return name, pin // Bare '#' with no digits.
	}
	if nDigits > 1 && name[hashPos+1] == '0' {
		return name, pin // Leading zeros are not allowed.
	}
	var val uint32
	for i := hashPos + 1; i < len(name); i++ {
		val = val*10 + uint32(name[i]-'0')
		if val > SubjectIDPinnedMax {
			return name, pin // Exceeds the valid pinned subject-ID range.
		}
	}
	return name[:hashPos], uint16(val)
}

// nameIsVerbatim reports whether name contains no substitution tokens ('*' or '>').
// Mirrors C name_is_verbatim().
func nameIsVerbatim(name string) bool {
	return !strings.ContainsAny(name, "*>")
}

// nameIsAbsolute reports whether name is absolute (begins with '/').
func nameIsAbsolute(name string) bool {
	return len(name) >= 1 && name[0] == NameSep
}

// nameIsHomeful reports whether name is homeful (begins with '~', optionally followed by '/').
func nameIsHomeful(name string) bool {
	return len(name) >= 1 && name[0] == NameHome && (len(name) == 1 || name[1] == NameSep)
}

// nameJoin joins left and right with a single separator, normalizing both sides so
// that duplicated/leading/trailing separators collapse. Either side may be empty.
// Mirrors C cy_name_join().
func nameJoin(left, right string) (string, bool) {
	ln, ok := normalizeName(left)
	if !ok {
		return "", false
	}
	rn, ok := normalizeName(right)
	if !ok {
		return "", false
	}
	switch {
	case ln == "":
		return rn, true
	case rn == "":
		return ln, true
	default:
		return ln + string(NameSep) + rn, true
	}
}

// nameResolveConstruct applies namespace/home expansion to a (pin-free) name,
// mirroring C name_resolve_construct(): absolute names are used as-is, homeful
// names are joined with home, and everything else is joined with the namespace
// (which itself is expanded against home if it is homeful).
func nameResolveConstruct(name, nameSpace, home string) (string, bool) {
	if nameIsAbsolute(name) {
		return normalizeName(name)
	}
	if nameIsHomeful(name) {
		return nameJoin(home, name[1:])
	}
	if nameIsHomeful(nameSpace) {
		nameSpace, _ = nameJoin(home, nameSpace[1:])
	}
	return nameJoin(nameSpace, name)
}

// Resolved models a fully resolved, normalized topic name together with its pin
// and verbatim (non-pattern) flag. It is the Go equivalent of C's cy_resolved_t.
// An invalid resolution has Ok == false (Name empty, Pin == PinNone).
type Resolved struct {
	// Name is the resolved, normalized, pin-free canonical name.
	Name string
	// Pin is the pinned subject-ID, or PinNone if the topic is not pinned.
	Pin uint16
	// Verbatim is false if the name is a pattern (contains substitution tokens).
	Verbatim bool
	// Ok reports whether resolution succeeded.
	Ok bool
}

// NameResolve resolves a user-supplied topic name into its canonical form,
// applying remapping (before namespace/home expansion), namespace/home expansion,
// pin handling, charset validation, the CY_TOPIC_NAME_MAX length limit, and the
// pinned-pattern prohibition. It is a faithful port of C cy_name_resolve().
//
// The remap table maps normalized "from" keys to verbatim "to" values (the stored
// value may itself carry a pin suffix or be absolute/homeful/relative). A matched
// rule discards the user's pin and substitutes the rule's pin; an unmatched name
// keeps the user's pin.
func NameResolve(remap map[string]string, name, nameSpace, home string) Resolved {
	bad := Resolved{Pin: PinNone}
	if name == "" {
		return bad
	}

	// Pre-strip the user's pin so pinned queries still match unpinned `from` keys
	// and so the user's pin is available as a fallback if no rule matches.
	nameUnpinned, userPin := nameConsumePinSuffix(name)
	if nameNormalizedLen(nameUnpinned) < 0 {
		return bad
	}

	effectiveName := nameUnpinned
	effectivePin := userPin
	if len(remap) > 0 {
		lookupKey, ok := normalizeName(nameUnpinned)
		if ok {
			if to, found := remap[lookupKey]; found {
				// A rule matched: substitute the stored `to` for the user's name and
				// discard the user's pin (the matched rule's pin wins outright).
				effectiveName, effectivePin = nameConsumePinSuffix(to)
			}
		}
	}

	if nameNormalizedLen(effectiveName) <= 0 {
		return bad
	}

	// Run the standard namespace/home expansion on whatever name we ended up with.
	res, ok := nameResolveConstruct(effectiveName, nameSpace, home)
	if !ok {
		return bad
	}
	if len(res) == 0 || len(res) > TopicNameMax {
		return bad
	}

	// Pinned topic names cannot be patterns.
	pinned := effectivePin <= SubjectIDPinnedMax
	verbatim := nameIsVerbatim(res)
	if pinned && !verbatim {
		return bad
	}

	return Resolved{Name: res, Pin: effectivePin, Verbatim: verbatim, Ok: true}
}

// Resolve resolves a name using this Cy instance's remap table, namespace, and home.
// The caller must hold c.mu.
func (c *Cy) Resolve(name string) Resolved {
	return NameResolve(c.remap, name, c.ns, c.home)
}
