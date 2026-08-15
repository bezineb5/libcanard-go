// Package tests provides tests for the Cy consensus/gossip layer.
package tests

import (
	"testing"

	"github.com/opencyphal/cy-go"
)

// deliverGossip builds a C-compatible gossip frame (24-byte header + name) and
// delivers it to the node on the given subject-ID, mimicking a transport that
// already routed the message to the node.
func deliverGossip(t *testing.T, node *cy.Cy, subjectID uint32, lage int8, hash uint64, evictions uint32, name string) {
	t.Helper()
	data := cy.MarshalGossipMessage(lage, hash, uint64(evictions), name)
	msg := cy.NewMessage(data)
	node.HandleMessage(cy.Lane{}, &subjectID, *cy.NewMessageTS(0, msg))
}

// deliverData builds a C-compatible data frame carrying inline CRDT gossip and
// delivers it on the given subject-ID.
func deliverData(t *testing.T, node *cy.Cy, subjectID uint32, lage int8, evictions uint32, hash uint64, payload []byte) {
	t.Helper()
	hdr := cy.NewHeader(cy.HeaderTypeMsgBE, lage, evictions, hash, 0)
	data := append(hdr.MarshalBinary(), payload...)
	msg := cy.NewMessage(data)
	node.HandleMessage(cy.Lane{}, &subjectID, *cy.NewMessageTS(0, msg))
}

// TestGossipAutoSubscribe verifies that a node with a matching pattern
// subscription auto-subscribes an unknown topic learned from a broadcast gossip
// that carries the topic name (pattern-based auto-subscribe, divergence #4).
func TestGossipAutoSubscribe(t *testing.T) {
	platform := NewMockPlatform()
	node, err := cy.New(platform, "nodeA", "", "")
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer node.Destroy()

	// Pattern subscriber that should match the advertised topic name.
	if _, err := node.Subscribe("sensors/>", 256); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	name := "sensors/temp"
	hash := cy.HashString(name)
	subjectID := node.BroadcastSubjectID()
	deliverGossip(t, node, subjectID, int8(cy.LAGEMin), hash, 0, name)

	if got := node.FindTopic(name); got == nil {
		t.Fatalf("expected auto-subscribed topic %q after gossip", name)
	}
}

// TestGossipCollisionArbitrationWire verifies that when a node receives a
// broadcast gossip claiming a subject-ID it already occupies for a different
// topic, the collision is arbitrated (the weaker topic re-evicts) and the
// node's subject-ID index stays consistent (divergence #6).
func TestGossipCollisionArbitrationWire(t *testing.T) {
	platform := NewMockPlatform()
	node, err := cy.New(platform, "nodeA", "", "")
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer node.Destroy()

	// Local topic occupying some subject-ID.
	alpha, err := node.FindOrCreateTopic("alpha", 256)
	if err != nil {
		t.Fatalf("FindOrCreateTopic failed: %v", err)
	}
	alphaSID := alpha.SubjectID()

	modulus := node.SubjectIDModulus()
	// Search for a remote topic name whose reachable subject-ID set includes
	// alpha's subject-ID, and whose hash would win the collision (so alpha is the
	// one re-evicted), making the test deterministic.
	var betaName string
	var betaEvictions uint32
	found := false
	for i := 0; i < 100000; i++ {
		candidate := "beta" + string(rune('a'+i%26)) + itoa(i)
		h := cy.HashString(candidate)
		for e := uint32(0); e < modulus; e++ {
			if cy.ComputeSubjectID(h, e, modulus) == alphaSID {
				// Want beta to win: both fresh -> tie on log-age -> smaller hash wins.
				if h < alpha.Hash() {
					betaName = candidate
					betaEvictions = e
					found = true
				}
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		t.Skip("could not find a deterministic colliding remote topic in the search budget")
	}

	subjectID := node.BroadcastSubjectID()
	deliverGossip(t, node, subjectID, int8(cy.LAGEMin), cy.HashString(betaName), betaEvictions, betaName)

	// Invariant: every non-pinned topic is the unique occupant of its subject-ID.
	for sid, top := range node.TopicsBySubjectID() {
		if top.SubjectID() != sid {
			t.Errorf("topic at subject-ID %d has subjectID %d", sid, top.SubjectID())
		}
	}
	// Because beta won, alpha must have been re-evicted to a new subject-ID.
	if alpha.SubjectID() == alphaSID {
		t.Errorf("alpha was not re-evicted after losing the collision (still at %d)", alphaSID)
	}
}

// TestInlineGossipKnownTopic verifies that a data frame carrying inline CRDT
// gossip for a known topic converges the topic's log-age/eviction state
// (inline/piggyback gossip, divergence #5).
func TestInlineGossipKnownTopic(t *testing.T) {
	platform := NewMockPlatform()
	node, err := cy.New(platform, "nodeA", "", "")
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer node.Destroy()

	topic, err := node.FindOrCreateTopic("shared", 256)
	if err != nil {
		t.Fatalf("FindOrCreateTopic failed: %v", err)
	}
	sid := topic.SubjectID()
	hash := topic.Hash()

	// A peer reports the topic as older (log-age 8) but consistent evictions (0),
	// so the frame is valid for the subject we received it on. The local topic
	// should adopt the older (larger) log-age via the inline CRDT merge.
	deliverData(t, node, sid, 8, 0, hash, []byte("payload"))

	if got := topic.LogAge(); got != 8 {
		t.Errorf("expected converged log-age 8, got %d", got)
	}
	if got := topic.SubjectID(); got != sid {
		t.Errorf("subject-ID changed unexpectedly: %d != %d", got, sid)
	}
}

// itoa is a tiny local integer-to-string helper to avoid importing strconv for
// test-only candidate names.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	buf := [12]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
