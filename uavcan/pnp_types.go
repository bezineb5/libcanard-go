package uavcan

// =============================================================================
// uavcan.pnp namespace - Plug-and-play node types
// =============================================================================

// NodeIDAllocationData is used for automatic node-ID allocation.
// Fixed Subject ID: 8165
// This message is published anonymously (without a node-ID).
type NodeIDAllocationData struct {
	// The node-ID being allocated.
	AllocatedNodeId uint8

	// The unique-ID of the node requesting the allocation.
	UniqueId [16]uint8

	// Reserved for future use.
	_ [7]uint8
}

// cluster namespace types for PnP consensus algorithm

// Entry represents an entry in the PnP cluster log.
type PnpClusterEntry struct {
	// The term number.
	Term uint64

	// The node-ID of the candidate that received the vote.
	CandidateNodeId uint8

	// Reserved for future use.
	_ [7]uint8
}

// AppendEntriesRequest is used in the PnP consensus algorithm.
// Fixed Service ID: 390
type PnpClusterAppendEntriesRequest struct {
	// The term number.
	Term uint64

	// The node-ID of the leader sending this request.
	LeaderId uint8

	// The index of the log entry immediately preceding the new ones.
	PrevLogIndex uint64

	// The term of the log entry immediately preceding the new ones.
	PrevLogTerm uint64

	// The entries to append to the log.
	Entries []PnpClusterEntry

	// The leader's commit index.
	LeaderCommit uint64
}

// AppendEntriesResponse is the response to an AppendEntries request.
type PnpClusterAppendEntriesResponse struct {
	// The term number.
	Term uint64

	// True if the follower accepted the entries.
	Success bool

	// The index of the last entry that was successfully appended.
	LastApplied uint64

	// Reserved for future use.
	_ [7]uint8
}

// RequestVoteRequest is used in the PnP consensus algorithm.
// Fixed Service ID: 391
type PnpClusterRequestVoteRequest struct {
	// The term number.
	Term uint64

	// The node-ID of the candidate requesting the vote.
	CandidateId uint8

	// The index of the candidate's last log entry.
	LastLogIndex uint64

	// The term of the candidate's last log entry.
	LastLogTerm uint64
}

// RequestVoteResponse is the response to a RequestVote request.
type PnpClusterRequestVoteResponse struct {
	// The term number.
	Term uint64

	// True if the voter granted the vote.
	VoteGranted bool

	// Reserved for future use.
	_ [7]uint8
}

// Discovery is used for PnP node discovery.
// Fixed Subject ID: 8164
type PnpClusterDiscovery struct {
	// The node-ID of the node sending this message.
	NodeId uint8

	// The unique-ID of the node.
	UniqueId [16]uint8

	// The current term number.
	Term uint64

	// Reserved for future use.
	_ [7]uint8
}
