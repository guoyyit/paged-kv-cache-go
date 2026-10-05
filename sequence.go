package pagedkv

// sequenceState describes whether a sequence can serve reads.
type sequenceState int

const (
	// seqActive: all logical blocks are resident and reads are served.
	seqActive sequenceState = iota
	// seqPreempted: all blocks were swapped to disk; state can be restored
	// before serving reads.
	seqPreempted
	// seqInvalid: swapped state could not be restored; the request must
	// recompute. Reads always fail with ErrNeedsRecompute.
	seqInvalid
)

// blockRef is one logical block's binding.
//
// For an active sequence blockID >= 0 identifies a resident physical block.
// For a preempted sequence blockID == -1 and the content must be fetched from
// the swap store by key (full blocks use the chained key, private tail blocks
// the raw content key).
type blockRef struct {
	blockID int
	key     []byte
}

const swappedBlock = -1

// Sequence is one request's ordered list of cache blocks.
type Sequence struct {
	tenant   string
	id       string
	priority int
	state    sequenceState
	refs     []blockRef
	length   int
	// order is the creation tick used as the deterministic tie-break for
	// preemption ("oldest first").
	order uint64
	// swappedData records, for a preempted sequence, the swap-store key and
	// length of each logical block in order.
	swapped []swapEntry
	// committed is true once the mapping was durably journaled.
	committed bool
	// lastCommit is the exact mapping written at the last commit; it stays
	// stable even if the sequence is later appended to but not recommitted.
	lastCommit []journalBlock
}

// swapEntry locates one swapped-out logical block in the store.
type swapEntry struct {
	key  []byte
	size int
}

// ID returns the sequence identifier.
func (s *Sequence) ID() string { return s.id }

// TenantID returns the owning tenant's identifier.
func (s *Sequence) TenantID() string { return s.tenant }

// Length reports the number of readable bytes for an active sequence.
func (s *Sequence) Length() int { return s.length }

// State is a snapshot of a sequence's residency at one point in time.
type State int

const (
	StateActive State = iota
	StatePreempted
	StateInvalid
)

func (s *Sequence) stateSnapshot() State {
	switch s.state {
	case seqPreempted:
		return StatePreempted
	case seqInvalid:
		return StateInvalid
	default:
		return StateActive
	}
}

// numBlocks is the number of logical blocks implied by length.
func numBlocks(length, blockSize int) int {
	if length == 0 {
		return 0
	}
	return (length-1)/blockSize + 1
}
