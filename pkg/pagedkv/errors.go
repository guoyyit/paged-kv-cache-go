package pagedkv

import "errors"

// Sentinel errors. Callers can distinguish failure reasons with errors.Is.
var (
	// ErrNotFound means the referenced sequence or tenant does not exist.
	ErrNotFound = errors.New("pagedkv: not found")
	// ErrInvalidArgument means an argument is malformed or out of range.
	ErrInvalidArgument = errors.New("pagedkv: invalid argument")
	// ErrTenantNotFound means the tenant is unknown.
	ErrTenantNotFound = errors.New("pagedkv: tenant not found")
	// ErrSequenceNotFound means the sequence is unknown.
	ErrSequenceNotFound = errors.New("pagedkv: sequence not found")
	// ErrOutOfRange means an offset lies outside the sequence or block.
	ErrOutOfRange = errors.New("pagedkv: offset out of range")
	// ErrTenantQuotaExceeded means the tenant has no quota headroom and no
	// reclaimable block could be found within that same tenant.
	ErrTenantQuotaExceeded = errors.New("pagedkv: tenant quota exceeded")
	// ErrResourceExhausted means the global block pool is exhausted and
	// preemption could not free enough memory.
	ErrResourceExhausted = errors.New("pagedkv: resource exhausted")
	// ErrSequencePreempted means the sequence was swapped out; the caller may
	// attempt Resume to bring it back.
	ErrSequencePreempted = errors.New("pagedkv: sequence preempted")
	// ErrNeedsRecompute means the sequence state cannot be fully restored
	// (corrupt or missing swap data) and must be recomputed from its prefix.
	ErrNeedsRecompute = errors.New("pagedkv: sequence needs recompute")
	// ErrSequenceCommitted means a mutation was attempted on a committed sequence.
	ErrSequenceCommitted = errors.New("pagedkv: sequence already committed")
	// ErrSequenceAborted means the sequence has been aborted.
	ErrSequenceAborted = errors.New("pagedkv: sequence aborted")
	// ErrCorrupt means a block or record failed its checksum validation.
	ErrCorrupt = errors.New("pagedkv: corrupt data")
	// ErrClosed means the manager has been closed.
	ErrClosed = errors.New("pagedkv: manager closed")
)
