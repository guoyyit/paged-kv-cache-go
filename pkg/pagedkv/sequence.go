package pagedkv

import (
	"fmt"
	"sort"
)

// seqState is the lifecycle state of a sequence.
type seqState uint8

const (
	seqActive seqState = iota + 1
	seqSwapped
	seqCommitted
	seqAborted
	// seqNeedsRecompute marks a sequence whose durable image is incomplete
	// (missing or corrupt swap/commit object). It can never serve data; the
	// caller must recompute it from an earlier valid prefix.
	seqNeedsRecompute
)

func (s seqState) String() string {
	switch s {
	case seqActive:
		return "active"
	case seqSwapped:
		return "swapped"
	case seqCommitted:
		return "committed"
	case seqAborted:
		return "aborted"
	case seqNeedsRecompute:
		return "needs_recompute"
	default:
		return "unknown"
	}
}

// Sequence is a request-scoped logical KV buffer backed by fixed-size blocks.
// Block slots are physical block ids; multiple sequences may reference the
// same block (prefix sharing). A referenced block's payload is immutable, so
// rewriting a shared prefix triggers copy-on-write for exactly the touched
// blocks; other sequences keep observing the bytes they already saw.
type Sequence struct {
	mgr    *Manager
	tenant string
	id     string
	state  seqState
	length int
	// blocks holds resident physical block ids while active or restored.
	blocks []int
	// hashes is the ordered content-hash chain; populated when content has
	// been published (commit/swap/restore) and kept current by every mutation.
	hashes []string
}

// State reports the current lifecycle state name.
func (s *Sequence) State() string {
	s.mgr.mu.Lock()
	defer s.mgr.mu.Unlock()
	return s.state.String()
}

// Length reports the logical length in bytes.
func (s *Sequence) Length() int {
	s.mgr.mu.Lock()
	defer s.mgr.mu.Unlock()
	return s.length
}

func (s *Sequence) requireActiveLocked() error {
	switch s.state {
	case seqActive:
		return nil
	case seqSwapped:
		return fmt.Errorf("%w: sequence %q is swapped out; call Resume",
			ErrSequencePreempted, s.id)
	case seqCommitted:
		return fmt.Errorf("%w: sequence %q", ErrSequenceCommitted, s.id)
	case seqNeedsRecompute:
		return fmt.Errorf("%w: sequence %q", ErrNeedsRecompute, s.id)
	default:
		return fmt.Errorf("%w: sequence %q", ErrSequenceAborted, s.id)
	}
}

// Append writes data immediately after the current logical end. It may touch a
// partially filled last block (copy-on-writing it when shared) and then adds
// full blocks; identical full-block content is shared across sequences.
func (s *Sequence) Append(data []byte) error {
	s.mgr.mu.Lock()
	defer s.mgr.mu.Unlock()
	if s.mgr.closed {
		return ErrClosed
	}
	if err := s.requireActiveLocked(); err != nil {
		return err
	}
	t := s.mgr.tenants[s.tenant]
	bs := s.mgr.cfg.BlockBytes
	off := 0

	// Fill the remaining tail of the last partial block first.
	if s.length%bs != 0 && off < len(data) {
		idx := len(s.blocks) - 1
		start := s.length % bs
		bid := s.blocks[idx]
		b := t.resident[bid]
		room := bs - start
		take := room
		if take > len(data) {
			take = len(data)
		}
		if b.refs == 1 && !b.cached {
			copy(b.payload[start:start+take], data[:take])
			newHash := hashPayload(b.payload)
			s.mgr.reindexHashLocked(t, b, newHash)
		} else {
			payload := make([]byte, bs)
			copy(payload, b.payload)
			copy(payload[start:start+take], data[:take])
			nb, err := s.mgr.allocBlockLocked(t, payload, s.id)
			if err != nil {
				return err
			}
			// Drop the old block's ownership using its current id before the
			// slot is repointed at the new block, otherwise the owner index
			// would leak the old mapping.
			s.mgr.releaseRefLocked(t, b, s.id)
			s.blocks[idx] = nb.id
			b = nb
		}
		off += take
		s.length += take
		s.hashes[idx] = hashPayload(t.resident[s.blocks[idx]].payload)
	}

	// Append whole zero-padded blocks.
	for off < len(data) {
		payload := make([]byte, bs)
		take := copy(payload, data[off:])
		nb, err := s.mgr.allocBlockLocked(t, payload, s.id)
		if err != nil {
			return err
		}
		s.blocks = append(s.blocks, nb.id)
		s.hashes = append(s.hashes, nb.hash)
		s.length += take
		off += take
	}
	return nil
}

// Rewrite overwrites [offset, offset+len(data)) of existing content. Every
// block that is shared with another sequence (or is a cached published page)
// is first copied-on-write, so the change is invisible to anyone else.
func (s *Sequence) Rewrite(offset int, data []byte) error {
	if offset < 0 {
		return fmt.Errorf("%w: negative offset", ErrOutOfRange)
	}
	s.mgr.mu.Lock()
	defer s.mgr.mu.Unlock()
	if s.mgr.closed {
		return ErrClosed
	}
	if err := s.requireActiveLocked(); err != nil {
		return err
	}
	if offset+len(data) > s.length {
		return fmt.Errorf("%w: [%d,%d) beyond length %d",
			ErrOutOfRange, offset, offset+len(data), s.length)
	}
	if len(data) == 0 {
		return nil
	}
	t := s.mgr.tenants[s.tenant]
	bs := s.mgr.cfg.BlockBytes

	first := offset / bs
	last := (offset + len(data) - 1) / bs
	replacements := map[int]*physicalBlock{}
	for i := first; i <= last; i++ {
		b := t.resident[s.blocks[i]]
		blockStart := i * bs
		lo := offset - blockStart
		if lo < 0 {
			lo = 0
		}
		hi := offset + len(data) - blockStart
		if hi > bs {
			hi = bs
		}
		srcOff := blockStart + lo - offset
		if b.refs == 1 && !b.cached {
			copy(b.payload[lo:hi], data[srcOff:srcOff+(hi-lo)])
			newHash := hashPayload(b.payload)
			s.mgr.reindexHashLocked(t, b, newHash)
			s.hashes[i] = newHash
			continue
		}
		payload := make([]byte, bs)
		copy(payload, b.payload)
		copy(payload[lo:hi], data[srcOff:srcOff+(hi-lo)])
		nb, err := s.mgr.allocBlockLocked(t, payload, s.id)
		if err != nil {
			return err
		}
		replacements[i] = nb
	}
	indices := make([]int, 0, len(replacements))
	for i := range replacements {
		indices = append(indices, i)
	}
	sort.Ints(indices)
	for _, i := range indices {
		old := t.resident[s.blocks[i]]
		s.blocks[i] = replacements[i].id
		s.hashes[i] = replacements[i].hash
		s.mgr.releaseRefLocked(t, old, s.id)
	}
	return nil
}

// Read returns a freshly allocated copy of [offset, offset+length). It never
// aliases block memory, so later eviction cannot mutate the result.
func (s *Sequence) Read(offset, length int) ([]byte, error) {
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("%w: negative range", ErrOutOfRange)
	}
	s.mgr.mu.Lock()
	defer s.mgr.mu.Unlock()
	if s.mgr.closed {
		return nil, ErrClosed
	}
	if offset+length > s.length {
		return nil, fmt.Errorf("%w: [%d,%d) beyond length %d",
			ErrOutOfRange, offset, offset+length, s.length)
	}
	switch s.state {
	case seqAborted:
		return nil, fmt.Errorf("%w: sequence %q", ErrSequenceAborted, s.id)
	case seqNeedsRecompute:
		return nil, fmt.Errorf("%w: sequence %q", ErrNeedsRecompute, s.id)
	case seqSwapped:
		return nil, fmt.Errorf("%w: sequence %q is swapped out; call Resume",
			ErrSequencePreempted, s.id)
	}
	t := s.mgr.tenants[s.tenant]
	bs := s.mgr.cfg.BlockBytes
	out := make([]byte, length)
	dstOff := 0
	remaining := length
	pos := offset
	for remaining > 0 {
		i := pos / bs
		inBlock := pos % bs
		take := bs - inBlock
		if take > remaining {
			take = remaining
		}
		var src []byte
		if i < len(s.blocks) {
			b := t.resident[s.blocks[i]]
			if b != nil {
				src = b.payload[inBlock : inBlock+take]
				s.mgr.touch(b)
			}
		}
		if src == nil {
			// Committed (or restored-then-evicted) content is paged in
			// straight from the checksummed content store.
			if i >= len(s.hashes) {
				return nil, fmt.Errorf("%w: sequence %q missing block %d",
					ErrCorrupt, s.id, i)
			}
			scratch := make([]byte, bs)
			if err := s.mgr.store.get(s.hashes[i], scratch); err != nil {
				return nil, err
			}
			src = scratch[inBlock : inBlock+take]
		}
		copy(out[dstOff:dstOff+take], src)
		dstOff += take
		remaining -= take
		pos += take
	}
	return out, nil
}

// Commit publishes the sequence: its full block chain is persisted, its pages
// become reusable prefix cache and later requests may share the same prefix.
// A committed sequence is immutable.
func (s *Sequence) Commit() error {
	s.mgr.mu.Lock()
	defer s.mgr.mu.Unlock()
	if s.mgr.closed {
		return ErrClosed
	}
	switch s.state {
	case seqCommitted:
		return nil
	case seqSwapped:
		return fmt.Errorf("%w: resume sequence %q before commit", ErrSequencePreempted, s.id)
	case seqNeedsRecompute:
		return fmt.Errorf("%w: sequence %q", ErrNeedsRecompute, s.id)
	case seqAborted:
		return fmt.Errorf("%w: sequence %q", ErrSequenceAborted, s.id)
	}
	t := s.mgr.tenants[s.tenant]
	hashes := make([]string, len(s.blocks))
	for i, bid := range s.blocks {
		b := t.resident[bid]
		h, err := s.mgr.store.put(b.payload)
		if err != nil {
			return err
		}
		hashes[i] = h
	}
	if s.mgr.journal != nil {
		if err := s.mgr.journal.append(journalRecord{
			Type: recCommit, Tenant: s.tenant, Sequence: s.id,
			Length: s.length, Hashes: hashes,
		}); err != nil {
			return err
		}
	}
	for _, bid := range s.blocks {
		b := t.resident[bid]
		b.published = true
		s.mgr.releaseRefLocked(t, b, s.id)
	}
	s.hashes = hashes
	s.blocks = nil
	s.state = seqCommitted
	t.cache.insert(hashes)
	return nil
}

// Abort discards an uncommitted or preempted sequence. Unpublished pages are
// returned to the pool immediately (no fragment buildup); durable state is
// tombstoned in the journal.
func (s *Sequence) Abort() error {
	s.mgr.mu.Lock()
	defer s.mgr.mu.Unlock()
	return s.mgr.abortLocked(s)
}

// Preempt swaps an active sequence out under memory pressure; the full image
// is durable and the sequence can later be resumed or (on failure) recomputed.
func (s *Sequence) Preempt() error {
	s.mgr.mu.Lock()
	defer s.mgr.mu.Unlock()
	if s.mgr.closed {
		return ErrClosed
	}
	if s.state == seqSwapped {
		return nil
	}
	if s.state != seqActive {
		return s.requireActiveLocked()
	}
	return s.mgr.swapSequenceLocked(s)
}

// Resume brings a swapped-out sequence back to active state, reusing resident
// pages with identical content and paging the rest in from the content store.
// If the stored image is missing or corrupt the sequence is permanently marked
// needs-recompute and ErrNeedsRecompute is returned; no partial content is ever
// exposed.
func (s *Sequence) Resume() error {
	s.mgr.mu.Lock()
	defer s.mgr.mu.Unlock()
	if s.mgr.closed {
		return ErrClosed
	}
	if s.state == seqActive {
		return nil
	}
	if s.state != seqSwapped {
		return s.requireActiveLocked()
	}
	t := s.mgr.tenants[s.tenant]
	sw, ok := t.swapped[s.id]
	if !ok {
		s.state = seqNeedsRecompute
		return fmt.Errorf("%w: sequence %q swap image missing", ErrNeedsRecompute, s.id)
	}
	newBlocks := make([]int, 0, len(sw.hashes))
	claimed := make([]*physicalBlock, 0, len(sw.hashes))
	for _, h := range sw.hashes {
		var b *physicalBlock
		if cached := t.findByHash(h); cached != nil {
			if cached.cached {
				t.markActive(cached)
			}
			cached.refs++
			t.addOwner(cached.id, s.id)
			s.mgr.touch(cached)
			b = cached
		} else {
			nb, err := s.mgr.loadHashLocked(t, h, s.id)
			if err != nil {
				for _, c := range claimed {
					s.mgr.releaseRefLocked(t, c, s.id)
				}
				s.state = seqNeedsRecompute
				if s.mgr.journal != nil {
					_ = s.mgr.journal.append(journalRecord{
						Type: recAbort, Tenant: s.tenant, Sequence: s.id,
						Reason: "resume-corrupt",
					})
				}
				if isCorruptOrMissing(err) {
					return fmt.Errorf("%w: sequence %q: %v", ErrNeedsRecompute, s.id, err)
				}
				return err
			}
			b = nb
		}
		claimed = append(claimed, b)
		newBlocks = append(newBlocks, b.id)
	}
	delete(t.swapped, s.id)
	s.blocks = newBlocks
	s.hashes = sw.hashes
	s.state = seqActive
	if s.mgr.journal != nil {
		_ = s.mgr.journal.append(journalRecord{
			Type: recRestore, Tenant: s.tenant, Sequence: s.id,
			Length: s.length, Hashes: s.hashes,
		})
	}
	return nil
}

func isCorruptOrMissing(err error) bool {
	return errorIs(err, ErrCorrupt) || errorIs(err, ErrNotFound)
}

func errorIs(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
