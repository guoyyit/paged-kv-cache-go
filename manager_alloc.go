package pagedkv

// ensureActive restores a preempted sequence before a mutating/reading op.
// If restoration is impossible the sequence is marked invalid.
func (m *Manager) ensureActive(seq *Sequence) error {
	switch seq.state {
	case seqActive:
		return nil
	case seqInvalid:
		return ErrNeedsRecompute
	case seqPreempted:
		return m.resumeLocked(seq)
	}
	return ErrNeedsRecompute
}

// popFreeLocked takes a free block id from the pool (caller holds lock).
func (m *Manager) popFreeLocked() (int, bool) {
	for len(m.freeList) > 0 {
		id := m.freeList[len(m.freeList)-1]
		m.freeList = m.freeList[:len(m.freeList)-1]
		blk := m.blocks[id]
		if blk.state == blockFree {
			return id, true
		}
	}
	return -1, false
}

// reclaimOneLocked obtains one free block for requester. Order of attempts:
//  1. the existing free list
//  2. LRU eviction of an unreferenced shared block (content remains on disk
//     if the store exists, so it can be re-fetched by key)
//  3. preemptive swap-out of another active sequence, freeing all its blocks
//
// It returns ErrOutOfMemory only when no victim exists.
func (m *Manager) reclaimOneLocked(requester *Sequence) (int, error) {
	if id, ok := m.popFreeLocked(); ok {
		return id, nil
	}
	if id := m.evictPol.pickVictimBlock(m); id >= 0 {
		m.evictSharedLocked(id)
		return m.popFreeOrPanic()
	}
	if victimID := m.preemptPol.pickVictimSequence(m, requester.id); victimID != "" {
		victim := m.sequences[victimID]
		if err := m.preemptLocked(victim); err != nil {
			return 0, err
		}
		if id, ok := m.popFreeLocked(); ok {
			return id, nil
		}
	}
	return 0, ErrOutOfMemory
}

func (m *Manager) popFreeOrPanic() (int, error) {
	if id, ok := m.popFreeLocked(); ok {
		return id, nil
	}
	return 0, ErrOutOfMemory
}

// evictSharedLocked drops an unreferenced shared block from memory. Its
// content key remains in the global key index only while resident; if a
// durable store exists the bytes were already written when the block
// finalized, so the cache entry can be repopulated transparently.
func (m *Manager) evictSharedLocked(id int) {
	blk := m.blocks[id]
	if blk.refs != 0 || blk.key == nil {
		return
	}
	delete(m.keyIndex, string(blk.key))
	blk.state = blockFree
	blk.data = nil
	blk.key = nil
	blk.refs = 0
	blk.lastUsed = 0
	blk.tenant = ""
	m.freeList = append(m.freeList, id)
}
