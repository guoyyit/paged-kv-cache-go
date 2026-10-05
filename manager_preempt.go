package pagedkv

import "errors"

// preemptLocked swaps every resident block of an active sequence to durable
// storage and detaches it from physical memory. The sequence becomes
// seqPreempted: its complete state lives in (swap entries -> store keys), so
// a later resume restores byte-for-byte identical content.
//
// Without a durable store, full preemption is impossible; the sequence is
// marked seqInvalid instead, and callers receive ErrNeedsRecompute. The
// manager never returns partially available content.
func (m *Manager) preemptLocked(seq *Sequence) error {
	if seq.state != seqActive {
		return nil
	}
	if m.store == nil {
		m.markInvalidLocked(seq)
		return ErrNeedsRecompute
	}

	B := m.cfg.BlockSize
	entries := make([]swapEntry, len(seq.refs))
	for i := range seq.refs {
		ref := &seq.refs[i]
		blk := m.blocks[ref.blockID]
		size := B
		if blk.key == nil {
			// Private tail: only the logical bytes matter.
			size = seq.length - i*B
			if size <= 0 {
				size = B
			}
		}

		if blk.key != nil {
			// Shared full block: ensure durable bytes, then drop our ref.
			key := blk.key
			if err := m.putStored(key, blk.data); err != nil {
				m.markInvalidLocked(seq)
				return err
			}
			entries[i] = swapEntry{key: append([]byte(nil), key...), size: size}
			blk.refs--
			if blk.refs <= 0 {
				// Last user: reclaim immediately (it can be refetched from
				// the store by any sequence sharing the same key).
				delete(m.keyIndex, string(blk.key))
				blk.state = blockFree
				blk.data = nil
				blk.key = nil
				blk.refs = 0
				blk.lastUsed = 0
				m.freeList = append(m.freeList, blk.id)
			}
		} else {
			// Private (possibly partial) tail block: store by content hash.
			key := contentKey(blk.data[:size])
			if err := m.putStored(key, blk.data[:size]); err != nil {
				m.markInvalidLocked(seq)
				return err
			}
			entries[i] = swapEntry{key: key, size: size}
			m.freePrivateLocked(blk.id, seq.tenant)
		}
		ref.blockID = swappedBlock
	}

	seq.swapped = entries
	seq.state = seqPreempted
	return nil
}

// putStored writes data for key if the manager does not know the key to be
// durably present yet.
func (m *Manager) putStored(key, data []byte) error {
	if m.storedKeys[string(key)] {
		return nil
	}
	if err := m.store.Put(key, data); err != nil {
		return err
	}
	m.storedKeys[string(key)] = true
	return nil
}

// resumeLocked reloads a preempted sequence's blocks in order, restoring it
// to seqActive with byte-identical content. If any swapped content is missing
// or corrupt, the sequence becomes seqInvalid and ErrNeedsRecompute is
// returned; nothing partial is exposed.
func (m *Manager) resumeLocked(seq *Sequence) error {
	if seq.state != seqPreempted {
		return nil
	}
	if m.store == nil || len(seq.swapped) == 0 {
		// Zero-length sequences have no blocks to restore.
		if seq.length == 0 {
			seq.state = seqActive
			return nil
		}
		m.markInvalidLocked(seq)
		return ErrNeedsRecompute
	}

	B := m.cfg.BlockSize
	t, err := m.tenantLocked(seq.tenant)
	if err != nil {
		return err
	}

	refs := make([]blockRef, len(seq.swapped))
	// bound tracks every ref acquired during this attempt (both newly
	// allocated blocks and existing shared blocks) so rollback is complete.
	type binding struct {
		id     int
		shared bool
	}
	var bound []binding
	rollback := func() {
		for _, b := range bound {
			blk := m.blocks[b.id]
			if b.shared {
				blk.refs--
				continue
			}
			m.freePrivateLocked(b.id, seq.tenant)
		}
	}

	// Reconstruct chained keys as we walk; the store key of the private
	// tail is a raw content hash, not a chained key.
	var prev []byte
	for i, ent := range seq.swapped {
		data, gerr := m.store.Get(ent.key)
		if gerr != nil {
			if errors.Is(gerr, ErrCorrupt) || errors.Is(gerr, ErrNotFound) {
				rollback()
				m.markInvalidLocked(seq)
				return ErrNeedsRecompute
			}
			rollback()
			return gerr
		}
		if len(data) < ent.size || (i < len(seq.swapped)-1 && len(data) != B) {
			rollback()
			m.markInvalidLocked(seq)
			return ErrNeedsRecompute
		}

		isTail := i == len(seq.swapped)-1 && ent.size < B
		if !isTail {
			// Shared full block: validate the chained key, then dedup.
			wantKey := chainKey(prev, data[:B])
			if !keyEqual(wantKey, ent.key) {
				rollback()
				m.markInvalidLocked(seq)
				return ErrNeedsRecompute
			}
			if existingID, ok := m.keyIndex[string(wantKey)]; ok {
				shared := m.blocks[existingID]
				shared.refs++
				refs[i] = blockRef{blockID: existingID, key: append([]byte(nil), wantKey...)}
				bound = append(bound, binding{id: existingID, shared: true})
				prev = wantKey
				m.tick++
				shared.lastUsed = m.tick
				continue
			}
			id, aerr := m.reclaimOneLocked(seq)
			if aerr != nil {
				rollback()
				if errors.Is(aerr, ErrOutOfMemory) {
					return aerr
				}
				return aerr
			}
			blk := m.blocks[id]
			blk.state = blockResident
			blk.data = make([]byte, B)
			copy(blk.data, data[:B])
			blk.key = append([]byte(nil), wantKey...)
			blk.refs = 1
			blk.tenant = ""
			m.tick++
			blk.lastUsed = m.tick
			m.keyIndex[string(wantKey)] = id
			m.storedKeys[string(wantKey)] = true
			refs[i] = blockRef{blockID: id, key: append([]byte(nil), wantKey...)}
			bound = append(bound, binding{id: id, shared: true})
			prev = wantKey
			continue
		}

		// Private tail block. Quota is charged only after the block lands;
		// a preemption triggered by global pressure here may only pick
		// sequences other than the one resuming (excluded inside reclaim),
		// and reclaimOneLocked itself never steals another tenant's private
		// quota — if the pool is exhausted by same-tenant tails and none can
		// be swapped, allocation fails loudly.
		if t.atQuota() {
			rollback()
			return ErrQuotaExceeded
		}
		id, aerr := m.reclaimOneLocked(seq)
		if aerr != nil {
			rollback()
			return aerr
		}
		blk := m.blocks[id]
		blk.state = blockResident
		blk.data = make([]byte, B)
		copy(blk.data, data[:ent.size])
		blk.key = nil
		blk.refs = 0
		blk.tenant = seq.tenant
		t.privateUsed++
		refs[i] = blockRef{blockID: id}
		bound = append(bound, binding{id: id, shared: false})
	}

	seq.refs = refs
	seq.swapped = nil
	seq.state = seqActive
	return nil
}

// markInvalidLocked flags the sequence as needing recomputation and releases
// any physical blocks it still owned.
func (m *Manager) markInvalidLocked(seq *Sequence) {
	if seq.state == seqActive {
		for i := range seq.refs {
			ref := &seq.refs[i]
			if ref.blockID < 0 {
				continue
			}
			blk := m.blocks[ref.blockID]
			if blk.key != nil {
				blk.refs--
			} else {
				m.freePrivateLocked(blk.id, seq.tenant)
			}
			ref.blockID = swappedBlock
		}
	}
	seq.state = seqInvalid
	seq.swapped = nil
	seq.refs = nil
}
