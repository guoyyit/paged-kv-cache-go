package pagedkv

// Append writes data to the end of a sequence.
//
// Semantics:
//   - Bytes fill the current private tail block; once a block is full it is
//     finalized: its chained content key identifies it and it joins the
//     shared, immutable cache.
//   - Equal prefixes finalize to equal keys and bind to the same physical
//     block (reference counted). Appending different data after a shared
//     point allocates a new private block, so a writer can never modify
//     content another request has already seen (copy-on-write).
//   - Private tail blocks are charged to the owning tenant's quota;
//     exceeding the quota fails with ErrQuotaExceeded and never reclaims
//     another tenant's blocks.
func (m *Manager) Append(seq *Sequence, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if cur, ok := m.sequences[seq.id]; !ok || cur != seq {
		return ErrSequenceNotFound
	}
	if err := m.ensureActive(seq); err != nil {
		return err
	}

	B := m.cfg.BlockSize
	pos := 0
	for pos < len(data) {
		// Ensure a writable private tail block exists.
		if seq.length == 0 || seq.length%B == 0 {
			if err := m.allocPrivateTailLocked(seq); err != nil {
				return err
			}
		}
		ref := &seq.refs[len(seq.refs)-1]
		blk := m.blocks[ref.blockID]
		off := seq.length % B
		n := copy(blk.data[off:], data[pos:])
		seq.length += n
		pos += n
		if off+n == B {
			if err := m.finalizeLocked(seq, len(seq.refs)-1); err != nil {
				return err
			}
		}
	}
	return nil
}

// allocPrivateTailLocked allocates one physical block as a new private tail
// for seq, enforcing the tenant quota and global memory pressure.
func (m *Manager) allocPrivateTailLocked(seq *Sequence) error {
	t, err := m.tenantLocked(seq.tenant)
	if err != nil {
		return err
	}
	if t.atQuota() {
		// Memory pressure within the tenant's own quota may preempt one of
		// the tenant's own sequences; other tenants are never touched.
		if !m.preemptSameTenantLocked(seq) {
			return ErrQuotaExceeded
		}
		if t.atQuota() {
			return ErrQuotaExceeded
		}
	}
	id, err := m.reclaimOneLocked(seq)
	if err != nil {
		return err
	}
	blk := m.blocks[id]
	blk.state = blockResident
	blk.data = make([]byte, m.cfg.BlockSize)
	blk.tenant = seq.tenant
	blk.key = nil
	blk.refs = 0
	t.privateUsed++
	seq.refs = append(seq.refs, blockRef{blockID: id})
	return nil
}

// preemptSameTenantLocked swaps out one other active sequence of the same
// tenant to release its private tail quota. Reports whether any progress was
// made.
func (m *Manager) preemptSameTenantLocked(requester *Sequence) bool {
	var victimID string
	var bestOrder uint64
	found := false
	for id, s := range m.sequences {
		if s.tenant != requester.tenant || s == requester || s.state != seqActive {
			continue
		}
		if !found || s.order < bestOrder || (s.order == bestOrder && id < victimID) {
			found = true
			victimID = id
			bestOrder = s.order
		}
	}
	if !found {
		return false
	}
	return m.preemptLocked(m.sequences[victimID]) == nil
}

// finalizeLocked turns the full private block at index i into a shared block
// keyed by its chained content, deduping against an existing resident block.
func (m *Manager) finalizeLocked(seq *Sequence, i int) error {
	ref := &seq.refs[i]
	blk := m.blocks[ref.blockID]
	var prev []byte
	if i > 0 {
		prev = seq.refs[i-1].key
	}
	key := chainKey(prev, blk.data)

	if existingID, ok := m.keyIndex[string(key)]; ok {
		// Identical prefix block already resident: drop our private copy
		// and reference the shared one.
		m.freePrivateLocked(ref.blockID, seq.tenant)
		shared := m.blocks[existingID]
		shared.refs++
		m.tick++
		shared.lastUsed = m.tick
		ref.blockID = existingID
		ref.key = key
		return nil
	}

	// Promote the private block to shared; the finalizing sequence holds
	// the one logical reference.
	t, _ := m.tenantLocked(seq.tenant)
	if t != nil {
		t.privateUsed--
	}
	blk.key = key
	blk.tenant = ""
	blk.refs = 1
	m.tick++
	blk.lastUsed = m.tick
	m.keyIndex[string(key)] = blk.id
	ref.key = key

	// Durable, content-addressed copy; failure does not lose memory state,
	// only persistence/swap capability.
	if m.store != nil && !m.storedKeys[string(key)] {
		if err := m.store.Put(key, blk.data); err == nil {
			m.storedKeys[string(key)] = true
		}
	}
	return nil
}

// freePrivateLocked returns a private block to the pool and releases quota.
func (m *Manager) freePrivateLocked(id int, tenantID string) {
	blk := m.blocks[id]
	blk.state = blockFree
	blk.data = nil
	blk.key = nil
	blk.tenant = ""
	blk.refs = 0
	m.freeList = append(m.freeList, id)
	if t, ok := m.tenants[tenantID]; ok {
		t.privateUsed--
	}
}
