package pagedkv

// Fork creates a new sequence that shares seq's existing blocks.
//
// Finalized (shared) blocks are bound by reference count and never copied;
// only the active private tail is copied immediately so that either branch can
// append or rewrite it without mutating bytes the other branch has observed.
// The fork belongs to the same tenant and is charged for its one private tail.
func (m *Manager) Fork(seq *Sequence, newID string) (*Sequence, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, ok := m.sequences[seq.id]
	if !ok || cur != seq {
		return nil, ErrSequenceNotFound
	}
	if err := m.ensureActive(seq); err != nil {
		return nil, err
	}
	if newID == "" {
		m.orderSeq++
		newID = "seq-" + itoa(m.orderSeq+1)
	}
	if _, dup := m.sequences[newID]; dup {
		return nil, ErrSequenceExists
	}

	t, err := m.tenantLocked(seq.tenant)
	if err != nil {
		return nil, err
	}
	B := m.cfg.BlockSize
	fork := &Sequence{
		tenant:   seq.tenant,
		id:       newID,
		priority: seq.priority,
		state:    seqActive,
		length:   seq.length,
	}
	refs := make([]blockRef, len(seq.refs))
	for i := range seq.refs {
		src := &seq.refs[i]
		blk := m.blocks[src.blockID]
		if blk.key != nil {
			blk.refs++
			refs[i] = blockRef{blockID: src.blockID, key: append([]byte(nil), src.key...)}
			continue
		}
		// Private tail: copy-on-write.
		if t.atQuota() {
			if !m.preemptSameTenantLocked(fork) && t.atQuota() {
				return nil, ErrQuotaExceeded
			}
			if t.atQuota() {
				return nil, ErrQuotaExceeded
			}
		}
		id, aerr := m.reclaimOneLocked(fork)
		if aerr != nil {
			return nil, aerr
		}
		nb := m.blocks[id]
		nb.state = blockResident
		nb.data = make([]byte, B)
		copy(nb.data, blk.data)
		nb.tenant = seq.tenant
		nb.key = nil
		nb.refs = 0
		t.privateUsed++
		refs[i] = blockRef{blockID: id}
	}
	m.orderSeq++
	fork.order = m.orderSeq
	fork.refs = refs
	m.sequences[newID] = fork
	return fork, nil
}

// itoa avoids pulling strconv just for generated ids.
func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
