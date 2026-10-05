package pagedkv

import (
	"fmt"
	"sort"
)

// RegisterTenant registers a tenant with a private-block quota. A zero or
// negative quota is rejected (a tenant must be able to make progress).
func (m *Manager) RegisterTenant(id string, quotaBlocks int) error {
	if id == "" || quotaBlocks <= 0 {
		return ErrInvalidArgument
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tenants[id]; ok {
		return ErrTenantExists
	}
	m.tenants[id] = &tenant{id: id, quotaBlocks: quotaBlocks}
	return nil
}

// tenantLocked returns the tenant or ErrTenantNotFound (caller holds lock).
func (m *Manager) tenantLocked(id string) (*tenant, error) {
	t, ok := m.tenants[id]
	if !ok {
		return nil, ErrTenantNotFound
	}
	return t, nil
}

// NewSequence creates a request sequence owned by tenantID.
func (m *Manager) NewSequence(tenantID, seqID string, priority int) (*Sequence, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, err := m.tenantLocked(tenantID); err != nil {
		return nil, err
	}
	if seqID == "" {
		// Auto-generate a unique, deterministic-enough identifier.
		seqID = fmt.Sprintf("seq-%d", m.orderSeq+1)
	}
	if _, dup := m.sequences[seqID]; dup {
		return nil, ErrSequenceExists
	}
	m.orderSeq++
	seq := &Sequence{
		tenant:   tenantID,
		id:       seqID,
		priority: priority,
		state:    seqActive,
		order:    m.orderSeq,
	}
	m.sequences[seqID] = seq
	return seq, nil
}

// ReleaseSequence frees a sequence's resident blocks and drops its refs.
// Private blocks return to the free pool; shared blocks have their reference
// counts decremented (reclaimable once zero). Swapped entries are left in the
// store until their owning committed mapping is superseded/removed — releasing
// an uncommitted sequence simply removes its swap entries.
func (m *Manager) ReleaseSequence(seq *Sequence) {
	if seq == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseSequenceLocked(seq, true)
}

// releaseSequenceLocked detaches a sequence from the manager. If journal is
// true and the mapping was committed, a release record is appended.
func (m *Manager) releaseSequenceLocked(seq *Sequence, persist bool) {
	cur, ok := m.sequences[seq.id]
	if !ok || cur != seq {
		// Already released (possibly replaced on recovery); treat as no-op.
		return
	}
	t, ok := m.tenants[seq.tenant]
	if !ok {
		t = nil
	}
	if seq.state == seqActive {
		for i := range seq.refs {
			ref := &seq.refs[i]
			if ref.blockID < 0 {
				continue
			}
			blk := m.blocks[ref.blockID]
			if blk.key == nil {
				// private tail block
				blk.state = blockFree
				blk.data = nil
				blk.tenant = ""
				if t != nil {
					t.privateUsed--
				}
				m.freeList = append(m.freeList, blk.id)
			} else {
				blk.refs--
			}
			ref.blockID = -1
		}
	}
	wasCommitted := seq.committed
	delete(m.sequences, seq.id)

	if persist && m.jrn != nil && wasCommitted {
		// Atomically rewrite the journal without the released mapping, so
		// the durable set stays exactly the set of committed sequences.
		_ = m.rewriteJournalLocked(func(j *journal) error {
			return j.appendFrame(recCheckpoint, nil)
		})
	}
}

// snapshotTenantIDs returns sorted tenant ids (deterministic diagnostics).
func (m *Manager) snapshotTenantIDs() []string {
	ids := make([]string, 0, len(m.tenants))
	for id := range m.tenants {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
