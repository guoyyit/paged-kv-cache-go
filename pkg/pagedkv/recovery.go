package pagedkv

import "fmt"

// reindexHashLocked updates the tenant's content-hash index after a private
// block is mutated in place. The previous hash mapping is only removed while
// it still points at this block, so shared dedup entries are never broken.
func (m *Manager) reindexHashLocked(t *tenant, b *physicalBlock, newHash string) {
	if b.hash == newHash {
		return
	}
	if id, ok := t.byHash[b.hash]; ok && id == b.id {
		delete(t.byHash, b.hash)
	}
	b.hash = newHash
	t.byHash[newHash] = b.id
}

// abortLocked discards a sequence in any state and releases everything it owns.
func (m *Manager) abortLocked(s *Sequence) error {
	if m.closed {
		return ErrClosed
	}
	if s.state == seqAborted {
		return nil
	}
	t := m.tenants[s.tenant]
	if s.state == seqActive {
		for _, bid := range s.blocks {
			b := t.resident[bid]
			if b == nil {
				continue
			}
			m.releaseRefLocked(t, b, s.id)
		}
		s.blocks = nil
	}
	if s.state == seqSwapped {
		delete(t.swapped, s.id)
	}
	s.hashes = nil
	s.state = seqAborted
	delete(m.seqs, s.id)
	delete(t.sequences, s.id)
	if m.journal != nil {
		if err := m.journal.append(journalRecord{
			Type: recAbort, Tenant: s.tenant, Sequence: s.id,
		}); err != nil {
			return err
		}
	}
	return nil
}

// recoverLocked replays the journal into a cold manager. The block pool starts
// empty; durable blocks are content-addressed and are paged back lazily on
// first access. A durable chain whose object is missing or fails its checksum
// is marked needs-recompute rather than served as a valid cache, and a bad
// prefix is never inserted into the tenant's cache trie.
func (m *Manager) recoverLocked() error {
	// Durable sequence state keyed by sequence id, applied in log order.
	type durable struct {
		tenant string
		state  seqState
		length int
		hashes []string
	}
	durableSeqs := map[string]*durable{}

	_, err := m.journal.scan(func(rec journalRecord) bool {
		switch rec.Type {
		case recTenant:
			if rec.Quota < 0 {
				delete(m.tenants, rec.Tenant)
				return true
			}
			if _, ok := m.tenants[rec.Tenant]; !ok {
				quota := rec.Quota
				if quota == 0 || quota > m.cfg.TotalBlocks {
					quota = m.cfg.DefaultTenantQuota
				}
				m.tenants[rec.Tenant] = newTenant(rec.Tenant, quota, newLRUPolicy())
			}
		case recCommit, recSwap, recRestore:
			durableSeqs[rec.Sequence] = &durable{
				tenant: rec.Tenant,
				state:  stateForRecord(rec.Type),
				length: rec.Length,
				hashes: append([]string(nil), rec.Hashes...),
			}
		case recAbort:
			delete(durableSeqs, rec.Sequence)
		}
		return true
	})
	if err != nil {
		return err
	}

	for id, d := range durableSeqs {
		t, ok := m.tenants[d.tenant]
		if !ok {
			// Tenant was deleted; content is unreferenced and GC'd below.
			m.corrupt++
			continue
		}
		complete := true
		if len(d.hashes) == 0 && d.length != 0 {
			complete = false
		}
		// Every persisted block must both exist and pass its checksum. A
		// single bad block invalidates the whole chain: it is safer to
		// recompute the request than to ever return partially valid content.
		for _, h := range d.hashes {
			scratch := make([]byte, m.cfg.BlockBytes)
			if err := m.store.get(h, scratch); err != nil {
				complete = false
				break
			}
		}
		s := &Sequence{
			mgr:    m,
			tenant: d.tenant,
			id:     id,
			length: d.length,
			hashes: append([]string(nil), d.hashes...),
		}
		t.sequences[id] = struct{}{}
		m.seqs[id] = s
		if !complete {
			s.state = seqNeedsRecompute
			m.corrupt++
			continue
		}
		switch d.state {
		case seqCommitted:
			s.state = seqCommitted
			t.cache.insert(d.hashes)
		default:
			// Swapped before crash (or restore in flight): stay preempted and
			// wait for an explicit Resume, which re-verifies every object.
			s.state = seqSwapped
			t.swapped[id] = &swappedSequence{
				id: id, length: d.length, hashes: append([]string(nil), d.hashes...),
			}
			t.cache.insert(d.hashes)
		}
	}
	return nil
}

func stateForRecord(rt journalRecordType) seqState {
	switch rt {
	case recCommit:
		return seqCommitted
	default:
		return seqSwapped
	}
}

var _ = fmt.Sprintf
