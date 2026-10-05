package pagedkv

import (
	"os"
	"path/filepath"
)

// CommitSequence durably records the sequence's block mapping after ensuring
// every block's content is in the swap store. On return the mapping is
// aligned with swapped data: a later Recover can restore the sequence
// entirely from disk.
//
// The record is made visible with a two-phase replacement: journal.log.tmp is
// fsynced and atomically renamed over journal.log. A crash therefore exposes
// either the old complete mapping or the new complete mapping, never a
// partially written one.
func (m *Manager) CommitSequence(seq *Sequence) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, ok := m.sequences[seq.id]
	if !ok || cur != seq {
		return ErrSequenceNotFound
	}
	if err := m.ensureActive(seq); err != nil {
		return err
	}
	if m.store == nil || m.jrn == nil {
		return ErrNoPersistence
	}

	B := m.cfg.BlockSize
	jb := make([]journalBlock, len(seq.refs))
	for i := range seq.refs {
		ref := &seq.refs[i]
		blk := m.blocks[ref.blockID]
		if blk.key != nil {
			if err := m.putStored(blk.key, blk.data); err != nil {
				return err
			}
			jb[i] = journalBlock{key: append([]byte(nil), blk.key...), size: B}
			continue
		}
		// Private tail: only the logical bytes are meaningful.
		size := seq.length - i*B
		if size <= 0 {
			size = B
		}
		key := contentKey(blk.data[:size])
		if err := m.putStored(key, blk.data[:size]); err != nil {
			return err
		}
		jb[i] = journalBlock{key: key, size: size}
	}

	js := &journalSequence{
		tenant:   seq.tenant,
		id:       seq.id,
		priority: seq.priority,
		order:    seq.order,
		blocks:   jb,
	}
	if err := m.rewriteJournalLocked(func(j *journal) error {
		if err := j.appendFrame(recSequence, encodeSequenceRecord(js)); err != nil {
			return err
		}
		return j.appendFrame(recCheckpoint, nil)
	}); err != nil {
		return err
	}
	seq.committed = true
	seq.lastCommit = jb
	return nil
}

// rewriteJournalLocked atomically replaces journal.log: it copies all
// currently committed mappings, then runs addExtra to append new records
// (typically the new commit followed by a checkpoint), fsyncs and renames.
func (m *Manager) rewriteJournalLocked(addExtra func(j *journal) error) error {
	dir := m.cfg.DataDir
	old := journalPath(dir)
	tmpName := filepath.Join(dir, "journal.log.tmp")

	_ = os.Remove(tmpName)
	f, err := os.OpenFile(tmpName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	j := &journal{dir: dir, f: f, w: newBufWriter(f)}
	if _, err := j.w.WriteString(journalMagic); err != nil {
		f.Close()
		_ = os.Remove(tmpName)
		return err
	}

	// Snapshot existing committed mappings in deterministic (creation) order.
	seqs := make([]*Sequence, 0, len(m.sequences))
	for _, s := range m.sequences {
		if s.committed {
			seqs = append(seqs, s)
		}
	}
	sortByOrder(seqs)
	for _, s := range seqs {
		js := sequenceToJournalLocked(s)
		if js == nil {
			continue
		}
		if err := j.appendFrame(recSequence, encodeSequenceRecord(js)); err != nil {
			f.Close()
			_ = os.Remove(tmpName)
			return err
		}
	}
	if err := addExtra(j); err != nil {
		f.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := j.sync(); err != nil {
		f.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, old); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	// Swap the manager's handle so subsequent appends target the new file.
	if m.jrn != nil {
		_ = m.jrn.close()
	}
	nj, err := openJournal(dir)
	if err != nil {
		return err
	}
	m.jrn = nj
	return nil
}

// sequenceToJournalLocked rebuilds the journal record for a previously
// committed sequence from its last committed block mapping.
func sequenceToJournalLocked(seq *Sequence) *journalSequence {
	if !seq.committed {
		return nil
	}
	return &journalSequence{
		tenant:   seq.tenant,
		id:       seq.id,
		priority: seq.priority,
		order:    seq.order,
		blocks:   seq.lastCommit,
	}
}
