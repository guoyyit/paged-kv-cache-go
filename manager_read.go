package pagedkv

// Read copies length bytes starting at offset out of seq.
//
// Failures carry a distinguishable reason:
//   - ErrSequenceNotFound: the sequence was released/never existed
//   - ErrUnauthorized: a caller passes a Sequence object owned by a tenant
//     that the manager no longer recognizes (cross-tenant guard)
//   - ErrNeedsRecompute: the sequence is preempted but its swapped state
//     cannot be fully restored
//   - ErrOutOfBounds: offset/length exceeds the written region
func (m *Manager) Read(seq *Sequence, offset, length int) ([]byte, error) {
	if offset < 0 || length < 0 {
		return nil, ErrOutOfBounds
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, ok := m.sequences[seq.id]
	if !ok || cur != seq {
		return nil, ErrSequenceNotFound
	}
	if _, ok := m.tenants[seq.tenant]; !ok {
		return nil, ErrUnauthorized
	}
	if err := m.ensureActive(seq); err != nil {
		return nil, err
	}
	if offset > seq.length || offset+length > seq.length {
		return nil, ErrOutOfBounds
	}
	if length == 0 {
		return []byte{}, nil
	}

	B := m.cfg.BlockSize
	out := make([]byte, length)
	dst := 0
	firstBlock := offset / B
	lastBlock := (offset + length - 1) / B
	for bi := firstBlock; bi <= lastBlock; bi++ {
		ref := &seq.refs[bi]
		blk := m.blocks[ref.blockID]
		// Only the final logical block can be a partial private block.
		start := 0
		if bi == firstBlock {
			start = offset % B
		}
		end := B
		blockBytes := seq.length - bi*B
		if blockBytes < B {
			end = blockBytes
		}
		n := copy(out[dst:], blk.data[start:end])
		dst += n
		// Refresh LRU position of referenced shared blocks.
		if blk.key != nil {
			m.tick++
			blk.lastUsed = m.tick
		}
	}
	return out, nil
}
