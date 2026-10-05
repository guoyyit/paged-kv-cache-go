package pagedkv

import (
	"os"
)

// Recover rebuilds a manager from cfg.DataDir.
//
// Committed block mappings are replayed from the journal and revalidated
// against the swap store: a mapping whose blocks are missing or fail
// checksum is dropped (and reported) rather than served as a valid cache,
// and it never blocks startup. Torn/partial journal frames and a missing
// journal are treated as "nothing committed", which is also non-fatal.
//
// Recovered sequences are restored in the seqPreempted state: their first
// Read/Append lazily reloads blocks by key (which dedups shared prefixes),
// so memory pressure at startup is avoided.
func Recover(cfg Config) (*Manager, []RecoveryIssue, error) {
	c, err := cfg.withDefaults()
	if err != nil {
		return nil, nil, err
	}
	if c.DataDir == "" {
		return nil, nil, ErrInvalidArgument
	}
	if _, err := os.Stat(c.DataDir); err != nil {
		return nil, nil, err
	}

	m, err := newManager(c)
	if err != nil {
		return nil, nil, err
	}
	records, issues, err := readJournal(c.DataDir)
	if err != nil {
		return nil, nil, err
	}

	for _, js := range records {
		// Ensure the tenant exists; a recovered mapping cannot invent a
		// tenant record. Give it an unlimited-ish quota only when the
		// original tenant is unknown: recovered blocks start swapped and
		// consume no resident quota until lazily resumed.
		t, ok := m.tenants[js.tenant]
		if !ok {
			t = &tenant{id: js.tenant, quotaBlocks: c.TotalBlocks}
			m.tenants[js.tenant] = t
		}

		// Validate every referenced block is present and checksum-valid;
		// also verify chained keys for full blocks. Alignment between the
		// mapping and swapped data is enforced here.
		entries := make([]swapEntry, 0, len(js.blocks))
		bad := false
		var prev []byte
		totalLen := 0
		for bi, jb := range js.blocks {
			data, gerr := m.store.Get(jb.key)
			if gerr != nil {
				bad = true
				issues = append(issues, RecoveryIssue{Reason: "sequence " + js.id + " dropped: block unreadable (" + gerr.Error() + ")"})
				break
			}
			isTail := bi == len(js.blocks)-1 && jb.size < c.BlockSize
			if !isTail {
				if len(data) != c.BlockSize {
					bad = true
					issues = append(issues, RecoveryIssue{Reason: "sequence " + js.id + " dropped: full block wrong size"})
					break
				}
				want := chainKey(prev, data)
				if !keyEqual(want, jb.key) {
					bad = true
					issues = append(issues, RecoveryIssue{Reason: "sequence " + js.id + " dropped: prefix chain mismatch"})
					break
				}
				prev = want
			} else if len(data) < jb.size {
				bad = true
				issues = append(issues, RecoveryIssue{Reason: "sequence " + js.id + " dropped: tail block truncated"})
				break
			}
			entries = append(entries, swapEntry{key: append([]byte(nil), jb.key...), size: jb.size})
			totalLen += jb.size
			m.storedKeys[string(jb.key)] = true
		}
		if bad {
			continue
		}
		if _, dup := m.sequences[js.id]; dup {
			issues = append(issues, RecoveryIssue{Reason: "duplicate sequence id in journal: " + js.id})
			continue
		}
		m.orderSeq++
		seq := &Sequence{
			tenant:    js.tenant,
			id:        js.id,
			priority:  js.priority,
			state:     seqPreempted,
			length:    totalLen,
			order:     js.order,
			swapped:   entries,
			committed: true,
			lastCommit: func() []journalBlock {
				out := make([]journalBlock, len(js.blocks))
				for i, b := range js.blocks {
					out[i] = journalBlock{key: append([]byte(nil), b.key...), size: b.size}
				}
				return out
			}(),
		}
		// Track resident-free swap-only sequence; it consumes no pool.
		refs := make([]blockRef, len(entries))
		for i := range refs {
			refs[i] = blockRef{blockID: swappedBlock, key: append([]byte(nil), entries[i].key...)}
		}
		seq.refs = refs
		m.sequences[js.id] = seq
	}

	if issues == nil {
		issues = []RecoveryIssue{}
	}
	return m, issues, nil
}
