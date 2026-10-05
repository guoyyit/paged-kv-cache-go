package pagedkv

import (
	"bytes"
	"testing"
)

func (m *Manager) residentCount() (free, resident int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, blk := range m.blocks {
		if blk.state == blockFree {
			free++
		} else {
			resident++
		}
	}
	return free, resident
}

// Repeated alternating long/short sequences must not produce monotonically
// growing, unreclaimable memory. After each round all private blocks are
// returned; only zero-reference shared cache pages may remain resident, and
// those must be fully reclaimable once the pool actually needs them.
func TestNoPersistentFragmentation(t *testing.T) {
	m := newTestManager(t, 40, 16)
	for i := 0; i < 3; i++ {
		_ = m.RegisterTenant(string(rune('a'+i)), 40)
	}

	const warmup = 2
	prevFree := -1
	for round := 0; round < 10; round++ {
		var held []*Sequence
		for i := 0; i < 6; i++ {
			tid := string(rune('a' + i%3))
			s, err := m.NewSequence(tid, seqID(round, i), 0)
			if err != nil {
				t.Fatal(err)
			}
			size := (i%2 + 1) * 48 // 48 or 96 bytes => 3 or 6 blocks
			data := bytes.Repeat([]byte{byte(round*16 + i + 1)}, size)
			if err := m.Append(s, data); err != nil {
				t.Fatalf("round %d seq %d: %v", round, i, err)
			}
			held = append(held, s)
		}
		for _, s := range held {
			m.ReleaseSequence(s)
		}

		free, resident := m.residentCount()
		for _, tnt := range m.tenants {
			if tnt.privateUsed != 0 {
				t.Fatalf("round %d: tenant %s leaked %d private blocks",
					round, tnt.id, tnt.privateUsed)
			}
		}
		for id, blk := range m.blocks {
			if blk.state == blockResident && (blk.refs != 0 || blk.tenant != "") {
				t.Fatalf("round %d: block %d unreclaimable refs=%d tenant=%q",
					round, id, blk.refs, blk.tenant)
			}
		}
		// After warm-up fills the shared cache the reclaim baseline must be
		// stable: no unreclaimable residue accumulates across the
		// alternating workload.
		if round >= warmup {
			if prevFree >= 0 && free != prevFree {
				t.Fatalf("round %d: free baseline drifted %d -> %d (unreclaimable growth)",
					round, prevFree, free)
			}
			prevFree = free
		}
		if resident > 40 {
			t.Fatalf("resident pages exceed pool: %d", resident)
		}
	}

	// Under pressure every residual shared page must be reclaimable.
	s, _ := m.NewSequence("a", "pressure", 0)
	big := bytes.Repeat([]byte{'Z'}, 40*16)
	if err := m.Append(s, big); err != nil {
		t.Fatalf("reclaim under pressure failed: %v", err)
	}
	got, err := m.Read(s, 0, len(big))
	if err != nil || !bytes.Equal(got, big) {
		t.Fatalf("pressure read wrong: %v", err)
	}
}

func seqID(round, i int) string {
	return "r" + itoa(uint64(round)) + "-" + itoa(uint64(i))
}
