package pagedkv

import (
	"bytes"
	"testing"
)

// Under an alternating long-sequence workload with abort/commit churn, the
// pool must not grow un-reclaimable fragments: after sequences finish, all
// pages return to the free pool and a fresh large allocation keeps succeeding.
func TestNoPersistentFragmentation(t *testing.T) {
	const bs = 4
	const total = 16
	m := newTestManager(t, bs, total)
	m.CreateTenant("a", TenantConfig{QuotaBlocks: total})

	rounds := 40
	for r := 0; r < rounds; r++ {
		ids := make([]*Sequence, 0, 4)
		for i := 0; i < 4; i++ {
			s, _ := m.NewSequence("a", seqID(r, i))
			// Distinct-ish content across sequences to avoid all-dedup.
			data := bytes.Repeat([]byte{byte('A' + i), byte('a' + r%8)}, bs)
			data = append(data, bytes.Repeat([]byte{byte('0' + (r+i)%10)}, bs)...)
			if err := s.Append(data); err != nil {
				t.Fatalf("round %d seq %d append: %v", r, i, err)
			}
			ids = append(ids, s)
		}
		// Half commit, half abort; both paths must release all pages.
		for i, s := range ids {
			if i%2 == 0 {
				if err := s.Commit(); err != nil {
					t.Fatalf("commit: %v", err)
				}
			} else {
				if err := s.Abort(); err != nil {
					t.Fatalf("abort: %v", err)
				}
			}
		}
	}
	// After the churn there are no live active references: committed content
	// lingers as freeable cached pages (bounded by the fixed pool), aborted
	// work returns pages immediately. Nothing stays pinned.
	if st, _ := m.TenantStats("a"); st.ActiveRefs != 0 {
		t.Fatalf("leaked active refs after churn: %+v", st)
	}
	if st, _ := m.TenantStats("a"); st.ResidentBlocks > total {
		t.Fatalf("resident set exceeded fixed pool: %+v", st)
	}
	// Evict everything the tenant caches by allocating a full-pool sequence.
	big, _ := m.NewSequence("a", "big")
	bigData := make([]byte, bs*total)
	for i := range bigData {
		bigData[i] = byte(i)
	}
	if err := big.Append(bigData); err != nil {
		t.Fatalf("full-pool allocation after churn: %v", err)
	}
	got, err := big.Read(0, len(bigData))
	if err != nil || !bytes.Equal(got, bigData) {
		t.Fatalf("post-churn data wrong: %v", err)
	}
	// The big sequence now owns exactly the whole pool; cached leftovers gone.
	if st, _ := m.TenantStats("a"); st.ActiveRefs != total {
		t.Fatalf("expected %d active refs after full allocation, got %+v", total, st)
	}
}

func seqID(r, i int) string {
	return "r" + itoa(r) + "-" + itoa(i)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for n > 0 {
		pos--
		b[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(b[pos:])
}

// Identical workloads against fresh managers must reclaim blocks in the same
// order (deterministic LRU with block-id tie-break).
func TestDeterministicEvictionOrder(t *testing.T) {
	run := func() []int {
		m, _ := New(Config{BlockBytes: 4, TotalBlocks: 8})
		m.CreateTenant("a", TenantConfig{QuotaBlocks: 8})
		evicted := []int{}
		// Commit several sequences to populate the cached pool, then force
		// eviction with a full-pool active sequence.
		var blocks [][]byte
		for i := 0; i < 6; i++ {
			s, _ := m.NewSequence("a", "c"+itoa(i))
			d := []byte{byte('p' + i), byte('q' + i), byte('r' + i), byte('s' + i)}
			s.Append(d)
			s.Commit()
			blocks = append(blocks, d)
		}
		// Record the LRU victim ordering directly through the policy.
		tt := m.tenants["a"]
		for {
			elig := map[int]bool{}
			for id, b := range tt.resident {
				if b.cached && b.freeable() {
					elig[id] = true
				}
			}
			v := tt.policy.victim(tt, elig)
			if v < 0 {
				break
			}
			evicted = append(evicted, v)
			m.evictCachedLocked(tt, tt.resident[v])
		}
		m.Close()
		return evicted
	}
	first := run()
	second := run()
	if len(first) == 0 {
		t.Fatal("no cached blocks produced")
	}
	if !intsEqual(first, second) {
		t.Fatalf("eviction order nondeterministic: %v vs %v", first, second)
	}
}

func intsEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
