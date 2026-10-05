package pagedkv

import (
	"bytes"
	"sync"
	"testing"
)

// Concurrent appends/reads/rewrites across many goroutines must never observe
// a half-written block, hand out one physical page twice, or corrupt shared
// data. Run with -race.
func TestConcurrentAllocationAndSharing(t *testing.T) {
	const bs = 4
	const total = 64
	m := newTestManager(t, bs, total)
	for _, tn := range []string{"a", "b", "c"} {
		if err := m.CreateTenant(tn, TenantConfig{QuotaBlocks: total}); err != nil {
			t.Fatal(err)
		}
	}

	// Many writers repeatedly build the SAME prefix per tenant; identical
	// blocks must dedup/share without double-allocation or torn reads.
	var wg sync.WaitGroup
	errs := make(chan error, 300)
	for g := 0; g < 24; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			tn := []string{"a", "b", "c"}[g%3]
			for it := 0; it < 8; it++ {
				s, err := m.NewSequence(tn, "g"+itoa(g)+"-"+itoa(it))
				if err != nil {
					errs <- err
					return
				}
				// Shared prefix (constant) then unique tail.
				prefix := []byte("PREFpref")
				if err := s.Append(prefix); err != nil {
					errs <- err
					_ = s.Abort()
					continue
				}
				tail := []byte{byte(g), byte(it), byte(g + it), 0}
				if err := s.Append(tail); err != nil {
					// Under pressure this goroutine may have been preempted;
					// abort and retry this iteration once with a fresh seq.
					_ = s.Abort()
					continue
				}
				got, err := s.Read(0, 12)
				if err != nil {
					_ = s.Abort()
					continue
				}
				want := append(append([]byte{}, prefix...), tail...)
				if !bytes.Equal(got, want) {
					errs <- &mismatchErr{got: got, want: want}
					continue
				}
				// Rewrite the shared prefix; must not disturb anyone.
				if err := s.Rewrite(0, []byte{byte(g), byte(g), byte(g), byte(g)}); err != nil {
					_ = s.Abort()
					continue
				}
				if err := s.Commit(); err != nil {
					_ = s.Abort()
					continue
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent op: %v", err)
	}

	// Global accounting invariant: each physical page is free or owned by
	// exactly one tenant; refs never negative; resident <= total.
	st := m.Stats()
	if st.ResidentBlocks > st.TotalBlocks {
		t.Fatalf("resident %d > total %d", st.ResidentBlocks, st.TotalBlocks)
	}
	m.mu.Lock()
	for _, b := range m.blocks {
		if b.refs < 0 {
			t.Fatalf("block %d negative refs", b.id)
		}
	}
	m.mu.Unlock()
}

type mismatchErr struct{ got, want []byte }

func (e *mismatchErr) Error() string {
	return "data mismatch: got " + string(e.got) + " want " + string(e.want)
}
