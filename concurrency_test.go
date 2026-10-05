package pagedkv

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// Many goroutines append identical prefixes concurrently: the final content
// must always be correct, and identical blocks must never be double-allocated
// (dedup leaves exactly one resident copy with matching ref counts).
func TestConcurrentSharedAppend(t *testing.T) {
	m, _ := newPersistentManager(t, 64, 8)
	_ = m.RegisterTenant("t1", 200)
	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	seqs := make([]*Sequence, n)
	prefix := []byte("SHARED-P1SHARED-P2SHARED-P3")
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := m.NewSequence("t1", fmt.Sprintf("c-%d", i), 0)
			if err != nil {
				errs <- err
				return
			}
			if err := m.Append(s, prefix); err != nil {
				errs <- err
				return
			}
			seqs[i] = s
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for i, s := range seqs {
		got, err := m.Read(s, 0, len(prefix))
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if !bytes.Equal(got, prefix) {
			t.Fatalf("seq %d content mismatch", i)
		}
	}
	// Every full block: all n sequences reference the same physical id.
	for bi := 0; bi < len(prefix)/8; bi++ {
		id := seqs[0].refs[bi].blockID
		for i := 1; i < n; i++ {
			if seqs[i].refs[bi].blockID != id {
				t.Fatalf("block %d not shared across all sequences", bi)
			}
		}
		if m.blocks[id].refs != n {
			t.Fatalf("block %d refs=%d, want %d", bi, m.blocks[id].refs, n)
		}
	}
}

// Mixed workload: many tenants, forks, appends, releases and commits under a
// tiny pool. No sequence may ever observe torn/incorrect bytes, and after all
// releases the whole pool must be reclaimable (no permanent leak).
func TestConcurrentMixedWorkload(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	m, err := NewManager(Config{DataDir: dir, BlockSize: 8, TotalBlocks: 12})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	const tenants = 4
	for ti := 0; ti < tenants; ti++ {
		if err := m.RegisterTenant(fmt.Sprintf("t%d", ti), 30); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for g := 0; g < 24; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			tid := fmt.Sprintf("t%d", g%tenants)
			s, err := m.NewSequence(tid, fmt.Sprintf("g%d", g), g%3)
			if err != nil {
				t.Errorf("new seq: %v", err)
				return
			}
			want := []byte{}
			complete := true
			for round := 0; round < 4; round++ {
				chunk := []byte(fmt.Sprintf("%d-%d-abc!", g, round))
				if err := m.Append(s, chunk); err != nil {
					// OOM / recompute under a tiny pool is a reported error,
					// never silent corruption of another sequence.
					complete = false
					break
				}
				want = append(want, chunk...)
			}
			// Verify while still inside the goroutine (no shared escape).
			if complete && m.SequenceState(s) == StateActive {
				got, rerr := m.Read(s, 0, len(want))
				if rerr == nil && !bytes.Equal(got, want) {
					t.Errorf("seq g%d torn read: %q want %q", g, got, want)
				}
			}
			if g%5 == 0 {
				fk, ferr := m.Fork(s, fmt.Sprintf("g%d-f", g))
				if ferr == nil {
					defer m.ReleaseSequence(fk)
				}
			}
			if g%7 == 0 && m.SequenceState(s) == StateActive {
				_ = m.CommitSequence(s)
			}
			defer m.ReleaseSequence(s)
		}(g)
	}
	wg.Wait()

	// After releasing everything, no resident block may be referenced and
	// every tenant's private accounting must be zero.
	for _, tnt := range m.tenants {
		if tnt.privateUsed != 0 {
			t.Errorf("tenant %s leaked private blocks: %d", tnt.id, tnt.privateUsed)
		}
	}
	for id, blk := range m.blocks {
		if blk.state == blockResident && blk.refs > 0 {
			t.Errorf("block %d still has refs %d after all releases", id, blk.refs)
		}
	}
}

// Concurrent commits of distinct sequences must all survive a restart.
func TestConcurrentCommitThenRecover(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	func() {
		m, _ := NewManager(Config{DataDir: dir, BlockSize: 8, TotalBlocks: 32})
		defer m.Close()
		_ = m.RegisterTenant("t1", 100)
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				s, _ := m.NewSequence("t1", fmt.Sprintf("k%d", g), 0)
				data := []byte(fmt.Sprintf("SEQ%d-val", g))
				if err := m.Append(s, data); err != nil {
					t.Errorf("append %d: %v", g, err)
					return
				}
				if err := m.CommitSequence(s); err != nil {
					t.Errorf("commit %d: %v", g, err)
				}
			}(g)
		}
		wg.Wait()
	}()
	m2, issues, err := Recover(Config{DataDir: dir, BlockSize: 8, TotalBlocks: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	if len(issues) != 0 {
		t.Fatalf("clean recovery expected, issues=%v", issues)
	}
	if len(m2.sequences) != 8 {
		t.Fatalf("recovered %d sequences, want 8", len(m2.sequences))
	}
	for g := 0; g < 8; g++ {
		s := m2.sequences[fmt.Sprintf("k%d", g)]
		if s == nil {
			t.Fatalf("k%d missing", g)
		}
		want := []byte(fmt.Sprintf("SEQ%d-val", g))
		got, err := m2.Read(s, 0, len(want))
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("k%d wrong: %q %v", g, got, err)
		}
	}
}
