package pagedkv

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

func newPersistentManager(t *testing.T, blocks, blockSize int) (*Manager, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	m, err := NewManager(Config{
		DataDir: dir, BlockSize: blockSize, TotalBlocks: blocks,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, dir
}

func TestPreemptSwapAndResume(t *testing.T) {
	m, _ := newPersistentManager(t, 4, 8)
	_ = m.RegisterTenant("t1", 100)
	a, _ := m.NewSequence("t1", "a", 0)
	b, _ := m.NewSequence("t1", "b", 0)

	dataA := []byte("AAAAAAAABBBBBBBBaa") // 2 full + private tail = 3 blocks
	dataB := []byte("XXXXXXXYYYYYYYYbb")  // 2 full + private tail = 3 blocks
	if err := m.Append(a, dataA); err != nil {
		t.Fatal(err)
	}
	if err := m.Append(b, dataB); err != nil {
		t.Fatalf("append b should trigger preemption, got %v", err)
	}
	if a.state != seqPreempted {
		t.Fatalf("a state = %v, want preempted", a.state)
	}
	if b.state != seqActive {
		t.Fatalf("b should be active")
	}
	// Reading a lazily resumes it; b may become preempted in turn.
	got, err := m.Read(a, 0, len(dataA))
	if err != nil {
		t.Fatalf("read resumed a: %v", err)
	}
	if !bytes.Equal(got, dataA) {
		t.Fatalf("a content after resume mismatch: %q", got)
	}
	got, err = m.Read(b, 0, len(dataB))
	if err != nil {
		t.Fatalf("read resumed b: %v", err)
	}
	if !bytes.Equal(got, dataB) {
		t.Fatalf("b content after resume mismatch: %q", got)
	}
}

// Without a store, preemption cannot preserve state: the victim must be
// marked invalid and reads must fail with ErrNeedsRecompute rather than
// return partial data.
func TestPreemptWithoutStoreMarksInvalid(t *testing.T) {
	m := newTestManager(t, 4, 8)
	_ = m.RegisterTenant("t1", 100)
	a, _ := m.NewSequence("t1", "a", 0)
	b, _ := m.NewSequence("t1", "b", 0)
	if err := m.Append(a, []byte("AAAAAAAABBBBBBBBaa")); err != nil {
		t.Fatal(err)
	}
	err := m.Append(b, []byte("XXXXXXXYYYYYYYYbb"))
	if !errors.Is(err, ErrNeedsRecompute) {
		t.Fatalf("append b should fail needing recompute, got %v", err)
	}
	if a.state != seqInvalid {
		t.Fatalf("a should be invalid, got %v", a.state)
	}
	if _, err := m.Read(a, 0, a.Length()); !errors.Is(err, ErrNeedsRecompute) {
		t.Fatalf("want ErrNeedsRecompute, got %v", err)
	}
}

// LRU reclaim of unreferenced shared blocks should satisfy pressure without
// preempting anyone.
func TestLRUEvictionPreventsPreemption(t *testing.T) {
	m, _ := newPersistentManager(t, 4, 8)
	_ = m.RegisterTenant("t1", 100)
	a, _ := m.NewSequence("t1", "a", 0)
	// 3 full blocks then release: all become unreferenced LRU entries
	// (durable in the store).
	if err := m.Append(a, []byte("AAAAAAAABBBBBBBBCCCCCCCC")); err != nil {
		t.Fatal(err)
	}
	m.ReleaseSequence(a)
	b, _ := m.NewSequence("t1", "b", 0)
	// 3 full blocks fit purely from LRU evictions; no sequence is alive to
	// preempt anyway — OOM would mean LRU failed.
	if err := m.Append(b, []byte("DDDDDDDDEEEEEEEEFFFFFFFF")); err != nil {
		t.Fatalf("expected LRU evictions to free blocks, got %v", err)
	}
	got, _ := m.Read(b, 0, b.Length())
	if string(got) != "DDDDDDDDEEEEEEEEFFFFFFFF" {
		t.Fatalf("b content wrong: %q", got)
	}
}

// Eviction order must be deterministic: block with the oldest lastUsed tick
// among unreferenced shared blocks is evicted first, id tie-break.
func TestLRUOrderDeterministic(t *testing.T) {
	m, _ := newPersistentManager(t, 3, 8)
	_ = m.RegisterTenant("t1", 100)
	mk := func(id, data string) *Sequence {
		s, _ := m.NewSequence("t1", id, 0)
		if err := m.Append(s, []byte(data)); err != nil {
			t.Fatal(err)
		}
		return s
	}
	a := mk("a", "AAAAAAAA") // block A finalized first (oldest tick)
	b := mk("b", "BBBBBBBB") // block B newer
	c := mk("c", "CCCCCCCC") // block C newest
	keyA := chainKey(nil, []byte("AAAAAAAA"))
	_ = keyA
	m.ReleaseSequence(a)
	m.ReleaseSequence(b)
	m.ReleaseSequence(c)
	// Allocate three blocks: evictions proceed C? no — oldest first: A, B, C.
	d := mk("d", "DDDDDDDD")
	if _, ok := m.keyIndex[string(chainKey(nil, []byte("AAAAAAAA")))]; ok {
		t.Fatal("oldest block A should be evicted first")
	}
	if _, ok := m.keyIndex[string(chainKey(nil, []byte("BBBBBBBB")))]; !ok {
		t.Fatal("block B should still be resident (only one eviction needed)")
	}
	if _, ok := m.keyIndex[string(chainKey(nil, []byte("CCCCCCCC")))]; !ok {
		t.Fatal("newest block C should still be resident")
	}
	_ = d
}
