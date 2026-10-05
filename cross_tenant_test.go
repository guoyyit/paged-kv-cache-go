package pagedkv

import (
	"errors"
	"testing"
)

// Two tenants computing the same full-block prefixes share the physical
// blocks (content-based dedup), while their logical sequences stay isolated:
// releasing one never breaks the other.
func TestCrossTenantPrefixSharedButIsolated(t *testing.T) {
	m := newTestManager(t, 32, 8)
	_ = m.RegisterTenant("ta", 32)
	_ = m.RegisterTenant("tb", 32)
	a, _ := m.NewSequence("ta", "a", 0)
	b, _ := m.NewSequence("tb", "b", 0)
	prefix := []byte("COMMON1!COMMON2!") // 16 bytes: exactly 2 full blocks
	if len(prefix) != 16 {
		t.Fatal("test data length")
	}
	_ = m.Append(a, prefix)
	_ = m.Append(b, prefix)
	if a.refs[0].blockID != b.refs[0].blockID || a.refs[1].blockID != b.refs[1].blockID {
		t.Fatal("identical prefix across tenants should physically share blocks")
	}
	ga, _ := m.Read(a, 0, 16)
	gb, _ := m.Read(b, 0, 16)
	if string(ga) != string(prefix) || string(gb) != string(prefix) {
		t.Fatal("cross-tenant shared reads wrong")
	}
	m.ReleaseSequence(a)
	gb2, err := m.Read(b, 0, 16)
	if err != nil || string(gb2) != string(prefix) {
		t.Fatalf("tb reads after ta release: %q %v", gb2, err)
	}
	if m.blocks[b.refs[0].blockID].refs != 1 {
		t.Fatal("shared ref not decremented on release")
	}
}

// Priority policy preempts the lowest-priority active sequence first.
func TestPriorityPreemption(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(Config{
		DataDir: dir, BlockSize: 8, TotalBlocks: 3,
		PreemptionPolicy: "priority",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	_ = m.RegisterTenant("t1", 100)
	l, _ := m.NewSequence("t1", "low", 1)
	h, _ := m.NewSequence("t1", "high", 10)
	_ = m.Append(l, []byte("AAAAAAAA")) // 1 block
	_ = m.Append(h, []byte("BBBBBBBB")) // 1 block, 1 free remains
	// third fills the last full block then needs a private tail: pressure.
	third, _ := m.NewSequence("t1", "third", 5)
	if err := m.Append(third, []byte("CCCCCCCCx")); err != nil {
		t.Fatal(err)
	}
	if m.SequenceState(l) != StatePreempted {
		t.Fatal("lowest-priority sequence must be preempted")
	}
	if m.SequenceState(h) != StateActive {
		t.Fatal("higher-priority sequence must stay active")
	}
	got, err := m.Read(l, 0, 8)
	if err != nil || string(got) != "AAAAAAAA" {
		t.Fatalf("low restore: %q %v", got, err)
	}
}

// Equal priority falls back to creation order (oldest first).
func TestPriorityTieBreak(t *testing.T) {
	dir := t.TempDir()
	m, err := NewManager(Config{
		DataDir: dir, BlockSize: 8, TotalBlocks: 3,
		PreemptionPolicy: "priority",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Close() })
	_ = m.RegisterTenant("t1", 100)
	older, _ := m.NewSequence("t1", "older", 5)
	newer, _ := m.NewSequence("t1", "newer", 5)
	_ = m.Append(older, []byte("OOOOOOOO"))
	_ = m.Append(newer, []byte("NNNNNNNN"))
	third, _ := m.NewSequence("t1", "third", 5)
	if err := m.Append(third, []byte("TTTTTTTTx")); err != nil { // full + private tail
		t.Fatal(err)
	}
	if m.SequenceState(older) != StatePreempted {
		t.Fatal("older sequence must preempt first on equal priority")
	}
	if m.SequenceState(newer) != StateActive {
		t.Fatal("newer sequence must remain active")
	}
}

func TestConfigValidation(t *testing.T) {
	if _, err := NewManager(Config{BlockSize: -1, TotalBlocks: 1}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative blocksize: %v", err)
	}
	if _, err := NewManager(Config{BlockSize: 8, TotalBlocks: -1}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative totalblocks: %v", err)
	}
	if _, err := NewManager(Config{BlockSize: 8, TotalBlocks: 1, EvictionPolicy: "weird"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("bad eviction policy: %v", err)
	}
	if _, err := NewManager(Config{BlockSize: 8, TotalBlocks: 1, PreemptionPolicy: "weird"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("bad preemption policy: %v", err)
	}
}

func TestReadNegativeBounds(t *testing.T) {
	m := newTestManager(t, 8, 8)
	_ = m.RegisterTenant("t", 8)
	s, _ := m.NewSequence("t", "s", 0)
	_ = m.Append(s, []byte("hi"))
	if _, err := m.Read(s, -1, 1); !errors.Is(err, ErrOutOfBounds) {
		t.Fatalf("negative offset: %v", err)
	}
	if _, err := m.Read(s, 0, -1); !errors.Is(err, ErrOutOfBounds) {
		t.Fatalf("negative length: %v", err)
	}
	if _, err := m.Read(s, 3, 1); !errors.Is(err, ErrOutOfBounds) {
		t.Fatalf("past end: %v", err)
	}
}
