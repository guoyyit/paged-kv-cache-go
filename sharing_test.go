package pagedkv

import (
	"bytes"
	"testing"
)

// Equal prefixes must resolve to the same physical shared blocks; divergent
// appends then allocate private blocks without touching shared content.
func TestPrefixSharing(t *testing.T) {
	m := newTestManager(t, 32, 8)
	_ = m.RegisterTenant("t1", 32)
	a, _ := m.NewSequence("t1", "a", 0)
	b, _ := m.NewSequence("t1", "b", 0)
	prefix := []byte("AAAAAAAABBBBBBBB") // two full blocks
	if err := m.Append(a, prefix); err != nil {
		t.Fatal(err)
	}
	if err := m.Append(b, prefix); err != nil {
		t.Fatal(err)
	}
	// Both full blocks must be physically shared.
	if a.refs[0].blockID != b.refs[0].blockID {
		t.Fatalf("first block not shared: %d vs %d", a.refs[0].blockID, b.refs[0].blockID)
	}
	if a.refs[1].blockID != b.refs[1].blockID {
		t.Fatalf("second block not shared")
	}
	if m.blocks[a.refs[0].blockID].refs != 2 {
		t.Fatalf("refs = %d, want 2", m.blocks[a.refs[0].blockID].refs)
	}

	// Diverge: different third block, shared prefix stays intact.
	if err := m.Append(a, []byte("CCCCCCCC")); err != nil {
		t.Fatal(err)
	}
	if err := m.Append(b, []byte("DDDDDDDD")); err != nil {
		t.Fatal(err)
	}
	if a.refs[0].blockID != b.refs[0].blockID || a.refs[2].blockID == b.refs[2].blockID {
		t.Fatal("shared prefix broken or divergent tail aliased")
	}
	ga, _ := m.Read(a, 0, a.Length())
	gb, _ := m.Read(b, 0, b.Length())
	if !bytes.Equal(ga, append(append([]byte{}, prefix...), []byte("CCCCCCCC")...)) ||
		!bytes.Equal(gb, append(append([]byte{}, prefix...), []byte("DDDDDDDD")...)) {
		t.Fatalf("divergent content wrong:\n%q\n%q", ga, gb)
	}
}

// Rewriting through a fork copies only the private tail; shared full blocks
// stay shared and the other branch sees unchanged content.
func TestForkCOW(t *testing.T) {
	m := newTestManager(t, 32, 8)
	_ = m.RegisterTenant("t1", 32)
	src, _ := m.NewSequence("t1", "src", 0)
	// 2 full blocks + private tail
	all := []byte("AAAAAAAABBBBBBBBtail!!")
	if err := m.Append(src, all); err != nil {
		t.Fatal(err)
	}
	fk, err := m.Fork(src, "fork")
	if err != nil {
		t.Fatal(err)
	}
	// Full blocks shared; private tail copied.
	if src.refs[0].blockID != fk.refs[0].blockID {
		t.Fatal("full blocks should be shared after fork")
	}
	if src.refs[2].blockID == fk.refs[2].blockID {
		t.Fatal("private tail must be copied, not shared")
	}
	// Grow fork's tail into a new block; original must be unchanged.
	if err := m.Append(fk, []byte("-more-bytes")); err != nil {
		t.Fatal(err)
	}
	gs, _ := m.Read(src, 0, src.Length())
	if string(gs) != string(all) {
		t.Fatalf("source mutated: %q", gs)
	}
	gf, _ := m.Read(fk, 0, fk.Length())
	if string(gf) != string(all)+"-more-bytes" {
		t.Fatalf("fork wrong: %q", gf)
	}
}

// Different content at the same prefix position must never share.
func TestNoFalseSharing(t *testing.T) {
	m := newTestManager(t, 32, 8)
	_ = m.RegisterTenant("t1", 32)
	a, _ := m.NewSequence("t1", "a", 0)
	b, _ := m.NewSequence("t1", "b", 0)
	_ = m.Append(a, []byte("AAAAAAAABBBBBBBB"))
	_ = m.Append(b, []byte("AAAAAAAACCCCCCCC"))
	if a.refs[0].blockID != b.refs[0].blockID {
		t.Fatal("first block should share")
	}
	if a.refs[1].blockID == b.refs[1].blockID {
		t.Fatal("second block with different prev+content must not share")
	}
}
