package pagedkv

import (
	"bytes"
	"errors"
	"testing"
)

// Identical prefix content across sequences must land on the same physical
// block (prefix sharing), while a rewrite of one branch must not change the
// bytes the other sequence already observed (copy-on-write).
func TestBlockSharingAndCOW(t *testing.T) {
	const bs = 8
	m := newTestManager(t, bs, 16)
	if err := m.CreateTenant("t1", TenantConfig{QuotaBlocks: 16}); err != nil {
		t.Fatal(err)
	}
	prefix := []byte("AAAAAAAA" + "BBBBBBBB" + "CCCCCCCC") // 3 full blocks
	s1, err := m.NewSequence("t1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	s2, err := m.NewSequence("t1", "s2")
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Append(prefix); err != nil {
		t.Fatal(err)
	}
	if err := s2.Append(prefix); err != nil {
		t.Fatal(err)
	}

	if len(s1.blocks) != 3 || len(s2.blocks) != 3 {
		t.Fatalf("expected 3 blocks each, got %d/%d", len(s1.blocks), len(s2.blocks))
	}
	for i := range prefix {
		_ = i
	}
	for i := 0; i < 3; i++ {
		if s1.blocks[i] != s2.blocks[i] {
			t.Fatalf("block %d not shared: %d vs %d", i, s1.blocks[i], s2.blocks[i])
		}
	}

	// s2 rewrites inside the shared first block.
	rewritten := []byte("ZZZZ")
	if err := s2.Rewrite(0, rewritten); err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	// s2 sees the new bytes; s1 keeps its original view.
	got2, _ := s2.Read(0, bs)
	if !bytes.Equal(got2[:4], rewritten) || !bytes.Equal(got2[4:], []byte("AAAA")) {
		t.Fatalf("s2 after rewrite = %q", got2)
	}
	got1, _ := s1.Read(0, bs)
	if !bytes.Equal(got1, []byte("AAAAAAAA")) {
		t.Fatalf("s1 view changed after s2 rewrite: %q", got1)
	}
	// The rewritten first block must now be a distinct physical page; the
	// untouched shared tail remains shared.
	if s1.blocks[0] == s2.blocks[0] {
		t.Fatal("COW did not split the rewritten block")
	}
	if s1.blocks[1] != s2.blocks[1] || s1.blocks[2] != s2.blocks[2] {
		t.Fatal("COW over-split: untouched blocks should remain shared")
	}

	// Append into a shared partial tail must also copy-on-write.
	s3, _ := m.NewSequence("t1", "s3")
	if err := s3.Append([]byte("123")); err != nil {
		t.Fatal(err)
	}
	s4, _ := m.NewSequence("t1", "s4")
	if err := s4.Append([]byte("123")); err != nil {
		t.Fatal(err)
	}
	if s3.blocks[0] != s4.blocks[0] {
		t.Fatal("identical partial tail not shared")
	}
	if err := s4.Append([]byte("45")); err != nil {
		t.Fatal(err)
	}
	g3, _ := s3.Read(0, 3)
	if string(g3) != "123" {
		t.Fatalf("s3 tail corrupted by s4 append: %q", g3)
	}
	g4, _ := s4.Read(0, 5)
	if string(g4) != "12345" {
		t.Fatalf("s4 append wrong: %q", g4)
	}
}

// Reading a swapped-out sequence returns a distinguishable preempted error;
// after resume it must produce byte-identical content.
func TestPreemptAndResume(t *testing.T) {
	const bs = 4
	m := newTestManager(t, bs, 6)
	if err := m.CreateTenant("t1", TenantConfig{QuotaBlocks: 6}); err != nil {
		t.Fatal(err)
	}
	holder, _ := m.NewSequence("t1", "holder")
	victim, _ := m.NewSequence("t1", "victim")
	if err := holder.Append([]byte("HHHHhhhh")); err != nil {
		t.Fatal(err)
	}
	// Distinct blocks so they cannot dedup onto the holder's pages.
	victimData := []byte("VVVVwwwwxxxxyyyy") // 4 unique blocks -> pool full (6)
	if err := victim.Append(victimData); err != nil {
		t.Fatal(err)
	}
	// Holder grows by 2; to make room the victim (the largest sequence) is
	// preempted and swapped to the content store.
	if err := holder.Append([]byte("XXXXzzzz")); err != nil {
		t.Fatalf("holder append: %v", err)
	}
	if victim.State() != seqSwapped.String() {
		t.Fatalf("victim state = %s, want swapped", victim.State())
	}
	if _, err := victim.Read(0, 4); !errors.Is(err, ErrSequencePreempted) {
		t.Fatalf("read swapped err = %v, want ErrSequencePreempted", err)
	}
	// Release the holder, then resume and verify full content.
	if err := holder.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := victim.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	got, err := victim.Read(0, len(victimData))
	if err != nil {
		t.Fatalf("Read after resume: %v", err)
	}
	if !bytes.Equal(got, victimData) {
		t.Fatalf("resumed content mismatch: got %q want %q", got, victimData)
	}
}
