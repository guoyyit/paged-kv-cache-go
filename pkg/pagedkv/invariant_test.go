package pagedkv

import "testing"

// Internal invariant: the owner index must exactly mirror live sequence slots
// after sharing, COW append/rewrite and abort.
func TestOwnerIndexInvariant(t *testing.T) {
	m := newTestManager(t, 4, 32)
	m.CreateTenant("a", TenantConfig{QuotaBlocks: 32})
	s1, _ := m.NewSequence("a", "s1")
	s2, _ := m.NewSequence("a", "s2")
	s1.Append([]byte("SAMEsame"))
	s2.Append([]byte("SAMEsame"))
	tt := m.tenants["a"]
	checkOwners := func(stage string) {
		t.Helper()
		want := map[int]map[string]bool{}
		for _, s := range []*Sequence{s1, s2} {
			if s.state != seqActive {
				continue
			}
			for _, bid := range s.blocks {
				set := want[bid]
				if set == nil {
					set = map[string]bool{}
					want[bid] = set
				}
				set[s.id] = true
			}
		}
		if len(want) != len(tt.owners) {
			t.Fatalf("%s: owner index size %d != live chains %d: got=%v want=%v",
				stage, len(tt.owners), len(want), tt.owners, want)
		}
		for id, set := range want {
			got := tt.owners[id]
			if len(got) != len(set) {
				t.Fatalf("%s: block %d owners %v != %v", stage, id, got, set)
			}
			for seq := range set {
				if !got[seq] {
					t.Fatalf("%s: block %d missing owner %s", stage, id, seq)
				}
			}
		}
	}
	checkOwners("after share")

	s2.Append([]byte("XX")) // COW the shared partial tail (len 8 -> 10)
	checkOwners("after s2 append COW")
	s1.Rewrite(0, []byte("zzzz")) // COW the shared first block
	checkOwners("after s1 rewrite COW")
	s2.Append([]byte("NNNNmmmm")) // new full blocks
	checkOwners("after s2 grow")

	if err := s1.Abort(); err != nil {
		t.Fatal(err)
	}
	if err := s2.Abort(); err != nil {
		t.Fatal(err)
	}
	if len(tt.owners) != 0 {
		t.Fatalf("owner index not empty after abort: %v", tt.owners)
	}
}
