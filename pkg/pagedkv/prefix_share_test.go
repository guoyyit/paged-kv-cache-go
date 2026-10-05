package pagedkv

import (
	"bytes"
	"testing"
)

// A committed prefix must be shared by later requests, and remain readable
// even after its resident pages have all been reclaimed (lazy paging from the
// content store).
func TestCommittedPrefixSharingAndLazyRead(t *testing.T) {
	dir := t.TempDir()
	m, _ := New(Config{BlockBytes: 4, TotalBlocks: 6, DataDir: dir})
	m.CreateTenant("a", TenantConfig{QuotaBlocks: 6})

	s1, _ := m.NewSequence("a", "s1")
	prefix := []byte("AAAAaaaaBBBB") // 3 blocks
	if err := s1.Append(prefix); err != nil {
		t.Fatal(err)
	}
	committedIDs := append([]int(nil), s1.blocks...)
	if err := s1.Commit(); err != nil {
		t.Fatal(err)
	}

	// New request appends the same prefix: it must dedup onto the committed
	// resident cached pages (same physical page ids) as a separate sequence.
	s2, _ := m.NewSequence("a", "s2")
	if err := s2.Append(prefix); err != nil {
		t.Fatal(err)
	}
	if !intsEqual(s2.blocks, committedIDs) {
		t.Fatalf("committed prefix not shared resident: s2=%v want=%v",
			s2.blocks, committedIDs)
	}
	s2.Abort()

	// Occupy the entire 6-page pool with a distinct sequence, forcing all
	// committed cached pages to be reclaimed (LRU) under the same tenant.
	fillData := make([]byte, 4*6)
	for i := 0; i < 6; i++ {
		fillData[i*4] = byte(200 + i)
	}
	fill, _ := m.NewSequence("a", "fill")
	if err := fill.Append(fillData); err != nil {
		t.Fatalf("fill pool: %v", err)
	}
	if m.tenants["a"].cachedCount != 0 {
		t.Fatalf("expected committed pages fully evicted, cached=%d",
			m.tenants["a"].cachedCount)
	}

	// s1 has no resident pages now; it must still read via lazy paging.
	got, err := s1.Read(0, len(prefix))
	if err != nil {
		t.Fatalf("lazy read committed: %v", err)
	}
	if !bytes.Equal(got, prefix) {
		t.Fatalf("lazy read mismatch: %q", got)
	}
}
