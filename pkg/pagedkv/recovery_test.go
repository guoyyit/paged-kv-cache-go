package pagedkv

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// After restart, committed block mappings must line up with persisted content
// and committed data must read back byte-identically (paged in lazily).
func TestRestartRecoversCommitted(t *testing.T) {
	dir := t.TempDir()
	cfg := func() Config {
		return Config{BlockBytes: 4, TotalBlocks: 8, DataDir: dir}
	}
	m, err := New(cfg())
	if err != nil {
		t.Fatal(err)
	}
	m.CreateTenant("a", TenantConfig{QuotaBlocks: 8})
	s, _ := m.NewSequence("a", "s1")
	data := []byte("AAAAaaaaBBBBbbbb")
	if err := s.Append(data); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart into a cold pool.
	m2, err := New(cfg())
	if err != nil {
		t.Fatalf("restart New: %v", err)
	}
	defer m2.Close()
	s2, err := m2.Get("a", "s1")
	if err != nil {
		t.Fatalf("get committed after restart: %v", err)
	}
	if s2.State() != seqCommitted.String() {
		t.Fatalf("state after restart = %s, want committed", s2.State())
	}
	got, err := s2.Read(0, len(data))
	if err != nil {
		t.Fatalf("read committed after restart: %v", err)
	}
	if string(got) != string(data) {
		t.Fatalf("committed data mismatch: %q vs %q", got, data)
	}
}

// A sequence that was swapped out before crash stays swapped after restart and
// can be resumed to its full content.
func TestRestartRecoversSwapped(t *testing.T) {
	dir := t.TempDir()
	cfg := func() Config {
		return Config{BlockBytes: 4, TotalBlocks: 6, DataDir: dir}
	}
	m, _ := New(cfg())
	m.CreateTenant("a", TenantConfig{QuotaBlocks: 6})
	holder, _ := m.NewSequence("a", "holder")
	victim, _ := m.NewSequence("a", "victim")
	holder.Append([]byte("HHHHhhhh"))
	data := []byte("VVVVwwwwxxxxyyyy")
	victim.Append(data)
	holder.Append([]byte("XXXXzzzz")) // forces victim preemption
	if victim.State() != seqSwapped.String() {
		t.Fatalf("expected swapped, got %s", victim.State())
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	m2, _ := New(cfg())
	defer m2.Close()
	v2, err := m2.Get("a", "victim")
	if err != nil {
		t.Fatal(err)
	}
	if v2.State() != seqSwapped.String() {
		t.Fatalf("victim after restart = %s, want swapped", v2.State())
	}
	if err := v2.Resume(); err != nil {
		t.Fatalf("resume after restart: %v", err)
	}
	got, err := v2.Read(0, len(data))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("swapped data mismatch: %q", got)
	}
}

// A corrupt/missing swapped object must never be served as valid cache: the
// sequence is marked needs-recompute, yet startup still succeeds.
func TestRestartCorruptObjectMarkedRecompute(t *testing.T) {
	dir := t.TempDir()
	cfg := func() Config {
		return Config{BlockBytes: 4, TotalBlocks: 6, DataDir: dir}
	}
	m, _ := New(cfg())
	m.CreateTenant("a", TenantConfig{QuotaBlocks: 6})
	holder, _ := m.NewSequence("a", "holder")
	victim, _ := m.NewSequence("a", "victim")
	holder.Append([]byte("HHHHhhhh"))
	victim.Append([]byte("VVVVwwwwxxxxyyyy"))
	holder.Append([]byte("XXXXzzzz"))
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt one object file after a clean shutdown.
	objRoot := filepath.Join(dir, "objects")
	var target string
	_ = walkObjects(objRoot, func(hash string) {
		if target == "" {
			target = hash
		}
	})
	if target == "" {
		t.Fatal("no object found to corrupt")
	}
	p := filepath.Join(objRoot, target[:2], target)
	if err := os.WriteFile(p, []byte("garbage-not-a-valid-object"), 0o644); err != nil {
		t.Fatal(err)
	}

	m2, err := New(cfg()) // must not fail / block startup
	if err != nil {
		t.Fatalf("New after corruption: %v", err)
	}
	defer m2.Close()
	v2, err := m2.Get("a", "victim")
	if err != nil {
		t.Fatal(err)
	}
	if v2.State() != seqNeedsRecompute.String() {
		t.Fatalf("victim state = %s, want needs_recompute", v2.State())
	}
	if _, err := v2.Read(0, 4); !errors.Is(err, ErrNeedsRecompute) {
		t.Fatalf("read corrupt err = %v, want ErrNeedsRecompute", err)
	}
	if m2.Stats().CorruptRecords < 1 {
		t.Fatalf("expected corrupt counter >= 1, got %d", m2.Stats().CorruptRecords)
	}
}

// A torn/corrupt journal tail is truncated on open and never blocks startup or
// gets replayed as a mapping.
func TestRestartTornJournalSkipped(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{BlockBytes: 4, TotalBlocks: 8, DataDir: dir}
	m, _ := New(cfg)
	m.CreateTenant("a", TenantConfig{QuotaBlocks: 8})
	s, _ := m.NewSequence("a", "s1")
	s.Append([]byte("AAAAaaaa"))
	s.Commit()
	m.Close()

	// Append garbage bytes to the journal (simulates a torn write).
	f, err := os.OpenFile(filepath.Join(dir, "journal.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("PJD1\x00\x00\x00\x10badcrc!!!partial"))
	f.Close()

	m2, err := New(cfg)
	if err != nil {
		t.Fatalf("New with torn journal: %v", err)
	}
	defer m2.Close()
	s2, err := m2.Get("a", "s1")
	if err != nil {
		t.Fatalf("intact record lost: %v", err)
	}
	got, err := s2.Read(0, 8)
	if err != nil || string(got) != "AAAAaaaa" {
		t.Fatalf("intact committed data not recovered: %q %v", got, err)
	}
}
