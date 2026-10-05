package pagedkv

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestCommitAndRecover(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	data := []byte("AAAAAAAABBBBBBBBt") // 2 full blocks + private tail

	func() {
		m, err := NewManager(Config{DataDir: dir, BlockSize: 8, TotalBlocks: 8})
		if err != nil {
			t.Fatal(err)
		}
		defer m.Close()
		_ = m.RegisterTenant("t1", 8)
		s, _ := m.NewSequence("t1", "persisted", 0)
		if err := m.Append(s, data); err != nil {
			t.Fatal(err)
		}
		if err := m.CommitSequence(s); err != nil {
			t.Fatalf("commit: %v", err)
		}
	}()

	m2, issues, err := Recover(Config{DataDir: dir, BlockSize: 8, TotalBlocks: 8})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	defer m2.Close()
	if len(issues) != 0 {
		t.Fatalf("unexpected recovery issues: %+v", issues)
	}
	s2 := m2.sequences["persisted"]
	if s2 == nil || s2.state != seqPreempted {
		t.Fatalf("recovered sequence wrong: %+v", s2)
	}
	got, err := m2.Read(s2, 0, len(data))
	if err != nil {
		t.Fatalf("read after recover: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data after recover mismatch: %q", got)
	}
}

// A missing block file invalidates exactly the mapping that references it;
// startup still succeeds and other mappings remain usable.
func TestRecoverDroppedBlockDoesNotBlockStartup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	goodData := []byte("keep-seq-data!!") // 2 full blocks
	var badKeys [][]byte
	func() {
		m, _ := NewManager(Config{DataDir: dir, BlockSize: 8, TotalBlocks: 8})
		defer m.Close()
		_ = m.RegisterTenant("t1", 8)
		s1, _ := m.NewSequence("t1", "bad", 0)
		_ = m.Append(s1, []byte("AAAAAAAABBBBBBBB"))
		if err := m.CommitSequence(s1); err != nil {
			t.Fatal(err)
		}
		for _, jb := range s1.lastCommit {
			badKeys = append(badKeys, jb.key)
		}
		s2, _ := m.NewSequence("t1", "good", 0)
		_ = m.Append(s2, goodData)
		if err := m.CommitSequence(s2); err != nil {
			t.Fatal(err)
		}
	}()

	// Delete exactly the second block file of the "bad" mapping. Both
	// sequences start with different bytes so their keys never overlap.
	badPath := blockPath(filepath.Join(dir, "blocks"), badKeys[1])
	if err := os.Remove(badPath); err != nil {
		t.Fatalf("remove bad block: %v", err)
	}

	m2, issues, err := Recover(Config{DataDir: dir, BlockSize: 8, TotalBlocks: 8})
	if err != nil {
		t.Fatalf("startup must not fail: %v", err)
	}
	defer m2.Close()
	if len(issues) == 0 {
		t.Fatal("expected a recovery issue for the broken mapping")
	}
	if _, ok := m2.sequences["bad"]; ok {
		t.Fatal("bad mapping must not be recovered with missing blocks")
	}
	good := m2.sequences["good"]
	if good == nil {
		t.Fatal("intact mapping must survive")
	}
	got, err := m2.Read(good, 0, good.Length())
	if err != nil || !bytes.Equal(got, goodData) {
		t.Fatalf("surviving mapping wrong: %q %v", got, err)
	}
}

// A corrupt block file (bad checksum) must never be served as valid cache.
func TestRecoverCorruptBlockRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	func() {
		m, _ := NewManager(Config{DataDir: dir, BlockSize: 8, TotalBlocks: 8})
		defer m.Close()
		_ = m.RegisterTenant("t1", 8)
		s, _ := m.NewSequence("t1", "s", 0)
		_ = m.Append(s, []byte("AAAAAAAABBBBBBBB"))
		if err := m.CommitSequence(s); err != nil {
			t.Fatal(err)
		}
	}()
	// Flip bytes inside every block file after the header but before the
	// checksum, so checksum validation must reject.
	blocksDir := filepath.Join(dir, "blocks")
	files, _ := os.ReadDir(blocksDir)
	for _, f := range files {
		p := filepath.Join(blocksDir, f.Name())
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > 20 {
			b[len(b)-20] ^= 0xFF
			_ = os.WriteFile(p, b, 0o644)
		}
	}
	m2, issues, err := Recover(Config{DataDir: dir, BlockSize: 8, TotalBlocks: 8})
	if err != nil {
		t.Fatalf("startup must succeed: %v", err)
	}
	defer m2.Close()
	if len(m2.sequences) != 0 {
		t.Fatalf("corrupt mapping must not be recovered, got %d", len(m2.sequences))
	}
	if len(issues) == 0 {
		t.Fatal("expected corruption issues to be reported")
	}
}

// A partially written journal (tail truncation) must be ignored at the tail
// without blocking startup.
func TestRecoverTornJournal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	func() {
		m, _ := NewManager(Config{DataDir: dir, BlockSize: 8, TotalBlocks: 8})
		defer m.Close()
		_ = m.RegisterTenant("t1", 8)
		s, _ := m.NewSequence("t1", "s", 0)
		_ = m.Append(s, []byte("AAAAAAAA"))
		_ = m.CommitSequence(s)
	}()
	jp := filepath.Join(dir, journalName)
	b, err := os.ReadFile(jp)
	if err != nil {
		t.Fatal(err)
	}
	// Append junk then truncate: simulate a torn record.
	b = append(b, 0x09, 0x00, 0x00, 0x00, 0x05, 0xff, 0xff)
	_ = os.WriteFile(jp, b, 0o644)

	m2, _, err := Recover(Config{DataDir: dir, BlockSize: 8, TotalBlocks: 8})
	if err != nil {
		t.Fatalf("torn journal must not block startup: %v", err)
	}
	defer m2.Close()
	s := m2.sequences["s"]
	if s == nil {
		t.Fatal("committed mapping before torn tail should survive")
	}
	got, err := m2.Read(s, 0, 8)
	if err != nil || string(got) != "AAAAAAAA" {
		t.Fatalf("surviving data wrong: %q %v", got, err)
	}
}

func TestRecoverEmptyDir(t *testing.T) {
	dir := t.TempDir()
	m, issues, err := Recover(Config{DataDir: dir, BlockSize: 8, TotalBlocks: 8})
	if err != nil {
		t.Fatalf("empty dir recovery: %v", err)
	}
	defer m.Close()
	if len(issues) != 0 || len(m.sequences) != 0 {
		t.Fatalf("empty state expected, issues=%v", issues)
	}
}
