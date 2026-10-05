package pagedkv

import (
	"bytes"
	"testing"
)

func newTestManager(t *testing.T, blockBytes, totalBlocks int) *Manager {
	t.Helper()
	m, err := New(Config{BlockBytes: blockBytes, TotalBlocks: totalBlocks})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func TestSmokeAppendRead(t *testing.T) {
	m := newTestManager(t, 4, 8)
	if err := m.CreateTenant("t1", TenantConfig{QuotaBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	s, err := m.NewSequence("t1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("abcdefgh"), 3) // 24 bytes => 6 blocks of 4
	if err := s.Append(payload); err != nil {
		t.Fatalf("Append: %v", err)
	}
	got, err := s.Read(0, len(payload))
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("data mismatch: got %q want %q", got, payload)
	}
	partial, err := s.Read(5, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(partial, payload[5:12]) {
		t.Fatalf("partial mismatch: got %q want %q", partial, payload[5:12])
	}
}
