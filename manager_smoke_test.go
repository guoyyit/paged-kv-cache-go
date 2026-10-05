package pagedkv

import (
	"bytes"
	"testing"
)

func newTestManager(t *testing.T, blocks, blockSize int) *Manager {
	t.Helper()
	m, err := NewManager(Config{BlockSize: blockSize, TotalBlocks: blocks})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func TestAppendReadBasic(t *testing.T) {
	m := newTestManager(t, 16, 8)
	if err := m.RegisterTenant("t1", 16); err != nil {
		t.Fatal(err)
	}
	seq, err := m.NewSequence("t1", "s1", 0)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("abcdefgh"), 3) // 24 bytes, 3 full blocks
	if err := m.Append(seq, data); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if seq.Length() != 24 {
		t.Fatalf("length = %d, want 24", seq.Length())
	}
	got, err := m.Read(seq, 0, 24)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data mismatch")
	}
	// Partial read with offset
	got, err = m.Read(seq, 5, 10)
	if err != nil || !bytes.Equal(got, data[5:15]) {
		t.Fatalf("offset read mismatch: %v %q", err, got)
	}
	// Out of bounds
	if _, err := m.Read(seq, 20, 10); err != ErrOutOfBounds {
		t.Fatalf("want ErrOutOfBounds, got %v", err)
	}
}

func TestPartialTail(t *testing.T) {
	m := newTestManager(t, 16, 8)
	_ = m.RegisterTenant("t1", 16)
	seq, _ := m.NewSequence("t1", "s1", 0)
	if err := m.Append(seq, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err := m.Read(seq, 0, 5)
	if err != nil || string(got) != "hello" {
		t.Fatalf("got %q, %v", got, err)
	}
	if err := m.Append(seq, []byte(" world!!!")); err != nil {
		t.Fatal(err)
	}
	got, _ = m.Read(seq, 0, seq.Length())
	if string(got) != "hello world!!!" {
		t.Fatalf("got %q", got)
	}
}
