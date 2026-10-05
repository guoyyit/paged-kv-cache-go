package pagedkv

import (
	"errors"
	"testing"
)

func TestTenantRegistrationErrors(t *testing.T) {
	m := newTestManager(t, 32, 8)
	if err := m.RegisterTenant("t1", 4); err != nil {
		t.Fatal(err)
	}
	if err := m.RegisterTenant("t1", 4); !errors.Is(err, ErrTenantExists) {
		t.Fatalf("dup tenant: %v", err)
	}
	if err := m.RegisterTenant("", 4); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty id: %v", err)
	}
	if err := m.RegisterTenant("t2", 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("zero quota: %v", err)
	}
	if _, err := m.NewSequence("ghost", "s", 0); !errors.Is(err, ErrTenantNotFound) {
		t.Fatalf("unknown tenant: %v", err)
	}
}

// Only the private tail is charged per tenant; filling full blocks converts
// them into global shared cache and frees quota.
func TestQuotaChargesPrivateOnly(t *testing.T) {
	m := newTestManager(t, 64, 8)
	_ = m.RegisterTenant("t1", 1)
	s, _ := m.NewSequence("t1", "s", 0)
	// Quota 1: can hold one private tail; once it fills it becomes shared
	// and the next tail is allowed.
	if err := m.Append(s, []byte("AAAAAAAA")); err != nil { // 1 full block
		t.Fatalf("first full block: %v", err)
	}
	if err := m.Append(s, []byte("BBBBBBBB")); err != nil {
		t.Fatalf("second full block: %v", err)
	}
	// One more tail in flight is allowed exactly up to quota.
	if err := m.Append(s, []byte("x")); err != nil {
		t.Fatalf("private tail within quota: %v", err)
	}
	// No further tail can be created while the private one is open, unless
	// it gets filled first.
	s2, _ := m.NewSequence("t1", "s2", 0)
	if err := m.Append(s2, []byte("y")); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("quota exceeded expected, got %v", err)
	}
}

// One tenant hitting its quota must never reclaim another tenant's private
// blocks, and must never read another tenant's bytes.
func TestTenantIsolationUnderQuota(t *testing.T) {
	m := newTestManager(t, 10, 8)
	_ = m.RegisterTenant("t1", 1)
	_ = m.RegisterTenant("t2", 1)

	s1, _ := m.NewSequence("t1", "s1", 0)
	s2, _ := m.NewSequence("t2", "s2", 0)
	// Both tenants occupy a private tail with distinct content.
	if err := m.Append(s1, []byte("tnnt-1")); err != nil { // 6 bytes: stays private
		t.Fatal(err)
	}
	if err := m.Append(s2, []byte("tnnt-2")); err != nil { // 6 bytes: stays private
		t.Fatal(err)
	}

	// t1 wants more private tails: quota must block it without touching t2.
	s1b, _ := m.NewSequence("t1", "s1b", 0)
	err := m.Append(s1b, []byte("more!"))
	if !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("want ErrQuotaExceeded, got %v", err)
	}

	// t2's content is intact and readable only through its own sequence.
	got, err := m.Read(s2, 0, 6)
	if err != nil || string(got) != "tnnt-2" {
		t.Fatalf("t2 content corrupted: %q %v", got, err)
	}
}

// Released sequences return both private blocks and shared refs so quota
// accounting stays balanced.
func TestReleaseRefundsQuota(t *testing.T) {
	m := newTestManager(t, 16, 8)
	_ = m.RegisterTenant("t1", 2)
	s, _ := m.NewSequence("t1", "s", 0)
	_ = m.Append(s, []byte("AAAAAAAABBBBBBBBx"))
	if m.tenants["t1"].privateUsed != 1 {
		t.Fatalf("privateUsed = %d, want 1", m.tenants["t1"].privateUsed)
	}
	m.ReleaseSequence(s)
	if m.tenants["t1"].privateUsed != 0 {
		t.Fatalf("privateUsed after release = %d, want 0", m.tenants["t1"].privateUsed)
	}
	s2, _ := m.NewSequence("t1", "s2", 0)
	if err := m.Append(s2, []byte("y")); err != nil {
		t.Fatalf("reallocation after release failed: %v", err)
	}
}
