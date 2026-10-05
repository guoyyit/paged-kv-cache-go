package pagedkv

import (
	"errors"
	"testing"
)

// One tenant filling the pool must never have its active blocks evicted to serve
// another tenant; cross-tenant access is indistinguishable from "not found".
func TestTenantIsolation(t *testing.T) {
	m := newTestManager(t, 4, 8)
	if err := m.CreateTenant("a", TenantConfig{QuotaBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	if err := m.CreateTenant("b", TenantConfig{QuotaBlocks: 8}); err != nil {
		t.Fatal(err)
	}
	sa, _ := m.NewSequence("a", "sa")
	sb, _ := m.NewSequence("b", "sb")
	// a occupies the entire pool with 8 distinct active blocks (32 bytes).
	aData := []byte("AAAAaaaaBBBBbbbbCCCCccccDDDDdddd")
	if err := sa.Append(aData); err != nil {
		t.Fatalf("a fill: %v", err)
	}
	// Tenant b asks for one block while the pool is full with a's active data.
	// It can only reclaim/preempt within tenant b, which is empty, so this
	// must fail instead of stealing from a.
	err := sb.Append([]byte("XXXX"))
	if err == nil {
		t.Fatal("tenant b appended by stealing tenant a's blocks")
	}
	if !errors.Is(err, ErrTenantQuotaExceeded) && !errors.Is(err, ErrResourceExhausted) {
		t.Fatalf("unexpected err: %v", err)
	}
	// a's data must be intact and resident.
	got, err := sa.Read(0, len(aData))
	if err != nil {
		t.Fatalf("tenant a data unreadable after b pressure: %v", err)
	}
	if string(got) != string(aData) {
		t.Fatalf("tenant a content corrupted: %q", got)
	}
	stA, _ := m.TenantStats("a")
	if stA.ResidentBlocks != 8 {
		t.Fatalf("tenant a resident blocks = %d, want 8", stA.ResidentBlocks)
	}

	// b cannot read a's sequence: cross-tenant Get must look like not found.
	if _, err := m.Get("b", "sa"); !errors.Is(err, ErrSequenceNotFound) {
		t.Fatalf("cross-tenant Get err = %v, want ErrSequenceNotFound", err)
	}
	// Unknown tenant and unknown sequence are distinguishable sentinels.
	if _, err := m.Get("nope", "x"); !errors.Is(err, ErrTenantNotFound) {
		t.Fatalf("unknown tenant err = %v, want ErrTenantNotFound", err)
	}
	if _, err := m.Get("a", "missing"); !errors.Is(err, ErrSequenceNotFound) {
		t.Fatalf("unknown seq err = %v, want ErrSequenceNotFound", err)
	}
}

// A hard per-tenant quota caps resident blocks even when the global pool has
// room for more.
func TestTenantQuotaCap(t *testing.T) {
	m := newTestManager(t, 4, 16)
	m.CreateTenant("a", TenantConfig{QuotaBlocks: 2})
	sa, _ := m.NewSequence("a", "sa")
	if err := sa.Append([]byte("AAAAaaaa")); err != nil {
		t.Fatal(err)
	}
	if err := sa.Append([]byte("BBBB")); !errors.Is(err, ErrTenantQuotaExceeded) {
		t.Fatalf("over-quota append err = %v, want ErrTenantQuotaExceeded", err)
	}
}

// Reads/writes past the end or at negative offsets are rejected with a
// distinct out-of-range reason.
func TestOutOfRangeRejected(t *testing.T) {
	m := newTestManager(t, 4, 4)
	m.CreateTenant("a", TenantConfig{QuotaBlocks: 4})
	s, _ := m.NewSequence("a", "s")
	if err := s.Append([]byte("12345")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(4, 2); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("read past end err = %v, want ErrOutOfRange", err)
	}
	if _, err := s.Read(-1, 1); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("negative read err = %v, want ErrOutOfRange", err)
	}
	if err := s.Rewrite(4, []byte("xx")); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("rewrite past end err = %v, want ErrOutOfRange", err)
	}
	if err := s.Rewrite(-1, []byte("x")); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("negative rewrite err = %v, want ErrOutOfRange", err)
	}
}

// Committed prefixes are private per tenant even when content is identical:
// tenant b cannot resolve tenant a's committed chain through its own cache.
func TestCommittedPrefixTenantScoped(t *testing.T) {
	m := newTestManager(t, 4, 8)
	m.CreateTenant("a", TenantConfig{QuotaBlocks: 8})
	m.CreateTenant("b", TenantConfig{QuotaBlocks: 8})
	sa, _ := m.NewSequence("a", "sa")
	if err := sa.Append([]byte("SAMEsame")); err != nil {
		t.Fatal(err)
	}
	if err := sa.Commit(); err != nil {
		t.Fatal(err)
	}
	ta := m.tenants["a"]
	tb := m.tenants["b"]
	depth, node := ta.cache.match([]string{hashPayload([]byte("SAME"))})
	if depth != 1 || node == nil {
		t.Fatalf("tenant a cache should know committed prefix, depth=%d", depth)
	}
	depth, _ = tb.cache.match([]string{hashPayload([]byte("SAME"))})
	if depth != 0 {
		t.Fatalf("tenant b must not see tenant a prefix, depth=%d", depth)
	}
}
