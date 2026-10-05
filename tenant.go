package pagedkv

// tenant tracks per-tenant quota and resident private block usage.
//
// Only private tail blocks are charged against a tenant's quota; finalized
// full blocks become global, content-addressed shared cache and are never
// counted against any tenant. This guarantees that reclaiming or overwriting
// one tenant's blocks can never consume another tenant's quota.
type tenant struct {
	id          string
	quotaBlocks int
	privateUsed int // resident private blocks currently held
}

// atQuota reports whether allocating one more private block would exceed quota.
func (t *tenant) atQuota() bool {
	return t.privateUsed >= t.quotaBlocks
}
