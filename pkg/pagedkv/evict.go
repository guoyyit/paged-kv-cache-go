package pagedkv

// evictionPolicy chooses reclaimable cached blocks in deterministic order.
type evictionPolicy interface {
	// victim returns the block id of the next block to evict among eligible
	// block ids, or -1. eligible is supplied by the manager so that pages
	// forming the currently growing sequence's own chain are excluded: under
	// pressure that sequence must preempt someone else rather than lose its
	// own prefix.
	victim(t *tenant, eligible map[int]bool) int
}

// lruPolicy orders eligible cached blocks by (atime, block id) ascending.
// The block-id tie-break makes the reclaim order a pure function of the
// sequence of operations: identical inputs always reclaim identical blocks.
type lruPolicy struct{}

func newLRUPolicy() *lruPolicy { return &lruPolicy{} }

func (p *lruPolicy) victim(t *tenant, eligible map[int]bool) int {
	best := -1
	var bestAtime uint64
	for id := range eligible {
		b := t.resident[id]
		if b == nil || !b.cached || !b.freeable() {
			continue
		}
		if best == -1 || b.atime < bestAtime || (b.atime == bestAtime && id < best) {
			best = id
			bestAtime = b.atime
		}
	}
	return best
}
