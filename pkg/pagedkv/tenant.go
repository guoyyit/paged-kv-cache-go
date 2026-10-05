package pagedkv

// tenant owns block accounting, the cached-prefix index and swap records.
// A tenant can only be reclaimed against itself: satisfying tenant A never
// evicts blocks held by tenant B, and cached content of one tenant is never
// readable by another.
type tenant struct {
	id    string
	quota int
	// resident maps block id -> physical block for blocks held by this tenant.
	resident map[int]*physicalBlock
	// byHash dedups resident blocks carrying identical content (prefix share).
	byHash map[string]int
	cache  *prefixTrie
	// cachedCount counts freeable blocks retained only for prefix reuse.
	cachedCount int
	// swapped holds sequences currently living in the content store.
	swapped map[string]*swappedSequence
	// sequences tracks live (active or swapped) sequence ids.
	sequences map[string]struct{}
	// owners records, per resident block id, the active sequences currently
	// referencing that block (sharing). It lets eviction protect the chain of
	// the sequence that is currently growing.
	owners map[int]map[string]bool
	policy evictionPolicy
}

// swappedSequence is the durable image of a preempted sequence: length plus
// the ordered content hashes of its blocks. It is enough to rebuild the full
// logical buffer after eviction or restart.
type swappedSequence struct {
	id     string
	length int
	hashes []string
}

func newTenant(id string, quota int, policy evictionPolicy) *tenant {
	return &tenant{
		id:        id,
		quota:     quota,
		resident:  map[int]*physicalBlock{},
		byHash:    map[string]int{},
		cache:     newPrefixTrie(),
		swapped:   map[string]*swappedSequence{},
		sequences: map[string]struct{}{},
		owners:    map[int]map[string]bool{},
		policy:    policy,
	}
}

// residentCount is the number of physical pages currently held by the tenant
// (active references plus freeable cached pages).
func (t *tenant) residentCount() int { return len(t.resident) }

// activeRefs counts referenced sequence slots across resident blocks. A shared
// block contributes one reference per sequence slot pointing at it.
func (t *tenant) activeRefs() int {
	n := 0
	for _, b := range t.resident {
		n += b.refs
	}
	return n
}

// register indexes a newly assigned physical block.
func (t *tenant) register(b *physicalBlock) {
	t.resident[b.id] = b
	b.tenant = t.id
	if b.hash != "" {
		t.byHash[b.hash] = b.id
	}
}

// unregister drops a reclaimed block from all tenant indexes.
func (t *tenant) unregister(b *physicalBlock) {
	delete(t.resident, b.id)
	delete(t.owners, b.id)
	if id, ok := t.byHash[b.hash]; ok && id == b.id {
		delete(t.byHash, b.hash)
	}
	t.cache.prune(b.id)
	if b.cached {
		t.cachedCount--
	}
}

// addOwner records that seq references block id.
func (t *tenant) addOwner(id int, seq string) {
	set := t.owners[id]
	if set == nil {
		set = map[string]bool{}
		t.owners[id] = set
	}
	set[seq] = true
}

// removeOwner drops one sequence reference record.
func (t *tenant) removeOwner(id int, seq string) {
	if set, ok := t.owners[id]; ok {
		delete(set, seq)
		if len(set) == 0 {
			delete(t.owners, id)
		}
	}
}

// blockOwnedBySeq reports whether seq currently references the block.
func (t *tenant) blockOwnedBySeq(b *physicalBlock, seq string) bool {
	return t.owners[b.id][seq]
}

// findByHash returns a resident block with identical content, or nil.
func (t *tenant) findByHash(hash string) *physicalBlock {
	if id, ok := t.byHash[hash]; ok {
		return t.resident[id]
	}
	return nil
}

// markCached publishes a fully unreferenced block as a reusable prefix page.
func (t *tenant) markCached(b *physicalBlock) {
	if !b.cached {
		b.cached = true
		t.cachedCount++
	}
}

func (t *tenant) markActive(b *physicalBlock) {
	if b.cached {
		b.cached = false
		t.cachedCount--
	}
}
