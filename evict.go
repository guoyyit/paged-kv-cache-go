package pagedkv

import "sort"

// evictionPolicy chooses an unreferenced resident shared block to drop.
type evictionPolicy interface {
	// pickVictimBlock returns a block id or -1.
	pickVictimBlock(m *Manager) int
}

// preemptionPolicy chooses an active sequence to swap out under pressure.
type preemptionPolicy interface {
	// pickVictimSequence returns a sequence id or "". exclude is never chosen
	// (it is the request that currently needs the block).
	pickVictimSequence(m *Manager, exclude string) string
}

// lruEviction chooses the unreferenced shared resident block whose lastUsed
// tick is oldest. Equal ticks (possible only in synthetic setups) break ties
// by lower block id, so reclaim order is a deterministic function of state.
type lruEviction struct{}

func (lruEviction) pickVictimBlock(m *Manager) int {
	bestID := -1
	var bestTick uint64
	found := false
	for id, blk := range m.blocks {
		if blk.state != blockResident || blk.key == nil {
			continue // only finalized shared blocks participate in LRU
		}
		if blk.refs > 0 {
			continue // never evict a block still referenced
		}
		if !found || blk.lastUsed < bestTick ||
			(blk.lastUsed == bestTick && id < bestID) {
			found = true
			bestID = id
			bestTick = blk.lastUsed
		}
	}
	return bestID
}

// oldestPreemption swaps out the active sequence with the smallest creation
// tick (ties broken by sequence id): earliest-created requests yield first.
type oldestPreemption struct{}

func (oldestPreemption) pickVictimSequence(m *Manager, exclude string) string {
	var best string
	var bestOrder uint64
	found := false
	for id, seq := range m.sequences {
		if seq.state != seqActive || id == exclude {
			continue
		}
		if !found || seq.order < bestOrder ||
			(seq.order == bestOrder && id < best) {
			found = true
			best = id
			bestOrder = seq.order
		}
	}
	return best
}

// priorityPreemption swaps out the active sequence with the lowest priority;
// creation order (then id) deterministically breaks ties.
type priorityPreemption struct{}

func (priorityPreemption) pickVictimSequence(m *Manager, exclude string) string {
	candidates := make([]*Sequence, 0)
	for id, seq := range m.sequences {
		if seq.state != seqActive || id == exclude {
			continue
		}
		candidates = append(candidates, seq)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := candidates[i], candidates[j]
		if a.priority != b.priority {
			return a.priority < b.priority
		}
		if a.order != b.order {
			return a.order < b.order
		}
		return a.id < b.id
	})
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0].id
}

func newEvictionPolicy(name string) evictionPolicy {
	switch name {
	case policyLRU:
		return lruEviction{}
	default:
		return lruEviction{}
	}
}

func newPreemptionPolicy(name string) preemptionPolicy {
	switch name {
	case policyPriority:
		return priorityPreemption{}
	case policyOldest:
		fallthrough
	default:
		return oldestPreemption{}
	}
}
