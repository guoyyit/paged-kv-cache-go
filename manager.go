package pagedkv

import "sync"

// Manager owns the block pool, tenants, sequences and persistence.
//
// All state transitions take the single manager mutex; methods that touch
// disk do so while holding it. This keeps the visible state of any block
// self-consistent: a reader can never observe half-written block content, and
// each physical block is handed to at most one logical owner at a time.
type Manager struct {
	mu sync.RWMutex

	cfg   Config
	store SwapStore // nil when persistence is disabled
	jrn   *journal

	blocks     map[int]*block  // physical pool, indexed by block id
	freeList   []int           // ids of free blocks
	keyIndex   map[string]int  // chained key -> resident shared block id
	storedKeys map[string]bool // content keys already durably stored
	tenants    map[string]*tenant
	sequences  map[string]*Sequence

	tick       uint64 // monotonic, drives LRU and creation ordering
	orderSeq   uint64
	evictPol   evictionPolicy
	preemptPol preemptionPolicy
}

// Close flushes the journal and releases all resources. After Close the
// manager must not be used.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.jrn != nil {
		err := m.jrn.sync()
		if cerr := m.jrn.close(); err == nil {
			err = cerr
		}
		return err
	}
	return nil
}

// SequenceState returns a point-in-time residency snapshot for seq. Use this
// instead of reading Sequence fields directly: another request's allocation
// can preempt this sequence at any time.
func (m *Manager) SequenceState(seq *Sequence) State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cur, ok := m.sequences[seq.id]
	if !ok || cur != seq {
		return StateInvalid
	}
	return cur.stateSnapshot()
}
