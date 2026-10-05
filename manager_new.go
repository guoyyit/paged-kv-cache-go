package pagedkv

import (
	"os"
)

// NewManager builds a manager from cfg (applying defaults and validation).
func NewManager(cfg Config) (*Manager, error) {
	c, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	return newManager(c)
}

// newManager assumes cfg is already validated.
func newManager(cfg Config) (*Manager, error) {
	m := &Manager{
		cfg:        cfg,
		blocks:     make(map[int]*block, cfg.TotalBlocks),
		keyIndex:   make(map[string]int),
		storedKeys: make(map[string]bool),
		tenants:    make(map[string]*tenant),
		sequences:  make(map[string]*Sequence),
		evictPol:   newEvictionPolicy(cfg.EvictionPolicy),
		preemptPol: newPreemptionPolicy(cfg.PreemptionPolicy),
	}
	// All blocks start free. LIFO free list is fine because allocation
	// order is not observable: identity is key-based, not id-based.
	for id := 0; id < cfg.TotalBlocks; id++ {
		m.blocks[id] = &block{id: id, state: blockFree}
		m.freeList = append(m.freeList, id)
	}

	if cfg.DataDir != "" {
		if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
			return nil, err
		}
		store, err := NewFileStore(cfg.DataDir)
		if err != nil {
			return nil, err
		}
		m.store = store
		j, err := openJournal(cfg.DataDir)
		if err != nil {
			return nil, err
		}
		m.jrn = j
	}
	return m, nil
}
