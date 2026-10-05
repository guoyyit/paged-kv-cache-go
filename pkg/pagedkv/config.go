package pagedkv

import (
	"errors"
	"time"
)

// EvictionPolicy names the reclaim order used under memory pressure.
// Only "lru" is currently implemented. The order is deterministic:
// blocks are ordered by (last-access logical clock, block id), so identical
// inputs always produce identical reclaim decisions.
type EvictionPolicy string

const (
	EvictLRU EvictionPolicy = "lru"
)

// Config configures a Manager.
type Config struct {
	// BlockBytes is the fixed payload size of a block in bytes. Required > 0.
	BlockBytes int
	// TotalBlocks is the size of the global physical block pool. Required > 0.
	TotalBlocks int
	// DataDir holds the content store and recovery journal. Empty disables
	// persistence (swap still works in memory; nothing survives restart).
	DataDir string
	// DefaultTenantQuota is the resident block quota applied to tenants
	// created without an explicit quota. 0 means TotalBlocks.
	DefaultTenantQuota int
	// Policy selects the eviction policy. Zero value means EvictLRU.
	Policy EvictionPolicy
	// SyncWAL forces an fsync after each committed journal record.
	SyncWAL bool
	// MaxSwapBytes caps on-disk swap+cache usage. 0 means unlimited.
	MaxSwapBytes int64
	// GCInterval between idle journal/content garbage collections.
	// Zero disables background GC.
	GCInterval time.Duration
}

func (c Config) validate() error {
	if c.BlockBytes <= 0 {
		return errors.New("pagedkv: BlockBytes must be > 0")
	}
	if c.TotalBlocks <= 0 {
		return errors.New("pagedkv: TotalBlocks must be > 0")
	}
	switch c.Policy {
	case "", EvictLRU:
	default:
		return errors.New("pagedkv: unsupported eviction policy")
	}
	if c.DefaultTenantQuota < 0 || c.MaxSwapBytes < 0 {
		return errors.New("pagedkv: negative quota or swap limit")
	}
	return nil
}

// TenantConfig describes a tenant and its quota.
type TenantConfig struct {
	// QuotaBlocks is the maximum number of resident physical blocks the tenant
	// may hold (active + cached). 0 falls back to the manager default.
	QuotaBlocks int
}

// Stats reports manager-wide counters.
type Stats struct {
	TotalBlocks       int64
	ResidentBlocks    int64
	ActiveRefs        int64
	CachedBlocks      int64
	SwappedBlocks     int64
	Tenants           int64
	Sequences         int64
	CorruptRecords    int64
	ContentStoreBytes int64
}

// TenantStats reports per-tenant counters.
type TenantStats struct {
	TenantID       string
	QuotaBlocks    int
	ResidentBlocks int
	ActiveRefs     int
	CachedBlocks   int
	SwappedBlocks  int
	Sequences      int
}
