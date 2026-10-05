package pagedkv

const (
	policyLRU      = "lru"
	policyOldest   = "oldest"
	policyPriority = "priority"
)

// Config configures a Manager.
type Config struct {
	// DataDir is the directory used for swapped blocks and the write-ahead
	// journal. Empty disables persistence (in-memory operation only).
	DataDir string
	// BlockSize is the fixed size in bytes of every block (>= 1).
	BlockSize int
	// TotalBlocks is the number of physical blocks in memory (>= 1).
	TotalBlocks int
	// EvictionPolicy selects reclaim ordering for unreferenced shared
	// blocks: "lru" (least-recently-used, deterministic tie-break by id).
	EvictionPolicy string
	// PreemptionPolicy selects which resident sequence is swapped out
	// under memory pressure: "oldest" (creation order) or "priority"
	// (lowest priority first, creation order as deterministic tie-break).
	PreemptionPolicy string
}

// DefaultConfig returns a config usable for small local deployments.
func DefaultConfig() Config {
	return Config{
		DataDir:          "",
		BlockSize:        4096,
		TotalBlocks:      256,
		EvictionPolicy:   policyLRU,
		PreemptionPolicy: policyOldest,
	}
}

// withDefaults fills zero-valued fields with defaults and validates the result.
func (c Config) withDefaults() (Config, error) {
	// Explicit negative sizes are invalid (zero means "use the default").
	if c.BlockSize < 0 || c.TotalBlocks < 0 {
		return c, ErrInvalidArgument
	}
	d := DefaultConfig()
	if c.DataDir != "" {
		d.DataDir = c.DataDir
	}
	if c.BlockSize > 0 {
		d.BlockSize = c.BlockSize
	}
	if c.TotalBlocks > 0 {
		d.TotalBlocks = c.TotalBlocks
	}
	if c.EvictionPolicy != "" {
		d.EvictionPolicy = c.EvictionPolicy
	}
	if c.PreemptionPolicy != "" {
		d.PreemptionPolicy = c.PreemptionPolicy
	}
	if d.BlockSize <= 0 || d.TotalBlocks <= 0 {
		return d, ErrInvalidArgument
	}
	switch d.EvictionPolicy {
	case policyLRU:
	default:
		return d, ErrInvalidArgument
	}
	switch d.PreemptionPolicy {
	case policyOldest, policyPriority:
	default:
		return d, ErrInvalidArgument
	}
	return d, nil
}
