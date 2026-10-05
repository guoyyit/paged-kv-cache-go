package pagedkv

// physicalBlock is one fixed-size page of physical memory in the global pool.
// Payload is immutable for as long as the block carries active references or
// is published as a cached block; writers take a copy-on-write copy instead,
// which is what guarantees independent sequences never observe each other's
// rewrites.
type physicalBlock struct {
	id      int
	tenant  string
	hash    string
	payload []byte
	// refs is the number of sequence slots pointing at this block.
	refs int
	// pinned briefly marks a block non-evictable during multi-step operations.
	pinned int
	// atime is the logical-clock timestamp of the last access (LRU order).
	atime uint64
	// cached marks a block retained only as reusable committed/swapped prefix.
	cached bool
	// published means the block's content is durable (part of a committed or
	// swapped chain), so on last release it becomes a cache page rather than
	// being returned immediately to the free pool.
	published bool
}

func newPhysicalBlock(id, blockBytes int) *physicalBlock {
	return &physicalBlock{id: id, payload: make([]byte, blockBytes)}
}

// freeable reports whether the block can be reclaimed without preemption.
func (b *physicalBlock) freeable() bool { return b.refs == 0 && b.pinned == 0 }
