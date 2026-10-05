package pagedkv

import (
	"bytes"
	"crypto/sha256"
)

// blockState describes the residency of a physical block.
type blockState int

const (
	blockFree blockState = iota
	blockResident
)

// keyLen is the length in bytes of every content/prefix key.
const keyLen = sha256.Size

var zeroKey [keyLen]byte

// block is one fixed-size physical page.
//
// A block is either:
//   - shared: a finalized, immutable full block, addressed by its chained
//     content key, referenced by one or more logical block refs. When its
//     reference count drops to zero it becomes an reclaimable LRU entry.
//   - private: the unfilled tail block owned by exactly one sequence
//     (key == nil, tenant == owner). It is never read by anyone else and is
//     freed (or swapped out as a whole unit) directly.
type block struct {
	id       int
	state    blockState
	data     []byte // len == cfg.BlockSize for resident blocks
	key      []byte // chained content key for shared blocks; nil for private
	refs     int    // logical references held by active sequences
	lastUsed uint64 // monotonic LRU tick; smaller is older
	tenant   string // owner of a private block; "" when shared/free
}

// chainKey derives the key of a full block whose preceding full block had
// key prev (empty for the first block). Chaining over the previous key means
// equal contents at different prefix positions produce different keys, while
// equal prefixes share deterministically.
func chainKey(prev, content []byte) []byte {
	h := sha256.New()
	if len(prev) == 0 {
		h.Write(zeroKey[:])
	} else {
		h.Write(prev)
	}
	h.Write(content)
	return h.Sum(nil)
}

// contentKey addresses an arbitrary byte sequence in the swap store. Private
// tail blocks are stored by their raw content hash.
func contentKey(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

func keyEqual(a, b []byte) bool { return bytes.Equal(a, b) }
