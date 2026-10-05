package pagedkv

import (
	"bufio"
	"encoding/hex"
	"io"
	"sort"
)

func hexEncode(b []byte) string { return hex.EncodeToString(b) }

func newBufWriter(w io.Writer) *bufio.Writer { return bufio.NewWriter(w) }

// sortByOrder orders sequences by creation tick, then id (deterministic).
func sortByOrder(seqs []*Sequence) {
	sort.SliceStable(seqs, func(i, j int) bool {
		a, b := seqs[i], seqs[j]
		if a.order != b.order {
			return a.order < b.order
		}
		return a.id < b.id
	})
}
