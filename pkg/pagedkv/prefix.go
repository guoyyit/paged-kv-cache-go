package pagedkv

// prefixNode is one level of a per-tenant content-addressed prefix trie.
// A node represents the full block prefix reaching it; blockHash is the hash
// of that block's content and children extend the prefix one block further.
// Because the key at every edge is the block content hash, two committed
// chains share trie nodes exactly when their prefix bytes are identical.
type prefixNode struct {
	children  map[string]*prefixNode
	blockHash string
	// blockID points at a resident copy when one is still available.
	blockID int
}

func newPrefixNode() *prefixNode {
	return &prefixNode{children: map[string]*prefixNode{}, blockID: -1}
}

// prefixTrie indexes committed prefixes by their chain of block content hashes.
type prefixTrie struct {
	root *prefixNode
}

func newPrefixTrie() *prefixTrie {
	return &prefixTrie{root: newPrefixNode()}
}

// match returns the length (in blocks) of the longest committed prefix shared
// with hashes, plus the node reached at that depth. A length of 0 returns the
// root node so callers can insert from it.
func (t *prefixTrie) match(hashes []string) (int, *prefixNode) {
	node := t.root
	depth := 0
	for depth < len(hashes) {
		next, ok := node.children[hashes[depth]]
		if !ok {
			break
		}
		node = next
		depth++
	}
	return depth, node
}

// insert registers a committed block-hash chain, creating missing nodes.
func (t *prefixTrie) insert(hashes []string) {
	node := t.root
	for _, h := range hashes {
		next, ok := node.children[h]
		if !ok {
			next = newPrefixNode()
			next.blockHash = h
			node.children[h] = next
		}
		node = next
	}
}

// nodeAt walks the full chain and returns its terminal node, or nil if any
// edge is missing. The empty chain resolves to the root.
func (t *prefixTrie) nodeAt(hashes []string) *prefixNode {
	node := t.root
	for _, h := range hashes {
		next, ok := node.children[h]
		if !ok {
			return nil
		}
		node = next
	}
	return node
}

// prune removes the resident block pointer for a block id, so future prefix
// matches reload content from the content store instead of using a freed page.
func (t *prefixTrie) prune(blockID int) {
	var walk func(*prefixNode) bool
	walk = func(n *prefixNode) bool {
		if n.blockID == blockID {
			n.blockID = -1
		}
		for _, c := range n.children {
			if walk(c) {
				return true
			}
		}
		return false
	}
	walk(t.root)
}
