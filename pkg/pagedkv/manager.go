package pagedkv

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"
)

// Manager owns the global physical block pool, tenants and persistence. It is
// safe for concurrent use: one metadata mutex guards block/tenant/sequence
// state, while a block's payload is immutable once published. Readers
// therefore never see a half-written block and a physical page is never
// handed out twice.
type Manager struct {
	cfg      Config
	mu       sync.Mutex
	closed   bool
	clock    uint64
	blocks   []*physicalBlock
	freeIDs  []int
	tenants  map[string]*tenant
	seqs     map[string]*Sequence
	store    *contentStore
	journal  *journal
	corrupt  int64
	gcCancel context.CancelFunc
	wg       sync.WaitGroup
}

// New creates a Manager and, when DataDir is set, replays the recovery log.
// Partial or corrupt records/objects are skipped (counted in Stats.CorruptRecords)
// and never block startup.
func New(cfg Config) (*Manager, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.DefaultTenantQuota == 0 || cfg.DefaultTenantQuota > cfg.TotalBlocks {
		cfg.DefaultTenantQuota = cfg.TotalBlocks
	}
	m := &Manager{
		cfg:     cfg,
		blocks:  make([]*physicalBlock, cfg.TotalBlocks),
		tenants: map[string]*tenant{},
		seqs:    map[string]*Sequence{},
	}
	for i := 0; i < cfg.TotalBlocks; i++ {
		m.blocks[i] = newPhysicalBlock(i, cfg.BlockBytes)
		m.freeIDs = append(m.freeIDs, i)
	}
	store, err := newContentStore(cfg.DataDir, cfg.MaxSwapBytes)
	if err != nil {
		return nil, err
	}
	m.store = store
	if cfg.DataDir != "" {
		j, skipped, err := openJournal(cfg.DataDir, cfg.SyncWAL)
		if err != nil {
			return nil, err
		}
		m.journal = j
		m.corrupt += skipped
		if err := m.recoverLocked(); err != nil {
			return nil, err
		}
		m.gcUnreferencedLocked()
	}
	if cfg.GCInterval > 0 && m.journal != nil {
		ctx, cancel := context.WithCancel(context.Background())
		m.gcCancel = cancel
		m.wg.Add(1)
		go m.gcLoop(ctx)
	}
	return m, nil
}

func (m *Manager) nextClock() uint64 {
	m.clock++
	return m.clock
}

// CreateTenant registers a tenant with an optional block quota.
func (m *Manager) CreateTenant(id string, tc TenantConfig) error {
	if id == "" {
		return fmt.Errorf("%w: empty tenant id", ErrInvalidArgument)
	}
	quota := tc.QuotaBlocks
	if quota == 0 {
		quota = m.cfg.DefaultTenantQuota
	}
	if quota < 0 || quota > m.cfg.TotalBlocks {
		return fmt.Errorf("%w: quota %d outside pool size %d",
			ErrInvalidArgument, quota, m.cfg.TotalBlocks)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if _, ok := m.tenants[id]; ok {
		return fmt.Errorf("%w: tenant %q", ErrInvalidArgument, id)
	}
	t := newTenant(id, quota, newLRUPolicy())
	m.tenants[id] = t
	if m.journal != nil {
		if err := m.journal.append(journalRecord{Type: recTenant, Tenant: id, Quota: quota}); err != nil {
			delete(m.tenants, id)
			return err
		}
	}
	return nil
}

// DeleteTenant removes a tenant that holds no live sequences.
func (m *Manager) DeleteTenant(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	t, ok := m.tenants[id]
	if !ok {
		return fmt.Errorf("%w: tenant %q", ErrTenantNotFound, id)
	}
	if len(t.sequences) > 0 {
		return fmt.Errorf("%w: tenant %q still has %d sequences",
			ErrInvalidArgument, id, len(t.sequences))
	}
	delete(m.tenants, id)
	if m.journal != nil {
		if err := m.journal.append(journalRecord{Type: recTenant, Tenant: id, Quota: -1}); err != nil {
			return err
		}
	}
	return nil
}

// NewSequence starts an empty active sequence for a tenant.
func (m *Manager) NewSequence(tenantID, seqID string) (*Sequence, error) {
	if seqID == "" {
		return nil, fmt.Errorf("%w: empty sequence id", ErrInvalidArgument)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	t, ok := m.tenants[tenantID]
	if !ok {
		return nil, fmt.Errorf("%w: tenant %q", ErrTenantNotFound, tenantID)
	}
	if _, ok := m.seqs[seqID]; ok {
		return nil, fmt.Errorf("%w: sequence %q", ErrInvalidArgument, seqID)
	}
	s := &Sequence{
		mgr:    m,
		tenant: tenantID,
		id:     seqID,
		state:  seqActive,
	}
	m.seqs[seqID] = s
	t.sequences[seqID] = struct{}{}
	return s, nil
}

// Get returns a sequence belonging to tenantID. Sequences of other tenants
// are never visible: an id owned by another tenant looks like not found.
func (m *Manager) Get(tenantID, seqID string) (*Sequence, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	if _, ok := m.tenants[tenantID]; !ok {
		return nil, fmt.Errorf("%w: tenant %q", ErrTenantNotFound, tenantID)
	}
	s, ok := m.seqs[seqID]
	if !ok || s.tenant != tenantID {
		return nil, fmt.Errorf("%w: sequence %q", ErrSequenceNotFound, seqID)
	}
	return s, nil
}

func (m *Manager) tenantLocked(tenantID string) (*tenant, error) {
	t, ok := m.tenants[tenantID]
	if !ok {
		return nil, fmt.Errorf("%w: tenant %q", ErrTenantNotFound, tenantID)
	}
	return t, nil
}

// touch records an LRU access under the metadata lock.
func (m *Manager) touch(b *physicalBlock) { b.atime = m.nextClock() }

// allocBlockLocked obtains one physical page holding payload (exactly
// BlockBytes) for tenant t. Identical content already resident is shared.
// Otherwise it, in order: takes a free global page, evicts one of the same
// tenant's cached pages in deterministic LRU order, or preempts (swaps out)
// one of the tenant's own active sequences. Other tenants' pages are never
// touched. excludeSeq is the sequence currently growing, which cannot preempt
// itself.
func (m *Manager) allocBlockLocked(t *tenant, payload []byte, excludeSeq string) (*physicalBlock, error) {
	hash := hashPayload(payload)
	if b := t.findByHash(hash); b != nil {
		if b.cached {
			t.markActive(b)
		}
		b.refs++
		t.addOwner(b.id, excludeSeq)
		m.touch(b)
		return b, nil
	}
	for {
		// 1) The tenant has quota headroom and the global pool has a free page.
		if t.residentCount() < t.quota && len(m.freeIDs) > 0 {
			id := m.freeIDs[len(m.freeIDs)-1]
			m.freeIDs = m.freeIDs[:len(m.freeIDs)-1]
			b := m.blocks[id]
			b.tenant = t.id
			b.hash = hash
			copy(b.payload, payload)
			b.refs = 1
			b.pinned = 0
			b.atime = m.nextClock()
			b.cached = false
			b.published = false
			t.register(b)
			t.addOwner(b.id, excludeSeq)
			return b, nil
		}
		// 2) No headroom/free page: reclaim one of THIS tenant's cached pages.
		eligible := m.eligibleCachedLocked(t, excludeSeq)
		if len(eligible) > 0 {
			id := t.policy.victim(t, eligible)
			if id >= 0 {
				m.evictCachedLocked(t, t.resident[id])
				continue
			}
		}
		// 3) Still nothing: preempt one of THIS tenant's own active sequences.
		if victim := m.preemptVictimLocked(t, excludeSeq); victim != nil {
			if err := m.swapSequenceLocked(victim); err != nil {
				return nil, err
			}
			continue
		}
		// 4) Tenant at quota and cannot self-reclaim: quota violation.
		if t.residentCount() >= t.quota {
			return nil, fmt.Errorf("%w: tenant %q quota %d",
				ErrTenantQuotaExceeded, t.id, t.quota)
		}
		// 5) Under quota but the whole global pool is pinned by other tenants.
		return nil, fmt.Errorf("%w: pool full; tenant %q cannot reclaim",
			ErrResourceExhausted, t.id)
	}
}

// eligibleCachedLocked returns freeable cached pages that do not belong to the
// growing sequence's own live chain.
func (m *Manager) eligibleCachedLocked(t *tenant, ownerSeq string) map[int]bool {
	eligible := map[int]bool{}
	for id, b := range t.resident {
		if b.cached && b.freeable() && !t.blockOwnedBySeq(b, ownerSeq) {
			eligible[id] = true
		}
	}
	return eligible
}

// evictCachedLocked returns one freeable cached page of t to the global pool.
func (m *Manager) evictCachedLocked(t *tenant, b *physicalBlock) {
	t.unregister(b)
	m.resetBlockLocked(b)
	m.freeIDs = append(m.freeIDs, b.id)
	// Keep free ids sorted ascending so allocation is deterministic.
	sort.Ints(m.freeIDs)
}

// resetBlockLocked clears all ownership and content of a page returned to the
// global free pool, so it never retains a previous tenant's data.
func (m *Manager) resetBlockLocked(b *physicalBlock) {
	b.tenant = ""
	b.hash = ""
	b.refs = 0
	b.pinned = 0
	b.atime = 0
	b.cached = false
	b.published = false
}

// preemptVictimLocked chooses the active sequence of tenant t whose swap frees
// the most pages; ties break by smallest sequence id. This makes preemption
// deterministic for identical workloads.
func (m *Manager) preemptVictimLocked(t *tenant, excludeSeq string) *Sequence {
	var victim *Sequence
	best := 0
	ids := make([]string, 0, len(t.sequences))
	for id := range t.sequences {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		s := m.seqs[id]
		if s == nil || s.tenant != t.id || s.state != seqActive || id == excludeSeq {
			continue
		}
		// Count distinct physical pages that become reclaimable when this
		// sequence is swapped: pages whose sole owning sequence is itself.
		// (Repeated identical blocks in one chain share a page and count once.)
		seen := map[int]bool{}
		exclusive := 0
		for _, bid := range s.blocks {
			if seen[bid] {
				continue
			}
			seen[bid] = true
			owners := t.owners[bid]
			if len(owners) == 1 && owners[id] {
				exclusive++
			}
		}
		// Only a candidate that actually frees at least one page is useful;
		// swapping any other sequence would loop without making room.
		if exclusive > best {
			best = exclusive
			victim = s
		}
	}
	return victim
}

// swapSequenceLocked persists every block of an active sequence to the content
// store (write-ahead), appends a swap record, then drops its references. Pages
// become published cached pages and are immediately reclaimable, while the
// sequence can be fully resumed from the stored hashes. A persistence failure
// leaves the sequence active and returns the error.
func (m *Manager) swapSequenceLocked(s *Sequence) error {
	t := m.tenants[s.tenant]
	hashes := make([]string, len(s.blocks))
	for i, bid := range s.blocks {
		b := t.resident[bid]
		if b == nil {
			return fmt.Errorf("%w: sequence %q lost block %d", ErrCorrupt, s.id, bid)
		}
		hash, err := m.store.put(b.payload)
		if err != nil {
			return err
		}
		hashes[i] = hash
	}
	if m.journal != nil {
		if err := m.journal.append(journalRecord{
			Type: recSwap, Tenant: s.tenant, Sequence: s.id,
			Length: s.length, Hashes: hashes,
		}); err != nil {
			return err
		}
	}
	t.swapped[s.id] = &swappedSequence{id: s.id, length: s.length, hashes: hashes}
	t.cache.insert(hashes)
	for _, bid := range s.blocks {
		b := t.resident[bid]
		if b == nil {
			continue
		}
		b.refs--
		t.removeOwner(bid, s.id)
		b.published = true
		if b.refs == 0 {
			t.markCached(b)
		}
	}
	s.state = seqSwapped
	s.blocks = nil
	s.hashes = hashes
	return nil
}

// loadHashLocked pages a persisted block hash back into a resident physical
// page (possibly sharing identical resident content).
func (m *Manager) loadHashLocked(t *tenant, hash string, excludeSeq string) (*physicalBlock, error) {
	payload := make([]byte, m.cfg.BlockBytes)
	if err := m.store.get(hash, payload); err != nil {
		return nil, err
	}
	return m.allocBlockLocked(t, payload, excludeSeq)
}

// releaseRefLocked drops one sequence slot's reference on b. Published blocks
// become freeable cached pages; unpublished (uncommitted) content is returned
// to the pool immediately, so aborted work never accumulates as fragments.
func (m *Manager) releaseRefLocked(t *tenant, b *physicalBlock, seq string) {
	b.refs--
	t.removeOwner(b.id, seq)
	if b.refs > 0 {
		return
	}
	if b.published {
		t.markCached(b)
		return
	}
	m.evictCachedLocked(t, b)
}

// Stats returns manager-wide counters.
func (m *Manager) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := Stats{
		TotalBlocks:    int64(m.cfg.TotalBlocks),
		Tenants:        int64(len(m.tenants)),
		CorruptRecords: m.corrupt,
	}
	for _, b := range m.blocks {
		if b.tenant != "" {
			st.ResidentBlocks++
			if b.refs > 0 {
				st.ActiveRefs += int64(b.refs)
			}
			if b.cached {
				st.CachedBlocks++
			}
		}
	}
	for _, t := range m.tenants {
		st.SwappedBlocks += int64(len(t.swapped))
		st.Sequences += int64(len(t.sequences))
	}
	if m.store != nil {
		st.ContentStoreBytes = m.store.bytes()
	}
	return st
}

// TenantStats returns per-tenant counters.
func (m *Manager) TenantStats(id string) (TenantStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return TenantStats{}, ErrClosed
	}
	t, ok := m.tenants[id]
	if !ok {
		return TenantStats{}, fmt.Errorf("%w: tenant %q", ErrTenantNotFound, id)
	}
	st := TenantStats{
		TenantID:       id,
		QuotaBlocks:    t.quota,
		ResidentBlocks: t.residentCount(),
		CachedBlocks:   t.cachedCount,
		SwappedBlocks:  len(t.swapped),
		Sequences:      len(t.sequences),
	}
	for _, b := range t.resident {
		st.ActiveRefs += b.refs
	}
	return st, nil
}

func (m *Manager) gcLoop(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.GCInterval)
	defer ticker.Stop()
	defer m.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.mu.Lock()
			if m.closed {
				m.mu.Unlock()
				return
			}
			m.compactLocked()
			m.mu.Unlock()
		}
	}
}

// referencedHashesLocked collects every content hash still needed by durable
// sequences (swapped or committed/restored with hash chains).
func (m *Manager) referencedHashesLocked() map[string]struct{} {
	live := map[string]struct{}{}
	for _, t := range m.tenants {
		for _, sw := range t.swapped {
			for _, h := range sw.hashes {
				live[h] = struct{}{}
			}
		}
	}
	for _, s := range m.seqs {
		if s.state == seqNeedsRecompute || s.state == seqAborted {
			continue
		}
		for _, h := range s.hashes {
			live[h] = struct{}{}
		}
	}
	return live
}

func (m *Manager) gcUnreferencedLocked() {
	if m.store == nil || m.store.dir == "" {
		return
	}
	live := m.referencedHashesLocked()
	root := m.store.dir + "/objects"
	_ = walkObjects(root, func(hash string) {
		if _, ok := live[hash]; !ok {
			_ = m.store.delete(hash)
		}
	})
}

func walkObjects(root string, fn func(hash string)) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) != 2 {
			continue
		}
		sub := root + "/" + e.Name()
		files, err := os.ReadDir(sub)
		if err != nil {
			continue
		}
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			fn(f.Name())
		}
	}
	return nil
}

// compactLocked rewrites the journal from current durable state and removes
// content objects nobody references anymore.
func (m *Manager) compactLocked() {
	if m.journal == nil {
		return
	}
	recs := []journalRecord{}
	for _, t := range m.tenants {
		recs = append(recs, journalRecord{Type: recTenant, Tenant: t.id, Quota: t.quota})
	}
	ids := make([]string, 0, len(m.seqs))
	for id := range m.seqs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		s := m.seqs[id]
		switch s.state {
		case seqCommitted:
			recs = append(recs, journalRecord{
				Type: recCommit, Tenant: s.tenant, Sequence: s.id,
				Length: s.length, Hashes: s.hashes,
			})
		case seqSwapped:
			recs = append(recs, journalRecord{
				Type: recSwap, Tenant: s.tenant, Sequence: s.id,
				Length: s.length, Hashes: s.hashes,
			})
		case seqActive:
			if len(s.hashes) > 0 {
				recs = append(recs, journalRecord{
					Type: recRestore, Tenant: s.tenant, Sequence: s.id,
					Length: s.length, Hashes: s.hashes,
				})
			}
		}
	}
	if err := m.journal.rewrite(recs); err == nil {
		m.gcUnreferencedLocked()
	}
}

// Close persists state and stops background work.
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	if m.gcCancel != nil {
		m.gcCancel()
	}
	var err error
	if m.journal != nil {
		m.compactLocked()
		err = m.journal.close()
	}
	m.mu.Unlock()
	m.wg.Wait()
	return err
}
