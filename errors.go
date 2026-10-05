package pagedkv

// Sentinel errors. Callers can distinguish failure reasons with errors.Is.
var (
	// ErrTenantNotFound is returned when a tenant is not registered.
	ErrTenantNotFound = kvError("pagedkv: tenant not found")
	// ErrTenantExists is returned when registering a duplicate tenant.
	ErrTenantExists = kvError("pagedkv: tenant already exists")
	// ErrQuotaExceeded is returned when a tenant exceeds its block quota.
	ErrQuotaExceeded = kvError("pagedkv: tenant block quota exceeded")
	// ErrOutOfMemory is returned when no block can be reclaimed or swapped.
	ErrOutOfMemory = kvError("pagedkv: no free blocks and no reclaimable victim")
	// ErrUnauthorized is returned on cross-tenant access to another tenant's data.
	ErrUnauthorized = kvError("pagedkv: cross-tenant access denied")
	// ErrOutOfBounds is returned when a read targets an unwritten offset/range.
	ErrOutOfBounds = kvError("pagedkv: read out of bounds")
	// ErrNeedsRecompute is returned when swapped state cannot be fully restored.
	ErrNeedsRecompute = kvError("pagedkv: cache state lost, recomputation required")
	// ErrCorrupt is returned when persisted data fails its checksum.
	ErrCorrupt = kvError("pagedkv: persisted block is corrupt")
	// ErrSequenceNotFound is returned when a sequence id is unknown.
	ErrSequenceNotFound = kvError("pagedkv: sequence not found")
	// ErrInvalidArgument is returned for bad configuration or arguments.
	ErrInvalidArgument = kvError("pagedkv: invalid argument")
)

type kvError string

func (e kvError) Error() string { return string(e) }

// ErrNotFound is returned when a swapped/persisted key is absent.
var ErrNotFound = kvError("pagedkv: persisted key not found")

// ErrSequenceExists is returned when creating a sequence with a used id.
var ErrSequenceExists = kvError("pagedkv: sequence id already exists")

// ErrNoPersistence is returned by commit when the manager has no data dir.
var ErrNoPersistence = kvError("pagedkv: persistence disabled (no DataDir)")
