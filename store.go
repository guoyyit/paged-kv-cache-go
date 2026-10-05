package pagedkv

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
)

// blockFileMagic marks block-store files.
var blockFileMagic = []byte("PGKBLCK1")

// SwapStore persists block contents keyed by their content key.
//
// Put is atomic: a new version is written to a temp file and renamed, so a
// crash leaves either the previous complete file or the new complete file,
// never a torn write. Every file is checksummed; Get rejects corrupt files
// with ErrCorrupt instead of returning partial data.
type SwapStore interface {
	// Put atomically stores data under key (best-effort durable on return).
	Put(key, data []byte) error
	// Get loads data stored under key. A missing key yields ErrNotFound;
	// a torn or corrupted file yields ErrCorrupt.
	Get(key []byte) ([]byte, error)
	// Has reports whether key is present and readable.
	Has(key []byte) bool
}

// fileStore implements SwapStore with one checksummed file per block.
type fileStore struct {
	dir string
}

// NewFileStore opens (and if needed creates) a block store in dir.
func NewFileStore(dir string) (SwapStore, error) {
	if dir == "" {
		return nil, ErrInvalidArgument
	}
	if err := os.MkdirAll(filepath.Join(dir, "blocks"), 0o755); err != nil {
		return nil, err
	}
	return &fileStore{dir: filepath.Join(dir, "blocks")}, nil
}

func blockPath(dir string, key []byte) string {
	return filepath.Join(dir, hexEncode(key)+".blk")
}

func (s *fileStore) Put(key, data []byte) error {
	if len(key) == 0 {
		return ErrInvalidArgument
	}
	final := blockPath(s.dir, key)
	tmp, err := os.CreateTemp(s.dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	sum := sha256.Sum256(data)
	hdr := make([]byte, len(blockFileMagic)+4)
	copy(hdr, blockFileMagic)
	binary.BigEndian.PutUint32(hdr[len(blockFileMagic):], uint32(len(data)))
	if _, err := tmp.Write(hdr); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if _, err := tmp.Write(sum[:]); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, final); err != nil {
		cleanup()
		return err
	}
	return nil
}

func (s *fileStore) Get(key []byte) ([]byte, error) {
	f, err := os.Open(blockPath(s.dir, key))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	defer f.Close()

	hdr := make([]byte, len(blockFileMagic)+4)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return nil, ErrCorrupt
	}
	if string(hdr[:len(blockFileMagic)]) != string(blockFileMagic) {
		return nil, ErrCorrupt
	}
	n := int(binary.BigEndian.Uint32(hdr[len(blockFileMagic):]))
	if n <= 0 {
		return nil, ErrCorrupt
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, ErrCorrupt
	}
	gotSum := make([]byte, sha256.Size)
	if _, err := io.ReadFull(f, gotSum); err != nil {
		return nil, ErrCorrupt
	}
	wantSum := sha256.Sum256(data)
	if !keyEqual(gotSum, wantSum[:]) {
		return nil, ErrCorrupt
	}
	// Reject trailing junk as well.
	if n1, err := f.Read(make([]byte, 1)); err == nil && n1 > 0 {
		return nil, ErrCorrupt
	} else if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return data, nil
}

func (s *fileStore) Has(key []byte) bool {
	_, err := s.Get(key)
	return err == nil
}
