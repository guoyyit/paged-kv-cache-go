package pagedkv

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// trailerLen terminates every object: uint32 payload length + uint32 CRC32
// (IEEE) of the payload. Torn writes therefore fail verification instead of
// being read back as valid blocks.
const trailerLen = 8

// contentStore persists immutable block payloads, addressed by the SHA-256 hash
// of their content. It backs both swap-out and restart recovery. Objects are
// written through a temp file plus atomic rename, so a crash never leaves a
// partially written object under its final name.
type contentStore struct {
	dir       string
	maxBytes  int64
	usedBytes int64
	// mem backs swap-out when persistence is disabled (DataDir == ""). It
	// makes preemption/resume work inside one process; nothing survives exit.
	mem map[string][]byte
}

func newContentStore(dir string, maxBytes int64) (*contentStore, error) {
	if dir == "" {
		return &contentStore{dir: "", maxBytes: maxBytes, mem: map[string][]byte{}}, nil
	}
	objDir := filepath.Join(dir, "objects")
	if err := os.MkdirAll(objDir, 0o755); err != nil {
		return nil, err
	}
	s := &contentStore{dir: dir, maxBytes: maxBytes}
	if err := s.scan(); err != nil {
		return nil, err
	}
	return s, nil
}

// scan sums the on-disk size of every object file. Unreferenced or corrupt
// objects are left in place here; the manager reconciles references during
// journal replay and garbage-collects what nobody points at.
func (s *contentStore) scan() error {
	root := filepath.Join(s.dir, "objects")
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		s.usedBytes += fi.Size()
		return nil
	})
}

func objectPath(dir, hash string) string {
	return filepath.Join(dir, "objects", hash[:2], hash)
}

// put persists payload and returns its content hash. Identical content is
// stored once. An existing but corrupt object is replaced atomically.
func (s *contentStore) put(payload []byte) (string, error) {
	hash := hashPayload(payload)
	if s.dir == "" {
		if _, ok := s.mem[hash]; !ok {
			size := int64(len(payload) + trailerLen)
			if s.maxBytes > 0 && s.usedBytes+size > s.maxBytes {
				return "", fmt.Errorf("%w: content store limit exceeded", ErrResourceExhausted)
			}
			cp := make([]byte, len(payload))
			copy(cp, payload)
			s.mem[hash] = cp
			s.usedBytes += size
		}
		return hash, nil
	}
	path := objectPath(s.dir, hash)
	if s.exists(hash) {
		// Verify the resident object; a torn file must not satisfy reads.
		f, err := os.Open(path)
		if err == nil {
			err = verifyObject(f, payload)
			f.Close()
		}
		if err == nil {
			return hash, nil
		}
		// Corrupt duplicate: fall through and rewrite it.
	}
	size := int64(len(payload) + trailerLen)
	exists := fileExists(path)
	if !exists && s.maxBytes > 0 && s.usedBytes+size > s.maxBytes {
		return "", fmt.Errorf("%w: content store limit exceeded", ErrResourceExhausted)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	var randBuf [8]byte
	if _, err := rand.Read(randBuf[:]); err != nil {
		return "", err
	}
	tmp := fmt.Sprintf("%s.tmp.%x", path, randBuf[:])
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return "", err
	}
	if _, err := f.Write(payload); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}
	trailer := make([]byte, trailerLen)
	binary.BigEndian.PutUint32(trailer[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(trailer[4:8], crc32.ChecksumIEEE(payload))
	if _, err := f.Write(trailer); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return "", err
	}
	syncDir(filepath.Dir(path))
	if !exists {
		s.usedBytes += size
	}
	return hash, nil
}

// get verifies and loads the object for hash into dst, which must have the
// exact payload length. ErrCorrupt is returned for any checksum/length mismatch.
func (s *contentStore) get(hash string, dst []byte) error {
	if s.dir == "" {
		payload, ok := s.mem[hash]
		if !ok {
			return fmt.Errorf("%w: object %s", ErrNotFound, hash)
		}
		if len(payload) != len(dst) {
			return fmt.Errorf("%w: object %s length mismatch", ErrCorrupt, hash)
		}
		copy(dst, payload)
		return nil
	}
	f, err := os.Open(objectPath(s.dir, hash))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: object %s: %v", ErrNotFound, hash, err)
		}
		return err
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	if len(data) < trailerLen {
		return fmt.Errorf("%w: object %s truncated", ErrCorrupt, hash)
	}
	payload := data[:len(data)-trailerLen]
	trailer := data[len(data)-trailerLen:]
	wantLen := binary.BigEndian.Uint32(trailer[0:4])
	wantCRC := binary.BigEndian.Uint32(trailer[4:8])
	if int(wantLen) != len(payload) || len(dst) != len(payload) ||
		crc32.ChecksumIEEE(payload) != wantCRC {
		return fmt.Errorf("%w: object %s checksum/length mismatch", ErrCorrupt, hash)
	}
	copy(dst, payload)
	return nil
}

func (s *contentStore) delete(hash string) error {
	if s.dir == "" {
		if payload, ok := s.mem[hash]; ok {
			delete(s.mem, hash)
			s.usedBytes -= int64(len(payload) + trailerLen)
		}
		return nil
	}
	path := objectPath(s.dir, hash)
	fi, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	s.usedBytes -= fi.Size()
	return nil
}

func (s *contentStore) exists(hash string) bool {
	if s.dir == "" {
		return false
	}
	_, err := os.Stat(objectPath(s.dir, hash))
	return err == nil
}

func (s *contentStore) bytes() int64 { return s.usedBytes }

func (s *contentStore) sync() error { return nil }

func hashPayload(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func verifyObject(f *os.File, wantPayload []byte) error {
	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	if len(data) < trailerLen {
		return fmt.Errorf("%w: truncated", ErrCorrupt)
	}
	payload := data[:len(data)-trailerLen]
	trailer := data[len(data)-trailerLen:]
	if binary.BigEndian.Uint32(trailer[0:4]) != uint32(len(payload)) ||
		binary.BigEndian.Uint32(trailer[4:8]) != crc32.ChecksumIEEE(payload) ||
		len(payload) != len(wantPayload) {
		return fmt.Errorf("%w: mismatch", ErrCorrupt)
	}
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}
