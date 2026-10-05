package pagedkv

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Frame layout on disk:
//
//	magic     [4]byte "PJD1"
//	length    uint32 BE  (length of JSON payload)
//	crc       uint32 BE  (CRC32/IEEE of the JSON payload)
//	payload   JSON encoded journalRecord
//
// Any torn or corrupt tail frame is detected on open, counted and truncated;
// it can never be replayed as a valid mapping.
var journalMagic = [4]byte{'P', 'J', 'D', '1'}

const journalHeaderLen = 12

// journalRecordType tags append-only recovery records.
type journalRecordType uint8

const (
	recTenant   journalRecordType = 1
	recSequence journalRecordType = 2
	recCommit   journalRecordType = 3
	recAbort    journalRecordType = 4
	recSwap     journalRecordType = 5
	recRestore  journalRecordType = 6
)

// journalRecord is one durable recovery record.
type journalRecord struct {
	Type     journalRecordType `json:"type"`
	Tenant   string            `json:"tenant,omitempty"`
	Sequence string            `json:"seq,omitempty"`
	Quota    int               `json:"quota,omitempty"`
	Length   int               `json:"length,omitempty"`
	Hashes   []string          `json:"hashes,omitempty"`
	Reason   string            `json:"reason,omitempty"`
}

// journal is an append-only recovery log with a checksum per record.
type journal struct {
	path    string
	file    *os.File
	syncWAL bool
}

func openJournal(dir string, syncWAL bool) (*journal, int64, error) {
	if dir == "" {
		return nil, 0, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, 0, err
	}
	path := filepath.Join(dir, "journal.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, 0, err
	}
	j := &journal{path: path, file: f, syncWAL: syncWAL}
	validLen, skipped, err := j.recoverTail()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	if _, err := f.Seek(validLen, io.SeekStart); err != nil {
		f.Close()
		return nil, 0, err
	}
	return j, skipped, nil
}

// recoverTail scans frames, returns the byte offset of the first bad/partial
// frame and the number of corrupt bytes dropped, and truncates the file there.
func (j *journal) recoverTail() (int64, int64, error) {
	data, err := io.ReadAll(j.file)
	if err != nil {
		return 0, 0, err
	}
	off := int64(0)
	var corrupt int64
	for off+journalHeaderLen <= int64(len(data)) {
		var magic [4]byte
		copy(magic[:], data[off:off+4])
		if magic != journalMagic {
			corrupt = int64(len(data)) - off
			break
		}
		payloadLen := int64(binary.BigEndian.Uint32(data[off+4 : off+8]))
		frameEnd := off + journalHeaderLen + payloadLen
		if payloadLen <= 0 || frameEnd > int64(len(data)) {
			corrupt = int64(len(data)) - off
			break
		}
		payload := data[off+journalHeaderLen : frameEnd]
		crc := binary.BigEndian.Uint32(data[off+8 : off+12])
		var rec journalRecord
		if crc32.ChecksumIEEE(payload) != crc || json.Unmarshal(payload, &rec) != nil {
			corrupt = int64(len(data)) - off
			break
		}
		off = frameEnd
	}
	if off < int64(len(data)) {
		if err := j.file.Truncate(off); err != nil {
			return 0, 0, err
		}
		syncFile(j.file)
	}
	return off, corrupt, nil
}

func (j *journal) append(rec journalRecord) error {
	if j == nil {
		return nil
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if len(payload) > 0xffffffff {
		return fmt.Errorf("%w: journal record too large", ErrInvalidArgument)
	}
	header := make([]byte, journalHeaderLen)
	copy(header[0:4], journalMagic[:])
	binary.BigEndian.PutUint32(header[4:8], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[8:12], crc32.ChecksumIEEE(payload))
	if _, err := j.file.Write(header); err != nil {
		return err
	}
	if _, err := j.file.Write(payload); err != nil {
		return err
	}
	if j.syncWAL {
		if err := syncFile(j.file); err != nil {
			return err
		}
	}
	return nil
}

// scan replays intact records in order. Returning false from fn stops the
// replay. The scan always starts at the truncated valid tail.
func (j *journal) scan(fn func(journalRecord) bool) (int, error) {
	if j == nil {
		return 0, nil
	}
	data, err := os.ReadFile(j.path)
	if err != nil {
		return 0, err
	}
	count := 0
	off := int64(0)
	for off+journalHeaderLen <= int64(len(data)) {
		payloadLen := int64(binary.BigEndian.Uint32(data[off+4 : off+8]))
		frameEnd := off + journalHeaderLen + payloadLen
		if payloadLen <= 0 || frameEnd > int64(len(data)) {
			break
		}
		payload := data[off+journalHeaderLen : frameEnd]
		crc := binary.BigEndian.Uint32(data[off+8 : off+12])
		if crc32.ChecksumIEEE(payload) != crc {
			break
		}
		var rec journalRecord
		if json.Unmarshal(payload, &rec) != nil {
			break
		}
		count++
		if !fn(rec) {
			break
		}
		off = frameEnd
	}
	return count, nil
}

// rewrite atomically replaces the log contents with recs (compaction).
func (j *journal) rewrite(recs []journalRecord) error {
	if j == nil {
		return nil
	}
	tmp := j.path + ".compact"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	tmpJ := &journal{file: f}
	for _, rec := range recs {
		if err := tmpJ.append(rec); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := syncFile(f); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, j.path); err != nil {
		os.Remove(tmp)
		return err
	}
	syncDir(filepath.Dir(j.path))
	j.file.Close()
	j.file, err = os.OpenFile(j.path, os.O_RDWR|os.O_APPEND, 0o644)
	return err
}

func (j *journal) close() error {
	if j == nil || j.file == nil {
		return nil
	}
	return syncFile(j.file)
}

func syncFile(f *os.File) error {
	if f == nil {
		return nil
	}
	return f.Sync()
}
