package pagedkv

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	journalName   = "journal.log"
	journalMagic  = "PGKJRNL1"
	recSequence   = 1 // commit (create or replace) a sequence mapping
	recRelease    = 2 // a sequence was released; its mapping is obsolete
	recCheckpoint = 3 // durable barrier: all preceding records are complete
)

// journal is an append-only, checksummed log of committed block mappings.
//
// Frame layout: [totalLen uint32][crc32 uint32][type byte][payload...]
// totalLen covers type+payload+crc; crc covers type+payload. Recovery stops
// at the first torn or bad frame: later records are never partially applied,
// and startup is never blocked by them.
type journal struct {
	dir string
	f   *os.File
	w   *bufio.Writer
}

func journalPath(dir string) string { return filepath.Join(dir, journalName) }

func openJournal(dir string) (*journal, error) {
	if dir == "" {
		return nil, ErrInvalidArgument
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := journalPath(dir)
	needMagic := true
	if info, err := os.Stat(path); err == nil && info.Size() > 0 {
		needMagic = false
	}
	f, err := os.OpenFile(path,
		os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if needMagic {
		if _, err := f.WriteString(journalMagic); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
	}
	return &journal{dir: dir, f: f, w: bufio.NewWriter(f)}, nil
}

func (j *journal) close() error {
	if j == nil || j.f == nil {
		return nil
	}
	if err := j.w.Flush(); err != nil {
		return err
	}
	return j.f.Close()
}

// appendFrame writes one framed record (buffered; call sync to persist).
func (j *journal) appendFrame(recType byte, payload []byte) error {
	// Frame: [total u32][type u8][payload...][crc32],
	// total = 1 + len(payload) (type + payload, crc excluded).
	total := uint32(1 + len(payload))
	buf := make([]byte, 4+total+4)
	binary.BigEndian.PutUint32(buf[0:4], total)
	buf[4] = recType
	copy(buf[5:], payload)
	crc := crc32.ChecksumIEEE(buf[4 : 4+total])
	binary.BigEndian.PutUint32(buf[4+total:], crc)
	_, err := j.w.Write(buf)
	return err
}

// sync flushes buffered frames and fsyncs the file.
func (j *journal) sync() error {
	if err := j.w.Flush(); err != nil {
		return err
	}
	return j.f.Sync()
}

// RecoveryIssue describes one skipped corrupt or partial record at startup.
type RecoveryIssue struct {
	Reason string
}

// journalSequence is one recovered committed mapping.
type journalSequence struct {
	tenant   string
	id       string
	priority int
	order    uint64
	blocks   []journalBlock
}

// journalBlock is one recovered logical block: size logical bytes and the
// swap-store key holding the full block content.
type journalBlock struct {
	key  []byte
	size int
}

// readJournal replays the journal. The final checkpoint (if present) is the
// last commit boundary guaranteed durable; records after it are ignored.
// The file is parsed as a whole byte slice so torn writes and bad frames can
// never desynchronize subsequent frames.
func readJournal(dir string) ([]journalSequence, []RecoveryIssue, error) {
	raw, err := os.ReadFile(journalPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	issues := []RecoveryIssue{}
	if len(raw) == 0 {
		return nil, nil, nil
	}
	if len(raw) < len(journalMagic) || string(raw[:len(journalMagic)]) != journalMagic {
		return nil, []RecoveryIssue{{Reason: "bad or partial journal magic; journal ignored"}}, nil
	}
	pos := len(journalMagic)

	type frame struct {
		recType byte
		payload []byte
	}
	var frames []frame
scan:
	for pos < len(raw) {
		if pos+4 > len(raw) {
			issues = append(issues, RecoveryIssue{Reason: "truncated frame header; tail ignored"})
			break scan
		}
		// Frame layout: [total u32][type u8][payload...][crc32],
		// total = 1 + len(payload), frame length = total + 8.
		total := int(binary.BigEndian.Uint32(raw[pos : pos+4]))
		end := pos + 4 + total + 4
		if total < 1 || end > len(raw) {
			issues = append(issues, RecoveryIssue{Reason: "truncated or illegal frame; tail ignored"})
			break scan
		}
		covered := raw[pos+4 : pos+4+total]
		wantCRC := binary.BigEndian.Uint32(raw[pos+4+total : end])
		if crc32.ChecksumIEEE(covered) != wantCRC {
			issues = append(issues, RecoveryIssue{Reason: "frame checksum mismatch; tail ignored"})
			break scan
		}
		if crc32.ChecksumIEEE(covered) != wantCRC {
			issues = append(issues, RecoveryIssue{Reason: "frame checksum mismatch; tail ignored"})
			break scan
		}
		frames = append(frames, frame{recType: covered[0], payload: covered[1:]})
		pos = end
	}

	lastCheckpoint := -1
	for i, fr := range frames {
		if fr.recType == recCheckpoint {
			lastCheckpoint = i
		}
	}

	committed := map[string]*journalSequence{}
	var order []string
	limit := len(frames)
	if lastCheckpoint >= 0 {
		limit = lastCheckpoint
	}
	for i := 0; i < limit; i++ {
		fr := frames[i]
		switch fr.recType {
		case recSequence:
			js, perr := decodeSequenceRecord(fr.payload)
			if perr != nil {
				issues = append(issues, RecoveryIssue{Reason: "bad sequence record skipped: " + perr.Error()})
				continue
			}
			if _, ok := committed[js.id]; !ok {
				order = append(order, js.id)
			}
			committed[js.id] = js
		case recRelease:
			if id, perr := decodeReleaseRecord(fr.payload); perr == nil {
				if _, ok := committed[id]; ok {
					delete(committed, id)
					for k, v := range order {
						if v == id {
							order = append(order[:k], order[k+1:]...)
							break
						}
					}
				}
			} else {
				issues = append(issues, RecoveryIssue{Reason: "bad release record skipped: " + perr.Error()})
			}
		case recCheckpoint:
			// Earlier checkpoint; everything before it is included anyway.
		default:
			issues = append(issues, RecoveryIssue{Reason: fmt.Sprintf("unknown record type %d skipped", fr.recType)})
		}
	}

	out := make([]journalSequence, 0, len(order))
	for _, id := range order {
		out = append(out, *committed[id])
	}
	return out, issues, nil
}

// Payload encoders (length-prefixed strings, kept tiny on purpose).

func putUint64(b []byte, v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return append(b, buf[:]...)
}

func getUint64(b []byte) (uint64, []byte, error) {
	if len(b) < 8 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	return binary.BigEndian.Uint64(b[:8]), b[8:], nil
}

func putBytes(b, s []byte) []byte {
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], uint32(len(s)))
	b = append(b, l[:]...)
	return append(b, s...)
}

func getBytes(b []byte) ([]byte, []byte, error) {
	if len(b) < 4 {
		return nil, nil, io.ErrUnexpectedEOF
	}
	n := int(binary.BigEndian.Uint32(b[:4]))
	b = b[4:]
	if len(b) < n {
		return nil, nil, io.ErrUnexpectedEOF
	}
	return b[:n], b[n:], nil
}

// encodeSequenceRecord builds the payload for a committed sequence mapping.
func encodeSequenceRecord(js *journalSequence) []byte {
	var b []byte
	b = putBytes(b, []byte(js.tenant))
	b = putBytes(b, []byte(js.id))
	b = putUint64(b, uint64(js.priority))
	b = putUint64(b, js.order)
	b = putUint64(b, uint64(len(js.blocks)))
	for _, blk := range js.blocks {
		b = putBytes(b, blk.key)
		b = putUint64(b, uint64(blk.size))
	}
	return b
}

func decodeSequenceRecord(b []byte) (*journalSequence, error) {
	var err error
	js := &journalSequence{}
	if js.tenant, b, err = getStr(b); err != nil {
		return nil, err
	}
	if js.id, b, err = getStr(b); err != nil {
		return nil, err
	}
	var prio uint64
	if prio, b, err = getUint64(b); err != nil {
		return nil, err
	}
	js.priority = int(prio)
	if js.order, b, err = getUint64(b); err != nil {
		return nil, err
	}
	var n uint64
	if n, b, err = getUint64(b); err != nil {
		return nil, err
	}
	for i := uint64(0); i < n; i++ {
		var key []byte
		if key, b, err = getBytes(b); err != nil {
			return nil, err
		}
		var size uint64
		if size, b, err = getUint64(b); err != nil {
			return nil, err
		}
		js.blocks = append(js.blocks, journalBlock{key: key, size: int(size)})
	}
	return js, nil
}

func getStr(b []byte) (string, []byte, error) {
	s, rest, err := getBytes(b)
	return string(s), rest, err
}

func encodeReleaseRecord(id string) []byte {
	return putBytes(nil, []byte(id))
}

func decodeReleaseRecord(b []byte) (string, error) {
	s, _, err := getStr(b)
	return s, err
}
