package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	outFileName = "segment-"
	bufSize     = 8192
)

type hashIndex map[string]int64

type PutOp struct {
	entry Entry
	resp  chan error
}

type KeyPosition struct {
	segment  *Segment
	position int64
}

type IndexOp struct {
	isBeingWritten bool
	key            string
	index          int64
}

type Segment struct {
	outOffset int64
	index     hashIndex
	filePath  string
}

type Db struct {
	out       *os.File
	outPath   string
	outOffset int64

	dir              string
	lastSegmentIndex int
	segmentSize      int64
	indexOps         chan IndexOp
	keyPositions     chan *KeyPosition
	putOps           chan PutOp
	putDone          chan error

	index     hashIndex
	segments  []*Segment
	indexLock sync.Mutex
	fileLock  sync.Mutex
}

var (
	ErrNotFound = fmt.Errorf("record does not exist")
)

func NewDb(dir string, segmentSize int64) (*Db, error) {
	db := &Db{
		segments: make([]*Segment, 0),
		dir:      dir,

		segmentSize:  segmentSize,
		indexOps:     make(chan IndexOp),
		keyPositions: make(chan *KeyPosition),
		putOps:       make(chan PutOp),
		putDone:      make(chan error),
	}

	if err := db.createSegment(); err != nil {
		return nil, err
	}

	if err := db.recoverAll(); err != nil && err != io.EOF {
		return nil, err
	}

	go db.IndexRoutine()
	go db.PutRoutine()

	return db, nil
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) IndexRoutine() {
	for op := range db.indexOps {
		db.indexLock.Lock()
		if op.isBeingWritten {
			db.setKey(op.key, op.index)
		} else {
			s, p, err := db.getSegmentAndPosition(op.key)
			if err != nil {
				db.keyPositions <- nil
			} else {
				db.keyPositions <- &KeyPosition{s, p}
			}
		}
		db.indexLock.Unlock()
	}
}

func (db *Db) PutRoutine() {
	for {
		op := <-db.putOps
		db.fileLock.Lock()
		length := op.entry.GetLength()
		stat, err := db.out.Stat()
		if err != nil {
			op.resp <- err
			db.fileLock.Unlock()
			continue
		}
		if stat.Size()+length > db.segmentSize {
			err := db.createSegment()
			if err != nil {
				op.resp <- err
				db.fileLock.Unlock()
				continue
			}
		}
		n, err := db.out.Write(op.entry.Encode())
		if err == nil {
			db.indexOps <- IndexOp{
				isBeingWritten: true,
				key:            op.entry.key,
				index:          int64(n),
			}
		}
		op.resp <- nil
		db.fileLock.Unlock()
	}
}

func (db *Db) createSegment() error {
	path := db.newFileName()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return err
	}
	newSegment := &Segment{
		filePath: path,
		index:    make(hashIndex),
	}

	db.out = f
	db.outOffset = 0
	db.outPath = path
	db.segments = append(db.segments, newSegment)
	if len(db.segments) >= 3 {
		go db.Compact()
	}

	return nil
}

func (db *Db) newFileName() string {
	filePath := filepath.Join(db.dir, fmt.Sprintf("%s%d", outFileName, db.lastSegmentIndex))
	db.lastSegmentIndex++
	return filePath
}

func (db *Db) Compact() {
	path := db.newFileName()
	newSegment := &Segment{
		filePath: path,
		index:    make(hashIndex),
	}
	var offset int64
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return
	}
	lastSegmentIndex := len(db.segments) - 2
	for i := 0; i <= lastSegmentIndex; i++ {
		s := db.segments[i]
		for key, index := range s.index {
			if i < lastSegmentIndex {
				isInNewerSegments := findKey(db.segments[i+1:lastSegmentIndex+1], key)
				if isInNewerSegments {
					continue
				}
			}
			value, _ := s.getFromSegment(index)
			e := Entry{
				key:   key,
				value: value,
			}
			n, err := f.Write(e.Encode())
			if err == nil {
				newSegment.index[key] = offset
				offset += int64(n)
			}
		}
	}
	db.segments = []*Segment{newSegment, db.getLastSegment()}
}

func (db *Db) recoverAll() error {
	for _, segment := range db.segments {
		if err := db.recoverSegment(segment); err != nil {
			return err
		}
	}
	return nil
}

func (db *Db) recoverSegment(segment *Segment) error {
	f, err := os.Open(segment.filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := db.recover(f); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (db *Db) recover(f *os.File) error {
	var buf [bufSize]byte
	var err error

	in := bufio.NewReaderSize(f, bufSize)
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = in.Peek(bufSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufSize {
			data = buf[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = in.Read(data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var e Entry
			e.Decode(data)
			db.setKey(e.key, int64(n))
		}
	}
	return err
}

func findKey(segments []*Segment, key string) bool {
	for _, s := range segments {
		if _, ok := s.index[key]; ok {
			return true
		}
	}
	return false
}

func (db *Db) setKey(key string, n int64) {
	db.getLastSegment().index[key] = db.outOffset
	db.outOffset += n
}

func (db *Db) getSegmentAndPosition(key string) (*Segment, int64, error) {
	for i := range db.segments {
		s := db.segments[len(db.segments)-i-1]
		pos, ok := s.index[key]
		if ok {
			return s, pos, nil
		}
	}

	return nil, 0, ErrNotFound
}

func (db *Db) getPosition(key string) *KeyPosition {
	op := IndexOp{
		isBeingWritten: false,
		key:            key,
	}
	db.indexOps <- op
	return <-db.keyPositions
}

func (db *Db) getLastSegment() *Segment {
	return db.segments[len(db.segments)-1]
}

func (db *Db) Get(key string) (string, error) {
	keyPos := db.getPosition(key)
	if keyPos == nil {
		return "", ErrNotFound
	}
	value, err := keyPos.segment.getFromSegment(keyPos.position)
	if err != nil {
		return "", err
	}
	return value, nil
}

func (db *Db) Put(key, value string) error {
	resp := make(chan error)
	db.putOps <- PutOp{
		entry: Entry{
			key:   key,
			value: value,
		},
		resp: resp,
	}
	err := <-resp
	close(resp)
	return err
}

func (s *Segment) getFromSegment(position int64) (string, error) {
	file, err := os.Open(s.filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}
