package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"godb/internal/datastructures"
	"godb/internal/tooling/guard"
	"os"
	"path/filepath"
	"slices"
	"strconv"
)

type SSTableSearcher struct {
	path     string
	sstables []SSTableRead
}

func NewSSTableSearcher(dbpath string) *SSTableSearcher {
	p := filepath.Join(dbpath, SSTablesDir)
	return &SSTableSearcher{path: p, sstables: make([]SSTableRead, 0)}
}

func (s *SSTableSearcher) Start() error {
	if err := s.loadSSTables(); err != nil {
		return fmt.Errorf("load sstables: %w", err)
	}

	return nil
}

func (s *SSTableSearcher) loadSSTables() error {
	files, err := os.ReadDir(s.path)
	if err != nil {
		return fmt.Errorf("read dir: %w", err)
	}

	for _, file := range files {
		fInfo, err := file.Info()
		if err != nil {
			return fmt.Errorf("file info: %w", err)
		}

		fname := fInfo.Name()

		if len(fname) < 3 || fname[len(fname)-SSTableFileSuffixLen:] != SSTableFileSuffix {
			continue
		}

		fsize := fInfo.Size()
		fpath := filepath.Join(s.path, fname)
		f, err := os.Open(fpath)
		if err != nil {
			return fmt.Errorf("file open: %w", err)
		}

		if fsize < footerByteSize {
			return errors.New("file size smaller than footer size")
		}

		var buf []byte

		footerOffset := fsize - footerByteSize
		buf = make([]byte, footerByteSize)
		if _, err = f.ReadAt(buf, footerOffset); err != nil {
			return fmt.Errorf("file footer read at: %w", err)
		}

		indexOffset := binary.LittleEndian.Uint32(buf[:4])
		indexSize := binary.LittleEndian.Uint32(buf[4:8])
		bloomFilterOffset := binary.LittleEndian.Uint32(buf[8:12])
		bloomFilterSize := binary.LittleEndian.Uint32(buf[12:16])
		magicNumber := binary.LittleEndian.Uint32(buf[16:20])

		if magicNumber != DBMagicNumber {
			continue
		}

		index := make([]SSTableIndexEntry, 0)

		buf = make([]byte, indexSize)
		if _, err = f.ReadAt(buf, int64(indexOffset)); err != nil {
			return fmt.Errorf("file index read at: %w", err)
		}

		off := 0
		for off < int(indexSize) {
			keyLen := binary.LittleEndian.Uint32(buf[off : off+uint32Bytes])
			off += uint32Bytes
			key := buf[off : off+int(keyLen)]
			off += int(keyLen)
			offset := binary.LittleEndian.Uint32(buf[off : off+uint32Bytes])
			off += uint32Bytes

			index = append(index, SSTableIndexEntry{
				KeyLen: keyLen,
				Key:    key,
				Offset: offset,
			})
		}

		off = int(bloomFilterSize)
		buf = make([]byte, bloomFilterSize)
		if _, err = f.ReadAt(buf, int64(bloomFilterOffset)); err != nil {
			return fmt.Errorf("file bloomfilter read at: %w", err)
		}

		numOfHashFuncs := binary.LittleEndian.Uint32(buf[off-uint32Bytes:])
		off -= uint32Bytes
		numOfBits := binary.LittleEndian.Uint32(buf[off-uint32Bytes : off])
		off -= uint32Bytes
		bitArray := buf[:off]

		bloomFilter := datastructures.NewBloomFilter(numOfHashFuncs, numOfBits, bitArray)

		sstable := &SSTableRead{
			FileName:       fInfo.Name(),
			Index:          index,
			BloomFilter:    bloomFilter,
			DataBlocksSize: int(indexOffset),
		}
		s.sstables = append(s.sstables, *sstable)

		if err := f.Close(); err != nil {
			return fmt.Errorf("file close: %w", err)
		}
	}

	slices.SortFunc(s.sstables, func(a, b SSTableRead) int {
		aName, err := strconv.Atoi(a.FileName[:len(a.FileName)-SSTableFileSuffixLen])
		guard.Assert(err == nil, "should always be convertable")
		bName, err := strconv.Atoi(b.FileName[:len(b.FileName)-SSTableFileSuffixLen])
		guard.Assert(err == nil, "should always be convertable")

		switch {
		case aName > bName:
			return -1
		case bName < aName:
			return 1
		default:
			return 0
		}
	})

	return nil
}

func (s *SSTableSearcher) Search(key string) ([]byte, bool, error) {
	k := []byte(key)
	for _, sstable := range s.sstables {
		if ok := sstable.BloomFilter.Contains(k); !ok {
			continue
		}

		fpath := filepath.Join(s.path, sstable.FileName)
		f, err := os.Open(fpath)
		if err != nil {
			return nil, false, fmt.Errorf("open file: %w", err)
		}

		searchPos := 0
		low := 0
		high := len(sstable.Index) - 1

		for low <= high {
			mid := low + (high-low)/2

			if string(sstable.Index[mid].Key) <= key {
				searchPos = mid
				low = mid + 1
			} else {
				high = mid - 1
			}
		}

		datablockOffset := int(sstable.Index[searchPos].Offset)
		datablockSize := 0
		if searchPos == len(sstable.Index)-1 {
			datablockSize = sstable.DataBlocksSize - datablockOffset
		} else {
			nextDatablockOffset := sstable.Index[searchPos+1].Offset
			datablockSize = int(nextDatablockOffset) - datablockOffset
		}

		buf := make([]byte, datablockSize)
		if _, err := f.ReadAt(buf, int64(datablockOffset)); err != nil {
			return nil, false, fmt.Errorf("open file: %w", err)
		}

		restartTableLen := int(binary.LittleEndian.Uint32(buf[len(buf)-uint32Bytes:]))

		restartTable := make([]uint32, restartTableLen)
		offset := datablockSize - uint32Bytes
		for i := restartTableLen - 1; i >= 0; i-- {
			res := binary.LittleEndian.Uint32(buf[offset-uint32Bytes : offset])
			restartTable[i] = res
			offset -= uint32Bytes
		}
		restartTableStart := offset

		searchOffset := 0
		low = 0
		high = len(restartTable) - 1
		for low <= high {
			mid := low + (high-low)/2

			keyOffset := int(restartTable[mid])
			offset := keyOffset + uint32Bytes
			unSharedKeylen := binary.LittleEndian.Uint32(buf[offset : offset+uint32Bytes])
			offset += 2 * uint32Bytes
			KeySuffix := buf[offset : offset+int(unSharedKeylen)]

			if string(KeySuffix) <= key {
				searchOffset = mid
				low = mid + 1
			} else {
				high = mid - 1
			}
		}

		offset = int(restartTable[searchOffset])
		previousKey := []byte("")
		for offset < restartTableStart {
			sharedKeyLen := binary.LittleEndian.Uint32(buf[offset : offset+uint32Bytes])
			offset += uint32Bytes
			unSharedKeylen := binary.LittleEndian.Uint32(buf[offset : offset+uint32Bytes])
			offset += uint32Bytes
			valueLen := binary.LittleEndian.Uint32(buf[offset : offset+uint32Bytes])
			offset += uint32Bytes
			keySuffix := buf[offset : offset+int(unSharedKeylen)]
			offset += int(unSharedKeylen)
			currentKey := append(previousKey[:sharedKeyLen], keySuffix...)
			if bytes.Equal(currentKey, k) {
				value := buf[offset : offset+int(valueLen)]
				return value, !bytes.Equal(value, tombstone), nil
			}
			previousKey = currentKey
			offset += int(valueLen)
		}
	}

	return nil, false, nil
}
