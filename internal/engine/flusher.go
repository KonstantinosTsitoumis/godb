package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"godb/internal/tooling/guard"
	"os"
	"path/filepath"
	"sync"
)

type Flusher struct {
	fChan          chan *MemTable
	rOnlyMemTables []*MemTable
	mu             sync.Mutex

	maxWorkers           int
	maxDatablockByteSize int
	path                 string

	active bool
}

var (
	ErrFlusherNotActive     = errors.New("flusher not active")
	ErrFlusherAlreadyActive = errors.New("flusher already active")
)

var seq = 0

func NewFlusher(path string, maxWorkers, maxDatablockByteSize int) *Flusher {
	return &Flusher{
		rOnlyMemTables:       make([]*MemTable, 0),
		mu:                   sync.Mutex{},
		maxWorkers:           maxWorkers,
		maxDatablockByteSize: maxDatablockByteSize,
		path:                 path,
	}
}

func (f *Flusher) AppendROnlyMemTable(m *MemTable) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.rOnlyMemTables = append(f.rOnlyMemTables, m)
}

func (f *Flusher) ROnlyMemTables() []*MemTable {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]*MemTable{}, f.rOnlyMemTables...)
}

func (f *Flusher) Start(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.active {
		return ErrFlusherAlreadyActive
	}

	f.fChan = make(chan *MemTable, 3)

	for range f.maxWorkers {
		go f.worker(ctx)
	}

	f.active = true
	return nil
}

func (f *Flusher) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case memTable, ok := <-f.fChan:
			if !ok {
				return
			}

			f.flush(memTable)

			// Delete
			f.mu.Lock()
			for i, v := range f.rOnlyMemTables {
				if v == memTable {
					f.rOnlyMemTables = append(
						(f.rOnlyMemTables)[:i],
						(f.rOnlyMemTables)[i+1:]...,
					)
					break
				}
			}
			f.mu.Unlock()
		}
	}
}

func (f *Flusher) Stop() error {
	if !f.active {
		return ErrFlusherNotActive
	}

	close(f.fChan)

	f.active = false
	return nil
}

func (f *Flusher) EnqueueToBeFlushed(m *MemTable) {
	f.fChan <- m
}

func (f *Flusher) flush(m *MemTable) error {
	sstable := NewSSTableWriteFromMemTable(m, f.maxDatablockByteSize)

	flag := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	filename := fmt.Sprint(seq) + SSTableFileSuffix
	seq++
	p := filepath.Join(f.path, SSTablesDir, filename)
	file, err := os.OpenFile(p, flag, 0644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	defer func() {
		fErr := file.Close()
		guard.Assert(
			fErr == nil,
			"This raises only if it was already closed..",
		)
	}()

	for _, datablock := range sstable.Datablocks {
		buf := make([]byte, 0, datablock.EntriesByteSize+datablock.RestartTableSize)

		for _, entry := range datablock.Entries {
			buf = binary.LittleEndian.AppendUint32(buf, entry.SharedKeyLen)
			buf = binary.LittleEndian.AppendUint32(buf, entry.UnsharedKeyLen)
			buf = binary.LittleEndian.AppendUint32(buf, entry.ValueLen)
			buf = append(buf, entry.KeySuffix...)
			buf = append(buf, entry.Value...)
		}

		for _, entry := range datablock.RestartTable {
			buf = binary.LittleEndian.AppendUint32(buf, entry)
		}

		buf = binary.LittleEndian.AppendUint32(buf, datablock.RestartTableLen)

		if _, err := file.Write(buf); err != nil {
			return fmt.Errorf("file write datablock: %w", err)
		}
	}

	buf := make([]byte, 0, sstable.Footer.IndexSize)
	for _, entry := range sstable.Index {
		buf = binary.LittleEndian.AppendUint32(buf, entry.KeyLen)
		buf = append(buf, entry.Key...)
		buf = binary.LittleEndian.AppendUint32(buf, entry.Offset)
	}

	if _, err := file.Write(buf); err != nil {
		return fmt.Errorf("file write index: %w", err)
	}

	buf = make([]byte, 0, sstable.Footer.BloomFilterSize)
	buf = append(buf, sstable.BloomFilter.BitArray...)
	buf = binary.LittleEndian.AppendUint32(buf, sstable.BloomFilter.NumOfBits)
	buf = binary.LittleEndian.AppendUint32(buf, sstable.BloomFilter.NumOfHashFuncs)

	if _, err := file.Write(buf); err != nil {
		return fmt.Errorf("file write bloomfilter: %w", err)
	}

	buf = make([]byte, 0, 5*uint32Bytes)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(sstable.Footer.IndexOffset))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(sstable.Footer.IndexSize))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(sstable.Footer.BloomFilterOffset))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(sstable.Footer.BloomFilterSize))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(sstable.Footer.MagicNumber))
	if _, err := file.Write(buf); err != nil {
		return fmt.Errorf("file write footer: %w", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("file sync: %w", err)
	}

	return nil
}
