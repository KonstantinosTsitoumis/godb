package engine

import "godb/internal/datastructures"

const (
	sharedKeyLenBytes      = uint32Bytes
	unSharedKeyLenBytes    = uint32Bytes
	valueLenBytes          = uint32Bytes
	restartTableLenBytes   = uint32Bytes
	restartTableEntryBytes = uint32Bytes
	indexKeyLenBytes       = uint32Bytes
	indexOffsetBytes       = uint32Bytes
)

type (
	SSTableDataBlock struct {
		Entries         []*SSTableDataBlockEntry
		RestartTable    []uint32
		RestartTableLen uint32

		// Helper, Does not get written to file
		EntriesSize      int
		RestartTableSize int
	}
	SSTableDataBlockEntry struct {
		SharedKeyLen   uint32
		UnsharedKeyLen uint32
		ValueLen       uint32
		KeySuffix      []byte
		Value          []byte
	}
	SSTableIndexEntry struct {
		KeyLen uint32
		Key    []byte
		Offset uint32
	}
	SSTableFooter struct {
		IndexOffset       uint64
		IndexSize         uint64
		BloomFilterOffset uint64
		BloomFilterSize   uint64
		MagicNumber       uint64
	}
)

type SSTable struct {
	Datablocks  []*SSTableDataBlock
	Index       []*SSTableIndexEntry
	BloomFilter *datastructures.BloomFilter
	Footer      *SSTableFooter
}

func NewSSTableFromMemTable(m *MemTable, datablockMaxEntriesByteSize int) *SSTable {
	// prefered Optimization over Readabillity to do only one loop incase we have million of entries
	const restartInterval = 4

	entries := m.Entries()

	index := make([]*SSTableIndexEntry, 0)
	bloomFilterSet := make(map[string]struct{})
	datablocks := make([]*SSTableDataBlock, 0)

	previousKey := ""
	currentDataBlock := &SSTableDataBlock{RestartTableSize: restartTableLenBytes}
	currentDataBlock.RestartTable = make([]uint32, 0)
	datablocks = append(datablocks, currentDataBlock)
	for _, entry := range entries {
		if currentDataBlock.RestartTableSize+currentDataBlock.EntriesSize >= datablockMaxEntriesByteSize {
			// Reset for new Datablock
			currentDataBlock = &SSTableDataBlock{RestartTableSize: restartTableLenBytes}
			currentDataBlock.RestartTable = make([]uint32, 0)
			datablocks = append(datablocks, currentDataBlock)
			previousKey = ""
		}

		// is restart point
		if len(currentDataBlock.Entries)%(restartInterval) == 0 {
			// Reset previous Key and add restart point to the table
			previousKey = ""
			currentDataBlock.RestartTable = append(
				currentDataBlock.RestartTable,
				uint32(currentDataBlock.EntriesSize),
			)
			currentDataBlock.RestartTableSize += restartTableEntryBytes
			currentDataBlock.RestartTableLen += 1
		}

		// Shared Key Length
		loops := min(len(previousKey), len(entry.Key))
		sharedKey := make([]byte, 0, loops)
		for i := range loops {
			if entry.Key[i] != previousKey[i] {
				break
			}

			sharedKey = append(sharedKey, entry.Key[i])
		}

		sharedKeyLen := uint32(len(sharedKey))

		// Un Shared Key Length
		unSharedKeyLen := uint32(len(entry.Key)) - sharedKeyLen

		// Value Len
		// Value
		var valueLen uint32
		var value []byte
		if entry.Tombstone {
			valueLen = tombstoneLen
			value = tombstone
		} else {
			valueLen = uint32(len(entry.Value))
			value = entry.Value
		}

		// Key Suffix
		keySuffix := []byte(entry.Key[sharedKeyLen:])

		dataBlockEntry := &SSTableDataBlockEntry{
			SharedKeyLen:   sharedKeyLen,
			UnsharedKeyLen: unSharedKeyLen,
			ValueLen:       valueLen,
			KeySuffix:      keySuffix,
			Value:          value,
		}

		currentDataBlock.Entries = append(currentDataBlock.Entries, dataBlockEntry)
		bloomFilterSet[entry.Key] = struct{}{}
		currentDataBlock.EntriesSize += sharedKeyLenBytes + unSharedKeyLenBytes + valueLenBytes + len(keySuffix) + len(value)
		previousKey = entry.Key
	}

	offset := 0
	indexSize := 0
	for _, datablock := range datablocks {
		key := datablock.Entries[0].KeySuffix
		keyLen := len(key)

		e := &SSTableIndexEntry{
			KeyLen: uint32(keyLen),
			Key:    datablock.Entries[0].KeySuffix,
			Offset: uint32(offset),
		}

		index = append(index, e)
		indexSize += indexKeyLenBytes + keyLen + indexOffsetBytes
		offset += datablock.EntriesSize + datablock.RestartTableSize
	}

	indexOffset := offset
	bloomfilter := datastructures.NewBloomFilterFromSet(7, uint32(len(bloomFilterSet)*10), bloomFilterSet)
	bloomFilterOffset := indexOffset + indexSize

	footer := &SSTableFooter{
		IndexOffset:       uint64(indexOffset),
		IndexSize:         uint64(indexSize),
		BloomFilterOffset: uint64(bloomFilterOffset),
		BloomFilterSize:   uint64(bloomfilter.ByteSize()),
		MagicNumber:       uint64(DBMagicNumber),
	}

	return &SSTable{
		Datablocks:  datablocks,
		BloomFilter: bloomfilter,
		Index:       index,
		Footer:      footer,
	}
}
