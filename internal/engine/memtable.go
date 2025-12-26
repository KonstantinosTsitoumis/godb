package engine

import (
	"bytes"
	"errors"
	"fmt"
	"godb/internal/datastructures"
)

type MemTable struct {
	sList  *datastructures.SkipList[[]byte]
	frozen bool
}

var (
	tombstone         = []byte("__TOMBSTONE__")
	ErrMemTableFrozen = errors.New("memtable is frozen")
)

func NewMemTable(maxLevel, probability int) (*MemTable, error) {
	sList, err := datastructures.NewSkipList[[]byte](maxLevel, probability)
	if err != nil {
		return nil, fmt.Errorf("new skip list: %w", err)
	}

	return &MemTable{sList: sList}, nil
}

func (m *MemTable) Insert(key string, value []byte) error {
	if m.frozen {
		return ErrMemTableFrozen
	}

	m.sList.Insert(key, value)

	return nil
}

func (m *MemTable) Delete(key string) error {
	return m.Insert(key, tombstone)
}

func (m *MemTable) Search(key string) ([]byte, bool, bool) {
	v, ok := m.sList.Search(key)
	if ok && bytes.Equal(v, tombstone) {
		return []byte{}, true, false
	}

	return v, false, ok
}

func (m *MemTable) Size() int {
	return m.sList.ContentSize()
}

func (m *MemTable) Freeze() {
	m.frozen = true
}
