package engine

import (
	"bytes"
	"fmt"
	"godb/internal/datastructures"
)

type MemTable struct {
	sList *datastructures.SkipList[[]byte]
}

var tombstone = []byte("__TOMBSTONE__")

func NewMemTable(maxLevel, probability int) (*MemTable, error) {
	sList, err := datastructures.NewSkipList[[]byte](maxLevel, probability)
	if err != nil {
		return nil, fmt.Errorf("new skip list: %w", err)
	}

	return &MemTable{sList: sList}, nil
}

func (m *MemTable) Insert(key string, value []byte) {
	m.sList.Insert(key, value)
}

func (m *MemTable) Delete(key string) {
	m.sList.Insert(key, tombstone)
}

func (m *MemTable) Search(key string) ([]byte, bool) {
	v, ok := m.sList.Search(key)
	if !ok || bytes.Equal(v, tombstone) {
		return []byte{}, false
	}

	return v, true
}
