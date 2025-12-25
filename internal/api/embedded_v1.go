package api

import (
	"fmt"
	"godb/internal/engine"
)

type Database struct {
	path     string
	wal      *engine.WAL
	memTable *engine.MemTable

	maxLevel            int
	skipListProbability int
}

func NewDatabase(path string) *Database {
	return &Database{
		path: path,

		maxLevel:            4,
		skipListProbability: 50,
	}
}

func (d *Database) Start() error {
	wal, err := engine.NewWAL(d.path)
	if err != nil {
		return fmt.Errorf("new wal: %w", err)
	}
	d.wal = wal

	memTable, err := engine.NewMemTable(d.maxLevel, d.skipListProbability)
	if err != nil {
		return fmt.Errorf("new mem table: %w", err)
	}
	d.memTable = memTable

	entries, err := d.wal.Load()
	if err != nil {
		return fmt.Errorf("load wal: %w", err)
	}
	if len(entries) > 0 {
		for _, v := range entries {
			if v.Op == engine.WALDEL {
				memTable.Delete(string(v.Key))
			}

			memTable.Insert(string(v.Key), v.Value)
		}
	}

	return nil
}

func (d *Database) Put(key string, value []byte) error {
	if err := d.wal.Append(1, []byte(key), value); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}
	d.memTable.Insert(key, value)

	return nil
}

func (d *Database) Get(key string) ([]byte, bool) {
	return d.memTable.Search(key)
}

func (d *Database) Delete(key string) {
	d.memTable.Delete(key)
}

func Close() {}
