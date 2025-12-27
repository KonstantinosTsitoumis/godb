package api

import (
	"context"
	"fmt"
	"godb/internal/engine"
	"godb/internal/tooling/guard"
	"sync"
)

type Database struct {
	ctx     context.Context
	ctxcncl context.CancelFunc

	// Engine Items
	wal      *engine.WAL
	memTable *engine.MemTable
	flusher  *engine.Flusher

	// Mutexes
	mu *sync.Mutex

	// General Configuration
	path string

	// MemTable Configuration
	maxLevel            int
	skipListProbability int
	maxSize             int

	// Flusher Configuration
	flusherMaxWorkers int
}

func NewDatabase(path string) *Database {
	ctx := context.Background()
	ctx, ctxcncl := context.WithCancel(ctx)

	return &Database{
		ctx:     ctx,
		ctxcncl: ctxcncl,

		mu: &sync.Mutex{},

		path: path,

		maxLevel:            4,
		skipListProbability: 50,
		maxSize:             10,

		flusherMaxWorkers: 3,
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
			var err error
			if v.Op == engine.WALDEL {
				err = memTable.Delete(string(v.Key))
			} else {
				err = memTable.Insert(string(v.Key), v.Value)
			}
			guard.Assert(
				err == nil,
				`
				Only reason to receive error here is memTable being frozen 
				which is not possible
				`,
			)
		}
	}

	d.flusher = engine.NewFlusher(d.flusherMaxWorkers)
	if err := d.flusher.Start(d.ctx); err != nil {
		return fmt.Errorf("flusher start: %w", err)
	}

	return nil
}

func (d *Database) Put(key string, value []byte) error {
	if err := d.wal.Append(engine.WALPUT, []byte(key), value); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}

	err := d.memTable.Insert(key, value)
	guard.Assert(err == nil, "This should never be a frozen memtable")

	if d.memTable.Size() > d.maxSize {
		d.rotateMemTable()
	}

	return nil
}

func (d *Database) Get(key string) ([]byte, bool) {
	v, isTombstone, ok := d.memTable.Search(key)
	switch {
	case isTombstone:
		return nil, false
	case ok:
		return v, true
	}

	return d.searchInROMemTables(key)
}

func (d *Database) Delete(key string) error {
	if err := d.wal.Append(engine.WALDEL, []byte(key), nil); err != nil {
		return fmt.Errorf("wal append: %w", err)
	}

	err := d.memTable.Delete(key)
	guard.Assert(err == nil, "This should never be a frozen memtable")

	return nil
}

func (d *Database) Stop() error {
	if err := d.flusher.Stop(); err != nil {
		return fmt.Errorf("flusher stop: %w", err)
	}

	return nil
}

// Helpers
func (d *Database) searchInROMemTables(key string) ([]byte, bool) {
	for i := len(d.flusher.ROnlyMemTables()) - 1; i >= 0; i-- {
		v, isTombstone, ok := d.flusher.ROnlyMemTables()[i].Search(key)
		switch {
		case isTombstone:
			return nil, false
		case ok:
			return v, true
		}
	}

	return nil, false
}

func (d *Database) rotateMemTable() {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Double checking
	if d.memTable.Size() < d.maxSize {
		return
	}

	newMemTable, err := engine.NewMemTable(d.maxLevel, d.skipListProbability)
	guard.Assert(
		err == nil,
		`
		Errors from the memtable creation are from NewSkipList Validation. 
		Should never happen here
		`,
	)

	oldMemTable := d.memTable
	d.flusher.AppendROnlyMemTable(oldMemTable)
	d.flusher.EnqueueToBeFlushed(oldMemTable)
	d.memTable = newMemTable
	oldMemTable.Freeze()
}
