package engine

import (
	"context"
	"sync"
)

type Flusher struct {
	fChan          <-chan *MemTable
	rOnlyMemTables []*MemTable
	mu             sync.Mutex
}

func NewFlusher() *Flusher {
	return &Flusher{
		fChan:          make(chan *MemTable),
		rOnlyMemTables: make([]*MemTable, 0),
		mu:             sync.Mutex{},
	}
}

func (f *Flusher) AppendROnlyMemTable(m *MemTable) {
	f.rOnlyMemTables = append(f.rOnlyMemTables, m)
}

func (f *Flusher) ROnlyMemTables() []*MemTable {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]*MemTable{}, f.rOnlyMemTables...)
}

func (f *Flusher) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case memTable := <-f.fChan:
				flush(memTable)

				f.mu.Lock()
				for i, v := range f.rOnlyMemTables {
					if v == memTable {
						// Delete
						f.rOnlyMemTables = append(
							(f.rOnlyMemTables)[:i],
							(f.rOnlyMemTables)[i+1:]...,
						)
					}
				}
				f.mu.Unlock()
			}
		}
	}()
}

func (f *Flusher) EnqueueToBeFlushed(m *MemTable) {
}

func flush(memTable *MemTable) {}
