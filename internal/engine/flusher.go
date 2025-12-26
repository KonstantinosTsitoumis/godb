package engine

import (
	"context"
	"errors"
	"sync"
)

type Flusher struct {
	fChan          chan *MemTable
	rOnlyMemTables []*MemTable
	mu             sync.Mutex

	maxWorkers int
	active     bool
}

var (
	ErrFlusherNotActive     = errors.New("flusher not active")
	ErrFlusherAlreadyActive = errors.New("flusher already active")
)

func NewFlusher(maxWorkers int) *Flusher {
	return &Flusher{
		rOnlyMemTables: make([]*MemTable, 0),
		mu:             sync.Mutex{},
		maxWorkers:     maxWorkers,
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
	if f.active {
		return ErrFlusherAlreadyActive
	}

	f.fChan = make(chan *MemTable, 3)

	worker := func() {
		for {
			select {
			case <-ctx.Done():
				return
			case memTable, ok := <-f.fChan:
				if !ok {
					return
				}

				flush(memTable)

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

	for range f.maxWorkers {
		go worker()
	}

	f.active = true
	return nil
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

func flush(memTable *MemTable) {}
