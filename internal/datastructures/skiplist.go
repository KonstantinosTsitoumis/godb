package datastructures

import (
	"errors"
	"fmt"
	"math/rand"
)

type node[T any] struct {
	Key   string
	Value T
	Next  []*node[T]
}

func newNode[T any](key string, value T, maxLevel int) *node[T] {
	nexts := make([]*node[T], maxLevel)
	return &node[T]{Key: key, Value: value, Next: nexts}
}

type SkipList[T any] struct {
	maxLevel    int
	probability int

	header       *node[T]
	level        int
	contentsSize int

	debug [][]string
}

func NewSkipList[T any](maxLevel, probability int) (*SkipList[T], error) {
	if probability > 100 || probability < 0 {
		return nil, errors.ErrUnsupported
	}
	if maxLevel <= 0 {
		return nil, errors.ErrUnsupported
	}

	nexts := make([]*node[T], maxLevel)

	return &SkipList[T]{
		header:       &node[T]{Next: nexts},
		probability:  probability,
		maxLevel:     maxLevel,
		contentsSize: 0,
	}, nil
}

func (s *SkipList[T]) Insert(key string, value T) {
	toUpdate := make([]*node[T], s.maxLevel)

	x := s.header
	for i := s.level; i >= 0; i-- {
		for x.Next[i] != nil && x.Next[i].Key < key {
			x = x.Next[i]
		}

		toUpdate[i] = x
	}

	insertionLevel := randomLevel(s.maxLevel, s.probability)
	if insertionLevel > s.level {
		for i := s.level + 1; i <= insertionLevel; i++ {
			toUpdate[i] = s.header
		}
		s.level = insertionLevel
	}

	n := newNode(key, value, insertionLevel+1)
	for i := 0; i <= insertionLevel; i++ {
		n.Next[i] = toUpdate[i].Next[i]
		toUpdate[i].Next[i] = n
	}

	// Add to the contentsSize
	// TODO: define a way to get bytes.
	// Temporarily just increment content size
	s.contentsSize += 1
	s.debug = s.Debug()
}

func randomLevel(maxLevel int, probability int) int {
	lvl := 0

	for lvl < maxLevel-1 {
		if rand.Intn(100) > probability {
			break
		}

		lvl++
	}

	return lvl
}

func (s *SkipList[T]) Debug() [][]string {
	levels := make([][]string, s.level+1)

	for i := 0; i <= s.level; i++ {
		level := []string{}
		x := s.header

		for x.Next[i] != nil {
			n := x.Next[i]
			level = append(
				level,
				fmt.Sprintf("%s:%v", n.Key, n.Value),
			)
			x = n
		}

		levels[i] = level
	}

	return levels
}

func (s *SkipList[T]) Search(key string) (T, bool) {
	x := s.header
	for i := s.level; i >= 0; i-- {
		for x.Next[i] != nil && x.Next[i].Key < key {
			x = x.Next[i]
		}
	}

	if x.Next[0] != nil && x.Next[0].Key == key {
		return x.Next[0].Value, true
	}

	return *new(T), false
}

func (s *SkipList[T]) ContentSize() int {
	return s.contentsSize
}
