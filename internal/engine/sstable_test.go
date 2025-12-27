package engine_test

import (
	"godb/internal/engine"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemTableToSSTable(t *testing.T) {
	mem, err := engine.NewMemTable(3, 50)
	require.NoError(t, err)

	mem.Insert("apple", []byte("fruit"))
	mem.Insert("apricot", []byte("fruit"))
	mem.Insert("apricotpie", []byte("dessert"))
	mem.Insert("banana", []byte("fruit"))
	mem.Insert("berry", []byte("fruit"))
	mem.Insert("blueberry", []byte("fruit"))
	mem.Insert("blackberry", []byte("fruit"))
	mem.Insert("cherry", []byte("fruit"))
	mem.Insert("cranberry", []byte("fruit"))
	mem.Insert("date", []byte("fruit"))
	mem.Insert("dragonfruit", []byte("fruit"))
	mem.Insert("elderberry", []byte("fruit"))
	mem.Insert("fig", []byte("fruit"))
	mem.Insert("grape", []byte("new fruit"))
	mem.Insert("grape", []byte("fruit"))
	mem.Insert("grape", []byte("old fruit"))
	mem.Insert("grapefruit", []byte("fruit"))
	mem.Insert("kiwi", []byte("fruit"))
	mem.Insert("kumquat", []byte("fruit"))
	mem.Insert("lemon", []byte("fruit"))
	mem.Insert("lime", []byte("fruit"))
	mem.Insert("mango", []byte("fruit"))
	mem.Insert("nectarine", []byte("fruit"))
	mem.Insert("orange", []byte("fruit"))
	mem.Insert("papaya", []byte("fruit"))
	mem.Insert("peach", []byte("fruit"))
	mem.Insert("pear", []byte("fruit"))
	mem.Insert("pineapple", []byte("fruit"))
	mem.Insert("plum", []byte("fruit"))
	mem.Insert("pomegranate", []byte("fruit"))
	mem.Insert("raspberry", []byte("fruit"))
	mem.Insert("strawberry", []byte("fruit"))

	res := engine.NewSSTableFromMemTable(mem, 500)
	ok := res.BloomFilter.Contains([]byte("strawberry"))
	require.True(t, ok)
	ok = res.BloomFilter.Contains([]byte("Nope"))
	require.False(t, ok)
}
