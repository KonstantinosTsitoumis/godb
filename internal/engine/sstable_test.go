package engine_test

import (
	_ "context"
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
	mem.Delete("apple")
	mem.Insert("pear", []byte("fruit"))
	mem.Insert("pineapple", []byte("fruit"))
	mem.Insert("plum", []byte("fruit"))
	mem.Insert("pomegranate", []byte("fruit"))
	mem.Insert("raspberry", []byte("fruit"))
	mem.Insert("strawberry", []byte("fruit"))

	// f := engine.NewFlusher("../../db", 3, 1000)
	// err = f.Start(context.Background())
	// require.NoError(t, err)
	// f.EnqueueToBeFlushed(mem)

	s := engine.NewSSTableSearcher("../../db")
	err = s.Start()
	require.NoError(t, err)
	val, _, err := s.Search("apple")
	require.NoError(t, err)
	require.Equal(t, val, []byte("__TOMBSTONE__"))

	// err = f.Stop()
	require.NoError(t, err)
}
