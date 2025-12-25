package datastructures_test

import (
	"godb/internal/datastructures"
	"godb/internal/tooling/guard"
	"testing"
)

func Test(t *testing.T) {
	t.Run("should run", func(t *testing.T) {
		skiplist, err := datastructures.NewSkipList[string](4, 50)
		guard.Assert(err == nil, "no error")

		skiplist.Insert("1", "ena")
		skiplist.Insert("2", "dyo")
		skiplist.Insert("3", "tria")
		skiplist.Insert("10", "deka")
		skiplist.Insert("100", "ekato")
		skiplist.Insert("5", "pente")
		skiplist.Insert("5", "pente 2")
		skiplist.Insert("5", "pente 3")
		skiplist.Insert("13", "dekatria")
		skiplist.Insert("20", "ikosi")
		skiplist.Insert("5", "tombstone")
		v, ok := skiplist.Search("5")
		print(v)
		print(ok)
		v, ok = skiplist.Search("5")
		print(v)
		print(ok)

		result := skiplist.String()
		print(result)
	})
}
