package api_test

import (
	"godb/internal/api"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test(t *testing.T) {
	t.Run("should run", func(t *testing.T) {
		db := api.NewDatabase("../../db")
		err := db.Start()
		require.NoError(t, err)

		key := "a"
		value := []byte("this is a test")
		err = db.Put(key, value)
		require.NoError(t, err)

		key = "b"
		value = []byte("Hi this is me")
		err = db.Put(key, value)
		require.NoError(t, err)

		key = "b"
		value = []byte("Hi this is me 2")
		err = db.Put(key, value)
		require.NoError(t, err)

		key = "k"
		value = []byte("hm")
		err = db.Put(key, value)
		require.NoError(t, err)

		key = "a"
		value = []byte("hm...")
		err = db.Put(key, value)
		require.NoError(t, err)

		require.NoError(t, err)
		v, _ := db.Get("b")
		require.Equal(t, []byte("Hi this is me 2"), v)
		v, _ = db.Get("a")
		require.Equal(t, []byte("hm..."), v)

		db.Delete("a")
		_, ok := db.Get("a")
		require.False(t, ok)
	})
}
