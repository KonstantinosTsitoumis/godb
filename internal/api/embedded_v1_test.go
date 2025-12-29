package api_test

import (
	"fmt"
	"godb/internal/api"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDatabase_MediumDataset(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db := api.NewDatabase(dir)

	require.NoError(t, db.Start())

	const (
		users          = 200
		columnsPerUser = 5
	)

	// ----------------------------------------------------
	// Phase 1: Insert users × columns
	// ----------------------------------------------------
	for u := 1; u <= users; u++ {
		for c := 1; c <= columnsPerUser; c++ {
			key := fmt.Sprintf("user:%d:field:%d", u, c)
			value := []byte(fmt.Sprintf("value-%d-%d", u, c))

			require.NoError(t, db.Put(key, value))
		}
	}

	// ----------------------------------------------------
	// Phase 2: Overwrite some values
	// (simulate updates)
	// ----------------------------------------------------
	for u := 1; u <= users; u++ {
		key := fmt.Sprintf("user:%d:field:1", u)
		value := []byte(fmt.Sprintf("updated-%d", u))

		require.NoError(t, db.Put(key, value))
	}

	// ----------------------------------------------------
	// Phase 3: Validate reads
	// ----------------------------------------------------
	for u := 1; u <= users; u++ {
		for c := 1; c <= columnsPerUser; c++ {
			key := fmt.Sprintf("user:%d:field:%d", u, c)

			v, ok := db.Get(key)
			require.True(t, ok)

			if c == 1 {
				require.Equal(t, []byte(fmt.Sprintf("updated-%d", u)), v)
			} else {
				require.Equal(t, []byte(fmt.Sprintf("value-%d-%d", u, c)), v)
			}
		}
	}

	// ----------------------------------------------------
	// Phase 4: Delete every 10th user
	// ----------------------------------------------------
	for u := 10; u <= users; u += 10 {
		for c := 1; c <= columnsPerUser; c++ {
			key := fmt.Sprintf("user:%d:field:%d", u, c)
			require.NoError(t, db.Delete(key))
		}
	}

	// ----------------------------------------------------
	// Phase 5: Validate deletes + survivors
	// ----------------------------------------------------
	for u := 1; u <= users; u++ {
		for c := 1; c <= columnsPerUser; c++ {
			key := fmt.Sprintf("user:%d:field:%d", u, c)
			v, ok := db.Get(key)

			if u%10 == 0 {
				require.False(t, ok)
				require.Nil(t, v)
			} else {
				require.True(t, ok)
			}
		}
	}

	require.NoError(t, db.Stop())
}
