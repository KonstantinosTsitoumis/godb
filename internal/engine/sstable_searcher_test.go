package engine_test

import (
	"godb/internal/engine"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	s := engine.NewSSTableSearcher("../../db")
	err := s.Start()
	require.NoError(t, err)
	val, ok, err := s.Search("user:1:email")
	require.NoError(t, err)
	require.NotNil(t, val)
	require.True(t, ok)

	print("S")
}
