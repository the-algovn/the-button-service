//go:build integration

package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedpandaBoots(t *testing.T) {
	brokers := StartRedpanda(t)
	require.NotEmpty(t, brokers)
	require.NotEmpty(t, brokers[0])
}
