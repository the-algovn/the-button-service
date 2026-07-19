//go:build integration

// Requires a running podman machine:
//
//	export DOCKER_HOST="unix://$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}')"
//	export TESTCONTAINERS_RYUK_DISABLED=true
package testutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// StartPostgres runs postgres:18-alpine and returns a pgx URL.
func StartPostgres(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := tcpostgres.Run(ctx, "postgres:18-alpine",
		tcpostgres.WithDatabase("the_button"),
		tcpostgres.WithUsername("the_button"),
		tcpostgres.WithPassword("test"),
		tcpostgres.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)
	url, err := c.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	return url
}

// StartRedis runs redis:7.4-alpine and returns a redis:// URL.
func StartRedis(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := tcredis.Run(ctx, "redis:7.4-alpine")
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)
	url, err := c.ConnectionString(ctx)
	require.NoError(t, err)
	return url
}

// StartRedpanda runs a single-node Redpanda broker and returns its Kafka seed
// broker address. Redpanda speaks the Kafka protocol, so franz-go connects to
// it unchanged.
func StartRedpanda(t *testing.T) []string {
	t.Helper()
	ctx := context.Background()
	c, err := redpanda.Run(ctx, "redpandadata/redpanda:v24.2.7")
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)
	seed, err := c.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return []string{seed}
}
