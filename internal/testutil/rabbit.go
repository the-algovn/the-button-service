//go:build integration

package testutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

// StartRabbit runs rabbitmq:4.1-management-alpine and returns the AMQP URL.
func StartRabbit(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := tcrabbit.Run(ctx, "rabbitmq:4.1-management-alpine")
	testcontainers.CleanupContainer(t, c)
	require.NoError(t, err)
	url, err := c.AmqpURL(ctx)
	require.NoError(t, err)
	return url
}
