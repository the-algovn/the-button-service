//go:build integration

package testutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/the-algovn/the-button-service/internal/migrate"
)

// Migrate applies the embedded goose migrations to url, failing the test on
// error. Since the service no longer applies schema at startup, every
// integration test that needs tables calls this right after StartPostgres.
//
// Only call this from the test's own goroutine: require.* calls t.FailNow(),
// which Go forbids elsewhere. Concurrent callers want MigrateE.
func Migrate(t *testing.T, url string) {
	t.Helper()
	require.NoError(t, MigrateE(url))
}

// MigrateE is Migrate without the testing hooks, for callers that run it off
// the test goroutine (see Migrate's note) or want to assert on the error.
func MigrateE(url string) error {
	_, err := migrate.Up(context.Background(), url)
	return err
}
