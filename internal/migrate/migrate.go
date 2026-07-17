// Package migrate applies the embedded goose migrations. It is the single
// place the goose provider and its advisory-lock session locker are wired:
// cmd/the-button-migrate (the Argo PreSync Job), internal/testutil, and
// load/soak all call Up rather than repeating the setup. The service itself
// never applies schema — see the 2026-07-17 sqlc+goose migrations spec.
package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx", for goose
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/the-algovn/the-button-service/internal/db"
)

// lockKey is the same advisory-lock ID the retired startup schema-apply used
// (internal/store's schemaLockKey). goose's version table does not protect
// against two runners racing, so the session locker does.
const lockKey int64 = 7238410394821017561

// minSafeDownVersion is the lowest goose version Down will reverse from
// without force. Migration 001 creates user_clicks and user_achievements;
// reversing it (current version <= 1) DROPS both tables — every click and
// achievement the product has ever recorded. Reversing 002 (current
// version 2) only recreates counter_outbox and is safe.
const minSafeDownVersion int64 = 1

// newProvider wires the goose provider shared by Up and Down: the embedded
// migrations, a pgx *sql.DB, and the advisory-lock session locker. On success
// the caller owns sqlDB and must close it; on error sqlDB is already closed.
func newProvider(url string) (p *goose.Provider, sqlDB *sql.DB, err error) {
	sub, err := fs.Sub(db.Migrations, "migrations")
	if err != nil {
		return nil, nil, err
	}
	sqlDB, err = sql.Open("pgx", url)
	if err != nil {
		return nil, nil, err
	}

	locker, err := lock.NewPostgresSessionLocker(lock.WithLockID(lockKey))
	if err != nil {
		sqlDB.Close()
		return nil, nil, err
	}
	p, err = goose.NewProvider(goose.DialectPostgres, sqlDB, sub, goose.WithSessionLocker(locker))
	if err != nil {
		sqlDB.Close()
		return nil, nil, err
	}
	return p, sqlDB, nil
}

// Up applies every pending migration to url and returns one line per applied
// migration — empty when there was nothing to do.
func Up(ctx context.Context, url string) ([]string, error) {
	p, sqlDB, err := newProvider(url)
	if err != nil {
		return nil, err
	}
	defer sqlDB.Close()

	results, err := p.Up(ctx)
	if err != nil {
		return nil, err
	}
	applied := make([]string, 0, len(results))
	for _, r := range results {
		applied = append(applied, fmt.Sprintf("version=%d path=%s empty=%t duration=%s",
			r.Source.Version, r.Source.Path, r.Empty, r.Duration))
	}
	return applied, nil
}

// Down reverses exactly the one most-recently-applied migration and returns a
// human-readable line describing what was reversed, mirroring Up's line
// format. There is no way to reverse more than one migration per call — that
// is goose's contract, not a limitation added here.
//
// Unless force is true, Down refuses to reverse migration 001 (i.e. when the
// current version is already <= minSafeDownVersion): 001's Down drops
// user_clicks and user_achievements, destroying every click and achievement
// in production. Pass force only for a deliberate teardown of a
// throwaway/dev database.
func Down(ctx context.Context, url string, force bool) (string, error) {
	p, sqlDB, err := newProvider(url)
	if err != nil {
		return "", err
	}
	defer sqlDB.Close()

	var r *goose.MigrationResult
	if force {
		r, err = p.Down(ctx)
	} else {
		var version int64
		version, err = p.GetDBVersion(ctx)
		if err != nil {
			return "", err
		}
		if version <= minSafeDownVersion {
			return "", fmt.Errorf("refusing to reverse migration 001 (current version=%d): its Down DROPS user_clicks and user_achievements — every click and achievement in production; pass -force-destructive only for a throwaway/dev database", version)
		}
		// Pin the reversal to exactly the version just checked instead of
		// calling p.Down (which would reverse whatever is latest at the
		// time it runs, not at the time the guard checked). GetDBVersion
		// above acquires the advisory lock, reads, and releases it; without
		// this pin, two concurrent unforced Downs could both read version=2,
		// both pass the guard, and the second (racing in after the first
		// already reversed 002) would go on to reverse 001. ApplyVersion
		// re-acquires the same advisory lock and re-verifies under it that
		// this exact version is still applied before running its Down, so
		// the second caller gets ErrNotApplied instead of silently
		// destroying production data.
		r, err = p.ApplyVersion(ctx, version, false)
	}
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("version=%d path=%s empty=%t duration=%s",
		r.Source.Version, r.Source.Path, r.Empty, r.Duration), nil
}
