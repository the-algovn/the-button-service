// Package db holds the sqlc-generated data-access layer and the embedded goose
// migrations that are the single source of truth for the schema: sqlc reads
// migrations/ directly (it applies only the -- +goose Up sections), and
// cmd/the-button-migrate applies them via an Argo PreSync Job. The service
// itself never touches schema — see the 2026-07-17 sqlc+goose migrations spec.
package db

import "embed"

// Migrations is rooted at internal/db/; callers want fs.Sub(Migrations, "migrations").
//
//go:embed migrations/*.sql
var Migrations embed.FS
