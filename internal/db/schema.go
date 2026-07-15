// Package db holds the sqlc-generated data-access layer and the embedded
// schema that is the single source of truth for both codegen (sqlc reads
// schema.sql) and the idempotent startup application (spec §7: no migration
// framework).
package db

import _ "embed"

//go:embed schema.sql
var Schema string
