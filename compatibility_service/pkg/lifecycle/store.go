// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// InstalledPackage is one package installed into a running orchestrator.
type InstalledPackage struct {
	PackageID      string `json:"package_id"`
	Label          string `json:"label"`
	Type           string `json:"type"`
	Address        string `json:"address"`
	InstalledByMSP string `json:"installed_by_msp"`
	InstalledAt    string `json:"installed_at"`

	MetadataJSON   []byte `json:"-"`
	ConnectionJSON []byte `json:"-"`
	PackageTGZ     []byte `json:"-"`
}

// Store keeps lifecycle state for one running orchestrator process.
type Store struct {
	db *sql.DB
}

// NewMemoryStore creates a volatile SQLite lifecycle database. Installed
// package records disappear when the orchestrator process exits.
func NewMemoryStore() (*Store, error) {
	db, err := sql.Open("sqlite", fmt.Sprintf("file:compat_lifecycle_%d?mode=memory&cache=shared", time.Now().UnixNano()))
	if err != nil {
		return nil, err
	}
	store := &Store{db: db}
	if err := store.init(context.Background()); err != nil {
		db.Close() //nolint:errcheck
		return nil, err
	}
	return store, nil
}

func (s *Store) init(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS installed_packages (
	package_id TEXT PRIMARY KEY,
	label TEXT NOT NULL,
	type TEXT NOT NULL,
	address TEXT NOT NULL,
	metadata_json BLOB NOT NULL,
	connection_json BLOB NOT NULL,
	package_tgz BLOB NOT NULL,
	installed_by_msp TEXT NOT NULL,
	installed_at TEXT NOT NULL
)`)
	return err
}

// Install records a package. Installing the same package ID again is idempotent.
func (s *Store) Install(ctx context.Context, pkg InstalledPackage) error {
	if pkg.PackageID == "" {
		return fmt.Errorf("package id is required")
	}
	if pkg.InstalledAt == "" {
		pkg.InstalledAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO installed_packages (
	package_id, label, type, address, metadata_json, connection_json, package_tgz, installed_by_msp, installed_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(package_id) DO UPDATE SET
	label = excluded.label,
	type = excluded.type,
	address = excluded.address,
	metadata_json = excluded.metadata_json,
	connection_json = excluded.connection_json,
	package_tgz = excluded.package_tgz,
	installed_by_msp = excluded.installed_by_msp,
	installed_at = excluded.installed_at`,
		pkg.PackageID,
		pkg.Label,
		pkg.Type,
		pkg.Address,
		pkg.MetadataJSON,
		pkg.ConnectionJSON,
		pkg.PackageTGZ,
		pkg.InstalledByMSP,
		pkg.InstalledAt,
	)
	return err
}

// ListInstalled returns all installed packages in deterministic order.
func (s *Store) ListInstalled(ctx context.Context) ([]InstalledPackage, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT package_id, label, type, address, metadata_json, connection_json, package_tgz, installed_by_msp, installed_at
FROM installed_packages
ORDER BY label, package_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var packages []InstalledPackage
	for rows.Next() {
		var pkg InstalledPackage
		if err := rows.Scan(
			&pkg.PackageID,
			&pkg.Label,
			&pkg.Type,
			&pkg.Address,
			&pkg.MetadataJSON,
			&pkg.ConnectionJSON,
			&pkg.PackageTGZ,
			&pkg.InstalledByMSP,
			&pkg.InstalledAt,
		); err != nil {
			return nil, err
		}
		packages = append(packages, pkg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return packages, nil
}

// Close releases the SQLite connection.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
