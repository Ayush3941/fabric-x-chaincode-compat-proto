// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"database/sql"
	"errors"
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

// ChaincodeDefinition is the shared lifecycle definition. It deliberately does
// not carry a package ID because each organization keeps its own local package
// mapping.
type ChaincodeDefinition struct {
	Name         string `json:"name"`
	Version      string `json:"version"`
	Sequence     int64  `json:"sequence"`
	InitRequired bool   `json:"init_required"`
}

// ApprovedDefinition is one organization's local approval for a shared
// definition and its local installed package.
type ApprovedDefinition struct {
	Definition     ChaincodeDefinition `json:"definition"`
	PackageID      string              `json:"package_id"`
	ApprovingMSP   string              `json:"approving_msp"`
	ApprovedAt     string              `json:"approved_at"`
	PackageAddress string              `json:"package_address,omitempty"`
}

// CommittedDefinition is the active lifecycle definition known by this
// orchestrator process.
type CommittedDefinition struct {
	Definition  ChaincodeDefinition `json:"definition"`
	CommittedAt string              `json:"committed_at"`
	Initialized bool                `json:"initialized"`
}

// ResolvedChaincodeConnection is org-local CCAAS connection information
// resolved lazily after lifecycle commit state is known.
type ResolvedChaincodeConnection struct {
	MSPID    string `json:"msp_id"`
	Name     string `json:"name"`
	Version  string `json:"version"`
	Sequence int64  `json:"sequence"`
	Address  string `json:"address"`
	TLSMode  string `json:"tls_mode,omitempty"`
	CachedAt string `json:"cached_at"`
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
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS approved_definitions (
	ccid TEXT NOT NULL,
	name TEXT NOT NULL,
	version TEXT NOT NULL,
	sequence INTEGER NOT NULL,
	package_id TEXT NOT NULL,
	init_required INTEGER NOT NULL,
	approving_msp TEXT NOT NULL,
	approved_at TEXT NOT NULL,
	PRIMARY KEY(ccid, approving_msp)
)`)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS committed_definitions (
	ccid TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	version TEXT NOT NULL,
	sequence INTEGER NOT NULL,
	init_required INTEGER NOT NULL,
	initialized INTEGER NOT NULL,
	committed_at TEXT NOT NULL
)`)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS resolved_connections (
	msp_id TEXT NOT NULL,
	ccid TEXT NOT NULL,
	name TEXT NOT NULL,
	version TEXT NOT NULL,
	sequence INTEGER NOT NULL,
	address TEXT NOT NULL,
	tls_mode TEXT NOT NULL,
	cached_at TEXT NOT NULL,
	PRIMARY KEY(msp_id, ccid, sequence)
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

// GetInstalled returns one installed package by package ID.
func (s *Store) GetInstalled(ctx context.Context, packageID string) (InstalledPackage, bool, error) {
	if packageID == "" {
		return InstalledPackage{}, false, fmt.Errorf("package id is required")
	}
	row := s.db.QueryRowContext(ctx, `
SELECT package_id, label, type, address, metadata_json, connection_json, package_tgz, installed_by_msp, installed_at
FROM installed_packages
WHERE package_id = ?`, packageID)
	pkg, err := scanInstalledPackage(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return InstalledPackage{}, false, nil
		}
		return InstalledPackage{}, false, err
	}
	return pkg, true, nil
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
		pkg, err := scanInstalledPackage(rows)
		if err != nil {
			return nil, err
		}
		packages = append(packages, pkg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return packages, nil
}

// ApproveForMyOrg records a local organization's current approval for a ccid.
// Re-approving the exact same definition is idempotent. New approvals must use
// a greater sequence and match the next lifecycle sequence for the committed
// definition.
func (s *Store) ApproveForMyOrg(ctx context.Context, def ChaincodeDefinition, packageID, approvingMSP string) (ApprovedDefinition, error) {
	if err := validateDefinition(def); err != nil {
		return ApprovedDefinition{}, err
	}
	ccid := definitionCCID(def)
	if packageID == "" {
		return ApprovedDefinition{}, fmt.Errorf("package id is required")
	}
	if approvingMSP == "" {
		return ApprovedDefinition{}, fmt.Errorf("approving MSP is required")
	}
	pkg, ok, err := s.GetInstalled(ctx, packageID)
	if err != nil {
		return ApprovedDefinition{}, err
	}
	if !ok {
		return ApprovedDefinition{}, fmt.Errorf("package %s is not installed", packageID)
	}

	existing, ok, err := s.GetApproval(ctx, def.Name, def.Version, approvingMSP)
	if err != nil {
		return ApprovedDefinition{}, err
	}
	if ok {
		if approvalMatches(existing, def, packageID) {
			return existing, nil
		}
		if existing.Definition.Sequence >= def.Sequence {
			return ApprovedDefinition{}, fmt.Errorf("approval for ccid=%s msp=%s already has sequence=%d; requested sequence=%d must be greater",
				ccid, approvingMSP, existing.Definition.Sequence, def.Sequence)
		}
	}
	if err := s.validateNextSequence(ctx, def); err != nil {
		return ApprovedDefinition{}, err
	}

	approval := ApprovedDefinition{
		Definition:     def,
		PackageID:      packageID,
		ApprovingMSP:   approvingMSP,
		ApprovedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		PackageAddress: pkg.Address,
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO approved_definitions (
	ccid, name, version, sequence, package_id, init_required, approving_msp, approved_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(ccid, approving_msp) DO UPDATE SET
	name = excluded.name,
	version = excluded.version,
	sequence = excluded.sequence,
	package_id = excluded.package_id,
	init_required = excluded.init_required,
	approved_at = excluded.approved_at`,
		ccid,
		def.Name,
		def.Version,
		def.Sequence,
		packageID,
		boolInt(def.InitRequired),
		approvingMSP,
		approval.ApprovedAt,
	)
	if err != nil {
		return ApprovedDefinition{}, err
	}
	return approval, nil
}

// GetApproval returns one local approval for a chaincode ID and MSP.
func (s *Store) GetApproval(ctx context.Context, name, version, approvingMSP string) (ApprovedDefinition, bool, error) {
	ccid, err := ccidFromNameVersion(name, version)
	if err != nil {
		return ApprovedDefinition{}, false, err
	}
	row := s.db.QueryRowContext(ctx, `
SELECT a.name, a.version, a.sequence, a.package_id, a.init_required, a.approving_msp, a.approved_at, p.address
FROM approved_definitions a
JOIN installed_packages p ON p.package_id = a.package_id
WHERE a.ccid = ? AND a.approving_msp = ?`,
		ccid, approvingMSP)
	approval, err := scanApproval(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ApprovedDefinition{}, false, nil
		}
		return ApprovedDefinition{}, false, err
	}
	return approval, true, nil
}

// HasMatchingApproval returns true when this MSP has approved the exact shared
// definition fields.
func (s *Store) HasMatchingApproval(ctx context.Context, def ChaincodeDefinition, approvingMSP string) (bool, error) {
	if err := validateDefinition(def); err != nil {
		return false, err
	}
	approval, ok, err := s.GetApproval(ctx, def.Name, def.Version, approvingMSP)
	if err != nil || !ok {
		return false, err
	}
	return definitionMatches(approval.Definition, def), nil
}

// ValidateCommitDefinition checks whether a shared definition can be committed
// without mutating local lifecycle state.
func (s *Store) ValidateCommitDefinition(ctx context.Context, def ChaincodeDefinition, localMSP string) error {
	approved, err := s.HasMatchingApproval(ctx, def, localMSP)
	if err != nil {
		return err
	}
	if !approved {
		return fmt.Errorf("definition name=%s sequence=%d is not approved by %s",
			def.Name, def.Sequence, localMSP)
	}
	return s.validateNextSequence(ctx, def)
}

// CommitDefinition marks a shared definition active in this process. The
// definition must already have a matching local approval.
func (s *Store) CommitDefinition(ctx context.Context, def ChaincodeDefinition, localMSP string) (CommittedDefinition, error) {
	if err := s.ValidateCommitDefinition(ctx, def, localMSP); err != nil {
		return CommittedDefinition{}, err
	}

	return s.upsertCommitted(ctx, CommittedDefinition{
		Definition:  def,
		CommittedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Initialized: !def.InitRequired,
	})
}

// UpsertCommittedFromLedger hydrates committed lifecycle state from the shared
// Fabric-X lifecycle ledger. It does not require local package approval because
// package and connection data remain org-local.
func (s *Store) UpsertCommittedFromLedger(ctx context.Context, def ChaincodeDefinition, initialized bool) (CommittedDefinition, error) {
	if err := validateDefinition(def); err != nil {
		return CommittedDefinition{}, err
	}
	existing, ok, err := s.GetCommitted(ctx, def.Name, def.Version)
	if err != nil {
		return CommittedDefinition{}, err
	}
	if ok {
		if existing.Definition.Sequence > def.Sequence {
			return existing, nil
		}
		if existing.Definition.Sequence == def.Sequence &&
			existing.Definition.InitRequired == def.InitRequired &&
			existing.Initialized == initialized {
			return existing, nil
		}
	}
	return s.upsertCommitted(ctx, CommittedDefinition{
		Definition:  def,
		CommittedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Initialized: initialized,
	})
}

func (s *Store) upsertCommitted(ctx context.Context, committed CommittedDefinition) (CommittedDefinition, error) {
	def := committed.Definition
	if err := validateDefinition(def); err != nil {
		return CommittedDefinition{}, err
	}
	if committed.CommittedAt == "" {
		committed.CommittedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO committed_definitions (
	ccid, name, version, sequence, init_required, initialized, committed_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(ccid) DO UPDATE SET
	name = excluded.name,
	version = excluded.version,
	sequence = excluded.sequence,
	init_required = excluded.init_required,
	initialized = excluded.initialized,
	committed_at = excluded.committed_at`,
		definitionCCID(def),
		def.Name,
		def.Version,
		def.Sequence,
		boolInt(def.InitRequired),
		boolInt(committed.Initialized),
		committed.CommittedAt,
	)
	if err != nil {
		return CommittedDefinition{}, err
	}
	return committed, nil
}

// MarkInitialized records that the committed definition's required init
// transaction has committed. Calling it again is idempotent.
func (s *Store) MarkInitialized(ctx context.Context, def ChaincodeDefinition, localMSP string) (CommittedDefinition, error) {
	if err := validateDefinition(def); err != nil {
		return CommittedDefinition{}, err
	}
	_ = localMSP

	committed, ok, err := s.GetCommitted(ctx, def.Name, def.Version)
	if err != nil {
		return CommittedDefinition{}, err
	}
	if !ok {
		return CommittedDefinition{}, fmt.Errorf("definition name=%s version=%s is not committed", def.Name, def.Version)
	}
	if !definitionMatches(committed.Definition, def) {
		return CommittedDefinition{}, fmt.Errorf("committed definition does not match init definition name=%s version=%s sequence=%d",
			def.Name, def.Version, def.Sequence)
	}
	if !committed.Definition.InitRequired {
		return CommittedDefinition{}, fmt.Errorf("definition name=%s version=%s does not require init", def.Name, def.Version)
	}
	if committed.Initialized {
		return committed, nil
	}

	committed.Initialized = true
	committed.CommittedAt = time.Now().UTC().Format(time.RFC3339Nano)
	_, err = s.db.ExecContext(ctx, `
UPDATE committed_definitions
SET initialized = 1, committed_at = ?
WHERE ccid = ?`,
		committed.CommittedAt,
		definitionCCID(def),
	)
	if err != nil {
		return CommittedDefinition{}, err
	}
	return committed, nil
}

// GetCommitted returns the active definition for a chaincode name and version.
func (s *Store) GetCommitted(ctx context.Context, name, version string) (CommittedDefinition, bool, error) {
	ccid, err := ccidFromNameVersion(name, version)
	if err != nil {
		return CommittedDefinition{}, false, err
	}
	row := s.db.QueryRowContext(ctx, `
SELECT name, version, sequence, init_required, initialized, committed_at
FROM committed_definitions
WHERE ccid = ?`, ccid)
	committed, err := scanCommitted(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return CommittedDefinition{}, false, nil
		}
		return CommittedDefinition{}, false, err
	}
	return committed, true, nil
}

// ListCommitted returns committed definitions. If name and version are set, it
// returns at most one definition.
func (s *Store) ListCommitted(ctx context.Context, name, version string) ([]CommittedDefinition, error) {
	query := `
SELECT name, version, sequence, init_required, initialized, committed_at
FROM committed_definitions`
	var args []any
	if name != "" {
		query += ` WHERE name = ?`
		args = append(args, name)
	}
	if version != "" {
		if name == "" {
			query += ` WHERE version = ?`
		} else {
			query += ` AND version = ?`
		}
		args = append(args, version)
	}
	query += ` ORDER BY name, version`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var committed []CommittedDefinition
	for rows.Next() {
		item, err := scanCommitted(rows)
		if err != nil {
			return nil, err
		}
		committed = append(committed, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return committed, nil
}

// ResolveCommittedPackage resolves the active definition to the local installed
// CCAAS package for this MSP. If name/version are empty, exactly one committed
// definition must exist.
func (s *Store) ResolveCommittedPackage(ctx context.Context, name, version, localMSP string) (CommittedDefinition, InstalledPackage, bool, error) {
	definitions, err := s.ListCommitted(ctx, name, version)
	if err != nil {
		return CommittedDefinition{}, InstalledPackage{}, false, err
	}
	if len(definitions) == 0 {
		return CommittedDefinition{}, InstalledPackage{}, false, nil
	}
	if (name == "" || version == "") && len(definitions) > 1 {
		return CommittedDefinition{}, InstalledPackage{}, false, fmt.Errorf("%d committed definitions are active; chaincode name and version are required",
			len(definitions))
	}
	committed := definitions[0]
	approval, ok, err := s.GetApproval(ctx, committed.Definition.Name, committed.Definition.Version, localMSP)
	if err != nil {
		return CommittedDefinition{}, InstalledPackage{}, false, err
	}
	if !ok || !definitionMatches(approval.Definition, committed.Definition) {
		return CommittedDefinition{}, InstalledPackage{}, false, fmt.Errorf("local MSP %s has no package approval for committed definition name=%s sequence=%d",
			localMSP, committed.Definition.Name, committed.Definition.Sequence)
	}
	pkg, ok, err := s.GetInstalled(ctx, approval.PackageID)
	if err != nil {
		return CommittedDefinition{}, InstalledPackage{}, false, err
	}
	if !ok {
		return CommittedDefinition{}, InstalledPackage{}, false, fmt.Errorf("approved package %s is no longer installed", approval.PackageID)
	}
	return committed, pkg, true, nil
}

// GetResolvedConnection returns a cached org-local connection for a committed
// definition sequence.
func (s *Store) GetResolvedConnection(ctx context.Context, mspID string, def ChaincodeDefinition) (ResolvedChaincodeConnection, bool, error) {
	if mspID == "" {
		return ResolvedChaincodeConnection{}, false, fmt.Errorf("MSP ID is required")
	}
	ccid, err := ccidFromNameVersion(def.Name, def.Version)
	if err != nil {
		return ResolvedChaincodeConnection{}, false, err
	}
	row := s.db.QueryRowContext(ctx, `
SELECT msp_id, name, version, sequence, address, tls_mode, cached_at
FROM resolved_connections
WHERE msp_id = ? AND ccid = ? AND sequence = ?`,
		mspID, ccid, def.Sequence)
	conn, err := scanResolvedConnection(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ResolvedChaincodeConnection{}, false, nil
		}
		return ResolvedChaincodeConnection{}, false, err
	}
	return conn, true, nil
}

// PutResolvedConnection caches a successful org-local connection resolution.
func (s *Store) PutResolvedConnection(ctx context.Context, conn ResolvedChaincodeConnection) (ResolvedChaincodeConnection, error) {
	if conn.MSPID == "" {
		return ResolvedChaincodeConnection{}, fmt.Errorf("MSP ID is required")
	}
	def := ChaincodeDefinition{Name: conn.Name, Version: conn.Version, Sequence: conn.Sequence}
	if err := validateDefinition(def); err != nil {
		return ResolvedChaincodeConnection{}, err
	}
	if conn.Address == "" {
		return ResolvedChaincodeConnection{}, fmt.Errorf("chaincode connection address is required")
	}
	if conn.TLSMode == "" {
		conn.TLSMode = "none"
	}
	if conn.CachedAt == "" {
		conn.CachedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	ccid := definitionCCID(def)
	_, err := s.db.ExecContext(ctx, `
INSERT INTO resolved_connections (
	msp_id, ccid, name, version, sequence, address, tls_mode, cached_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(msp_id, ccid, sequence) DO UPDATE SET
	address = excluded.address,
	tls_mode = excluded.tls_mode,
	cached_at = excluded.cached_at`,
		conn.MSPID,
		ccid,
		conn.Name,
		conn.Version,
		conn.Sequence,
		conn.Address,
		conn.TLSMode,
		conn.CachedAt,
	)
	if err != nil {
		return ResolvedChaincodeConnection{}, err
	}
	return conn, nil
}

// Close releases the SQLite connection.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanInstalledPackage(row rowScanner) (InstalledPackage, error) {
	var pkg InstalledPackage
	err := row.Scan(
		&pkg.PackageID,
		&pkg.Label,
		&pkg.Type,
		&pkg.Address,
		&pkg.MetadataJSON,
		&pkg.ConnectionJSON,
		&pkg.PackageTGZ,
		&pkg.InstalledByMSP,
		&pkg.InstalledAt,
	)
	return pkg, err
}

func scanApproval(row rowScanner) (ApprovedDefinition, error) {
	var approval ApprovedDefinition
	var initRequired int
	err := row.Scan(
		&approval.Definition.Name,
		&approval.Definition.Version,
		&approval.Definition.Sequence,
		&approval.PackageID,
		&initRequired,
		&approval.ApprovingMSP,
		&approval.ApprovedAt,
		&approval.PackageAddress,
	)
	approval.Definition.InitRequired = initRequired != 0
	return approval, err
}

func scanCommitted(row rowScanner) (CommittedDefinition, error) {
	var committed CommittedDefinition
	var initRequired int
	var initialized int
	err := row.Scan(
		&committed.Definition.Name,
		&committed.Definition.Version,
		&committed.Definition.Sequence,
		&initRequired,
		&initialized,
		&committed.CommittedAt,
	)
	committed.Definition.InitRequired = initRequired != 0
	committed.Initialized = initialized != 0
	return committed, err
}

func scanResolvedConnection(row rowScanner) (ResolvedChaincodeConnection, error) {
	var conn ResolvedChaincodeConnection
	err := row.Scan(
		&conn.MSPID,
		&conn.Name,
		&conn.Version,
		&conn.Sequence,
		&conn.Address,
		&conn.TLSMode,
		&conn.CachedAt,
	)
	return conn, err
}

func validateDefinition(def ChaincodeDefinition) error {
	if def.Name == "" {
		return fmt.Errorf("chaincode name is required")
	}
	if def.Version == "" {
		return fmt.Errorf("version is required")
	}
	if def.Sequence <= 0 {
		return fmt.Errorf("sequence must be greater than zero")
	}
	return nil
}

func approvalMatches(approval ApprovedDefinition, def ChaincodeDefinition, packageID string) bool {
	return approval.PackageID == packageID && definitionMatches(approval.Definition, def)
}

func definitionMatches(left, right ChaincodeDefinition) bool {
	return left.Name == right.Name &&
		left.Version == right.Version &&
		left.Sequence == right.Sequence &&
		left.InitRequired == right.InitRequired
}

func (s *Store) validateNextSequence(ctx context.Context, def ChaincodeDefinition) error {
	existing, ok, err := s.GetCommitted(ctx, def.Name, def.Version)
	if err != nil {
		return err
	}
	ccid := definitionCCID(def)
	if !ok {
		if def.Sequence != 1 {
			return fmt.Errorf("initial sequence for ccid=%s must be 1; requested sequence=%d", ccid, def.Sequence)
		}
		return nil
	}
	expected := existing.Definition.Sequence + 1
	if def.Sequence != expected {
		return fmt.Errorf("next sequence for ccid=%s must be %d; active sequence=%d requested sequence=%d",
			ccid, expected, existing.Definition.Sequence, def.Sequence)
	}
	return nil
}

func definitionCCID(def ChaincodeDefinition) string {
	return DefinitionCCID(def)
}

func ccidFromNameVersion(name, version string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("chaincode name is required")
	}
	if version == "" {
		return "", fmt.Errorf("version is required")
	}
	return fmt.Sprintf("%s:%s", name, version), nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
