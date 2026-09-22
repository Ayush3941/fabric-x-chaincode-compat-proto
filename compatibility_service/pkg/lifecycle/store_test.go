// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"strings"
	"testing"
)

func TestApproveForMyOrgRequiresIncreasingSequence(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	installTestPackage(t, ctx, store, "pkg1", "127.0.0.1:9999")
	installTestPackage(t, ctx, store, "pkg2", "127.0.0.1:10000")

	def := testDefinition()
	firstApproval, err := store.ApproveForMyOrg(ctx, def, "pkg1", "org-0")
	if err != nil {
		t.Fatalf("ApproveForMyOrg failed: %v", err)
	}
	duplicateApproval, err := store.ApproveForMyOrg(ctx, def, "pkg1", "org-0")
	if err != nil {
		t.Fatalf("duplicate ApproveForMyOrg failed: %v", err)
	}
	if duplicateApproval.PackageID != firstApproval.PackageID || duplicateApproval.ApprovedAt != firstApproval.ApprovedAt {
		t.Fatalf("duplicate approval = package %s approved_at %s, want package %s approved_at %s",
			duplicateApproval.PackageID, duplicateApproval.ApprovedAt, firstApproval.PackageID, firstApproval.ApprovedAt)
	}
	if _, err := store.ApproveForMyOrg(ctx, def, "pkg2", "org-0"); err == nil || !strings.Contains(err.Error(), "already has sequence=1") {
		t.Fatalf("expected conflicting approval sequence error, got %v", err)
	}

	seq2 := ChaincodeDefinition{
		Name:         def.Name,
		Version:      def.Version,
		Sequence:     2,
		InitRequired: def.InitRequired,
	}
	if _, err := store.ApproveForMyOrg(ctx, seq2, "pkg2", "org-0"); err == nil || !strings.Contains(err.Error(), "initial sequence") {
		t.Fatalf("expected initial sequence error, got %v", err)
	}

	if _, err := store.CommitDefinition(ctx, def, "org-0"); err != nil {
		t.Fatalf("CommitDefinition failed: %v", err)
	}
	if _, err := store.ApproveForMyOrg(ctx, seq2, "pkg2", "org-0"); err != nil {
		t.Fatalf("ApproveForMyOrg sequence 2 failed: %v", err)
	}
	approval, ok, err := store.GetApproval(ctx, def.Name, def.Version, "org-0")
	if err != nil {
		t.Fatalf("GetApproval failed: %v", err)
	}
	if !ok {
		t.Fatal("expected approval")
	}
	if approval.Definition.Sequence != 2 || approval.PackageID != "pkg2" {
		t.Fatalf("approval = sequence %d package %s, want sequence 2 package pkg2", approval.Definition.Sequence, approval.PackageID)
	}
}

func TestCommitDefinitionResolvesLocalPackage(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	installTestPackage(t, ctx, store, "pkg1", "127.0.0.1:9999")
	def := testDefinition()
	if _, err := store.ApproveForMyOrg(ctx, def, "pkg1", "org-0"); err != nil {
		t.Fatalf("ApproveForMyOrg failed: %v", err)
	}
	if _, err := store.CommitDefinition(ctx, def, "org-0"); err != nil {
		t.Fatalf("CommitDefinition failed: %v", err)
	}
	if _, err := store.CommitDefinition(ctx, def, "org-0"); err == nil || !strings.Contains(err.Error(), "next sequence") {
		t.Fatalf("expected duplicate commit sequence error, got %v", err)
	}

	committed, pkg, ok, err := store.ResolveCommittedPackage(ctx, "", "", "org-0")
	if err != nil {
		t.Fatalf("ResolveCommittedPackage failed: %v", err)
	}
	if !ok {
		t.Fatal("expected committed definition")
	}
	if committed.Definition.Name != "sample" {
		t.Fatalf("name = %q, want sample", committed.Definition.Name)
	}
	if !committed.Initialized {
		t.Fatal("non-init-required definition should be initialized at commit")
	}
	if pkg.Address != "127.0.0.1:9999" {
		t.Fatalf("address = %q, want 127.0.0.1:9999", pkg.Address)
	}
}

func TestMarkInitialized(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	installTestPackage(t, ctx, store, "pkg1", "127.0.0.1:9999")
	def := testDefinition()
	def.InitRequired = true
	if _, err := store.ApproveForMyOrg(ctx, def, "pkg1", "org-0"); err != nil {
		t.Fatalf("ApproveForMyOrg failed: %v", err)
	}
	committed, err := store.CommitDefinition(ctx, def, "org-0")
	if err != nil {
		t.Fatalf("CommitDefinition failed: %v", err)
	}
	if committed.Initialized {
		t.Fatal("init-required definition should not be initialized at commit")
	}
	committed, err = store.MarkInitialized(ctx, def, "org-0")
	if err != nil {
		t.Fatalf("MarkInitialized failed: %v", err)
	}
	if !committed.Initialized {
		t.Fatal("expected initialized definition")
	}
	committed, err = store.MarkInitialized(ctx, def, "org-0")
	if err != nil {
		t.Fatalf("duplicate MarkInitialized failed: %v", err)
	}
	if !committed.Initialized {
		t.Fatal("duplicate mark should keep definition initialized")
	}
}

func TestUpsertCommittedFromLedgerDoesNotRequireApproval(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	def := testDefinition()
	def.InitRequired = true

	committed, err := store.UpsertCommittedFromLedger(ctx, def, false)
	if err != nil {
		t.Fatalf("UpsertCommittedFromLedger failed: %v", err)
	}
	if committed.Initialized {
		t.Fatal("expected hydrated init-required definition to remain uninitialized")
	}

	committed, err = store.MarkInitialized(ctx, def, "org-0")
	if err != nil {
		t.Fatalf("MarkInitialized without approval failed: %v", err)
	}
	if !committed.Initialized {
		t.Fatal("expected definition to be initialized")
	}
}

func TestResolvedConnectionCache(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	def := testDefinition()

	_, ok, err := store.GetResolvedConnection(ctx, "org-0", def)
	if err != nil {
		t.Fatalf("GetResolvedConnection failed: %v", err)
	}
	if ok {
		t.Fatal("expected empty cache")
	}

	cached, err := store.PutResolvedConnection(ctx, ResolvedChaincodeConnection{
		MSPID:    "org-0",
		Name:     def.Name,
		Version:  def.Version,
		Sequence: def.Sequence,
		Address:  "127.0.0.1:9999",
	})
	if err != nil {
		t.Fatalf("PutResolvedConnection failed: %v", err)
	}
	if cached.TLSMode != "none" {
		t.Fatalf("TLSMode = %q, want none", cached.TLSMode)
	}

	got, ok, err := store.GetResolvedConnection(ctx, "org-0", def)
	if err != nil {
		t.Fatalf("GetResolvedConnection cached failed: %v", err)
	}
	if !ok || got.Address != "127.0.0.1:9999" {
		t.Fatalf("cached connection = %#v ok=%t", got, ok)
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := NewMemoryStore()
	if err != nil {
		t.Fatalf("NewMemoryStore failed: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func installTestPackage(t *testing.T, ctx context.Context, store *Store, packageID, address string) {
	t.Helper()
	if err := store.Install(ctx, InstalledPackage{
		PackageID:      packageID,
		Label:          packageID,
		Type:           "external",
		Address:        address,
		MetadataJSON:   []byte("{}"),
		ConnectionJSON: []byte("{}"),
		PackageTGZ:     []byte("package"),
		InstalledByMSP: "org-0",
	}); err != nil {
		t.Fatalf("Install failed: %v", err)
	}
}

func testDefinition() ChaincodeDefinition {
	return ChaincodeDefinition{
		Name:     "sample",
		Version:  "1.0",
		Sequence: 1,
	}
}
