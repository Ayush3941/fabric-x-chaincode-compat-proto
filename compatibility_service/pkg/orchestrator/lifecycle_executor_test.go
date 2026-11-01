// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"testing"
	"time"

	"compatibility_service/pkg/lifecycle"

	sdk "github.com/hyperledger/fabric-x-sdk"
	efab "github.com/hyperledger/fabric-x-sdk/endorsement/fabric"
	"google.golang.org/protobuf/proto"
)

func TestInvocationSelectsActiveDefinitionByName(t *testing.T) {
	store, err := lifecycle.NewMemoryStore("channelqc4")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	def := lifecycle.ChaincodeDefinition{Name: "sample", Version: "2.0", Sequence: 2}
	if _, err := store.UpsertCommittedFromLedger(t.Context(), def, true); err != nil {
		t.Fatal(err)
	}
	service := &Service{lifecycle: store}
	for _, version := range []string{"", "1.0", "2.0"} {
		request, err := service.selectLifecycleDefinition(t.Context(), InvocationRequest{ChaincodeName: def.Name, ChaincodeVersion: version})
		if err != nil || request.definition == nil || request.definition.Definition != def || request.ChaincodeVersion != "2.0" {
			t.Fatalf("active definition lookup failed for proposal version %q: %+v %v", version, request, err)
		}
	}
	// Normalizing routing must never modify what the original client signed.
	org := newProposalTestMSP(t, "org-0")
	actor := org.issue(t, "client", time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	signed := proposalTestSigned(t, actor.signer, "")
	original := proto.Clone(signed)
	inv, err := efab.Parse(signed, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	request, err := requestFromProposal(inv)
	if err != nil {
		t.Fatal(err)
	}
	request.ChaincodeName = def.Name
	request.ClientSignedProposal = signed
	if _, err := service.selectLifecycleDefinition(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(signed, original) {
		t.Fatal("active definition routing rewrote the signed client proposal")
	}
}

func TestLifecycleEndpointCacheFollowsActiveSequence(t *testing.T) {
	store, err := lifecycle.NewMemoryStore("channelqc4")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	first := lifecycle.ChaincodeDefinition{Name: "sample", Version: "1.0", Sequence: 1}
	second := lifecycle.ChaincodeDefinition{Name: "sample", Version: "2.0", Sequence: 2}
	for _, pkg := range []lifecycle.InstalledPackage{
		{PackageID: "pkg1", Address: "127.0.0.1:9999"},
		{PackageID: "pkg2", Address: "127.0.0.1:10000"},
	} {
		pkg.MetadataJSON, pkg.ConnectionJSON, pkg.PackageTGZ = []byte("{}"), []byte("{}"), []byte("package")
		if err := store.Install(t.Context(), pkg); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.ApproveForMyOrg(t.Context(), first, "pkg1", "org-0"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitDefinition(t.Context(), first, "org-0"); err != nil {
		t.Fatal(err)
	}
	executor := newLifecycleExecutor(store, "org-0", nil, nil, sdk.NoOpLogger{})
	if _, ok, err := executor.resolveChaincodeConnection(t.Context(), first); err != nil || !ok {
		t.Fatalf("initial resolution failed: %v", err)
	}
	if _, err := store.ApproveForMyOrg(t.Context(), second, "pkg2", "org-0"); err != nil {
		t.Fatal(err)
	}
	conn, _, err := executor.resolveChaincodeConnection(t.Context(), first)
	if err != nil || conn.PackageID != "pkg1" {
		t.Fatalf("pending upgrade replaced active endpoint: %+v %v", conn, err)
	}
	if _, err := store.CommitDefinition(t.Context(), second, "org-0"); err != nil {
		t.Fatal(err)
	}
	conn, _, err = executor.resolveChaincodeConnection(t.Context(), second)
	if err != nil || conn.PackageID != "pkg2" || conn.Address != "127.0.0.1:10000" {
		t.Fatalf("upgraded revision reused stale endpoint: %+v %v", conn, err)
	}
}
