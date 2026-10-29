// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	sdk "github.com/hyperledger/fabric-x-sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLifecycleRPCRejectsBeforeStateAccess(t *testing.T) {
	f := newLifecycleAuthFixture(t)
	// Intentionally omit the store, ledger, and remote dependencies. Admission
	// failures must return before any operation can touch those dependencies.
	server := &Server{auth: f.authorizer, mspID: "org-0"}
	operations := []struct {
		payload any
		call    func(context.Context, *SignedRequest) error
	}{
		{InstallRequest{}, func(ctx context.Context, r *SignedRequest) error { _, err := server.Install(ctx, r); return err }},
		{QueryInstalledRequest{}, func(ctx context.Context, r *SignedRequest) error { _, err := server.QueryInstalled(ctx, r); return err }},
		{ApproveForMyOrgRequest{}, func(ctx context.Context, r *SignedRequest) error {
			_, err := server.ApproveForMyOrg(ctx, r)
			return err
		}},
		{CheckCommitReadinessRequest{}, func(ctx context.Context, r *SignedRequest) error {
			_, err := server.CheckCommitReadiness(ctx, r)
			return err
		}},
		{CommitRequest{}, func(ctx context.Context, r *SignedRequest) error { _, err := server.Commit(ctx, r); return err }},
		{QueryCommittedRequest{}, func(ctx context.Context, r *SignedRequest) error { _, err := server.QueryCommitted(ctx, r); return err }},
		{MarkInitializedRequest{}, func(ctx context.Context, r *SignedRequest) error {
			_, err := server.MarkInitialized(ctx, r)
			return err
		}},
	}
	for _, operation := range operations {
		name, _ := operationForPayload(operation.payload)
		t.Run(name, func(t *testing.T) {
			req := signedLifecycleTestRequest(t, f.nodes[0].Signer, operation.payload, name == OperationMarkInitialized)
			req.Signature = nil
			if err := operation.call(t.Context(), req); status.Code(err) != codes.Unauthenticated {
				t.Fatalf("bad signature was not rejected: %v", err)
			}
			if name == OperationInstall || name == OperationQueryInstalled || name == OperationApprove {
				req = signedLifecycleTestRequest(t, f.clients[0].Signer, operation.payload, false)
				if err := operation.call(t.Context(), req); status.Code(err) != codes.PermissionDenied {
					t.Fatalf("ordinary client reached administrative state: %v", err)
				}
			}
		})
	}
}

type lifecycleTestLedger struct {
	entries        map[string]LedgerDefinition
	submits, reads int
	readErr        error
}

func (l *lifecycleTestLedger) CommitLifecycleDefinition(_ context.Context, def ChaincodeDefinition) (LedgerCommitResult, error) {
	l.submits++
	l.put(def, !def.InitRequired)
	return LedgerCommitResult{TxID: "lifecycle-test", Status: "COMMITTED"}, nil
}
func (l *lifecycleTestLedger) ReadLifecycleDefinition(_ context.Context, def ChaincodeDefinition) (LedgerDefinition, bool, error) {
	l.reads++
	v, ok := l.entries[LedgerKey(def)]
	return v, ok, l.readErr
}
func (l *lifecycleTestLedger) put(def ChaincodeDefinition, initialized bool) {
	if l.entries == nil {
		l.entries = map[string]LedgerDefinition{}
	}
	l.entries[LedgerKey(def)] = LedgerDefinition{CCID: DefinitionCCID(def), Name: def.Name, Version: def.Version, Sequence: def.Sequence, InitRequired: def.InitRequired, Initialized: initialized}
}

func lifecycleTestServer(t *testing.T, f *lifecycleAuthFixture, org int, ledger LedgerBackend, remotes ...RemotePeer) *Server {
	t.Helper()
	store, err := NewMemoryStore()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	cfg := AuthConfig{Policies: map[string]string{}, Orchestrators: f.config.Orchestrators}
	for key, value := range f.config.Policies {
		cfg.Policies[key] = value
	}
	localMSP := f.orgs[org].Config.MSPID
	for _, operation := range []string{OperationInstall, OperationQueryInstalled, OperationApprove} {
		cfg.Policies[operation] = "OR('" + localMSP + ".admin')"
	}
	authorizer, err := NewAuthorizer(f.trust, "channelqc4", localMSP, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return NewServer(store, ServerConfig{ChannelID: "channelqc4", MSPID: localMSP, Auth: authorizer, Signer: f.nodes[org].Signer}, sdk.NoOpLogger{}, ledger, remotes...)
}

func TestInternalLifecycleUpdatesRequireCommittedLedgerState(t *testing.T) {
	f := newLifecycleAuthFixture(t)
	ledger := &lifecycleTestLedger{}
	server := lifecycleTestServer(t, f, 0, ledger)
	def := ChaincodeDefinition{Name: "sample", Version: "1.0", Sequence: 1, InitRequired: true}
	commit := signedLifecycleTestRequest(t, f.nodes[1].Signer, CommitRequest{Definition: def}, true)
	if _, err := server.Commit(t.Context(), commit); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("uncommitted definition applied: %v", err)
	}
	if _, ok, _ := server.store.GetCommitted(t.Context(), def.Name, def.Version); ok {
		t.Fatal("uncommitted definition changed local state")
	}
	ledger.put(def, false)
	if _, err := server.Commit(t.Context(), commit); err != nil {
		t.Fatal(err)
	}
	init := signedLifecycleTestRequest(t, f.nodes[1].Signer, MarkInitializedRequest{Definition: def}, true)
	if _, err := server.MarkInitialized(t.Context(), init); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("uncommitted initialization applied: %v", err)
	}
	committed, _, _ := server.store.GetCommitted(t.Context(), def.Name, def.Version)
	if committed.Initialized {
		t.Fatal("unconfirmed initialization changed local state")
	}
	wrongDef := def
	wrongDef.Sequence++
	if _, err := server.Commit(t.Context(), signedLifecycleTestRequest(t, f.nodes[1].Signer, CommitRequest{Definition: wrongDef}, true)); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("wrong ledger definition applied: %v", err)
	}
	ledger.readErr = errors.New("query unavailable")
	if _, err := server.MarkInitialized(t.Context(), init); status.Code(err) != codes.Unavailable {
		t.Fatalf("ledger read failure allowed initialization: %v", err)
	}
	ledger.readErr = nil
	ledger.put(def, true)
	if _, err := server.MarkInitialized(t.Context(), init); err != nil {
		t.Fatal(err)
	}
	committed, _, _ = server.store.GetCommitted(t.Context(), def.Name, def.Version)
	if !committed.Initialized || ledger.submits != 0 {
		t.Fatalf("internal update must only mirror confirmed ledger state: %+v submits=%d", committed, ledger.submits)
	}
}

type recordingLifecycleRemote struct {
	server   *Server
	requests []*SignedRequest
}

func (r *recordingLifecycleRemote) MSPID() string { return "org-1" }
func (r *recordingLifecycleRemote) CheckCommitReadiness(ctx context.Context, req *SignedRequest) (*CheckCommitReadinessResponse, error) {
	r.requests = append(r.requests, req)
	return r.server.CheckCommitReadiness(ctx, req)
}
func (r *recordingLifecycleRemote) Commit(ctx context.Context, req *SignedRequest) (*CommitResponse, error) {
	r.requests = append(r.requests, req)
	return r.server.Commit(ctx, req)
}
func (r *recordingLifecycleRemote) MarkInitialized(ctx context.Context, req *SignedRequest) (*MarkInitializedResponse, error) {
	r.requests = append(r.requests, req)
	return r.server.MarkInitialized(ctx, req)
}

func TestLifecycleAdministrativeWorkflowAndSignedCoordination(t *testing.T) {
	f := newLifecycleAuthFixture(t)
	ledger := &lifecycleTestLedger{}
	remote := &recordingLifecycleRemote{server: lifecycleTestServer(t, f, 1, ledger)}
	server := lifecycleTestServer(t, f, 0, ledger, remote)
	def := ChaincodeDefinition{Name: "sample", Version: "1.0", Sequence: 1}
	source := t.TempDir()
	writeTestFile(t, filepath.Join(source, "connection.json"), "{\"address\":\"127.0.0.1:9999\",\"tls_required\":false}")
	pkg, err := PackageExternalChaincode(PackageOptions{SourceDir: source, OutputPath: filepath.Join(t.TempDir(), "sample.tgz"), Label: "sample"})
	if err != nil {
		t.Fatal(err)
	}
	archive, err := os.ReadFile(pkg.Output)
	if err != nil {
		t.Fatal(err)
	}
	for org, node := range []*Server{server, remote.server} {
		installed, err := node.Install(t.Context(), signedLifecycleTestRequest(t, f.admins[org].Signer, InstallRequest{Package: archive}, false))
		if err != nil || installed.Package.PackageID != pkg.PackageID {
			t.Fatalf("admin install failed: %v %v", installed, err)
		}
		_, err = node.ApproveForMyOrg(t.Context(), signedLifecycleTestRequest(t, f.admins[org].Signer, ApproveForMyOrgRequest{Definition: def, PackageID: pkg.PackageID}, false))
		if err != nil {
			t.Fatal(err)
		}
	}
	readiness, err := server.CheckCommitReadiness(t.Context(), signedLifecycleTestRequest(t, f.clients[0].Signer, CheckCommitReadinessRequest{Definition: def}, false))
	if err != nil || !readiness.Approvals["org-0"] || !readiness.Approvals["org-1"] {
		t.Fatalf("readiness failed: %v %v", readiness, err)
	}
	// Writers may coordinate commitment after administrators have approved.
	result, err := server.Commit(t.Context(), signedLifecycleTestRequest(t, f.clients[0].Signer, CommitRequest{Definition: def}, false))
	if err != nil || result.Ledger.Status != "COMMITTED" || ledger.submits != 1 {
		t.Fatalf("commit failed: %v %v submits=%d", result, err, ledger.submits)
	}
	for _, node := range []*Server{server, remote.server} {
		queried, err := node.QueryCommitted(t.Context(), signedLifecycleTestRequest(t, f.clients[1].Signer, QueryCommittedRequest{Name: def.Name, Version: def.Version}, false))
		if err != nil || len(queried.Definitions) != 1 || !queried.Definitions[0].Initialized {
			t.Fatalf("committed definition missing: %v %v", queried, err)
		}
	}
	creator, _ := f.nodes[0].Signer.Serialize()
	if len(remote.requests) < 3 {
		t.Fatalf("missing coordination requests: %d", len(remote.requests))
	}
	for _, req := range remote.requests {
		if !req.Internal || !bytes.Equal(req.Creator, creator) {
			t.Fatal("original public request was reused for internal coordination")
		}
	}
}
