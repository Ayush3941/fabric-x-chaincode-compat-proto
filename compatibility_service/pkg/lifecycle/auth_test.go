// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"path/filepath"
	"testing"
	"time"

	"compatibility_service/internal/testutil"
	"compatibility_service/pkg/auth"
	mspapi "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type lifecycleAuthFixture struct {
	orgs                   [2]*testutil.MSP
	admins, clients, nodes [2]testutil.Actor
	trust                  *auth.Verifier
	config                 AuthConfig
	authorizer             *Authorizer
}

func newLifecycleAuthFixture(t *testing.T) *lifecycleAuthFixture {
	t.Helper()
	f := &lifecycleAuthFixture{}
	now := time.Now()
	for org, name := range []string{"org-0", "org-1"} {
		f.orgs[org] = testutil.NewMSP(t, name)
		f.admins[org] = f.orgs[org].Issue(t, "admin", now.Add(-time.Hour), now.Add(time.Hour))
		f.clients[org] = f.orgs[org].Issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
		f.nodes[org] = f.orgs[org].Issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	}
	var err error
	f.trust, err = auth.NewVerifier([]auth.MSPConfig{f.orgs[0].Config, f.orgs[1].Config})
	if err != nil {
		t.Fatal(err)
	}
	f.config = AuthConfig{Policies: map[string]string{}, Orchestrators: []OrchestratorIdentity{
		{MSPID: "org-0", CertPath: f.nodes[0].CertPath}, {MSPID: "org-1", CertPath: f.nodes[1].CertPath},
	}}
	for _, operation := range publicOperations {
		f.config.Policies[operation] = "OR('org-0.member','org-1.member')"
	}
	for _, operation := range []string{OperationInstall, OperationQueryInstalled, OperationApprove} {
		f.config.Policies[operation] = "OR('org-0.admin')"
	}
	f.authorizer, err = NewAuthorizer(f.trust, "channelqc4", "org-0", f.config)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func signedLifecycleTestRequest(t *testing.T, signer sdk.Signer, payload any, internal bool) *SignedRequest {
	t.Helper()
	req, err := signRequest(signer, "channelqc4", payload, internal)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func resignLifecycleTestRequest(t *testing.T, signer sdk.Signer, req *SignedRequest) {
	t.Helper()
	message, err := req.signingBytes()
	if err != nil {
		t.Fatal(err)
	}
	req.Signature, err = signer.Sign(message)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLifecycleTrustAndSignedContext(t *testing.T) {
	f := newLifecycleAuthFixture(t)
	rogue := testutil.NewMSP(t, "org-0").Issue(t, "client", time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	expired := f.orgs[0].Issue(t, "client", time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour))
	future := f.orgs[0].Issue(t, "client", time.Now().Add(time.Hour), time.Now().Add(2*time.Hour))
	for _, tc := range []struct {
		name   string
		signer sdk.Signer
		mutate func(*SignedRequest)
		resign bool
		want   codes.Code
	}{
		{name: "valid org0 client", signer: f.clients[0].Signer},
		{name: "valid org1 client", signer: f.clients[1].Signer},
		{name: "untrusted issuer claims org0", signer: rogue.Signer, want: codes.Unauthenticated},
		{name: "expired", signer: expired.Signer, want: codes.Unauthenticated},
		{name: "not yet valid", signer: future.Signer, want: codes.Unauthenticated},
		{name: "missing signature", mutate: func(r *SignedRequest) { r.Signature = nil }, want: codes.Unauthenticated},
		{name: "changed payload", mutate: func(r *SignedRequest) { r.Payload = []byte("{} ") }, want: codes.Unauthenticated},
		{name: "changed operation", mutate: func(r *SignedRequest) { r.Operation = OperationCommit }, want: codes.Unauthenticated},
		{name: "changed channel", mutate: func(r *SignedRequest) { r.ChannelID = "other" }, want: codes.Unauthenticated},
		{name: "changed internal flag", mutate: func(r *SignedRequest) { r.Internal = true }, want: codes.Unauthenticated},
		{name: "changed nonce", mutate: func(r *SignedRequest) { r.Nonce[0] ^= 1 }, want: codes.Unauthenticated},
		{name: "changed timestamp", mutate: func(r *SignedRequest) { r.IssuedAt++ }, want: codes.Unauthenticated},
		{name: "changed creator", mutate: func(r *SignedRequest) { r.Creator, _ = f.clients[1].Signer.Serialize() }, want: codes.Unauthenticated},
		{name: "signed wrong channel", mutate: func(r *SignedRequest) { r.ChannelID = "other" }, resign: true, want: codes.PermissionDenied},
		{name: "signed expired request", mutate: func(r *SignedRequest) { r.IssuedAt = time.Now().Add(-10 * time.Minute).UnixNano() }, resign: true, want: codes.Unauthenticated},
		{name: "signed future request", mutate: func(r *SignedRequest) { r.IssuedAt = time.Now().Add(10 * time.Minute).UnixNano() }, resign: true, want: codes.Unauthenticated},
		{name: "unknown MSP", mutate: func(r *SignedRequest) { setLifecycleTestMSP(t, r, "unknown") }, resign: true, want: codes.Unauthenticated},
		{name: "wrong claimed MSP", signer: f.clients[1].Signer, mutate: func(r *SignedRequest) { setLifecycleTestMSP(t, r, "org-0") }, resign: true, want: codes.Unauthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.signer == nil {
				tc.signer = f.clients[0].Signer
			}
			req := signedLifecycleTestRequest(t, tc.signer, QueryCommittedRequest{}, false)
			if tc.mutate != nil {
				tc.mutate(req)
			}
			if tc.resign {
				resignLifecycleTestRequest(t, tc.signer, req)
			}
			_, err := f.authorizer.check(t.Context(), req, OperationQueryCommitted)
			if status.Code(err) != tc.want {
				t.Fatalf("error=%v want=%s", err, tc.want)
			}
		})
	}
	// Definitions have identical JSON shapes in these methods. Their signatures
	// must still be unusable across readiness, commitment, and initialization.
	req := signedLifecycleTestRequest(t, f.admins[0].Signer, CheckCommitReadinessRequest{Definition: ChaincodeDefinition{Name: "sample", Version: "1.0", Sequence: 1}}, false)
	for _, operation := range []string{OperationCommit, OperationMarkInitialized} {
		if _, err := f.authorizer.check(t.Context(), req, operation); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("readiness signature reused for %s: %v", operation, err)
		}
	}
	legacy := &SignedRequest{Creator: req.Creator, Payload: req.Payload}
	legacy.Signature, _ = f.admins[0].Signer.Sign(legacy.Payload)
	if _, err := f.authorizer.check(t.Context(), legacy, OperationReadiness); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("legacy payload-only signature accepted: %v", err)
	}
}

func setLifecycleTestMSP(t *testing.T, req *SignedRequest, mspID string) {
	t.Helper()
	creator := &mspapi.SerializedIdentity{}
	if err := proto.Unmarshal(req.Creator, creator); err != nil {
		t.Fatal(err)
	}
	creator.Mspid = mspID
	var err error
	req.Creator, err = proto.Marshal(creator)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLifecycleOperationPermissionsAndInternalIdentity(t *testing.T) {
	f := newLifecycleAuthFixture(t)
	operations := []struct {
		name       string
		payload    any
		localAdmin bool
	}{
		{OperationInstall, InstallRequest{}, true}, {OperationQueryInstalled, QueryInstalledRequest{}, true},
		{OperationApprove, ApproveForMyOrgRequest{}, true}, {OperationReadiness, CheckCommitReadinessRequest{}, false},
		{OperationCommit, CommitRequest{}, false}, {OperationQueryCommitted, QueryCommittedRequest{}, false},
	}
	actors := []struct {
		name       string
		actor      testutil.Actor
		localAdmin bool
	}{{"admin0", f.admins[0], true}, {"admin1", f.admins[1], false}, {"client0", f.clients[0], false}, {"client1", f.clients[1], false}, {"node0", f.nodes[0], false}}
	for _, operation := range operations {
		for _, actor := range actors {
			t.Run(operation.name+"/"+actor.name, func(t *testing.T) {
				req := signedLifecycleTestRequest(t, actor.actor.Signer, operation.payload, false)
				_, err := f.authorizer.check(t.Context(), req, operation.name)
				allowed := !operation.localAdmin || actor.localAdmin
				if (err == nil) != allowed {
					t.Fatalf("authorization error=%v allowed=%t", err, allowed)
				}
			})
		}
	}
	now := time.Now()
	unlisted := f.orgs[0].Issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	for _, payload := range []any{CheckCommitReadinessRequest{}, CommitRequest{}, MarkInitializedRequest{}} {
		operation, _ := operationForPayload(payload)
		for _, actor := range []testutil.Actor{f.nodes[0], f.nodes[1], f.admins[0], f.clients[0], unlisted} {
			req := signedLifecycleTestRequest(t, actor.Signer, payload, true)
			_, err := f.authorizer.check(t.Context(), req, operation)
			allowed := actor.CertPath == f.nodes[0].CertPath || actor.CertPath == f.nodes[1].CertPath
			if (err == nil) != allowed {
				t.Fatalf("internal %s actor=%s error=%v allowed=%t", operation, actor.CertPath, err, allowed)
			}
		}
	}
	public := signedLifecycleTestRequest(t, f.clients[0].Signer, CommitRequest{}, false)
	for _, value := range []string{"true", "garbage"} {
		ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(localOnlyMetadata, value))
		if _, err := f.authorizer.check(ctx, public, OperationCommit); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("metadata bypass %q accepted: %v", value, err)
		}
	}
	if _, err := SignRequest(f.admins[0].Signer, "channelqc4", MarkInitializedRequest{}); err == nil {
		t.Fatal("public signing of internal-only update accepted")
	}
}

func TestLifecycleAuthorizationConfigurationFailsClosed(t *testing.T) {
	f := newLifecycleAuthFixture(t)
	for _, mutate := range []func(*AuthConfig){
		func(c *AuthConfig) { delete(c.Policies, OperationInstall) },
		func(c *AuthConfig) { c.Policies["unknown"] = "OR('org-0.member')" },
		func(c *AuthConfig) { c.Policies[OperationCommit] = "broken" },
		func(c *AuthConfig) { c.Policies[OperationCommit] = "OR('unknown.member')" },
		func(c *AuthConfig) { c.Orchestrators = nil },
		func(c *AuthConfig) { c.Orchestrators[0].MSPID = "org-1" },
		func(c *AuthConfig) { c.Orchestrators[0].CertPath = filepath.Join(t.TempDir(), "missing") },
		func(c *AuthConfig) { c.Orchestrators = append(c.Orchestrators, c.Orchestrators[0]) },
	} {
		cfg := AuthConfig{Policies: map[string]string{}, Orchestrators: append([]OrchestratorIdentity(nil), f.config.Orchestrators...)}
		for key, value := range f.config.Policies {
			cfg.Policies[key] = value
		}
		mutate(&cfg)
		if _, err := NewAuthorizer(f.trust, "channelqc4", "org-0", cfg); err == nil {
			t.Fatal("invalid lifecycle configuration accepted")
		}
	}
	creator, _ := f.nodes[0].Signer.Serialize()
	if err := f.authorizer.ValidateOrchestrator(creator); err != nil {
		t.Fatal(err)
	}
	creator, _ = f.clients[0].Signer.Serialize()
	if err := f.authorizer.ValidateOrchestrator(creator); err == nil {
		t.Fatal("ordinary client accepted as local Orchestrator signer")
	}
	var missing *Authorizer
	if _, err := missing.check(context.Background(), nil, OperationInstall); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing authorization did not fail closed: %v", err)
	}
}

func TestLifecycleRevokedIdentityRejected(t *testing.T) {
	f := newLifecycleAuthFixture(t)
	now := time.Now()
	crl, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{Number: big.NewInt(1), ThisUpdate: now.Add(-time.Minute), NextUpdate: now.Add(time.Hour), RevokedCertificateEntries: []x509.RevocationListEntry{{SerialNumber: f.clients[0].Certificate.SerialNumber, RevocationTime: now.Add(-time.Minute)}}}, f.orgs[0].CA, f.orgs[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	testutil.WriteFile(t, filepath.Join(f.orgs[0].Config.MSPDir, "crls", "revoked.pem"), pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crl}))
	trust, err := auth.NewVerifier([]auth.MSPConfig{f.orgs[0].Config, f.orgs[1].Config})
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := NewAuthorizer(trust, "channelqc4", "org-0", f.config)
	if err != nil {
		t.Fatal(err)
	}
	req := signedLifecycleTestRequest(t, f.clients[0].Signer, QueryCommittedRequest{}, false)
	if _, err := authorizer.check(t.Context(), req, OperationQueryCommitted); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("revoked identity accepted: %v", err)
	}
}
