// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"compatibility_service/internal/testutil"
	"compatibility_service/pkg/lifecycle"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"github.com/hyperledger/fabric-x-sdk/network"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestLifecycleMSPAuthorization(t *testing.T) {
	h := newHarness(t, harnessOptions{name: "lifecycle-auth"})
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	before := h.blockHeight(t)
	var admins, clients, nodes [2]identity.Signer
	for org := range 2 {
		var err error
		admins[org], err = identity.SignerFromMSP(h.artifact("peerOrganizations", fmt.Sprintf("peer-org-%d", org), "users", fmt.Sprintf("Admin@peer-org-%d", org), "msp"), fmt.Sprintf("org-%d", org))
		if err != nil {
			t.Fatal(err)
		}
		clients[org], err = identity.SignerFromMSP(h.clientMSP(org), fmt.Sprintf("org-%d", org))
		if err != nil {
			t.Fatal(err)
		}
		nodes[org], err = identity.SignerFromMSP(h.orchestratorMSP(org), fmt.Sprintf("org-%d", org))
		if err != nil {
			t.Fatal(err)
		}
	}
	rogue := testutil.NewMSP(t, "org-0").Issue(t, "admin", time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	definition := lifecycle.ChaincodeDefinition{Name: fmt.Sprintf("auth_uncommitted_%d", time.Now().UnixNano()), Version: "1.0", Sequence: 1, InitRequired: true}
	sign := func(signer sdk.Signer, payload any, internal bool) *lifecycle.SignedRequest {
		t.Helper()
		var req *lifecycle.SignedRequest
		var err error
		if internal {
			req, err = lifecycle.SignInternalRequest(signer, channelID, payload)
		} else {
			req, err = lifecycle.SignRequest(signer, channelID, payload)
		}
		if err != nil {
			t.Fatal(err)
		}
		return req
	}
	for org, port := range []int{h.ports.org0Orch, h.ports.org1Orch} {
		// The transport remains an ordinary org0 client even for requests signed
		// as an administrator or another organization. MSP admission is separate.
		connection, err := network.NewPeer(network.PeerConf{Address: fmt.Sprintf("127.0.0.1:%d", port), TLS: network.TLSConfig{
			Mode: network.TLSModeMTLS, CertPath: h.clientTLSCert(0), KeyPath: h.clientTLSKey(0), CACertPaths: []string{h.peerTLSCA(org)}, ServerName: "127.0.0.1",
		}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = connection.Close() })
		client := lifecycle.NewClient(connection.Connection(), channelID)
		if _, err := client.QueryInstalled(ctx, sign(admins[org], lifecycle.QueryInstalledRequest{}, false)); err != nil {
			t.Fatalf("org%d admin query failed: %v", org, err)
		}
		for _, tc := range []struct {
			name string
			req  *lifecycle.SignedRequest
			want codes.Code
		}{
			{"ordinary client", sign(clients[org], lifecycle.QueryInstalledRequest{}, false), codes.PermissionDenied},
			{"other organization admin", sign(admins[1-org], lifecycle.QueryInstalledRequest{}, false), codes.PermissionDenied},
			{"untrusted issuer", sign(rogue.Signer, lifecycle.QueryInstalledRequest{}, false), codes.Unauthenticated},
		} {
			t.Run(fmt.Sprintf("org%d/%s", org, tc.name), func(t *testing.T) {
				if _, err := client.QueryInstalled(ctx, tc.req); status.Code(err) != tc.want {
					t.Fatalf("error=%v want=%s", err, tc.want)
				}
			})
		}
		if _, err := client.Install(ctx, sign(clients[org], lifecycle.InstallRequest{}, false)); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("client installation reached package processing: %v", err)
		}
		if _, err := client.ApproveForMyOrg(ctx, sign(clients[org], lifecycle.ApproveForMyOrgRequest{Definition: definition}, false)); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("client approval accepted: %v", err)
		}
		readiness := sign(clients[org], lifecycle.CheckCommitReadinessRequest{Definition: definition}, false)
		if _, err := client.CheckCommitReadiness(ctx, readiness); err != nil {
			t.Fatalf("writer could not query readiness: %v", err)
		}
		if _, err := client.Commit(ctx, readiness); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("readiness signature reused for commit: %v", err)
		}
		if _, err := client.MarkInitialized(ctx, readiness); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("readiness signature reused for initialization: %v", err)
		}
		commit := sign(clients[org], lifecycle.CommitRequest{Definition: definition}, false)
		forged := metadata.AppendToOutgoingContext(ctx, "x-compat-lifecycle-local-only", "true")
		if _, err := client.Commit(forged, commit); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("unsigned local-only bypass accepted: %v", err)
		}
		if _, err := client.Commit(ctx, sign(clients[org], lifecycle.CommitRequest{Definition: definition}, true)); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("ordinary client impersonated internal coordinator: %v", err)
		}
		if _, err := client.MarkInitialized(ctx, sign(admins[org], lifecycle.MarkInitializedRequest{Definition: definition}, true)); status.Code(err) != codes.PermissionDenied {
			t.Fatalf("admin impersonated internal coordinator: %v", err)
		}
		if _, err := client.Commit(ctx, sign(nodes[1-org], lifecycle.CommitRequest{Definition: definition}, true)); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("uncommitted internal definition activated: %v", err)
		}
		if _, err := client.MarkInitialized(ctx, sign(nodes[1-org], lifecycle.MarkInitializedRequest{Definition: definition}, true)); status.Code(err) != codes.FailedPrecondition {
			t.Fatalf("uncommitted initialization activated: %v", err)
		}
		altered := sign(admins[org], lifecycle.QueryInstalledRequest{}, false)
		altered.Internal = true
		if _, err := client.QueryInstalled(ctx, altered); status.Code(err) != codes.Unauthenticated {
			t.Fatalf("unsigned mode change accepted: %v", err)
		}
		if _, err := client.QueryCommitted(ctx, sign(clients[1-org], lifecycle.QueryCommittedRequest{}, false)); err != nil {
			t.Fatalf("trusted writer query failed: %v", err)
		}
	}
	// Existing administrator CLI configuration and command syntax still work;
	// the ordinary client configuration is rejected by policy, not by parsing.
	for org, port := range []int{h.ports.org0Orch, h.ports.org1Orch} {
		runInitCommand(t, ctx, h, "./bin/orchestrator", "lifecycle", "queryinstalled", "-c", fmt.Sprintf("sampleconfig/admin-org%d.yaml", org), "--host", fmt.Sprintf("127.0.0.1:%d", port))
	}
	cmd := exec.CommandContext(ctx, "./bin/orchestrator", "lifecycle", "queryinstalled", "-c", "sampleconfig/client.yaml", "--host", fmt.Sprintf("127.0.0.1:%d", h.ports.org0Orch))
	cmd.Dir = h.compatDir
	if output, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(output), "PermissionDenied") {
		t.Fatalf("client CLI did not reach authorization rejection: %v %s", err, output)
	}
	assertHeightUnchanged(t, h, before)
	path := filepath.Join(h.root, "runtime", "compatibility_service", "lifecycle-auth")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	report, _ := json.MarshalIndent(map[string]any{"both_organizations_checked": true, "administrators_allowed": true, "clients_denied_admin_operations": true, "untrusted_enrollment_rejected": true, "signed_operation_enforced": true, "unsigned_local_only_denied": true, "internal_identity_enforced": true, "uncommitted_updates_denied": true, "block_height_before": before, "block_height_after": h.blockHeight(t)}, "", "  ")
	if err := os.WriteFile(filepath.Join(path, "authorization-result.json"), append(report, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}
