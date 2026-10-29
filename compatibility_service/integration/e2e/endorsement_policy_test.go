// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestDistinctSameOrgEndorsementsAndPolicyUpdate(t *testing.T) {
	root := projectRoot(t)
	ensureBinaries(t, root)
	ensureNetworkReady(t, root)
	h := &e2eHarness{
		root: root, compatDir: filepath.Join(root, "compatibility_service"), chaincodeDir: filepath.Join(root, "sample_external_chaincode"),
		ports: nextPorts(), proposalPolicy: "OR('org-0.member')", namespace: fmt.Sprintf("policy_e2e_%d", time.Now().UnixNano()),
	}
	dir := t.TempDir()
	logDir := filepath.Join(root, "runtime", "compatibility_service", "endorsement-policy")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	deployPolicy := namespacePolicyDeployer(t, h, dir)
	deployPolicy("create", "OR('org-0.member')")

	// One primary, one duplicate-certificate instance, and the independent org0B
	// peer signer all execute through the same real org0 CCaaS runtime.
	mainConfig := h.writeOrchestratorConfig(t, t.TempDir(), 0, h.ports.org0Orch, h.ports.org0CC, 0, h.ports.org1Orch, "60s", "25s")
	duplicateConfig := h.writeOrchestratorConfig(t, t.TempDir(), 0, h.ports.org1Orch, h.ports.org0CC, 0, h.ports.org0Orch, "60s", "25s")
	secondaryConfig := h.writeOrchestratorConfig(t, t.TempDir(), 0, h.ports.unusedOrg1, h.ports.org0CC, 0, h.ports.org0Orch, "60s", "25s")
	secondaryNode := h.artifact("peerOrganizations", "peer-org-0", "peers", "orchestrator0b.peer-org-0")
	secondaryBytes, err := os.ReadFile(secondaryConfig)
	if err != nil {
		t.Fatal(err)
	}
	secondaryBytes = []byte(strings.NewReplacer(
		"msp-dir: "+h.orchestratorMSP(0), "msp-dir: "+filepath.Join(secondaryNode, "msp"),
		h.orchestratorTLSCert(0), filepath.Join(secondaryNode, "tls", "server.crt"),
		h.orchestratorTLSKey(0), filepath.Join(secondaryNode, "tls", "server.key"),
	).Replace(string(secondaryBytes)))
	if err := os.WriteFile(secondaryConfig, secondaryBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	mainBytes, err := os.ReadFile(mainConfig)
	if err != nil {
		t.Fatal(err)
	}
	extra := fmt.Sprintf(`
  - msp-id: org-0
    endpoint:
      host: 127.0.0.1
      port: %d
    tls:
      mode: mtls
      cert-path: %s
      key-path: %s
      ca-cert-paths:
        - %s
      server-name: 127.0.0.1
`, h.ports.unusedOrg1, h.orchestratorTLSCert(0), h.orchestratorTLSKey(0), h.peerTLSCA(0))
	if err := os.WriteFile(mainConfig, []byte(strings.Replace(string(mainBytes), "\nproposal-auth:", extra+"\nproposal-auth:", 1)), 0o600); err != nil {
		t.Fatal(err)
	}
	h.startProcess(t, "policy-chaincode", h.chaincodeDir, logDir, nil, "./bin/e2e-chaincode", "-ccid", "0:sample", "-address", fmt.Sprintf("127.0.0.1:%d", h.ports.org0CC))
	waitForPort(t, h.ports.org0CC)
	for _, instance := range []struct {
		name string
		cfg  string
		port int
	}{{"policy-primary", mainConfig, h.ports.org0Orch}, {"policy-duplicate", duplicateConfig, h.ports.org1Orch}, {"policy-org0b", secondaryConfig, h.ports.unusedOrg1}} {
		h.startProcess(t, instance.name, h.compatDir, logDir, nil, "./bin/orchestrator", "-c", instance.cfg, "--log-level", "info:grpc=error")
		waitForPort(t, instance.port)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	baseline, _ := h.newSignedProposal(t, "compatv2", uniqueKey(t, "policy-baseline"), "baseline", uniqueKey(t, "delete"))
	first, err := h.invokeSigned(ctx, baseline)
	if err != nil || first.CommitStatus != "COMMITTED" {
		t.Fatalf("initial namespace policy did not commit: result=%+v error=%v", first, err)
	}

	// A new namespace version requires two distinct certificates, both in org0.
	// Existing Orchestrators must pick up version 1 without a restart.
	deployPolicy("update", "AND('org-0.member','org-0.member')")
	key, value := uniqueKey(t, "same-org-policy"), "two-distinct-org0-signers"
	proposal, _ := h.newSignedProposal(t, "compatv2", key, value, uniqueKey(t, "delete"))
	result, err := h.invokeSigned(ctx, proposal)
	if err != nil || result.CommitStatus != "COMMITTED" {
		t.Fatalf("two same-org signers did not commit after policy update: result=%+v error=%v", result, err)
	}
	t.Logf("distinct same-org endorsements committed: tx_id=%s block=%d namespace=%s version=1", result.TxID, result.BlockNum, h.namespace)
	// Finality notifications can arrive before the block-query index catches up.
	h.waitForHeight(t, result.BlockNum+1)
	conn, err := h.blockQueryConn()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	block, err := committerpb.NewBlockQueryServiceClient(conn).GetBlockByNumber(ctx, &committerpb.BlockNumber{Number: result.BlockNum})
	if err != nil {
		t.Fatal(err)
	}
	if int(result.TxNum) >= len(block.GetData().GetData()) {
		t.Fatalf("transaction index %d is missing from block %d", result.TxNum, result.BlockNum)
	}
	envelope := &common.Envelope{}
	if err := proto.Unmarshal(block.Data.Data[result.TxNum], envelope); err != nil {
		t.Fatal(err)
	}
	payload, err := protoutil.UnmarshalPayload(envelope.Payload)
	if err != nil {
		t.Fatal(err)
	}
	header := &common.ChannelHeader{}
	if err := proto.Unmarshal(payload.GetHeader().GetChannelHeader(), header); err != nil {
		t.Fatal(err)
	}
	if header.TxId != result.TxID {
		t.Fatalf("block contains transaction %s at expected position for %s", header.TxId, result.TxID)
	}
	native := &applicationpb.Tx{}
	if err := proto.Unmarshal(payload.Data, native); err != nil {
		t.Fatal(err)
	}
	if len(native.Namespaces) != 1 || native.Namespaces[0].NsId != h.namespace || native.Namespaces[0].NsVersion != 1 {
		t.Fatalf("transaction used the wrong namespace policy version: %v", native.Namespaces)
	}
	if len(native.Endorsements) != 1 || len(native.Endorsements[0].EndorsementsWithIdentity) != 2 {
		t.Fatalf("expected two distinct endorsements: %v", native.Endorsements)
	}
	seen := make(map[[32]byte]bool)
	expectedPeers := map[string]bool{"orchestrator0.peer-org-0": true, "orchestrator0b.peer-org-0": true}
	var signers []map[string]string
	for _, endorsement := range native.Endorsements[0].EndorsementsWithIdentity {
		if endorsement.Identity.GetMspId() != "org-0" {
			t.Fatalf("unexpected endorser MSP: %s", endorsement.Identity.GetMspId())
		}
		fingerprint := sha256.Sum256(endorsement.Identity.GetCertificate())
		if seen[fingerprint] {
			t.Fatal("duplicate certificate was counted as a second signer")
		}
		seen[fingerprint] = true
		pemBlock, _ := pem.Decode(endorsement.Identity.GetCertificate())
		if pemBlock == nil {
			t.Fatal("endorser certificate is not PEM")
		}
		cert, err := x509.ParseCertificate(pemBlock.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		if !expectedPeers[cert.Subject.CommonName] || len(cert.Subject.OrganizationalUnit) != 1 || cert.Subject.OrganizationalUnit[0] != "peer" {
			t.Fatalf("expected a distinct Orchestrator peer identity, got %s", cert.Subject)
		}
		delete(expectedPeers, cert.Subject.CommonName)
		signers = append(signers, map[string]string{"msp_id": endorsement.Identity.GetMspId(), "common_name": cert.Subject.CommonName, "sha256": fmt.Sprintf("%x", fingerprint)})
	}
	report, err := json.MarshalIndent(map[string]any{"namespace": h.namespace, "namespace_version": 1, "distinct_signers": len(seen), "signers": signers, "result": result}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "result.json"), append(report, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	blockJSON, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(block)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "block.json"), append(blockJSON, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func namespacePolicyDeployer(t *testing.T, h *e2eHarness, dir string) func(string, string) {
	t.Helper()
	binary := filepath.Join(h.root, "bin", "fxconfig")
	if _, err := os.Stat(binary); err != nil {
		t.Fatal("bin/fxconfig is missing; build the Fabric-X tools using the root README")
	}
	configs := make([]string, 2)
	for org := range configs {
		template, err := os.ReadFile(filepath.Join(h.root, "fxconfig", fmt.Sprintf("peer-org-%d.yaml", org)))
		if err != nil {
			t.Fatal(err)
		}
		configs[org] = filepath.Join(dir, fmt.Sprintf("fxconfig-org%d.yaml", org))
		if err := os.WriteFile(configs[org], []byte(strings.ReplaceAll(string(template), "ARTIFACTS_DIR", h.artifact())), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return func(operation, policy string) {
		t.Helper()
		run := func(args ...string) string {
			t.Helper()
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, binary, args...)
			cmd.Dir = h.root
			cmd.Env = append(os.Environ(), "FABRIC_LOGGING_SPEC=error")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("fxconfig %v: %v\n%s", args, err, out)
			}
			return string(out)
		}
		unsigned := filepath.Join(dir, operation+"-unsigned.json")
		args := []string{"namespace", operation, h.namespace, "--policy", policy, "--config", configs[0], "--output", unsigned}
		if operation == "update" {
			args = append(args, "--version", "0")
		}
		run(args...)
		signed0, signed1 := filepath.Join(dir, operation+"-org0.json"), filepath.Join(dir, operation+"-org1.json")
		run("tx", "endorse", unsigned, "--config", configs[0], "--output", signed0)
		run("tx", "endorse", unsigned, "--config", configs[1], "--output", signed1)
		merged := filepath.Join(dir, operation+"-merged.json")
		run("tx", "merge", signed0, signed1, "--output", merged)
		if out := run("tx", "submit", "--wait", merged, "--config", configs[0]); !strings.Contains(out, "Transaction status: COMMITTED") {
			t.Fatalf("namespace %s did not commit: %s", operation, out)
		}
	}
}
