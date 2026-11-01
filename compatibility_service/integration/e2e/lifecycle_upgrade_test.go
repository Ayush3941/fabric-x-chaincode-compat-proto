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
	"strconv"
	"strings"
	"testing"
	"time"

	"compatibility_service/pkg/orchestrator"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Runs the normal sample CCaaS, real lifecycle/client binaries and native
// Fabric-X ordering/validation. No ledger reset or mocked commit is used.
func TestLifecycleUpgradeUsesNameAndSequenceWithCompatV3(t *testing.T) {
	h := newHarness(t, harnessOptions{name: "lifecycle-upgrade", sampleChaincode: true, requestWait: "90s", finality: "25s"})
	if err := runBuild(h.root, h.compatDir, "go", "build", "-o", "bin/client", "./cmd/client"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	name := fmt.Sprintf("sample_upgrade_%d", time.Now().UnixNano())
	key, deleteKey := uniqueKey(t, "upgrade-value"), uniqueKey(t, "upgrade-delete")
	dir := filepath.Join(h.root, "runtime", "compatibility_service", "lifecycle-upgrade")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var commands []map[string]any
	command := func(binary string, args ...string) (string, error) {
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Dir = h.compatDir
		out, err := cmd.CombinedOutput()
		commands = append(commands, map[string]any{"binary": binary, "args": args, "output": string(out), "success": err == nil})
		return string(out), err
	}
	must := func(binary string, args ...string) string {
		out, err := command(binary, args...)
		if err != nil {
			t.Fatalf("%s %s: %v\n%s", binary, strings.Join(args, " "), err, out)
		}
		return out
	}
	ports := []int{h.ports.org0Orch, h.ports.org1Orch}
	argsFor := func(operation string, org int, version string, sequence int, extra ...string) []string {
		args := []string{"lifecycle", operation, "-c", fmt.Sprintf("sampleconfig/admin-org%d.yaml", org), "--host", fmt.Sprintf("127.0.0.1:%d", ports[org]), "-n", name}
		if version != "" {
			args = append(args, "-v", version)
		}
		if sequence != 0 {
			args = append(args, "--sequence", strconv.Itoa(sequence))
		}
		return append(args, extra...)
	}
	install := func(org, revision int) string {
		source := t.TempDir()
		connection := fmt.Sprintf(`{"address":"127.0.0.1:%d","tls_required":false}`, []int{h.ports.org0CC, h.ports.org1CC}[org])
		if err := os.WriteFile(filepath.Join(source, "connection.json"), []byte(connection), 0o600); err != nil {
			t.Fatal(err)
		}
		label := fmt.Sprintf("%s_org%d_revision%d", name, org, revision)
		out := must("./bin/orchestrator", "lifecycle", "install", "-c", fmt.Sprintf("sampleconfig/admin-org%d.yaml", org), "--host", fmt.Sprintf("127.0.0.1:%d", ports[org]), "--path", source, "--label", label)
		for _, line := range strings.Split(out, "\n") {
			if id, ok := strings.CutPrefix(line, "package_id="); ok {
				return id
			}
		}
		t.Fatal("install returned no package ID")
		return ""
	}
	invoke := func(org int, value, expectedOld, version string, sequence int) orchestrator.InvocationResponse {
		input, _ := json.Marshal(map[string]any{"Function": "compat-v3", "Args": []string{key, value, deleteKey}})
		// Version is deliberately omitted from every application request.
		out := must("./bin/client", "invoke", "-c", fmt.Sprintf("sampleconfig/client%s.yaml", []string{"", "-org1"}[org]), "--host", fmt.Sprintf("127.0.0.1:%d", ports[org]), "--namespace", namespace, "-n", name, string(input))
		var result orchestrator.InvocationResponse
		if err := json.Unmarshal([]byte(out), &result); err != nil {
			t.Fatal(err)
		}
		if result.CommitStatus != "COMMITTED" || !result.Submitted || len(result.LifecycleBindings) != 2 || result.ChaincodeEvent == nil || result.ChaincodeEvent.EventName != "compat-v3-event" {
			t.Fatalf("compat-v3 did not commit with both organizations and its event: %+v", result)
		}
		for _, binding := range result.LifecycleBindings {
			if binding.ChaincodeName != name || binding.ChaincodeVersion != version || binding.Sequence != int64(sequence) || binding.Namespace != namespace || binding.PackageID == "" {
				t.Fatalf("incorrect active revision binding: %+v", binding)
			}
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(result.Payload), &payload); err != nil {
			t.Fatal(err)
		}
		if expectedOld != "" && payload["committed_old_value"] != expectedOld {
			t.Fatalf("upgrade lost application state: %+v", payload)
		}
		h.waitForHeight(t, result.BlockNum+1)
		return result
	}
	query := func(org int, expected string) {
		input, _ := json.Marshal(map[string]any{"Function": "get", "Args": []string{key}})
		out := must("./bin/client", "query", "-c", fmt.Sprintf("sampleconfig/client%s.yaml", []string{"", "-org1"}[org]), "--host", fmt.Sprintf("127.0.0.1:%d", ports[org]), "--namespace", namespace, "-n", name, string(input))
		if strings.TrimSpace(out) != expected {
			t.Fatalf("read returned %q, expected %q", out, expected)
		}
	}
	packages := [][]string{make([]string, 2), make([]string, 2)}
	for org := range 2 {
		packages[0][org] = install(org, 1)
		must("./bin/orchestrator", argsFor("approveformyorg", org, "1.0", 1, "--package-id", packages[0][org])...)
	}
	commit1 := must("./bin/orchestrator", argsFor("commit", 0, "1.0", 1)...)
	first := invoke(0, "before-upgrade", "", "1.0", 1)
	for org := range 2 {
		query(org, "before-upgrade")
	}
	for org := range 2 {
		packages[1][org] = install(org, 2)
	}
	if out, err := command("./bin/orchestrator", argsFor("approveformyorg", 0, "2.0", 1, "--package-id", packages[1][0])...); err == nil {
		t.Fatalf("version change restarted sequence at 1: %s", out)
	}
	must("./bin/orchestrator", argsFor("approveformyorg", 0, "2.0", 2, "--package-id", packages[1][0])...)
	must("./bin/orchestrator", argsFor("approveformyorg", 1, "mismatched-version", 2, "--package-id", packages[1][1])...)
	readiness := must("./bin/orchestrator", argsFor("checkcommitreadiness", 0, "2.0", 2)...)
	if !strings.Contains(readiness, "org-0=true") || !strings.Contains(readiness, "org-1=false") {
		t.Fatalf("readiness did not compare version properties: %s", readiness)
	}
	if out, err := command("./bin/orchestrator", argsFor("commit", 0, "2.0", 2)...); err == nil {
		t.Fatalf("mismatched approvals committed: %s", out)
	}
	// Pending approvals leave the old runtime binding available until commitment.
	pending := invoke(0, "while-upgrade-pending", "before-upgrade", "1.0", 1)
	must("./bin/orchestrator", argsFor("approveformyorg", 1, "2.0", 2, "--package-id", packages[1][1])...)
	commit2 := must("./bin/orchestrator", argsFor("commit", 0, "2.0", 2)...)
	second := invoke(1, "after-upgrade", "while-upgrade-pending", "2.0", 2)
	for org := range 2 {
		query(org, "after-upgrade")
	}
	for org := range 2 {
		must("./bin/orchestrator", argsFor("approveformyorg", org, "2.1", 3, "--package-id", packages[1][org])...)
	}
	commit3 := must("./bin/orchestrator", argsFor("commit", 0, "2.1", 3)...)
	third := invoke(0, "after-third-revision", "after-upgrade", "2.1", 3)
	for org := range 2 {
		out := must("./bin/orchestrator", argsFor("querycommitted", org, "", 0)...)
		if strings.Count(out, "name="+name+"\n") != 1 || !strings.Contains(out, "version=2.1") || !strings.Contains(out, "sequence=3") {
			t.Fatalf("superseded versions still active: %s", out)
		}
		query(org, "after-third-revision")
	}
	// A new instance rebuilds only the active revision from the append-only index.
	newPort := h.ports.unusedOrg1
	cfg := h.writeOrchestratorConfig(t, t.TempDir(), 0, newPort, h.ports.org0CC, 1, h.ports.org1Orch, "90s", "25s")
	h.startProcess(t, "orchestrator-hydrated", h.compatDir, filepath.Join(h.root, "runtime", "compatibility_service", "e2e"), nil, "./bin/orchestrator", "-c", cfg, "--log-level", "info:grpc=error")
	waitForPort(t, newPort)
	hydrated := must("./bin/orchestrator", "lifecycle", "querycommitted", "-c", "sampleconfig/admin-org0.yaml", "--host", fmt.Sprintf("127.0.0.1:%d", newPort), "-n", name)
	if strings.Count(hydrated, "name="+name+"\n") != 1 || !strings.Contains(hydrated, "version=2.1") || !strings.Contains(hydrated, "sequence=3") {
		t.Fatalf("startup restored an old version: %s", hydrated)
	}
	conn, err := h.blockQueryConn()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	block, err := committerpb.NewBlockQueryServiceClient(conn).GetBlockByNumber(ctx, &committerpb.BlockNumber{Number: third.BlockNum})
	if err != nil {
		t.Fatal(err)
	}
	if int(third.TxNum) >= len(block.GetData().GetData()) {
		t.Fatal("transaction missing from ordered block")
	}
	envelope := &common.Envelope{}
	if err := proto.Unmarshal(block.Data.Data[third.TxNum], envelope); err != nil {
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
	if header.TxId != third.TxID || header.ChannelId != channelID {
		t.Fatalf("block transaction identity mismatch: %v", header)
	}
	native := &applicationpb.Tx{}
	if err := proto.Unmarshal(payload.Data, native); err != nil {
		t.Fatal(err)
	}
	if len(native.Namespaces) != 1 || native.Namespaces[0].NsId != namespace || len(native.Endorsements) != 1 || len(native.Endorsements[0].EndorsementsWithIdentity) != 2 {
		t.Fatalf("unexpected block transaction: %v", native)
	}
	orgs := map[string]bool{}
	for _, endorsement := range native.Endorsements[0].EndorsementsWithIdentity {
		orgs[endorsement.Identity.GetMspId()] = true
	}
	if !orgs["org-0"] || !orgs["org-1"] {
		t.Fatalf("missing organization endorsement: %v", orgs)
	}
	for filename, message := range map[string]proto.Message{"block.json": block, "native-transaction.json": native} {
		data, err := protojson.MarshalOptions{Indent: "  "}.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, filename), append(data, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	evidence := map[string]any{"channel": channelID, "chaincode_name": name, "namespace": namespace, "state_key": key, "package_ids": packages, "commit_1": commit1, "commit_2": commit2, "commit_3": commit3, "revision_1": first, "while_upgrade_pending": pending, "revision_2": second, "revision_3": third, "state_preserved": true, "version_omitted_from_client_requests": true, "startup_hydrated": hydrated, "commands": commands}
	data, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result.json"), append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("compat-v3 upgrade committed: name=%s versions=1.0/2.0/2.1 sequences=1/2/3 block=%d tx_id=%s evidence=%s", name, third.BlockNum, third.TxID, dir)
}
