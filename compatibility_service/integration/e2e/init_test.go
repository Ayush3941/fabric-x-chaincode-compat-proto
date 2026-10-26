// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"compatibility_service/pkg/lifecycle"
	"compatibility_service/pkg/orchestrator"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestSignedInitDispatchAndSequentialLifecycle(t *testing.T) {
	h := newHarness(t, harnessOptions{name: "signed-init", requestWait: "90s", finality: "25s"})
	if err := runBuild(h.root, h.compatDir, "go", "build", "-o", "bin/client", "./cmd/client"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	name := fmt.Sprintf("init_e2e_%d", time.Now().UnixNano())
	key := uniqueKey(t, "initialized")
	orgPorts := []int{h.ports.org0Orch, h.ports.org1Orch}
	packageIDs := make([]string, len(orgPorts))
	for org, port := range orgPorts {
		source := t.TempDir()
		connection := fmt.Sprintf("{\"address\":\"127.0.0.1:%d\",\"dial_timeout\":\"10s\",\"tls_required\":false}", []int{h.ports.org0CC, h.ports.org1CC}[org])
		if err := os.WriteFile(filepath.Join(source, "connection.json"), []byte(connection), 0o600); err != nil {
			t.Fatal(err)
		}
		pkg, err := lifecycle.PackageExternalChaincode(lifecycle.PackageOptions{
			SourceDir: source, OutputPath: filepath.Join(t.TempDir(), "chaincode.tgz"), Label: fmt.Sprintf("%s_org%d", name, org),
		})
		if err != nil {
			t.Fatal(err)
		}
		packageIDs[org] = pkg.PackageID
		adminConfig := fmt.Sprintf("sampleconfig/admin-org%d.yaml", org)
		host := fmt.Sprintf("127.0.0.1:%d", port)
		runInitCommand(t, ctx, h, "./bin/orchestrator", "lifecycle", "install", "-c", adminConfig, "--host", host, pkg.Output)
		runInitCommand(t, ctx, h, "./bin/orchestrator", "lifecycle", "approveformyorg", "-c", adminConfig, "--host", host,
			"-n", name, "-v", "1.0", "--sequence", "1", "--package-id", pkg.PackageID, "--init-required")
	}
	commonArgs := []string{"-c", "sampleconfig/admin-org0.yaml", "--host", fmt.Sprintf("127.0.0.1:%d", h.ports.org0Orch), "-n", name, "-v", "1.0"}
	lifecycleCommand := func(operation string, extra ...string) string {
		args := append([]string{"lifecycle", operation}, commonArgs...)
		return runInitCommand(t, ctx, h, "./bin/orchestrator", append(args, extra...)...)
	}
	readiness := lifecycleCommand("checkcommitreadiness", "--sequence", "1", "--init-required")
	if !strings.Contains(readiness, "org-0=true") || !strings.Contains(readiness, "org-1=true") {
		t.Fatalf("initialization definition is not ready: %s", readiness)
	}
	lifecycleCommand("commit", "--sequence", "1", "--init-required")
	committed := lifecycleCommand("querycommitted")
	if !strings.Contains(committed, "initialized=false") {
		t.Fatalf("init-required definition was already initialized: %s", committed)
	}

	// Both nodes independently reject unsigned routing overrides and changes
	// to the signed initialization flag before execution.
	before := h.blockHeight(t)
	for _, operation := range []string{orchestrator.GRPCOperationInvoke, orchestrator.GRPCOperationQuery} {
		normal := initSignedProposal(t, h, name, false, "get", key)
		if _, err := h.processSigned(ctx, 0, operation, normal); err == nil {
			t.Fatalf("normal %s was allowed before initialization", operation)
		}
	}
	for org := range 2 {
		for _, isInit := range []bool{false, true} {
			proposal := initSignedProposal(t, h, name, isInit, "initialize", key, "seed")
			forgedMetadata := metadata.AppendToOutgoingContext(ctx, orchestrator.GRPCInitMetadata, fmt.Sprintf("%t", !isInit))
			if _, err := h.processSigned(forgedMetadata, org, orchestrator.GRPCOperationEndorse, proposal); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("org%d accepted conflicting init metadata: %v", org, err)
			}
			decoded, err := protoutil.UnmarshalProposal(proposal.ProposalBytes)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := protoutil.UnmarshalChaincodeProposalPayload(decoded.Payload)
			if err != nil {
				t.Fatal(err)
			}
			spec, err := protoutil.UnmarshalChaincodeInvocationSpec(payload.Input)
			if err != nil {
				t.Fatal(err)
			}
			spec.ChaincodeSpec.Input.IsInit = !isInit
			payload.Input = protoutil.MarshalOrPanic(spec)
			decoded.Payload = protoutil.MarshalOrPanic(payload)
			proposal.ProposalBytes = protoutil.MarshalOrPanic(decoded)
			if _, err := h.processSigned(ctx, org, orchestrator.GRPCOperationEndorse, proposal); status.Code(err) != codes.Unauthenticated {
				t.Fatalf("org%d accepted unsigned change to init intent: %v", org, err)
			}
		}
	}
	if _, err := h.processSigned(ctx, 0, orchestrator.GRPCOperationQuery, initSignedProposal(t, h, name, true, "initialize", key, "seed")); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("query accepted init intent: %v", err)
	}
	failed := initSignedProposal(t, h, name, true, "initialize", key, "fail-init")
	if response, err := h.processSigned(ctx, 0, orchestrator.GRPCOperationInvoke, failed); err == nil && response.GetResponse().GetStatus() < 400 {
		t.Fatal("deliberately failed Init succeeded")
	}
	assertHeightUnchanged(t, h, before)
	committed = lifecycleCommand("querycommitted")
	if !strings.Contains(committed, "initialized=false") {
		t.Fatal("failed Init marked the definition initialized")
	}
	for org := range 2 {
		response, err := h.processSigned(ctx, org, orchestrator.GRPCOperationEndorse, initSignedProposal(t, h, name, true, "initialize", key, "fail-init"))
		if err != nil || response.GetResponse().GetStatus() < 400 || response.Endorsement != nil || len(response.Payload) != 0 {
			t.Fatalf("org%d endorsed failed Init writes: response=%v error=%v", org, response, err)
		}
	}

	// Exercise the real client binary: the init flag must enter the bytes it signs.
	input, err := json.Marshal(map[string]any{"Function": "initialize", "Args": []string{key, "seed"}})
	if err != nil {
		t.Fatal(err)
	}
	output := runInitCommand(t, ctx, h, "./bin/client", "invoke", "-c", "sampleconfig/client.yaml",
		"--host", fmt.Sprintf("127.0.0.1:%d", h.ports.org0Orch), "--namespace", namespace, "-n", name, "-v", "1.0", "--is-init", string(input))
	var result orchestrator.InvocationResponse
	if err := json.Unmarshal([]byte(output), &result); err != nil {
		t.Fatalf("decode init client response: %v\n%s", err, output)
	}
	if result.CommitStatus != "COMMITTED" || result.Payload != "init-called" || result.ChaincodeEvent == nil || result.ChaincodeEvent.EventName != "initialized" || len(result.LifecycleBindings) != 2 {
		t.Fatalf("Init did not commit through both organizations: %+v", result)
	}
	for org, port := range orgPorts {
		localDefinition := runInitCommand(t, ctx, h, "./bin/orchestrator", "lifecycle", "querycommitted", "-c",
			fmt.Sprintf("sampleconfig/admin-org%d.yaml", org), "--host", fmt.Sprintf("127.0.0.1:%d", port), "-n", name, "-v", "1.0")
		if !strings.Contains(localDefinition, "initialized=true") {
			t.Fatalf("org%d did not finalize initialization: %s", org, localDefinition)
		}
		read := initSignedProposal(t, h, name, false, "get", key)
		response, err := h.processSigned(ctx, org, orchestrator.GRPCOperationQuery, read)
		if err != nil {
			t.Fatal(err)
		}
		var queried orchestrator.InvocationResponse
		if err := json.Unmarshal(response.Response.Payload, &queried); err != nil || queried.Payload != "seed" {
			t.Fatalf("Invoke did not read Init's committed state on org%d: %+v error=%v", org, queried, err)
		}
	}
	after := h.blockHeight(t)
	if _, err := h.processSigned(ctx, 0, orchestrator.GRPCOperationInvoke, initSignedProposal(t, h, name, true, "initialize", key, "replacement")); err == nil {
		t.Fatal("sequential reinitialization was accepted")
	}
	assertHeightUnchanged(t, h, after)
	// Native Fabric-X validation rejects namespaces without writes. Surface that
	// outcome without marking a no-op Init successful in lifecycle state.
	noopName := name + "_noop"
	for org, port := range orgPorts {
		runInitCommand(t, ctx, h, "./bin/orchestrator", "lifecycle", "approveformyorg",
			"-c", fmt.Sprintf("sampleconfig/admin-org%d.yaml", org), "--host", fmt.Sprintf("127.0.0.1:%d", port),
			"-n", noopName, "-v", "1.0", "--sequence", "1", "--package-id", packageIDs[org], "--init-required")
	}
	runInitCommand(t, ctx, h, "./bin/orchestrator", "lifecycle", "commit",
		"-c", "sampleconfig/admin-org0.yaml", "--host", fmt.Sprintf("127.0.0.1:%d", h.ports.org0Orch),
		"-n", noopName, "-v", "1.0", "--sequence", "1", "--init-required")
	noopInput, err := json.Marshal(map[string]any{"Function": "initialize", "Args": []string{key, "noop-init"}})
	if err != nil {
		t.Fatal(err)
	}
	noopOutput := runInitCommand(t, ctx, h, "./bin/client", "invoke", "-c", "sampleconfig/client.yaml",
		"--host", fmt.Sprintf("127.0.0.1:%d", h.ports.org0Orch), "--namespace", namespace, "-n", noopName, "-v", "1.0", "--is-init", string(noopInput))
	var noopResult orchestrator.InvocationResponse
	if err := json.Unmarshal([]byte(noopOutput), &noopResult); err != nil || !noopResult.Submitted || noopResult.CommitStatus != "MALFORMED_NO_WRITES" || noopResult.Payload != "" || noopResult.ChaincodeEvent != nil {
		t.Fatalf("unexpected no-op Init validation outcome: result=%+v error=%v", noopResult, err)
	}
	for org, port := range orgPorts {
		localDefinition := runInitCommand(t, ctx, h, "./bin/orchestrator", "lifecycle", "querycommitted", "-c",
			fmt.Sprintf("sampleconfig/admin-org%d.yaml", org), "--host", fmt.Sprintf("127.0.0.1:%d", port), "-n", noopName, "-v", "1.0")
		if !strings.Contains(localDefinition, "initialized=false") {
			t.Fatalf("org%d marked rejected no-op Init initialized: %s", org, localDefinition)
		}
		if _, err := h.processSigned(ctx, org, orchestrator.GRPCOperationQuery, initSignedProposal(t, h, noopName, false, "get", key)); err == nil {
			t.Fatalf("org%d allowed ordinary execution after rejected no-op Init", org)
		}
	}
	dir := filepath.Join(h.root, "runtime", "compatibility_service", "init-support")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	evidence, err := json.MarshalIndent(map[string]any{
		"chaincode_name": name, "initialized_key": key, "invocation": result,
		"normal_calls_blocked_before_init": true, "failed_init_not_committed": true,
		"both_organizations_initialized": true, "ordinary_invoke_reads_seed": true,
		"sequential_reinitialization_rejected": true, "concurrent_initialization_guard_tested": false,
		"failed_init_endorsements_rejected": true, "noop_init": noopResult, "noop_definition_stays_uninitialized": true,
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result.json"), append(evidence, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("signed INIT committed at block %d with two endorsements; Init and Invoke dispatched separately", result.BlockNum)
}

func runInitCommand(t *testing.T, ctx context.Context, h *e2eHarness, binary string, args ...string) string {
	t.Helper()
	command := exec.CommandContext(ctx, binary, args...)
	command.Dir = h.compatDir
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s\n%s", binary, strings.Join(args, " "), err, output, stderr.String())
	}
	return string(output)
}

func initSignedProposal(t *testing.T, h *e2eHarness, name string, isInit bool, function string, args ...string) *peer.SignedProposal {
	t.Helper()
	signer, err := identity.SignerFromMSP(h.clientMSP(0), "org-0")
	if err != nil {
		t.Fatal(err)
	}
	creator, err := signer.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	nonce := randomNonce(t)
	input := [][]byte{[]byte(function)}
	for _, arg := range args {
		input = append(input, []byte(arg))
	}
	proposal, _, err := protoutil.CreateChaincodeProposalWithTxIDNonceAndTransient(
		protoutil.ComputeTxID(nonce, creator), common.HeaderType_ENDORSER_TRANSACTION, channelID,
		&peer.ChaincodeInvocationSpec{ChaincodeSpec: &peer.ChaincodeSpec{
			Type: peer.ChaincodeSpec_GOLANG, ChaincodeId: &peer.ChaincodeID{Name: name, Version: "1.0"},
			Input: &peer.ChaincodeInput{Args: input, IsInit: isInit},
		}}, nonce, creator, nil)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := protoutil.GetSignedProposal(proposal, signer)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}
