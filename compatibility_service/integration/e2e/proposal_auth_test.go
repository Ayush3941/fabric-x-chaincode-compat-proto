// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"compatibility_service/pkg/auth"
	"compatibility_service/pkg/helper"
	"compatibility_service/pkg/lifecycle"
	"compatibility_service/pkg/orchestrator"
	gwclient "github.com/hyperledger/fabric-gateway/pkg/client"
	gwidentity "github.com/hyperledger/fabric-gateway/pkg/identity"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/gateway"
	mspapi "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-sdk/network"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestClientProposalAuthentication(t *testing.T) {
	// Both MSPs are trusted, but only org0 clients are granted invocation access.
	// Org1 must still endorse an authorized org0 client's original proposal.
	h := newHarness(t, harnessOptions{name: "proposal-auth", proposalPolicy: "OR('org-0.member')"})
	before := h.blockHeight(t)
	key := uniqueKey(t, "authenticated")
	value := "value-from-authenticated-client"
	makeProposal := func(org int, claimedMSP string) *peer.SignedProposal {
		p, _ := h.newSignedProposalAs(t, randomNonce(t), org, claimedMSP, "compatv2", key, value, uniqueKey(t, "auth-delete"))
		return p
	}
	for _, tc := range []struct {
		name string
		make func() *peer.SignedProposal
		want codes.Code
	}{
		{name: "missing signature", want: codes.Unauthenticated, make: func() *peer.SignedProposal { p := makeProposal(0, ""); p.Signature = nil; return p }},
		{name: "invalid signature", want: codes.Unauthenticated, make: func() *peer.SignedProposal { p := makeProposal(0, ""); p.Signature = []byte("invalid"); return p }},
		{name: "tampered proposal", want: codes.Unauthenticated, make: func() *peer.SignedProposal {
			p := makeProposal(0, "")
			proposal, err := protoutil.UnmarshalProposal(p.ProposalBytes)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := protoutil.UnmarshalChaincodeProposalPayload(proposal.Payload)
			if err != nil {
				t.Fatal(err)
			}
			spec, err := protoutil.UnmarshalChaincodeInvocationSpec(payload.Input)
			if err != nil {
				t.Fatal(err)
			}
			spec.ChaincodeSpec.Input.Args[2] = []byte("tampered-value")
			payload.Input = protoutil.MarshalOrPanic(spec)
			proposal.Payload = protoutil.MarshalOrPanic(payload)
			p.ProposalBytes = protoutil.MarshalOrPanic(proposal)
			return p
		}},
		{name: "unknown MSP", want: codes.Unauthenticated, make: func() *peer.SignedProposal { return makeProposal(0, "untrusted-org") }},
		{name: "certificate outside claimed MSP", want: codes.Unauthenticated, make: func() *peer.SignedProposal { return makeProposal(1, "org-0") }},
		{name: "authenticated but unauthorized client", want: codes.PermissionDenied, make: func() *peer.SignedProposal { return makeProposal(1, "") }},
	} {
		for org := 0; org < 2; org++ {
			for _, operation := range []string{orchestrator.GRPCOperationQuery, orchestrator.GRPCOperationInvoke, orchestrator.GRPCOperationEndorse} {
				t.Run(fmt.Sprintf("%s/org%d/%s", tc.name, org, operation), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					_, err := h.processSigned(ctx, org, operation, tc.make())
					if status.Code(err) != tc.want {
						t.Fatalf("got %v, want %s", err, tc.want)
					}
				})
			}
		}
	}
	assertHeightUnchanged(t, h, before)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	proposal := makeProposal(0, "")
	remote, err := h.processSigned(ctx, 1, orchestrator.GRPCOperationEndorse, proposal)
	if err != nil || remote.GetResponse().GetStatus() != 200 {
		t.Fatalf("org1 did not endorse the original authorized org0 client: response=%v error=%v", remote, err)
	}
	result, err := h.invokeSigned(ctx, proposal)
	if err != nil {
		t.Fatalf("invoke authenticated client proposal: %v", err)
	}
	if !result.Submitted || result.CommitStatus != "COMMITTED" || result.ChaincodeEvent == nil {
		t.Fatalf("expected committed transaction and event, got %+v", result)
	}
	// A completed transaction must not bypass authentication on retries.
	proposal.Signature = nil
	if _, err := h.processSigned(ctx, 0, orchestrator.GRPCOperationInvoke, proposal); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unsigned replay of committed transaction was not rejected: %v", err)
	}
	query, _ := h.newSignedProposal(t, "get", key)
	response, err := h.processSigned(ctx, 1, orchestrator.GRPCOperationQuery, query)
	if err != nil {
		t.Fatalf("query committed state through org1: %v", err)
	}
	var queryResult orchestrator.InvocationResponse
	if err := json.Unmarshal(response.Response.Payload, &queryResult); err != nil {
		t.Fatal(err)
	}
	if queryResult.Payload != value {
		t.Fatalf("committed value = %q, want %q", queryResult.Payload, value)
	}
	if got := h.blockHeight(t); got != before+1 {
		t.Fatalf("expected exactly one new block for authorized invocation, height before=%d after=%d", before, got)
	}
	report, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(h.root, "runtime", "compatibility_service", "e2e", "proposal-auth-result.json")
	if err := os.WriteFile(path, append(report, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("authenticated proposal committed: tx_id=%s block=%d; result=%s", result.TxID, result.BlockNum, path)
}

func TestGatewayEvaluateSDK(t *testing.T) {
	name := fmt.Sprintf("gateway_eval_%d", time.Now().UnixNano())
	h := newHarness(t, harnessOptions{
		name: "gateway-evaluate", sampleChaincode: true, proposalPolicy: "OR('org-0.member')", requestWait: "60s", finality: "25s",
		chaincodeDefinition: &lifecycle.ChaincodeDefinition{Name: name, Version: "descriptive-label", Sequence: 1},
	})
	if err := runBuild(h.root, h.compatDir, "go", "build", "-o", "bin/client", "./cmd/client"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	ports := []int{h.ports.org0Orch, h.ports.org1Orch}
	definition := h.deployGatewayDefinition(t, ctx, name, "descriptive-label")
	key := uniqueKey(t, "gateway-binary")
	value := []byte{0, 255, 254, 128, 'o', 'k'}
	seed := initSignedProposal(t, h, name, false, "put", key, string(value))
	response, err := h.processSigned(ctx, 0, orchestrator.GRPCOperationInvoke, seed)
	if err != nil {
		t.Fatal(err)
	}
	var committed orchestrator.InvocationResponse
	if err := json.Unmarshal(response.Response.Payload, &committed); err != nil || committed.CommitStatus != "COMMITTED" {
		t.Fatalf("seed did not commit: %+v %v", committed, err)
	}
	h.waitForHeight(t, committed.BlockNum+1)
	before := h.blockHeight(t)
	client := h.gatewaySDK(t, 0, 0)
	contract := client.GetNetwork(channelID).GetContract(name + ":" + namespace)
	unsigned, err := contract.NewProposal("get", gwclient.WithArguments(key))
	if err != nil {
		t.Fatal(err)
	}
	unsignedBytes, err := unsigned.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	proposed := &gateway.ProposedTransaction{}
	if err := proto.Unmarshal(unsignedBytes, proposed); err != nil {
		t.Fatal(err)
	}
	proposal, err := protoutil.UnmarshalProposal(proposed.Proposal.ProposalBytes)
	if err != nil {
		t.Fatal(err)
	}
	proposalPayload, err := protoutil.UnmarshalChaincodeProposalPayload(proposal.Payload)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := protoutil.UnmarshalChaincodeInvocationSpec(proposalPayload.Input)
	if err != nil || spec.GetChaincodeSpec().GetChaincodeId().GetName() != name+":"+namespace || spec.GetChaincodeSpec().GetChaincodeId().GetVersion() != "" {
		t.Fatalf("SDK proposal routing or version differs: %v %v", spec, err)
	}
	forged, err := client.NewSignedProposal(unsignedBytes, []byte("invalid-signature"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := forged.EvaluateWithContext(ctx); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("SDK proposal with invalid signature was accepted: %v", err)
	}
	for _, targets := range [][]string{nil, {"org-0"}, {"org-1"}, {"unavailable-org", "org-1"}} {
		result, err := contract.EvaluateWithContext(ctx, "get", gwclient.WithArguments(key), gwclient.WithEndorsingOrganizations(targets...))
		if err != nil || !bytes.Equal(result, value) {
			t.Fatalf("SDK evaluation targets=%v changed binary response: %x %v", targets, result, err)
		}
	}
	// Both local and targeted remote evaluation may simulate writes, but those
	// writes and events must never reach ordering, finality, or committed state.
	for _, targets := range [][]string{nil, {"org-1"}} {
		result, err := contract.EvaluateWithContext(ctx, "put", gwclient.WithArguments(key, "discard-this-write"), gwclient.WithEndorsingOrganizations(targets...))
		if err != nil || string(result) != "discard-this-write" {
			t.Fatalf("write evaluation failed: %q %v", result, err)
		}
		result, err = contract.EvaluateWithContext(ctx, "get", gwclient.WithArguments(key))
		if err != nil || !bytes.Equal(result, value) {
			t.Fatalf("evaluated write changed committed state: %x %v", result, err)
		}
	}
	probeKey := uniqueKey(t, "gateway-discarded-event")
	probeResult, err := contract.EvaluateWithContext(ctx, "compat-v3", gwclient.WithArguments(probeKey, "not-committed", probeKey+"-delete"), gwclient.WithTransient(map[string][]byte{"secret": []byte("client-transient")}), gwclient.WithEndorsingOrganizations("org-1"))
	if err != nil {
		t.Fatal(err)
	}
	var probe map[string]any
	if err := json.Unmarshal(probeResult, &probe); err != nil {
		t.Fatal(err)
	}
	if probe["client_msp_id"] != "org-0" || probe["signed_proposal_present"] != true || probe["transient"].(map[string]any)["secret"] != "client-transient" {
		t.Fatalf("remote execution lost original client context: %v", probe)
	}
	decorations := probe["decorations"].(map[string]any)
	if decorations["compat.chaincode_name"] != name || decorations["compat.chaincode_version"] != "descriptive-label" || decorations["compat.chaincode_sequence"] != "1" {
		t.Fatalf("SDK did not use active definition: %v", decorations)
	}
	absent, err := contract.EvaluateWithContext(ctx, "get", gwclient.WithArguments(probeKey))
	if err != nil || len(absent) != 0 {
		t.Fatalf("evaluated compat-v3 wrote ledger state: %x %v", absent, err)
	}
	if _, err := contract.EvaluateWithContext(ctx, "compaterror"); status.Code(err) != codes.Unknown {
		t.Fatalf("chaincode error was not surfaced through SDK: %v", err)
	}
	if _, err := contract.EvaluateWithContext(ctx, "get", gwclient.WithArguments(key), gwclient.WithEndorsingOrganizations("unavailable-org")); status.Code(err) != codes.Unavailable {
		t.Fatalf("target organization was ignored: %v", err)
	}
	unauthorized := h.gatewaySDK(t, 1, 1).GetNetwork(channelID).GetContract(name + ":" + namespace)
	if _, err := unauthorized.EvaluateWithContext(ctx, "get", gwclient.WithArguments(key)); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("original unauthorized signer was accepted: %v", err)
	}
	for _, invalid := range []string{name, name + ":1:2"} {
		if _, err := client.GetNetwork(channelID).GetContract(invalid).EvaluateWithContext(ctx, "get", gwclient.WithArguments(key)); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("malformed route was accepted: %s %v", invalid, err)
		}
	}
	expired, expire := context.WithTimeout(ctx, time.Nanosecond)
	defer expire()
	<-expired.Done()
	if _, err := contract.EvaluateWithContext(expired, "get", gwclient.WithArguments(key)); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("SDK deadline was ignored: %v", err)
	}
	// Reuse the original SDK connection and the initiating Orchestrator's cached
	// remote contact while stopping and starting real processes on the same ports.
	restartedPIDs := make(map[string]map[string]int)
	restart := func(process string, port int) {
		before := h.processes[process].cmd.Process.Pid
		h.restartProcess(t, process)
		waitForPort(t, port)
		restartedPIDs[process] = map[string]int{"before": before, "after": h.processes[process].cmd.Process.Pid}
	}
	readAfterRestart := func(targets ...string) {
		deadline := time.Now().Add(12 * time.Second)
		for {
			attempt, cancel := context.WithTimeout(ctx, 5*time.Second)
			result, err := contract.EvaluateWithContext(attempt, "get", gwclient.WithArguments(key), gwclient.WithEndorsingOrganizations(targets...))
			cancel()
			if err == nil {
				if !bytes.Equal(result, value) {
					t.Fatalf("committed state changed after restart: %x", result)
				}
				return
			}
			// An established gRPC connection may briefly reconnect after its server
			// exits. Retry transport failures only; application failures are final.
			if (status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded) || time.Now().After(deadline) {
				t.Fatalf("evaluation did not recover after restart targets=%v: %v", targets, err)
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	recoveredDefinitions := make(map[string]string)
	for _, org := range []int{1, 0} {
		restart(fmt.Sprintf("orchestrator-org%d", org), ports[org])
		committed := runInitCommand(t, ctx, h, "./bin/orchestrator", "lifecycle", "querycommitted", "-c", fmt.Sprintf("sampleconfig/admin-org%d.yaml", org), "--host", fmt.Sprintf("127.0.0.1:%d", ports[org]), "-n", name)
		if strings.Count(committed, "name="+name+"\n") != 1 || !strings.Contains(committed, "version=descriptive-label") || !strings.Contains(committed, "sequence=1") {
			t.Fatalf("org%d did not restore the active definition: %s", org, committed)
		}
		installed := runInitCommand(t, ctx, h, "./bin/orchestrator", "lifecycle", "queryinstalled", "-c", fmt.Sprintf("sampleconfig/admin-org%d.yaml", org), "--host", fmt.Sprintf("127.0.0.1:%d", ports[org]))
		if strings.TrimSpace(installed) != "installed_packages=0" {
			t.Fatalf("org%d retained supposedly volatile package state: %s", org, installed)
		}
		recoveredDefinitions[fmt.Sprintf("org-%d", org)] = committed
		readAfterRestart()
		readAfterRestart("org-1")
	}
	for org, port := range []int{h.ports.org0CC, h.ports.org1CC} {
		restart(fmt.Sprintf("chaincode-org%d", org), port)
	}
	readAfterRestart()
	readAfterRestart("org-1")
	if _, err := forged.EvaluateWithContext(ctx); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("restart bypassed original proposal verification: %v", err)
	}
	// Generalized existing client command also uses the real Gateway SDK.
	input, _ := json.Marshal(map[string]any{"Function": "get", "Args": []string{key}})
	cli := runInitCommand(t, ctx, h, "./bin/client", "evaluate", "-c", "sampleconfig/client.yaml", "--host", fmt.Sprintf("127.0.0.1:%d", ports[0]), "--namespace", namespace, "-n", name, "--target-orgs", "org-1", string(input))
	if !bytes.Equal([]byte(cli), value) {
		t.Fatalf("evaluate CLI changed binary output: %x", []byte(cli))
	}
	assertHeightUnchanged(t, h, before)
	dir := filepath.Join(h.root, "runtime", "compatibility_service", "gateway-evaluate")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	report, err := json.MarshalIndent(map[string]any{
		"sdk_version": "v1.12.1", "chaincode_name": name, "gateway_contract": name + ":" + namespace, "committed_definition": definition,
		"seed": committed, "binary_payload_base64": base64.StdEncoding.EncodeToString(value), "height_before_evaluations": before,
		"height_after_evaluations": h.blockHeight(t), "local_and_remote_evaluation_passed": true,
		"writes_and_events_discarded": true, "original_client_and_transient_preserved": true,
		"authorization_errors_deadlines_and_routing_checked": true, "existing_client_evaluate_command_passed": true,
		"restarted_process_pids": restartedPIDs, "same_sdk_connection_reused_after_restart": true,
		"definitions_recovered_after_restart": recoveredDefinitions, "installed_packages_empty_after_restart": true,
		"post_restart_ccaas_resolution": "static resolver without legacy chaincode-service fallback",
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result.json"), append(report, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("Gateway SDK Evaluate passed: contract=%s:%s seed_block=%d evaluation_height=%d (unchanged)", name, namespace, committed.BlockNum, before)
}

func TestGatewayEndorseSDK(t *testing.T) {
	name := fmt.Sprintf("gateway_endorse_%d", time.Now().UnixNano())
	h := newHarness(t, harnessOptions{
		name: "gateway-endorse", sampleChaincode: true, proposalPolicy: "OR('org-0.member')", requestWait: "60s", finality: "25s",
		chaincodeDefinition: &lifecycle.ChaincodeDefinition{Name: name, Version: "2.0", Sequence: 1},
	})
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	definition := h.deployGatewayDefinition(t, ctx, name, "2.0")
	_, sign := h.gatewaySigningIdentity(t, 0)
	before := h.blockHeight(t)
	trust, err := auth.NewVerifier([]auth.MSPConfig{
		{MSPID: "org-0", MSPDir: h.artifact("peerOrganizations", "peer-org-0", "msp")},
		{MSPID: "org-1", MSPDir: h.artifact("peerOrganizations", "peer-org-1", "msp")},
	})
	if err != nil {
		t.Fatal(err)
	}
	key, value := uniqueKey(t, "gateway-endorsed-only"), []byte{0, 255, 254, 128, 'o', 'k'}
	var transactions []string
	var finalSigned *gateway.PreparedTransaction
	for _, serverOrg := range []int{0, 1} {
		client := h.gatewaySDK(t, serverOrg, 0)
		contract := client.GetNetwork(channelID).GetContract(name + ":" + namespace)
		for _, organizations := range [][]string{nil, {"org-1"}, {"org-1", "org-0"}} {
			proposal, err := contract.NewProposal("put", gwclient.WithBytesArguments([]byte(key), value), gwclient.WithTransient(map[string][]byte{"secret": []byte("unpublished-endorsement-input")}), gwclient.WithEndorsingOrganizations(organizations...))
			if err != nil {
				t.Fatal(err)
			}
			transaction, err := proposal.EndorseWithContext(ctx)
			if err != nil || !bytes.Equal(transaction.Result(), value) || transaction.TransactionID() != proposal.TransactionID() {
				t.Fatalf("SDK Endorse failed through org%d with organizations=%v: %v", serverOrg, organizations, err)
			}
			serialized, err := transaction.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			prepared := &gateway.PreparedTransaction{}
			if err := proto.Unmarshal(serialized, prepared); err != nil || len(prepared.Envelope.Signature) != 0 || bytes.Contains(serialized, []byte("unpublished-endorsement-input")) {
				t.Fatalf("prepared transaction was signed by the server or leaked transient data: %v", err)
			}
			native, payload := gatewayNativeTransaction(t, prepared.Envelope)
			data, err := native.Namespaces[0].ASN1Marshal(transaction.TransactionID(), native.Metadata)
			if err != nil {
				t.Fatal(err)
			}
			var actualMSPs []string
			for _, endorsement := range native.Endorsements[0].EndorsementsWithIdentity {
				creator, err := proto.Marshal(endorsement.Identity)
				if err != nil {
					t.Fatal(err)
				}
				identity, err := trust.Authenticate(creator, data, endorsement.Endorsement, time.Now())
				if err != nil {
					t.Fatalf("invalid native endorsement in SDK envelope: %v", err)
				}
				actualMSPs = append(actualMSPs, identity.GetMSPIdentifier())
			}
			wantMSPs := organizations
			if len(wantMSPs) == 0 {
				wantMSPs = []string{"org-0", "org-1"}
				slices.Sort(actualMSPs)
			}
			if strings.Join(actualMSPs, ",") != strings.Join(wantMSPs, ",") {
				t.Fatalf("native endorsements=%v want=%v", actualMSPs, wantMSPs)
			}
			// Exercise the SDK's offline signing without submitting an Endorse-only test.
			signedDigest, err := sign(transaction.Digest())
			if err != nil {
				t.Fatal(err)
			}
			signedTransaction, err := client.NewSignedTransaction(serialized, signedDigest)
			if err != nil {
				t.Fatal(err)
			}
			signedBytes, err := signedTransaction.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			if err := proto.Unmarshal(signedBytes, prepared); err != nil {
				t.Fatal(err)
			}
			signature, err := protoutil.UnmarshalSignatureHeader(payload.Header.SignatureHeader)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := trust.Authenticate(signature.Creator, prepared.Envelope.Payload, prepared.Envelope.Signature, time.Now()); err != nil {
				t.Fatalf("SDK client signature does not authorize the prepared payload: %v", err)
			}
			if result, err := contract.EvaluateWithContext(ctx, "get", gwclient.WithArguments(key)); err != nil || len(result) != 0 {
				t.Fatalf("endorsement changed committed ledger state: %x %v", result, err)
			}
			transactions = append(transactions, transaction.TransactionID())
			finalSigned = prepared
		}
		proposal, err := contract.NewProposal("compat-v3", gwclient.WithArguments(key, "simulated-only", key+"-delete"), gwclient.WithTransient(map[string][]byte{"secret": []byte("original-client-context")}))
		if err != nil {
			t.Fatal(err)
		}
		transaction, err := proposal.EndorseWithContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]any
		if err := json.Unmarshal(transaction.Result(), &result); err != nil || result["client_msp_id"] != "org-0" || result["signed_proposal_present"] != true || result["transient"].(map[string]any)["secret"] != "original-client-context" {
			t.Fatalf("multi-organization endorsement lost original client context: %v %v", result, err)
		}
	}
	restartedPIDs := make(map[string][]int)
	for _, org := range []int{1, 0} {
		process := fmt.Sprintf("orchestrator-org%d", org)
		oldPID := h.processes[process].cmd.Process.Pid
		h.restartProcess(t, process)
		waitForPort(t, []int{h.ports.org0Orch, h.ports.org1Orch}[org])
		restartedPIDs[process] = []int{oldPID, h.processes[process].cmd.Process.Pid}
	}
	recovered := h.gatewaySDK(t, 0, 0).GetNetwork(channelID).GetContract(name + ":" + namespace)
	proposal, err := recovered.NewProposal("put", gwclient.WithBytesArguments([]byte(key), value))
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := proposal.EndorseWithContext(ctx)
	if err != nil || !bytes.Equal(transaction.Result(), value) {
		t.Fatalf("Endorse did not recover definitions and endpoints after restart: %v", err)
	}
	transactions = append(transactions, transaction.TransactionID())
	assertHeightUnchanged(t, h, before)
	afterEndorsement := h.blockHeight(t)
	// Custom Invoke still submits and commits through the shared endorsement stage.
	signed := initSignedProposal(t, h, name, false, "put", key, string(value))
	response, err := h.processSigned(ctx, 0, orchestrator.GRPCOperationInvoke, signed)
	if err != nil {
		t.Fatal(err)
	}
	var committed orchestrator.InvocationResponse
	if err := json.Unmarshal(response.Response.Payload, &committed); err != nil || committed.CommitStatus != "COMMITTED" || len(committed.LifecycleBindings) != 2 {
		t.Fatalf("custom Invoke did not commit after refactoring: %+v %v", committed, err)
	}
	h.waitForHeight(t, committed.BlockNum+1)
	reader := h.gatewaySDK(t, 0, 0).GetNetwork(channelID).GetContract(name + ":" + namespace)
	if result, err := reader.EvaluateWithContext(ctx, "get", gwclient.WithArguments(key)); err != nil || !bytes.Equal(result, value) {
		t.Fatalf("custom Invoke lost its committed result: %x %v", result, err)
	}
	dir := filepath.Join(h.root, "runtime", "compatibility_service", "gateway-endorse")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	preparedBytes, err := proto.Marshal(finalSigned)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "signed-prepared-transaction.pb"), preparedBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	report, err := json.MarshalIndent(map[string]any{
		"sdk_version": "v1.12.1", "gateway_contract": name + ":" + namespace, "committed_definition": definition,
		"endorsed_transaction_ids": transactions, "height_before_endorsement": before, "height_after_endorsement": afterEndorsement,
		"both_initiating_organizations_tested": true, "native_signatures_verified": true, "client_envelope_signatures_verified": true,
		"transient_stripped_from_prepared_proposal": true, "original_client_context_preserved": true, "submit_not_called": true,
		"restarted_orchestrator_pids": restartedPIDs, "endorse_after_restart_passed": true,
		"custom_invoke": committed,
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result.json"), append(report, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("Gateway SDK Endorse passed: contract=%s:%s endorsement_height=%d (unchanged); custom Invoke committed block=%d", name, namespace, before, committed.BlockNum)
}

func TestGatewaySubmitSDK(t *testing.T) {
	name := fmt.Sprintf("gateway_submit_%d", time.Now().UnixNano())
	h := newHarness(t, harnessOptions{
		name: "gateway-submit", sampleChaincode: true, proposalPolicy: "OR('org-0.member')", requestWait: "60s", finality: "25s",
		chaincodeDefinition: &lifecycle.ChaincodeDefinition{Name: name, Version: "2.0", Sequence: 1},
	})
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	definition := h.deployGatewayDefinition(t, ctx, name, "2.0")
	key, value := uniqueKey(t, "gateway-submitted"), []byte{0, 255, 254, 128, 'o', 'k'}
	client0 := h.gatewaySDK(t, 0, 0)
	contract0 := client0.GetNetwork(channelID).GetContract(name + ":" + namespace)
	proposal, err := contract0.NewProposal("put", gwclient.WithBytesArguments([]byte(key), value), gwclient.WithTransient(map[string][]byte{"secret": []byte("not-recorded-by-submit")}))
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := proposal.EndorseWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	serialized, err := transaction.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	prepared := &gateway.PreparedTransaction{}
	if err := proto.Unmarshal(serialized, prepared); err != nil {
		t.Fatal(err)
	}
	native, _ := gatewayNativeTransaction(t, prepared.Envelope)
	if len(native.Metadata) != 3 || len(native.Endorsements[0].EndorsementsWithIdentity) != 2 || bytes.Contains(serialized, []byte("not-recorded-by-submit")) {
		t.Fatal("prepared transaction lost its signed binding, endorsements, or transient isolation")
	}
	for _, org := range []int{0, 1} {
		h.processes[fmt.Sprintf("chaincode-org%d", org)].stop()
	}
	oldPID := h.processes["orchestrator-org1"].cmd.Process.Pid
	// Keep the receiving instance's status unknown during pending retries. A
	// retry after confirmed completion is deliberately outside the local tracker.
	h.finalityProxy = h.startDeferredFinality(t, h.processes["orchestrator-org1"].cmd.Args[2])
	h.restartProcess(t, "orchestrator-org1")
	waitForPort(t, h.ports.org1Orch)
	newPID := h.processes["orchestrator-org1"].cmd.Process.Pid
	client1 := h.gatewaySDK(t, 1, 0)
	before := h.blockHeight(t)
	forged, err := client1.NewSignedTransaction(serialized, []byte("invalid-client-envelope-signature"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := forged.SubmitWithContext(ctx); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Submit accepted an invalid client signature: %v", err)
	}
	assertHeightUnchanged(t, h, before)
	// Submit to a different, restarted Orchestrator while both CCaaS services
	// are stopped. The signed prepared envelope is sufficient for submission.
	transaction, err = client1.NewTransaction(serialized)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	commit, err := transaction.SubmitWithContext(ctx)
	ackLatency := time.Since(started)
	if err != nil || commit.TransactionID() != proposal.TransactionID() {
		t.Fatalf("SDK Submit failed without an Endorse session or running chaincode: %v", err)
	}
	signedBytes, err := transaction.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		retry, err := client1.NewTransaction(signedBytes)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := retry.SubmitWithContext(ctx); err != nil {
			t.Fatalf("SDK Submit retry failed: %v", err)
		}
	}
	h.finalityProxy.visible.Store(true)
	committedStatus, err := commit.StatusWithContext(ctx)
	if err != nil || !committedStatus.Successful || committedStatus.TransactionID != proposal.TransactionID() {
		t.Fatalf("Gateway CommitStatus failed: %+v %v", committedStatus, err)
	}
	confirmed := h.waitForLedgerStatus(t, ctx, proposal.TransactionID())
	if committedStatus.BlockNumber != confirmed.Ref.BlockNum {
		t.Fatal("Gateway and native finality disagree on block number")
	}
	h.waitForHeight(t, confirmed.Ref.BlockNum+1)
	connection, err := h.blockQueryConn()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close() //nolint:errcheck
	block, err := committerpb.NewBlockQueryServiceClient(connection).GetBlockByNumber(ctx, &committerpb.BlockNumber{Number: confirmed.Ref.BlockNum})
	if err != nil {
		t.Fatal(err)
	}
	orderedEnvelope := &common.Envelope{}
	if err := proto.Unmarshal(block.Data.Data[confirmed.Ref.TxNum], orderedEnvelope); err != nil {
		t.Fatal(err)
	}
	orderedPayload, err := protoutil.UnmarshalPayload(orderedEnvelope.Payload)
	if err != nil {
		t.Fatal(err)
	}
	// The result bytes in the prepared action must be exactly the native bytes
	// recorded in the actual block, including both organization signatures.
	preparedPayload, _ := protoutil.UnmarshalPayload(prepared.Envelope.Payload)
	preparedTransaction := &peer.Transaction{}
	if err := proto.Unmarshal(preparedPayload.Data, preparedTransaction); err != nil {
		t.Fatal(err)
	}
	actionPayload := &peer.ChaincodeActionPayload{}
	if err := proto.Unmarshal(preparedTransaction.Actions[0].Payload, actionPayload); err != nil {
		t.Fatal(err)
	}
	responsePayload := &peer.ProposalResponsePayload{}
	if err := proto.Unmarshal(actionPayload.Action.ProposalResponsePayload, responsePayload); err != nil {
		t.Fatal(err)
	}
	action := &peer.ChaincodeAction{}
	if err := proto.Unmarshal(responsePayload.Extension, action); err != nil || !bytes.Equal(action.Results, orderedPayload.Data) {
		t.Fatalf("ordered native bytes differ from the client-signed transaction: %v", err)
	}
	logs, err := filepath.Glob(filepath.Join(h.root, "runtime", "compatibility_service", "e2e", sanitize(t.Name())+"-orchestrator-org1*.log"))
	if err != nil {
		t.Fatal(err)
	}
	submissions := 0
	for _, path := range logs {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		submissions += strings.Count(string(data), "tx="+proposal.TransactionID()+" submitting endorsed")
	}
	if submissions != 1 {
		t.Fatalf("SDK retries submitted the transaction %d times", submissions)
	}
	for _, org := range []int{0, 1} {
		h.restartProcess(t, fmt.Sprintf("chaincode-org%d", org))
		waitForPort(t, []int{h.ports.org0CC, h.ports.org1CC}[org])
	}
	contract1 := client1.GetNetwork(channelID).GetContract(name + ":" + namespace)
	if result, err := contract1.EvaluateWithContext(ctx, "get", gwclient.WithArguments(key)); err != nil || !bytes.Equal(result, value) {
		t.Fatalf("Gateway-submitted state was not committed: %x %v", result, err)
	}
	// Explicitly requesting only org0 yields a valid individual endorsement,
	// but Submit must reject it against this namespace's two-org policy.
	incomplete, err := contract0.NewProposal("put", gwclient.WithArguments(key+"-incomplete", "rejected"), gwclient.WithEndorsingOrganizations("org-0"))
	if err != nil {
		t.Fatal(err)
	}
	underendorsed, err := incomplete.EndorseWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	height := h.blockHeight(t)
	if _, err := underendorsed.SubmitWithContext(ctx); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Submit bypassed the namespace endorsement policy: %v", err)
	}
	assertHeightUnchanged(t, h, height)
	dir := filepath.Join(h.root, "runtime", "compatibility_service", "gateway-submit")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "signed-prepared-transaction.pb"), signedBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "block.pb"), protoutil.MarshalOrPanic(block), 0o600); err != nil {
		t.Fatal(err)
	}
	report, err := json.MarshalIndent(map[string]any{
		"sdk_version": "v1.12.1", "gateway_contract": name + ":" + namespace, "committed_definition": definition,
		"tx_id": proposal.TransactionID(), "status": confirmed.Status.String(), "block_num": confirmed.Ref.BlockNum,
		"submit_ack_latency_ms": ackLatency.Milliseconds(), "submissions_including_retries": submissions,
		"endorsing_gateway_org": "org-0", "submitting_gateway_org": "org-1", "submitter_restart_pids": []int{oldPID, newPID},
		"ccaas_stopped_during_submit": true, "byte_identical_native_transaction": true, "binary_payload_base64": base64.StdEncoding.EncodeToString(value),
		"invalid_client_signature_rejected": true, "insufficient_namespace_endorsements_rejected": true,
		"gateway_commit_status": committedStatus, "finality_checked_through": "Gateway CommitStatus and Fabric-X Query Service",
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result.json"), append(report, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("Gateway SDK Submit committed block=%d tx=%s; native bytes preserved; retries submitted once; CCaaS was offline", confirmed.Ref.BlockNum, proposal.TransactionID())
}

func TestGatewayCommitStatusSDK(t *testing.T) {
	name := fmt.Sprintf("gateway_status_%d", time.Now().UnixNano())
	h := newHarness(t, harnessOptions{
		name: "gateway-commit-status", sampleChaincode: true, requestWait: "45s", finality: "25s",
		chaincodeDefinition: &lifecycle.ChaincodeDefinition{Name: name, Version: "1.0", Sequence: 1},
	})
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	h.deployGatewayDefinition(t, ctx, name, "1.0")
	client0, client1 := h.gatewaySDK(t, 0, 0), h.gatewaySDK(t, 1, 1)
	contract := client0.GetNetwork(channelID).GetContract(name + ":" + namespace)
	key, value := uniqueKey(t, "gateway-status"), []byte{0, 255, 128, 'o', 'k'}
	proposal, err := contract.NewProposal("put", gwclient.WithBytesArguments([]byte(key), value))
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := proposal.EndorseWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := h.gatewayStatusBytes(t, 0, proposal.TransactionID())
	commit, err := client0.NewCommit(unsigned)
	if err != nil {
		t.Fatal(err)
	}
	type statusResult struct {
		status *gwclient.Status
		err    error
	}
	waiting := make(chan statusResult, 1)
	go func() { result, err := commit.StatusWithContext(ctx); waiting <- statusResult{result, err} }()
	// The transaction is endorsed but has not been submitted. A status request
	// must wait rather than report success, rejection, or a local-cache miss.
	select {
	case result := <-waiting:
		t.Fatalf("unsubmitted transaction returned a final outcome: %+v", result)
	case <-time.After(200 * time.Millisecond):
	}
	if _, err := transaction.SubmitWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	var committed *gwclient.Status
	select {
	case result := <-waiting:
		if result.err != nil || !result.status.Successful {
			t.Fatalf("pending status request failed: %+v %v", result.status, result.err)
		}
		committed = result.status
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	native := h.waitForLedgerStatus(t, ctx, proposal.TransactionID())
	if native.Ref.BlockNum != committed.BlockNumber {
		t.Fatal("SDK returned a wrong final block number")
	}
	// A restarted, different organization has no local submission record. It
	// must still return historical status using a different authorized reader.
	oldPID := h.processes["orchestrator-org1"].cmd.Process.Pid
	h.restartProcess(t, "orchestrator-org1")
	waitForPort(t, h.ports.org1Orch)
	historical, err := client1.NewCommit(h.gatewayStatusBytes(t, 1, proposal.TransactionID()))
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := historical.StatusWithContext(ctx)
	if err != nil || !recovered.Successful || recovered.BlockNumber != committed.BlockNumber {
		t.Fatalf("historical status after restart failed: %+v %v", recovered, err)
	}
	forged, err := client0.NewSignedCommit(unsigned, []byte("forged-status-signature"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := forged.StatusWithContext(ctx); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("forged status request accepted: %v", err)
	}
	// Standard SDK convenience submission now includes CommitStatus and returns
	// only after the write has a successful validation result.
	if _, err := contract.SubmitWithContext(ctx, "put", gwclient.WithArguments(key+"-combined", "combined-value")); err != nil {
		t.Fatalf("complete SDK submit-and-wait failed: %v", err)
	}
	if result, err := contract.EvaluateWithContext(ctx, "get", gwclient.WithArguments(key+"-combined")); err != nil || string(result) != "combined-value" {
		t.Fatalf("combined submission did not commit state: %q %v", result, err)
	}
	// A read-only simulation can be endorsed and ordered, but native validation
	// rejects it for having no writes. This must be a failed commit, not an RPC
	// timeout or a manufactured VALID result.
	rejectedProposal, err := contract.NewProposal("get", gwclient.WithArguments(key))
	if err != nil {
		t.Fatal(err)
	}
	rejectedTx, err := rejectedProposal.EndorseWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rejectedCommit, err := rejectedTx.SubmitWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := rejectedCommit.StatusWithContext(ctx)
	if err != nil || rejected.Successful || rejected.Code != peer.TxValidationCode_INVALID_WRITESET {
		t.Fatalf("native rejection lost: %+v %v", rejected, err)
	}
	// The combined SDK helper must also surface this as its CommitError.
	_, err = contract.SubmitWithContext(ctx, "get", gwclient.WithArguments(key))
	var commitError *gwclient.CommitError
	if !errors.As(err, &commitError) || commitError.Code != peer.TxValidationCode_INVALID_WRITESET {
		t.Fatalf("SDK did not report validation failure as CommitError: %v", err)
	}
	dir := filepath.Join(h.root, "runtime", "compatibility_service", "gateway-commit-status")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	blockConnection, err := h.blockQueryConn()
	if err != nil {
		t.Fatal(err)
	}
	defer blockConnection.Close() //nolint:errcheck
	block, err := committerpb.NewBlockQueryServiceClient(blockConnection).GetBlockByNumber(ctx, &committerpb.BlockNumber{Number: committed.BlockNumber})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "block.pb"), protoutil.MarshalOrPanic(block), 0o600); err != nil {
		t.Fatal(err)
	}
	report, err := json.MarshalIndent(map[string]any{
		"sdk_version": "v1.12.1", "contract": name + ":" + namespace, "pending_status": committed,
		"historical_status_after_restart": recovered, "restarted_org1_pids": []int{oldPID, h.processes["orchestrator-org1"].cmd.Process.Pid},
		"rejected_status": rejected, "invalid_status_signature_rejected": true, "combined_sdk_submit_and_wait": true,
		"combined_sdk_rejection_is_commit_error": true,
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result.json"), append(report, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("Gateway CommitStatus: pending -> VALID block=%d tx=%s; historical lookup after org1 restart; rejection=%s; full SDK submit-and-wait passed", committed.BlockNumber, proposal.TransactionID(), rejected.Code)
}

func (h *e2eHarness) gatewayStatusBytes(t *testing.T, signerOrg int, txID string) []byte {
	t.Helper()
	id, _ := h.gatewaySigningIdentity(t, signerOrg)
	request := &gateway.CommitStatusRequest{ChannelId: channelID, TransactionId: txID, Identity: protoutil.MarshalOrPanic(&mspapi.SerializedIdentity{Mspid: id.MspID(), IdBytes: id.Credentials()})}
	return protoutil.MarshalOrPanic(&gateway.SignedCommitStatusRequest{Request: protoutil.MarshalOrPanic(request)})
}

func TestGatewayChaincodeEventsSDK(t *testing.T) {
	name := fmt.Sprintf("gateway_events_%d", time.Now().UnixNano())
	h := newHarness(t, harnessOptions{name: "gateway-events", sampleChaincode: true, requestWait: "60s", finality: "25s", chaincodeDefinition: &lifecycle.ChaincodeDefinition{Name: name, Version: "1.0", Sequence: 1}})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	h.deployGatewayDefinition(t, ctx, name, "1.0")
	otherName := name + "_other"
	h.deployGatewayDefinition(t, ctx, otherName, "1.0")
	producer, observer := h.gatewaySDK(t, 0, 0), h.gatewaySDK(t, 1, 1)
	route := name + ":" + namespace
	liveCtx, stopLive := context.WithCancel(ctx)
	defer stopLive()
	events, err := observer.GetNetwork(channelID).ChaincodeEvents(liveCtx, route)
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(h.root, "runtime", "compatibility_service", "e2e", sanitize(t.Name())+"-orchestrator-org1.log")
	deadline := time.Now().Add(10 * time.Second)
	for {
		data, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "Gateway events subscribed channel="+channelID+" chaincode="+route+" ") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Gateway did not establish the live subscription")
		}
		time.Sleep(20 * time.Millisecond)
	}
	submitEvent := func(chaincode, label string) (*gwclient.Status, string) {
		t.Helper()
		key := uniqueKey(t, label)
		proposal, err := producer.GetNetwork(channelID).GetContract(chaincode+":"+namespace).NewProposal("compat-v3", gwclient.WithArguments(key, "event-value-"+label, key+"-delete"))
		if err != nil {
			t.Fatal(err)
		}
		transaction, err := proposal.EndorseWithContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		commit, err := transaction.SubmitWithContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, err := commit.StatusWithContext(ctx)
		if err != nil || !result.Successful {
			t.Fatalf("event transaction did not commit: %+v %v", result, err)
		}
		return result, key
	}
	first, key := submitEvent(name, "live")
	firstEvent := receiveGatewayEvent(t, ctx, events)
	var eventData map[string]any
	if err := json.Unmarshal(firstEvent.Payload, &eventData); err != nil || firstEvent.TransactionID != first.TransactionID || firstEvent.BlockNumber != first.BlockNumber || firstEvent.ChaincodeName != route || firstEvent.EventName != "compat-v3-event" || eventData["key"] != key {
		t.Fatalf("SDK live event changed: %+v payload=%s error=%v", firstEvent, firstEvent.Payload, err)
	}
	other, _ := submitEvent(otherName, "same-namespace")
	select {
	case event := <-events:
		t.Fatalf("another chaincode's event leaked through the same namespace: %+v", event)
	case <-time.After(300 * time.Millisecond):
	}
	customKey := uniqueKey(t, "custom-event")
	response, err := h.processSigned(ctx, 0, orchestrator.GRPCOperationInvoke, initSignedProposal(t, h, name, false, "compat-v3", customKey, "custom-event-value", customKey+"-delete"))
	if err != nil {
		t.Fatal(err)
	}
	var custom orchestrator.InvocationResponse
	if err := json.Unmarshal(response.Response.Payload, &custom); err != nil || custom.CommitStatus != "COMMITTED" || custom.ChaincodeEvent == nil || custom.ChaincodeEvent.ChaincodeID != namespace {
		t.Fatalf("custom Invoke event response changed: %+v %v", custom, err)
	}
	secondEvent := receiveGatewayEvent(t, ctx, events)
	customPayload, err := base64.StdEncoding.DecodeString(custom.ChaincodeEvent.PayloadBase64)
	if err != nil || secondEvent.TransactionID != custom.TxID || secondEvent.BlockNumber != custom.BlockNum || secondEvent.ChaincodeName != route || !bytes.Equal(secondEvent.Payload, customPayload) || secondEvent.EventName != custom.ChaincodeEvent.EventName {
		t.Fatalf("shared event paths disagreed: %+v %v", secondEvent, err)
	}
	checkpoint := &gwclient.InMemoryCheckpointer{}
	checkpoint.CheckpointChaincodeEvent(firstEvent)
	stopLive()
	for range events {
	}
	for _, org := range []int{0, 1} {
		h.processes[fmt.Sprintf("chaincode-org%d", org)].stop()
	}
	oldPID := h.processes["orchestrator-org1"].cmd.Process.Pid
	h.restartProcess(t, "orchestrator-org1")
	waitForPort(t, h.ports.org1Orch)
	newPID := h.processes["orchestrator-org1"].cmd.Process.Pid
	resumeCtx, stopResume := context.WithCancel(ctx)
	defer stopResume()
	resumed, err := observer.GetNetwork(channelID).ChaincodeEvents(resumeCtx, route, gwclient.WithCheckpoint(checkpoint))
	if err != nil {
		t.Fatal(err)
	}
	replayed := receiveGatewayEvent(t, ctx, resumed)
	if replayed.TransactionID != custom.TxID || !bytes.Equal(replayed.Payload, secondEvent.Payload) {
		t.Fatalf("checkpoint repeated or skipped the wrong event: %+v", replayed)
	}
	stopResume()
	for range resumed {
	}
	replayCtx, stopReplay := context.WithCancel(ctx)
	defer stopReplay()
	history, err := observer.GetNetwork(channelID).ChaincodeEvents(replayCtx, route, gwclient.WithStartBlock(0))
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []*gwclient.ChaincodeEvent{firstEvent, secondEvent} {
		actual := receiveGatewayEvent(t, ctx, history)
		if actual.TransactionID != expected.TransactionID || actual.BlockNumber != expected.BlockNumber || !bytes.Equal(actual.Payload, expected.Payload) {
			t.Fatalf("historical event replay changed ordering or payload: %+v", actual)
		}
	}
	stopReplay()
	for range history {
	}
	// Invalid signatures are rejected on the actual mTLS Gateway endpoint.
	connection := h.gatewayConnection(t, 1)
	request, err := observer.GetNetwork(channelID).NewChaincodeEventsRequest(route, gwclient.WithStartBlock(first.BlockNumber))
	if err != nil {
		t.Fatal(err)
	}
	serialized, _ := request.Bytes()
	signed := &gateway.SignedChaincodeEventsRequest{}
	_ = proto.Unmarshal(serialized, signed)
	signed.Signature = []byte("forged-event-reader-signature")
	forged, err := gateway.NewGatewayClient(connection.Connection()).ChaincodeEvents(ctx, signed)
	if err == nil {
		_, err = forged.Recv()
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("invalid event reader signature accepted: %v", err)
	}
	short, stopShort := context.WithTimeout(ctx, 250*time.Millisecond)
	defer stopShort()
	before := h.blockHeight(t)
	idle, err := observer.GetNetwork(channelID).ChaincodeEvents(short, route, gwclient.WithStartBlock(before))
	if err != nil {
		t.Fatal(err)
	}
	select {
	case event, ok := <-idle:
		if ok {
			t.Fatalf("idle stream fabricated event: %+v", event)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SDK stream did not close after deadline")
	}
	assertHeightUnchanged(t, h, before)
	// Read the actual blocks and verify both organization signatures cover the
	// new identity record, for both Gateway and custom invocation transactions.
	blockConnection, err := h.blockQueryConn()
	if err != nil {
		t.Fatal(err)
	}
	defer blockConnection.Close() //nolint:errcheck
	trust, err := auth.NewVerifier([]auth.MSPConfig{{MSPID: "org-0", MSPDir: h.artifact("peerOrganizations", "peer-org-0", "msp")}, {MSPID: "org-1", MSPDir: h.artifact("peerOrganizations", "peer-org-1", "msp")}})
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(h.root, "runtime", "compatibility_service", "gateway-events")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, committed := range []struct {
		id    string
		block uint64
		slots int
		file  string
	}{{first.TransactionID, first.BlockNumber, 3, "gateway-block.pb"}, {custom.TxID, custom.BlockNum, 2, "custom-block.pb"}} {
		block, err := committerpb.NewBlockQueryServiceClient(blockConnection).GetBlockByNumber(ctx, &committerpb.BlockNumber{Number: committed.block})
		if err != nil {
			t.Fatal(err)
		}
		if len(block.Data.Data) != 1 {
			t.Fatal("expected isolated test transaction block")
		}
		envelope := &common.Envelope{}
		_ = proto.Unmarshal(block.Data.Data[0], envelope)
		payload, err := protoutil.UnmarshalPayload(envelope.Payload)
		if err != nil {
			t.Fatal(err)
		}
		native := &applicationpb.Tx{}
		if err := proto.Unmarshal(payload.Data, native); err != nil {
			t.Fatal(err)
		}
		identity, err := helper.ChaincodeIdentityFromMetadata(native.Metadata)
		if err != nil || identity == nil || identity.ChaincodeName != name || identity.Namespace != namespace || identity.ChannelID != channelID || len(native.Metadata) != committed.slots {
			t.Fatalf("signed event identity missing: %+v %v", identity, err)
		}
		data, err := native.Namespaces[0].ASN1Marshal(committed.id, native.Metadata)
		if err != nil {
			t.Fatal(err)
		}
		if len(native.Endorsements[0].EndorsementsWithIdentity) != 2 {
			t.Fatal("event metadata lost an organization endorsement")
		}
		for _, endorsement := range native.Endorsements[0].EndorsementsWithIdentity {
			if _, err := trust.Authenticate(protoutil.MarshalOrPanic(endorsement.Identity), data, endorsement.Endorsement, time.Now()); err != nil {
				t.Fatalf("event identity not covered by native endorsement: %v", err)
			}
		}
		if err := os.WriteFile(filepath.Join(dir, committed.file), protoutil.MarshalOrPanic(block), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	report, err := json.MarshalIndent(map[string]any{
		"sdk": "Go Fabric Gateway v1.12.1", "route": route, "shared_namespace_other_route": otherName + ":" + namespace,
		"live_event": firstEvent, "custom_invoke_event": custom.ChaincodeEvent, "gateway_event_for_custom_invoke": secondEvent,
		"excluded_transaction": other, "checkpoint_replayed_event": replayed, "restart_pids": []int{oldPID, newPID},
		"ccaas_stopped_during_replay": true, "native_identity_signatures_verified": true, "forged_reader_rejected": true,
		"subscriber_deadline_closed_stream": true, "replay_did_not_generate_blocks": true,
		"replay_start_block": 0,
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "result.json"), append(report, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("Gateway events: live block=%d custom block=%d other-chaincode block=%d filtered; checkpoint and replay after org1 restart passed with CCaaS offline", first.BlockNumber, custom.BlockNum, other.BlockNumber)
}

func receiveGatewayEvent(t *testing.T, ctx context.Context, events <-chan *gwclient.ChaincodeEvent) *gwclient.ChaincodeEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("SDK chaincode event stream closed before expected event")
		}
		return event
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return nil
	}
}

func gatewayNativeTransaction(t *testing.T, envelope *common.Envelope) (*applicationpb.Tx, *common.Payload) {
	t.Helper()
	payload := &common.Payload{}
	if err := proto.Unmarshal(envelope.Payload, payload); err != nil {
		t.Fatal(err)
	}
	transaction := &peer.Transaction{}
	if err := proto.Unmarshal(payload.Data, transaction); err != nil || len(transaction.Actions) != 1 {
		t.Fatalf("invalid prepared Fabric transaction container: %v", err)
	}
	actionPayload := &peer.ChaincodeActionPayload{}
	if err := proto.Unmarshal(transaction.Actions[0].Payload, actionPayload); err != nil {
		t.Fatal(err)
	}
	proposal := &peer.ChaincodeProposalPayload{}
	if err := proto.Unmarshal(actionPayload.ChaincodeProposalPayload, proposal); err != nil || len(proposal.TransientMap) != 0 {
		t.Fatalf("prepared proposal contains transient data: %v", err)
	}
	hash, err := protoutil.GetProposalHash2(payload.Header, actionPayload.ChaincodeProposalPayload)
	if err != nil {
		t.Fatal(err)
	}
	response := &peer.ProposalResponsePayload{}
	if err := proto.Unmarshal(actionPayload.Action.ProposalResponsePayload, response); err != nil || !bytes.Equal(hash, response.ProposalHash) {
		t.Fatalf("prepared response lost the proposal binding: %v", err)
	}
	action := &peer.ChaincodeAction{}
	if err := proto.Unmarshal(response.Extension, action); err != nil {
		t.Fatal(err)
	}
	native := &applicationpb.Tx{}
	if err := proto.Unmarshal(action.Results, native); err != nil || len(native.Namespaces) != 1 || native.Namespaces[0].NsId != namespace || len(native.Endorsements) != 1 {
		t.Fatalf("invalid native transaction in Gateway envelope: %v", err)
	}
	return native, payload
}

// deployGatewayDefinition uses the normal folder-install lifecycle for SDK tests.
func (h *e2eHarness) deployGatewayDefinition(t *testing.T, ctx context.Context, name, version string) string {
	t.Helper()
	h.approveDefinition(t, ctx, lifecycle.ChaincodeDefinition{Name: name, Version: version, Sequence: 1})
	return runInitCommand(t, ctx, h, "./bin/orchestrator", "lifecycle", "commit", "-c", "sampleconfig/admin-org0.yaml", "--host", fmt.Sprintf("127.0.0.1:%d", h.ports.org0Orch), "-n", name, "-v", version, "--sequence", "1")
}

func (h *e2eHarness) approveDefinition(t *testing.T, ctx context.Context, def lifecycle.ChaincodeDefinition) {
	t.Helper()
	name, version := def.Name, def.Version
	ports := []int{h.ports.org0Orch, h.ports.org1Orch}
	for org, port := range ports {
		source := t.TempDir()
		connection := fmt.Sprintf(`{"address":"127.0.0.1:%d","tls_required":false}`, []int{h.ports.org0CC, h.ports.org1CC}[org])
		if err := os.WriteFile(filepath.Join(source, "connection.json"), []byte(connection), 0o600); err != nil {
			t.Fatal(err)
		}
		output := runInitCommand(t, ctx, h, "./bin/orchestrator", "lifecycle", "install", "-c", fmt.Sprintf("sampleconfig/admin-org%d.yaml", org), "--host", fmt.Sprintf("127.0.0.1:%d", port), "--path", source, "--label", fmt.Sprintf("%s_org%d", name, org))
		packageID := ""
		for _, line := range strings.Split(output, "\n") {
			if id, ok := strings.CutPrefix(line, "package_id="); ok {
				packageID = id
			}
		}
		if packageID == "" {
			t.Fatalf("missing installed package ID: %s", output)
		}
		args := []string{"lifecycle", "approveformyorg", "-c", fmt.Sprintf("sampleconfig/admin-org%d.yaml", org), "--host", fmt.Sprintf("127.0.0.1:%d", port), "-n", name, "-v", version, "--sequence", fmt.Sprint(def.Sequence), "--package-id", packageID}
		if def.InitRequired {
			args = append(args, "--init-required")
		}
		runInitCommand(t, ctx, h, "./bin/orchestrator", args...)
	}
}

// The ordinary SDK signer uses its default SHA-256 hash and the existing MSP
// private key. The TLS caller remains org0 even when the proposal signer is org1.
func (h *e2eHarness) gatewaySDK(t *testing.T, serverOrg, signerOrg int) *gwclient.Gateway {
	t.Helper()
	id, sign := h.gatewaySigningIdentity(t, signerOrg)
	connection := h.gatewayConnection(t, serverOrg)
	client, err := gwclient.Connect(id, gwclient.WithClientConnection(connection.Connection()), gwclient.WithSign(sign), gwclient.WithEvaluateTimeout(20*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func (h *e2eHarness) gatewayConnection(t *testing.T, serverOrg int) *network.Peer {
	t.Helper()
	connection, err := network.NewPeer(network.PeerConf{Address: fmt.Sprintf("127.0.0.1:%d", []int{h.ports.org0Orch, h.ports.org1Orch}[serverOrg]), TLS: network.TLSConfig{
		Mode: network.TLSModeMTLS, CertPath: h.clientTLSCert(0), KeyPath: h.clientTLSKey(0), CACertPaths: []string{h.peerTLSCA(serverOrg)}, ServerName: "127.0.0.1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func (h *e2eHarness) gatewaySigningIdentity(t *testing.T, signerOrg int) (gwidentity.Identity, gwidentity.Sign) {
	t.Helper()
	certs, err := filepath.Glob(filepath.Join(h.clientMSP(signerOrg), "signcerts", "*.pem"))
	if err != nil || len(certs) != 1 {
		t.Fatalf("find SDK enrollment certificate: %v %v", certs, err)
	}
	certificatePEM, err := os.ReadFile(certs[0])
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := gwidentity.CertificateFromPEM(certificatePEM)
	if err != nil {
		t.Fatal(err)
	}
	id, err := gwidentity.NewX509Identity(fmt.Sprintf("org-%d", signerOrg), certificate)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := filepath.Glob(filepath.Join(h.clientMSP(signerOrg), "keystore", "*_sk"))
	if err != nil || len(keys) != 1 {
		t.Fatalf("find SDK enrollment key: %v %v", keys, err)
	}
	keyPEM, err := os.ReadFile(keys[0])
	if err != nil {
		t.Fatal(err)
	}
	key, err := gwidentity.PrivateKeyFromPEM(keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	sign, err := gwidentity.NewPrivateKeySign(key)
	if err != nil {
		t.Fatal(err)
	}
	return id, sign
}
