// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"compatibility_service/pkg/orchestrator"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	// A cached successful transaction must not bypass authentication on retries.
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
