// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"testing"
	"time"

	"compatibility_service/pkg/helper"
	"compatibility_service/pkg/lifecycle"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	efab "github.com/hyperledger/fabric-x-sdk/endorsement/fabric"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestInitIntentComesFromAuthenticatedProposal(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	client := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	service := &Service{
		cfg: Config{ChannelID: "channelqc4", Namespace: "1"}, logger: sdk.NoOpLogger{},
		proposalAuth: proposalTestVerifier(t, []ProposalMSPConfig{org.config}, "OR('org-0.client')"),
	}
	for _, isInit := range []bool{false, true} {
		signed := initTestProposal(t, client.signer, isInit)
		inv, err := efab.Parse(signed, now)
		if err != nil {
			t.Fatal(err)
		}
		request, err := requestFromProposal(inv)
		if err != nil || request.IsInit != isInit {
			t.Fatalf("parsed init intent = %t, %v; want %t", request.IsInit, err, isInit)
		}
		for _, operation := range []string{GRPCOperationInvoke, GRPCOperationQuery, GRPCOperationEndorse} {
			for _, value := range []string{"true", "false", "invalid"} {
				if (value == "true") == isInit && value != "invalid" {
					continue
				}
				ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(GRPCOperationMetadata, operation, GRPCInitMetadata, value))
				if _, err := service.ProcessProposal(ctx, signed); status.Code(err) != codes.InvalidArgument {
					t.Fatalf("signed init=%t operation=%s metadata=%q: %v", isInit, operation, value, err)
				}
			}
		}
		// Changing the flag without signing again must fail authentication,
		// including for remote execute-only endorsement requests.
		tampered := setInitTestFlag(t, signed, !isInit)
		for _, operation := range []string{GRPCOperationInvoke, GRPCOperationEndorse} {
			ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(GRPCOperationMetadata, operation))
			if _, err := service.ProcessProposal(ctx, tampered); status.Code(err) != codes.Unauthenticated {
				t.Fatalf("tampered init intent was not rejected: %v", err)
			}
		}
	}
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs(GRPCOperationMetadata, GRPCOperationQuery))
	if _, err := service.ProcessProposal(ctx, initTestProposal(t, client.signer, true)); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("query accepted signed init intent: %v", err)
	}
}

func TestInitStateRequiresCommittedUninitializedDefinition(t *testing.T) {
	for _, scenario := range []string{"uninitialized", "initialized", "not-required", "missing"} {
		t.Run(scenario, func(t *testing.T) {
			store, err := lifecycle.NewMemoryStore()
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close() //nolint:errcheck
			def := lifecycle.ChaincodeDefinition{Name: "sample", Version: "1.0", Sequence: 1, InitRequired: scenario != "not-required"}
			if scenario != "missing" {
				if _, err := store.UpsertCommittedFromLedger(t.Context(), def, scenario == "initialized" || scenario == "not-required"); err != nil {
					t.Fatal(err)
				}
			}
			service := &Service{lifecycle: store}
			request := InvocationRequest{ChaincodeName: def.Name, ChaincodeVersion: def.Version, IsInit: true}
			for _, operation := range []string{GRPCOperationInvoke, GRPCOperationEndorse} {
				got, err := service.validateInitState(t.Context(), request, "1", operation)
				if scenario == "uninitialized" {
					if err != nil || got == nil || *got != def {
						t.Fatalf("valid initialization rejected: definition=%v error=%v", got, err)
					}
				} else if err == nil {
					t.Fatalf("initialization accepted for %s definition", scenario)
				}
			}
			request.IsInit = false
			for _, operation := range []string{GRPCOperationInvoke, GRPCOperationQuery, GRPCOperationEndorse} {
				_, err := service.validateInitState(t.Context(), request, "1", operation)
				if (err != nil) != (scenario == "uninitialized") {
					t.Fatalf("normal %s init gate for %s: %v", operation, scenario, err)
				}
			}
		})
	}
}

func TestHelperProposalPreservesInitIntent(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	actor := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	service := &Service{cfg: Config{ChannelID: "channelqc4"}, signer: actor.signer}
	for _, isInit := range []bool{false, true} {
		for _, originalClient := range []bool{false, true} {
			input := helper.ClientProposalContext{IsInit: isInit}
			if originalClient {
				inv, err := efab.Parse(initTestProposal(t, actor.signer, isInit), now)
				if err != nil {
					t.Fatal(err)
				}
				input.Creator, input.Nonce = inv.Creator, inv.Nonce
			}
			signed, err := service.helperProposal("1", "1.0", [][]byte{[]byte("initialize")}, nil, input)
			if err != nil {
				t.Fatal(err)
			}
			proposal, err := protoutil.UnmarshalProposal(signed.ProposalBytes)
			if err != nil {
				t.Fatal(err)
			}
			got, err := helper.ProposalIsInit(proposal)
			if err != nil || got != isInit {
				t.Fatalf("helper init intent = %t, %v; want %t", got, err, isInit)
			}
		}
	}
}

func initTestProposal(t *testing.T, signer identity.Signer, isInit bool) *peer.SignedProposal {
	t.Helper()
	changed := setInitTestFlag(t, proposalTestSigned(t, signer, ""), isInit)
	proposal, err := protoutil.UnmarshalProposal(changed.ProposalBytes)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := protoutil.GetSignedProposal(proposal, signer)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func setInitTestFlag(t *testing.T, signed *peer.SignedProposal, isInit bool) *peer.SignedProposal {
	t.Helper()
	proposal, err := protoutil.UnmarshalProposal(signed.ProposalBytes)
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
	spec.ChaincodeSpec.Input.IsInit = isInit
	payload.Input = protoutil.MarshalOrPanic(spec)
	proposal.Payload = protoutil.MarshalOrPanic(payload)
	return &peer.SignedProposal{ProposalBytes: protoutil.MarshalOrPanic(proposal), Signature: signed.Signature}
}
