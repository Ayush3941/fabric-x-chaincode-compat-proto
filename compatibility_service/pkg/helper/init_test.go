// SPDX-License-Identifier: Apache-2.0

package helper

import (
	"context"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHelperUsesProposalInitFlagAndRejectsChangedIntent(t *testing.T) {
	proposals := make(map[bool]*peer.SignedProposal)
	for _, isInit := range []bool{false, true} {
		proposal, _, err := protoutil.CreateChaincodeProposal(common.HeaderType_ENDORSER_TRANSACTION, "channelqc4",
			&peer.ChaincodeInvocationSpec{ChaincodeSpec: &peer.ChaincodeSpec{
				ChaincodeId: &peer.ChaincodeID{Name: "1"}, Input: &peer.ChaincodeInput{Args: [][]byte{[]byte("initialize")}, IsInit: isInit},
			}}, []byte("creator"))
		if err != nil {
			t.Fatal(err)
		}
		proposals[isInit] = &peer.SignedProposal{ProposalBytes: protoutil.MarshalOrPanic(proposal)}
	}
	for _, isInit := range []bool{false, true} {
		executions := 0
		service := &Service{
			channel: "channelqc4", logger: sdk.NoOpLogger{},
			executors: map[string]Executor{"*": executionFunc(func(_ context.Context, state *ExecutionContext, _ endorsement.Invocation) (endorsement.ExecutionResult, ExecutionMetadata, error) {
				executions++
				if state.IsInit() != isInit {
					t.Fatalf("execution init intent = %t, want %t", state.IsInit(), isInit)
				}
				if executions == 2 {
					state.PutState("asset", []byte("uncommitted"))
					return endorsement.ExecutionResult{RWS: state.Result(), Status: 500, Message: "execution failed"}, ExecutionMetadata{}, nil
				}
				return endorsement.Success(blocks.ReadWriteSet{}, "", nil, nil), ExecutionMetadata{}, nil
			})},
			builder: endorsementBuilderFunc(func(_ endorsement.Invocation, result endorsement.ExecutionResult) (*peer.ProposalResponse, error) {
				if result.Status != 200 {
					t.Fatal("failed execution reached the endorsement signer")
				}
				return &peer.ProposalResponse{Response: result.Response()}, nil
			}),
		}
		ctx := WithClientProposal(t.Context(), ClientProposalContext{SignedProposal: proposals[isInit], IsInit: isInit})
		if _, err := service.ProcessProposal(ctx, proposals[isInit]); err != nil {
			t.Fatal(err)
		}
		mismatch := WithClientProposal(t.Context(), ClientProposalContext{SignedProposal: proposals[!isInit], IsInit: !isInit})
		if _, err := service.ProcessProposal(mismatch, proposals[isInit]); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("helper accepted different init intent in original proposal: %v", err)
		}
		if executions != 1 {
			t.Fatalf("helper executed a mismatched request: executions=%d", executions)
		}
		response, err := service.ProcessProposal(ctx, proposals[isInit])
		if err != nil || response.Response.Status != 500 || response.Endorsement != nil || len(response.Payload) != 0 {
			t.Fatalf("failed execution produced an endorsement: response=%v error=%v", response, err)
		}
	}
}
