// SPDX-License-Identifier: Apache-2.0

package helper

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	"google.golang.org/protobuf/proto"
)

func TestGetStateIgnoresPendingWrites(t *testing.T) {
	for _, tc := range []struct {
		name      string
		committed VersionedValue
		pending   blocks.KVWrite
	}{
		{
			name:      "put existing key",
			committed: VersionedValue{Value: []byte("committed"), Version: &blocks.Version{BlockNum: 7}},
			pending:   blocks.KVWrite{Key: "asset", Value: []byte("pending")},
		},
		{
			name:    "put missing key",
			pending: blocks.KVWrite{Key: "asset", Value: []byte("pending")},
		},
		{
			name:      "delete existing key",
			committed: VersionedValue{Value: []byte("committed"), Version: &blocks.Version{BlockNum: 7}},
			pending:   blocks.KVWrite{Key: "asset", IsDelete: true},
		},
		{
			name:    "delete missing key",
			pending: blocks.KVWrite{Key: "asset", IsDelete: true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := &committerpb.View{Id: "execution-view"}
			calls := 0
			reader := stateReaderFunc(func(_ context.Context, gotView *committerpb.View, namespace, key string) (VersionedValue, error) {
				calls++
				if gotView != view || namespace != "1" || key != "asset" {
					t.Fatalf("unexpected committed-state read: view=%v namespace=%q key=%q", gotView, namespace, key)
				}
				return tc.committed, nil
			})
			execCtx := NewExecutionContextWithView(reader, "1", view)
			if tc.pending.IsDelete {
				execCtx.DelState(tc.pending.Key)
			} else {
				execCtx.PutState(tc.pending.Key, tc.pending.Value)
			}

			value, err := execCtx.GetState(context.Background(), "asset")
			if err != nil {
				t.Fatalf("GetState: %v", err)
			}
			if !bytes.Equal(value, tc.committed.Value) || (value == nil) != (tc.committed.Value == nil) {
				t.Fatalf("GetState = %q, want committed value %q", value, tc.committed.Value)
			}
			if calls != 1 {
				t.Fatalf("committed-state reads = %d, want 1", calls)
			}

			want := blocks.ReadWriteSet{
				Reads:  []blocks.KVRead{{Key: "asset", Version: tc.committed.Version}},
				Writes: []blocks.KVWrite{tc.pending},
			}
			if got := execCtx.Result(); !reflect.DeepEqual(got, want) {
				t.Fatalf("read/write set = %+v, want committed read dependency and pending write %+v", got, want)
			}
		})
	}
}

func TestGetStatePropagatesCommittedReadErrorWithPendingWrites(t *testing.T) {
	wantErr := errors.New("query service unavailable")
	for _, pending := range []blocks.KVWrite{
		{Key: "asset", Value: []byte("pending")},
		{Key: "asset", IsDelete: true},
	} {
		name := "put"
		if pending.IsDelete {
			name = "delete"
		}
		t.Run(name, func(t *testing.T) {
			reader := stateReaderFunc(func(context.Context, *committerpb.View, string, string) (VersionedValue, error) {
				return VersionedValue{}, wantErr
			})
			execCtx := NewExecutionContext(reader, "1")
			if pending.IsDelete {
				execCtx.DelState(pending.Key)
			} else {
				execCtx.PutState(pending.Key, pending.Value)
			}

			value, err := execCtx.GetState(context.Background(), "asset")
			if !errors.Is(err, wantErr) || value != nil {
				t.Fatalf("GetState = (%q, %v), want (nil, %v)", value, err, wantErr)
			}
			rws := execCtx.Result()
			if len(rws.Reads) != 0 || !reflect.DeepEqual(rws.Writes, []blocks.KVWrite{pending}) {
				t.Fatalf("read/write set after failed read = %+v; pending write must be preserved without a read dependency", rws)
			}
		})
	}
}

type stateReaderFunc func(context.Context, *committerpb.View, string, string) (VersionedValue, error)

func (f stateReaderFunc) BeginView(context.Context) (*committerpb.View, error) {
	return &committerpb.View{Id: "test-execution-view"}, nil
}

func (f stateReaderFunc) EndView(context.Context, *committerpb.View) error {
	return nil
}

func (f stateReaderFunc) GetState(ctx context.Context, view *committerpb.View, namespace, key string) (VersionedValue, error) {
	return f(ctx, view, namespace, key)
}

func TestProcessProposalPassesNamespacePolicyVersionToEndorsement(t *testing.T) {
	proposal, _, err := protoutil.CreateChaincodeProposal(common.HeaderType_ENDORSER_TRANSACTION, "channelqc4",
		&peer.ChaincodeInvocationSpec{ChaincodeSpec: &peer.ChaincodeSpec{ChaincodeId: &peer.ChaincodeID{Name: "1", Version: "1.0"}, Input: &peer.ChaincodeInput{Args: [][]byte{[]byte("put")}}}}, []byte("creator"))
	if err != nil {
		t.Fatal(err)
	}
	proposalBytes, err := proto.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	var seen endorsement.Invocation
	service := &Service{
		channel: "channelqc4", logger: sdk.NoOpLogger{},
		executors: map[string]Executor{"*": executionFunc(func(_ context.Context, _ *ExecutionContext, inv endorsement.Invocation) (endorsement.ExecutionResult, ExecutionMetadata, error) {
			return endorsement.Success(blocks.ReadWriteSet{}, "", nil, []byte("result")), ExecutionMetadata{}, nil
		})},
		builder: endorsementBuilderFunc(func(inv endorsement.Invocation, result endorsement.ExecutionResult) (*peer.ProposalResponse, error) {
			seen = inv
			return &peer.ProposalResponse{Response: result.Response()}, nil
		}),
	}
	ctx := WithClientProposal(context.Background(), ClientProposalContext{NamespaceVersion: 9})
	if _, err := service.ProcessProposal(ctx, &peer.SignedProposal{ProposalBytes: proposalBytes}); err != nil {
		t.Fatal(err)
	}
	if seen.NsVersion != 9 || seen.ChaincodeVersion != "1.0" {
		t.Fatalf("endorsement namespace version = %d, chaincode version = %q; want 9 and 1.0", seen.NsVersion, seen.ChaincodeVersion)
	}
}

type executionFunc func(context.Context, *ExecutionContext, endorsement.Invocation) (endorsement.ExecutionResult, ExecutionMetadata, error)

func (f executionFunc) Execute(ctx context.Context, state *ExecutionContext, inv endorsement.Invocation) (endorsement.ExecutionResult, ExecutionMetadata, error) {
	return f(ctx, state, inv)
}

type endorsementBuilderFunc func(endorsement.Invocation, endorsement.ExecutionResult) (*peer.ProposalResponse, error)

func (f endorsementBuilderFunc) Endorse(inv endorsement.Invocation, result endorsement.ExecutionResult) (*peer.ProposalResponse, error) {
	return f(inv, result)
}
