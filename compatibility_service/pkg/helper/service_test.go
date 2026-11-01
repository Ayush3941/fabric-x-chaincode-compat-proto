// SPDX-License-Identifier: Apache-2.0

package helper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
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

func TestExecutionViewPreservesSnapshotAcrossDifferentKeys(t *testing.T) {
	committed := map[string]*committerpb.Row{
		"asset": {Key: []byte("asset"), Value: []byte("old"), Version: 7},
		"other": {Key: []byte("other"), Value: []byte("other-old"), Version: 7},
	}
	views := make(map[string]map[string]*committerpb.Row)
	begins, reads, ends := 0, 0, 0
	client := &queryViewClient{
		begin: func(_ context.Context, params *committerpb.ViewParameters) (*committerpb.View, error) {
			if params.IsoLevel != committerpb.IsoLevel_REPEATABLE_READ {
				t.Fatalf("isolation = %v, want repeatable read", params.IsoLevel)
			}
			begins++
			id := fmt.Sprintf("view-%d", begins)
			views[id] = nil // Establish the snapshot on its first GetRows.
			return &committerpb.View{Id: id}, nil
		},
		rows: func(_ context.Context, query *committerpb.Query) (*committerpb.Rows, error) {
			reads++
			if query.View == nil || query.View.Id == "" {
				t.Fatal("state read has no view")
			}
			snapshot, ok := views[query.View.Id]
			if !ok {
				t.Fatal("state read uses an unknown view")
			}
			if snapshot == nil {
				snapshot = make(map[string]*committerpb.Row)
				for key, row := range committed {
					snapshot[key] = proto.Clone(row).(*committerpb.Row)
				}
				views[query.View.Id] = snapshot
			}
			result := &committerpb.RowsNamespace{NsId: query.Namespaces[0].NsId}
			for _, key := range query.Namespaces[0].Keys {
				if row := snapshot[string(key)]; row != nil {
					result.Rows = append(result.Rows, proto.Clone(row).(*committerpb.Row))
				}
			}
			return &committerpb.Rows{Namespaces: []*committerpb.RowsNamespace{result}}, nil
		},
		end: func(_ context.Context, view *committerpb.View) (*emptypb.Empty, error) {
			ends++
			delete(views, view.Id)
			return &emptypb.Empty{}, nil
		},
	}
	reader := NewQueryServiceStateReader(client)
	execution := NewExecutionContext(reader, "1")
	execution.PutState("asset", []byte("pending"))
	if begins != 0 || execution.QueryView() != nil {
		t.Fatal("a view was opened before the first state read")
	}
	if value, err := execution.GetState(t.Context(), "asset"); err != nil || string(value) != "old" {
		t.Fatalf("first read = %q, %v", value, err)
	}
	committed["asset"] = &committerpb.Row{Key: []byte("asset"), Value: []byte("new"), Version: 8}
	committed["other"] = &committerpb.Row{Key: []byte("other"), Value: []byte("other-new"), Version: 8}
	committed["created"] = &committerpb.Row{Key: []byte("created"), Value: []byte("new-key"), Version: 8}
	for key, want := range map[string]string{"asset": "old", "other": "other-old", "created": ""} {
		value, err := execution.GetState(t.Context(), key)
		if err != nil || string(value) != want {
			t.Fatalf("snapshot read %q = %q, %v; want %q", key, value, err, want)
		}
	}
	for _, read := range execution.Result().Reads {
		if read.Key == "created" {
			if read.Version != nil {
				t.Fatal("key created after the snapshot must retain an absent-key dependency")
			}
		} else if !reflect.DeepEqual(read.Version, &blocks.Version{BlockNum: 7}) {
			t.Fatalf("read dependency was replaced: %+v", read)
		}
	}
	if err := execution.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := execution.Close(t.Context()); err != nil || ends != 1 {
		t.Fatalf("view cleanup is not idempotent: ends=%d err=%v", ends, err)
	}
	if _, err := execution.GetState(t.Context(), "asset"); err == nil || reads != 4 || begins != 1 {
		t.Fatal("closed execution reopened a view or read state")
	}
	next := NewExecutionContext(reader, "1")
	if value, err := next.GetState(t.Context(), "asset"); err != nil || string(value) != "new" {
		t.Fatalf("next invocation read = %q, %v; want new", value, err)
	}
	if begins != 2 {
		t.Fatalf("independent invocations opened %d views, want 2", begins)
	}
	if err := next.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestExecutionViewFailuresNeverFallBackOrReopen(t *testing.T) {
	unavailable := errors.New("query service unavailable")
	expired := errors.New("query view expired")
	for _, scenario := range []string{"begin-failed", "nil-view", "empty-view", "expired-view"} {
		t.Run(scenario, func(t *testing.T) {
			begins, reads, ends := 0, 0, 0
			client := &queryViewClient{
				begin: func(context.Context, *committerpb.ViewParameters) (*committerpb.View, error) {
					begins++
					switch scenario {
					case "begin-failed":
						return nil, unavailable
					case "nil-view":
						return nil, nil
					case "empty-view":
						return &committerpb.View{}, nil
					default:
						return &committerpb.View{Id: "snapshot"}, nil
					}
				},
				rows: func(context.Context, *committerpb.Query) (*committerpb.Rows, error) {
					reads++
					return nil, expired
				},
				end: func(context.Context, *committerpb.View) (*emptypb.Empty, error) {
					ends++
					return &emptypb.Empty{}, nil
				},
			}
			execution := NewExecutionContext(NewQueryServiceStateReader(client), "1")
			_, firstErr := execution.GetState(t.Context(), "asset")
			_, secondErr := execution.GetState(t.Context(), "asset")
			if firstErr == nil || secondErr != firstErr || execution.Err() != firstErr || begins != 1 {
				t.Fatalf("read failure was not retained: first=%v second=%v begins=%d", firstErr, secondErr, begins)
			}
			if err := execution.SetQueryView(&committerpb.View{Id: "replacement"}); err == nil {
				t.Fatal("failed execution accepted a replacement view")
			}
			_ = execution.Close(t.Context())
			wantReads, wantEnds := 0, 0
			if scenario == "expired-view" {
				wantReads, wantEnds = 1, 1
			}
			if reads != wantReads || ends != wantEnds || len(execution.Result().Reads) != 0 {
				t.Fatalf("unexpected RPCs/dependencies after failure: reads=%d ends=%d", reads, ends)
			}
		})
	}
}

func TestConcurrentStateReadsShareOneInvocationView(t *testing.T) {
	var begins, ends atomic.Int32
	client := &queryViewClient{
		begin: func(context.Context, *committerpb.ViewParameters) (*committerpb.View, error) {
			begins.Add(1)
			return &committerpb.View{Id: "one-view"}, nil
		},
		rows: func(_ context.Context, query *committerpb.Query) (*committerpb.Rows, error) {
			if query.View.GetId() != "one-view" {
				return nil, errors.New("wrong view")
			}
			ns := query.Namespaces[0]
			return &committerpb.Rows{Namespaces: []*committerpb.RowsNamespace{{
				NsId: ns.NsId, Rows: []*committerpb.Row{{Key: ns.Keys[0], Value: []byte("value"), Version: 7}},
			}}}, nil
		},
		end: func(context.Context, *committerpb.View) (*emptypb.Empty, error) {
			ends.Add(1)
			return &emptypb.Empty{}, nil
		},
	}
	execution := NewExecutionContext(NewQueryServiceStateReader(client), "1")
	var workers sync.WaitGroup
	for i := range 16 {
		workers.Go(func() {
			if _, err := execution.GetState(t.Context(), fmt.Sprintf("key-%d", i)); err != nil {
				t.Error(err)
			}
			execution.PutState(fmt.Sprintf("write-%d", i), []byte("pending"))
			_ = execution.QueryView()
			_ = execution.Result()
		})
	}
	workers.Wait()
	if err := execution.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if begins.Load() != 1 || ends.Load() != 1 || len(execution.Result().Reads) != 16 {
		t.Fatalf("concurrent execution used begins=%d ends=%d reads=%d", begins.Load(), ends.Load(), len(execution.Result().Reads))
	}
}

func TestHelperOwnsViewCleanupOnEveryExecutionExit(t *testing.T) {
	for _, scenario := range []string{"success", "executor-error", "canceled", "ignored-read-error", "endorsement-error", "cleanup-error", "blind-write"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			begins, ends, endorsements := 0, 0, 0
			client := &queryViewClient{
				begin: func(_ context.Context, params *committerpb.ViewParameters) (*committerpb.View, error) {
					begins++
					if params.IsoLevel != committerpb.IsoLevel_REPEATABLE_READ || params.TimeoutMilliseconds == 0 || params.TimeoutMilliseconds > 30_000 {
						t.Fatalf("unexpected view parameters: %v", params)
					}
					return &committerpb.View{Id: "private-helper-view"}, nil
				},
				rows: func(context.Context, *committerpb.Query) (*committerpb.Rows, error) {
					if scenario == "ignored-read-error" {
						return nil, errors.New("snapshot expired")
					}
					return &committerpb.Rows{}, nil
				},
				end: func(cleanupCtx context.Context, view *committerpb.View) (*emptypb.Empty, error) {
					ends++
					deadline, ok := cleanupCtx.Deadline()
					if cleanupCtx.Err() != nil || !ok || time.Until(deadline) > 2*time.Second || view.Id != "private-helper-view" {
						t.Fatalf("cleanup must use a live, bounded context for the invocation view")
					}
					if scenario == "cleanup-error" {
						return nil, errors.New("cleanup unavailable")
					}
					return &emptypb.Empty{}, nil
				},
			}
			service := &Service{
				channel: "channelqc4", logger: sdk.NoOpLogger{}, stateReader: NewQueryServiceStateReader(client),
				executors: map[string]Executor{"*": executionFunc(func(ctx context.Context, state *ExecutionContext, _ endorsement.Invocation) (endorsement.ExecutionResult, ExecutionMetadata, error) {
					if scenario == "blind-write" {
						state.PutState("asset", []byte("value"))
					} else {
						_, _ = state.GetState(ctx, "asset")
					}
					if scenario == "executor-error" {
						return endorsement.ExecutionResult{}, ExecutionMetadata{}, errors.New("chaincode failed")
					}
					if scenario == "canceled" {
						cancel()
						return endorsement.ExecutionResult{}, ExecutionMetadata{}, ctx.Err()
					}
					return endorsement.Success(state.Result(), "", nil, []byte("result")), ExecutionMetadata{}, nil
				})},
				builder: endorsementBuilderFunc(func(_ endorsement.Invocation, result endorsement.ExecutionResult) (*peer.ProposalResponse, error) {
					endorsements++
					if scenario == "endorsement-error" {
						return nil, errors.New("signing failed")
					}
					return &peer.ProposalResponse{Response: result.Response()}, nil
				}),
			}
			proposal, _, err := protoutil.CreateChaincodeProposal(common.HeaderType_ENDORSER_TRANSACTION, "channelqc4",
				&peer.ChaincodeInvocationSpec{ChaincodeSpec: &peer.ChaincodeSpec{ChaincodeId: &peer.ChaincodeID{Name: "1"}, Input: &peer.ChaincodeInput{Args: [][]byte{[]byte("read")}}}}, []byte("creator"))
			if err != nil {
				t.Fatal(err)
			}
			proposalBytes, err := proto.Marshal(proposal)
			if err != nil {
				t.Fatal(err)
			}
			_, err = service.ProcessProposal(ctx, &peer.SignedProposal{ProposalBytes: proposalBytes})
			wantError := scenario == "executor-error" || scenario == "canceled" || scenario == "ignored-read-error" || scenario == "endorsement-error"
			if (err != nil) != wantError {
				t.Fatalf("execution error = %v, want error=%t", err, wantError)
			}
			wantViews := 1
			if scenario == "blind-write" {
				wantViews = 0
			}
			if begins != wantViews || ends != wantViews {
				t.Fatalf("view lifecycle: begins=%d ends=%d, want %d each", begins, ends, wantViews)
			}
			if scenario == "ignored-read-error" && endorsements != 0 {
				t.Fatal("helper endorsed an execution that ignored a failed state read")
			}
		})
	}
}

type queryViewClient struct {
	committerpb.QueryServiceClient
	begin func(context.Context, *committerpb.ViewParameters) (*committerpb.View, error)
	rows  func(context.Context, *committerpb.Query) (*committerpb.Rows, error)
	end   func(context.Context, *committerpb.View) (*emptypb.Empty, error)
}

func (c *queryViewClient) BeginView(ctx context.Context, params *committerpb.ViewParameters, _ ...grpc.CallOption) (*committerpb.View, error) {
	return c.begin(ctx, params)
}

func (c *queryViewClient) GetRows(ctx context.Context, query *committerpb.Query, _ ...grpc.CallOption) (*committerpb.Rows, error) {
	return c.rows(ctx, query)
}

func (c *queryViewClient) EndView(ctx context.Context, view *committerpb.View, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.end(ctx, view)
}
