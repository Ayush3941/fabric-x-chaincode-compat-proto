// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"compatibility_service/pkg/config"
	"compatibility_service/pkg/helper"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/network"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestCompareCanonicalResultsAcceptsMatchingRemoteResults(t *testing.T) {
	local := testHelperExecutionResult("tx1", 200, "ok", []byte("payload"), []byte("tx-payload"))
	remote := []helperExecutionResult{
		testHelperExecutionResult("tx1", 200, "ok", []byte("payload"), []byte("tx-payload")),
	}

	if err := compareCanonicalResults(local, remote); err != nil {
		t.Fatalf("compareCanonicalResults returned error: %v", err)
	}
}

func TestCompareCanonicalResultsRejectsPayloadMismatch(t *testing.T) {
	local := testHelperExecutionResult("tx1", 200, "ok", []byte("payload"), []byte("tx-payload"))
	remote := []helperExecutionResult{
		testHelperExecutionResult("tx1", 200, "ok", []byte("different"), []byte("tx-payload")),
	}

	err := compareCanonicalResults(local, remote)
	if err == nil || !strings.Contains(err.Error(), "response payload mismatch") {
		t.Fatalf("expected payload mismatch error, got %v", err)
	}
}

func TestMergeEndorsementsKeepsOneProposalAndAllResponses(t *testing.T) {
	local := testHelperExecutionResult("tx1", 200, "ok", []byte("payload"), []byte("tx-payload"))
	remote := []helperExecutionResult{
		testHelperExecutionResult("tx1", 200, "ok", []byte("payload"), []byte("tx-payload")),
	}

	merged, err := mergeEndorsements(local, remote)
	if err != nil {
		t.Fatalf("mergeEndorsements returned error: %v", err)
	}
	if merged.Proposal != local.Endorsement.Proposal {
		t.Fatal("expected merged endorsement to use local proposal")
	}
	if len(merged.Responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(merged.Responses))
	}
}

func testHelperExecutionResult(txID string, status int32, message string, responsePayload, txPayload []byte) helperExecutionResult {
	proposal := &peer.Proposal{Header: []byte(txID)}
	response := &peer.Response{
		Status:  status,
		Message: message,
		Payload: responsePayload,
	}
	proposalResponse := &peer.ProposalResponse{
		Response: response,
		Payload:  txPayload,
		Endorsement: &peer.Endorsement{
			Endorser:  []byte("endorser-" + txID),
			Signature: []byte("signature-" + txID),
		},
	}
	return helperExecutionResult{
		TxID:     txID,
		Response: response,
		Endorsement: sdk.Endorsement{
			Proposal:  proposal,
			Responses: []*peer.ProposalResponse{proposalResponse},
		},
	}
}

func TestCollectNamespaceEndorsements(t *testing.T) {
	org0, org1, org2 := newProposalTestMSP(t, "org-0"), newProposalTestMSP(t, "org-1"), newProposalTestMSP(t, "org-2")
	rogue := newProposalTestMSP(t, "org-1")
	now := time.Now()
	issue := func(org proposalTestMSP, role string) proposalTestActor {
		return org.issue(t, role, now.Add(-time.Hour), now.Add(time.Hour))
	}
	peer0, peer0b, admin0, client0 := issue(org0, "peer"), issue(org0, "peer"), issue(org0, "admin"), issue(org0, "client")
	peer1, client1, peer2, roguePeer := issue(org1, "peer"), issue(org1, "client"), issue(org2, "peer"), issue(rogue, "peer")
	trust := proposalTestVerifier(t, []ProposalMSPConfig{org0.config, org1.config, org2.config}, "OR('org-0.member')")
	proposal := proposalTestSigned(t, client0.signer, "")

	type endpoint struct {
		mspID       string
		actor       proposalTestActor
		unavailable bool
		badSig      bool
		alternateID bool
		mismatch    bool
	}
	for _, tc := range []struct {
		name      string
		policy    string
		local     proposalTestActor
		endpoints []endpoint
		wantMSPs  []string
		wantCalls []int32
		wantErr   string
	}{
		{name: "local member needs no remote", policy: "OR('org-0.member')", local: peer0, endpoints: []endpoint{{mspID: "org-1", actor: peer1}}, wantMSPs: []string{"org-0"}, wantCalls: []int32{0}},
		{name: "unconditional policy retains native endorsement", policy: "OutOf(0,'org-0.peer')", local: client0, wantMSPs: []string{"org-0"}},
		{name: "AND across organizations", policy: "AND('org-0.peer','org-1.peer')", local: peer0, endpoints: []endpoint{{mspID: "org-1", actor: peer1}}, wantMSPs: []string{"org-0", "org-1"}, wantCalls: []int32{1}},
		{name: "wrong role does not satisfy peer", policy: "OR('org-1.peer')", local: client0, endpoints: []endpoint{{mspID: "org-1", actor: client1}, {mspID: "org-1", actor: peer1}}, wantMSPs: []string{"org-1"}, wantCalls: []int32{1, 1}},
		{name: "peer and admin in same organization", policy: "AND('org-0.peer','org-0.admin')", local: peer0, endpoints: []endpoint{{mspID: "org-0", actor: peer0b}, {mspID: "org-0", actor: admin0}}, wantMSPs: []string{"org-0", "org-0"}, wantCalls: []int32{1, 1}},
		{name: "two distinct peers from same organization", policy: "AND('org-0.peer','org-0.peer')", local: peer0, endpoints: []endpoint{{mspID: "org-0", actor: peer0, alternateID: true}, {mspID: "org-0", actor: peer0b}}, wantMSPs: []string{"org-0", "org-0"}, wantCalls: []int32{1, 1}},
		{name: "same certificate cannot be counted twice", policy: "AND('org-0.peer','org-0.peer')", local: peer0, endpoints: []endpoint{{mspID: "org-0", actor: peer0, alternateID: true}}, wantCalls: []int32{1}, wantErr: "not satisfied"},
		{name: "overlapping member and peer roles need signer ordering", policy: "AND('org-0.member','org-0.peer')", local: peer0, endpoints: []endpoint{{mspID: "org-0", actor: client0}}, wantMSPs: []string{"org-0", "org-0"}, wantCalls: []int32{1}},
		{name: "OR falls back to another organization", policy: "OR('org-1.peer','org-2.peer')", local: peer0, endpoints: []endpoint{{mspID: "org-1", actor: peer1, unavailable: true}, {mspID: "org-2", actor: peer2}}, wantMSPs: []string{"org-2"}, wantCalls: []int32{1, 1}},
		{name: "OutOf uses an available alternative", policy: "OutOf(2,'org-0.peer','org-1.peer','org-2.peer')", local: peer0, endpoints: []endpoint{{mspID: "org-1", actor: peer1, unavailable: true}, {mspID: "org-2", actor: peer2}}, wantMSPs: []string{"org-0", "org-2"}, wantCalls: []int32{1, 1}},
		{name: "nested policy", policy: "AND('org-0.peer',OR('org-1.peer','org-2.peer'))", local: peer0, endpoints: []endpoint{{mspID: "org-1", actor: peer1}}, wantMSPs: []string{"org-0", "org-1"}, wantCalls: []int32{1}},
		{name: "nested local branch cannot reuse one signer", policy: "OR(AND('org-0.peer','org-0.peer'),'org-1.peer')", local: peer0, endpoints: []endpoint{{mspID: "org-1", actor: peer1}}, wantMSPs: []string{"org-1"}, wantCalls: []int32{1}},
		{name: "invalid signature falls back", policy: "AND('org-0.peer','org-1.peer')", local: peer0, endpoints: []endpoint{{mspID: "org-1", actor: peer1, badSig: true}, {mspID: "org-1", actor: peer1}}, wantMSPs: []string{"org-0", "org-1"}, wantCalls: []int32{1, 1}},
		{name: "unauthenticated result cannot cause an execution disagreement", policy: "AND('org-0.peer','org-1.peer')", local: peer0, endpoints: []endpoint{{mspID: "org-1", actor: peer1, badSig: true, mismatch: true}, {mspID: "org-1", actor: peer1}}, wantMSPs: []string{"org-0", "org-1"}, wantCalls: []int32{1, 1}},
		{name: "untrusted signer rejected", policy: "AND('org-0.peer','org-1.peer')", local: peer0, endpoints: []endpoint{{mspID: "org-1", actor: roguePeer}}, wantCalls: []int32{1}, wantErr: "not satisfied"},
		{name: "actual MSP must match requested MSP", policy: "AND('org-0.peer','org-1.peer')", local: peer0, endpoints: []endpoint{{mspID: "org-1", actor: peer0b}}, wantCalls: []int32{1}, wantErr: "does not match requested MSP"},
		{name: "valid but different execution aborts", policy: "AND('org-0.peer','org-1.peer')", local: peer0, endpoints: []endpoint{{mspID: "org-1", actor: peer1, mismatch: true}}, wantCalls: []int32{1}, wantErr: "response payload mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := endorsementTestPolicy(t, tc.policy)
			local := endorsementTestResult(t, tc.local, proposal, policy.Version, "result")
			resolver := &staticRemoteOrchestratorResolver{contacts: make(map[string][]*OrchestratorContact)}
			t.Cleanup(func() { _ = resolver.Close() })
			var servers []*endorsementTestServer
			for _, ep := range tc.endpoints {
				payload := "result"
				if ep.mismatch {
					payload = "different-result"
				}
				result := endorsementTestResult(t, ep.actor, proposal, policy.Version, payload)
				response := result.Endorsement.Responses[0]
				if ep.badSig {
					response.Endorsement.Signature = []byte("invalid-signature")
				}
				if ep.alternateID {
					// Unknown protobuf fields change the serialized bytes, but not
					// the authenticated signing identity.
					response.Endorsement.Endorser = append(response.Endorsement.Endorser, 0xa0, 0x06, 0x01)
				}
				server := &endorsementTestServer{proposal: proposal, response: response, unavailable: ep.unavailable}
				contact := startEndorsementTestServer(t, ep.mspID, server)
				resolver.contacts[ep.mspID] = append(resolver.contacts[ep.mspID], contact)
				servers = append(servers, server)
			}
			service := &Service{cfg: Config{ChannelID: "channelqc4", Identity: &config.IdentityConfig{MspID: "org-0"}}, proposalAuth: trust, remotes: resolver, logger: sdk.NoOpLogger{}}
			req := InvocationRequest{ClientTxID: local.TxID, ClientSignedProposal: proposal, Namespace: "1", ChaincodeName: "sample", ChaincodeVersion: "1.0"}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			end, _, err := service.collectNamespaceEndorsements(ctx, policy, req, local)
			for i, server := range servers {
				if got := server.calls.Load(); got != tc.wantCalls[i] {
					t.Errorf("endpoint %d calls = %d, want %d", i, got, tc.wantCalls[i])
				}
			}
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("collect endorsements: %v", err)
			}
			if len(end.Responses) != len(tc.wantMSPs) {
				t.Fatalf("selected %d signatures, want %d", len(end.Responses), len(tc.wantMSPs))
			}
			for i, response := range end.Responses {
				id := &msppb.Identity{}
				if err := proto.Unmarshal(response.Endorsement.Endorser, id); err != nil || id.MspId != tc.wantMSPs[i] {
					t.Fatalf("signer %d identity = %v, error = %v, want MSP %s", i, id, err, tc.wantMSPs[i])
				}
			}
			compiled, err := newNamespaceEndorsementPolicy(policy, trust.identities)
			if err != nil {
				t.Fatal(err)
			}
			if err := compiled.verifySelection(end, local.TxID); err != nil {
				t.Fatalf("Fabric-X rejected the selected signatures: %v", err)
			}
			if _, err := createCompatFabricXTx(end.Proposal, end.Responses...); err != nil {
				t.Fatalf("package selected transaction: %v", err)
			}
		})
	}
}

func TestThresholdNamespaceEndorsements(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	actor := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	other := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	publicKey, err := x509.MarshalPKIXPublicKey(actor.cert.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	policy := NamespacePolicySnapshot{Namespace: "1", Version: 7, Policy: &applicationpb.NamespacePolicy{Rule: &applicationpb.NamespacePolicy_ThresholdRule{ThresholdRule: &applicationpb.ThresholdRule{Scheme: "ECDSA", PublicKey: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicKey})}}}}
	proposal := proposalTestSigned(t, actor.signer, "")
	service := &Service{cfg: Config{Identity: &config.IdentityConfig{MspID: "org-0"}}, logger: sdk.NoOpLogger{}}
	// A native key policy authenticates its configured key independently of the
	// MSPs trusted for client proposals. Explicit organization targeting adds an
	// MSP requirement, while automatic key-policy endorsement keeps that model.
	clientOrg := newProposalTestMSP(t, "clients")
	service.proposalAuth = proposalTestVerifier(t, []ProposalMSPConfig{clientOrg.config}, "OR('clients.member')")
	for i, signer := range []proposalTestActor{actor, other} {
		local := endorsementTestResult(t, signer, proposal, policy.Version, "result")
		_, _, err := service.collectNamespaceEndorsements(context.Background(), policy, InvocationRequest{ClientTxID: local.TxID}, local)
		if (err == nil) != (i == 0) {
			t.Fatalf("threshold signer %d error = %v", i, err)
		}
		if i == 0 {
			_, _, err = service.collectNamespaceEndorsements(context.Background(), policy, InvocationRequest{ClientTxID: local.TxID, EndorsingOrganizations: []string{"org-0"}}, local)
			if err == nil {
				t.Fatal("a key alone satisfied explicit MSP targeting")
			}
		}
	}
}

type endorsementTestServer struct {
	peer.UnimplementedEndorserServer
	proposal    *peer.SignedProposal
	response    *peer.ProposalResponse
	unavailable bool
	calls       atomic.Int32
}

func (s *endorsementTestServer) ProcessProposal(ctx context.Context, proposal *peer.SignedProposal) (*peer.ProposalResponse, error) {
	s.calls.Add(1)
	if !proto.Equal(proposal, s.proposal) {
		return nil, status.Error(codes.InvalidArgument, "original client proposal was changed")
	}
	if operation, err := operationFromContext(ctx); err != nil || operation != GRPCOperationEndorse {
		return nil, status.Error(codes.InvalidArgument, "expected execute-only endorsement request")
	}
	if values := metadata.ValueFromIncomingContext(ctx, GRPCNamespaceMetadata); len(values) != 1 || values[0] != "1" {
		return nil, status.Error(codes.InvalidArgument, "expected native namespace")
	}
	if s.unavailable {
		return nil, status.Error(codes.Unavailable, "test endorser unavailable")
	}
	return proto.Clone(s.response).(*peer.ProposalResponse), nil
}

func startEndorsementTestServer(t *testing.T, mspID string, handler *endorsementTestServer) *OrchestratorContact {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	peer.RegisterEndorserServer(server, handler)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	endpoint, err := endpointFromAddress(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	contact, err := newOrchestratorContact(RemoteOrchestratorConfig{MSPID: mspID, Endpoint: endpoint, TLS: config.TLSConfig{Mode: "none"}}, sdk.NoOpLogger{})
	if err != nil {
		t.Fatal(err)
	}
	return contact
}

func TestFinalityTimeoutRemainsSubmittedWithoutBackgroundChecks(t *testing.T) {
	var observed atomic.Pointer[committerpb.TxStatus]
	var queries atomic.Int32
	service, submitter, record, end := newFinalityTestService(t, &finalityTestQuery{get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		queries.Add(1)
		return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{observed.Load()}}, nil
	}})
	out, err := service.submitAndWaitFinality(context.Background(), end, InvocationResponse{TxID: record.Key, Status: 200}, record.Key, record, nil)
	if err != nil || !out.Submitted || !out.FinalityPending || out.CommitStatus != "" || out.ChaincodeEvent != nil {
		t.Fatalf("timeout must return pending submission: response=%+v err=%v", out, err)
	}
	service.idempotency.complete(record, out, nil)
	service.idempotency.complete(record, out, context.DeadlineExceeded)
	assertFinalityState(t, service, record, idempotencySubmitted)
	observed.Store(finalityTestStatus(record.Key, committerpb.Status_COMMITTED))
	before := queries.Load()
	// Even a now-visible commit must not start polling after the RPC ended.
	time.Sleep(200 * time.Millisecond)
	if queries.Load() != before {
		t.Fatal("status checks continued after the request returned")
	}
	assertFinalityState(t, service, record, idempotencySubmitted)

	const retries = 8
	var wg sync.WaitGroup
	results := make([]InvocationResponse, retries)
	retryErrors := make([]error, retries)
	for i := range results {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			service.reconcilePending(ctx)
			results[index], retryErrors[index] = service.replaySubmission(ctx, record)
		}(i)
	}
	wg.Wait()
	for i, result := range results {
		if retryErrors[i] != nil || result.FinalityPending || result.CommitStatus != "COMMITTED" || result.BlockNum != 42 || result.TxNum != 3 {
			t.Fatalf("retry %d did not recover finality: response=%+v err=%v", i, result, retryErrors[i])
		}
		if result.ChaincodeEvent == nil || result.ChaincodeEvent.EventName != "event" || result.ChaincodeEvent.Payload != "event-value" {
			t.Fatalf("retry %d lost committed event: %+v", i, result.ChaincodeEvent)
		}
	}
	if submitter.calls.Load() != 1 || queries.Load() != before+1 {
		t.Fatalf("duplicate work: submissions=%d status queries=%d", submitter.calls.Load(), queries.Load())
	}
	assertFinalityState(t, service, record, idempotencyCompleted)
	service.idempotency.complete(record, InvocationResponse{}, context.Canceled)
	replayed, err := service.replaySubmission(context.Background(), record)
	if err != nil || replayed.CommitStatus != "COMMITTED" {
		t.Fatalf("known finality changed: response=%+v err=%v", replayed, err)
	}
}

func TestLazyReconciliationBatchesPendingRecords(t *testing.T) {
	var queried []string
	var calls int
	service, submitter, _, end := newFinalityTestService(t, &finalityTestQuery{get: func(_ context.Context, query *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		calls++
		queried = append([]string(nil), query.TxIds...)
		return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{
			finalityTestStatus("a", committerpb.Status_COMMITTED),
			finalityTestStatus("b", committerpb.Status_ABORTED_MVCC_CONFLICT),
			finalityTestStatus("c", committerpb.Status_STATUS_UNSPECIFIED),
			finalityTestStatus("unrequested", committerpb.Status_COMMITTED), nil,
		}}, nil
	}})
	var initialized atomic.Int32
	records := make(map[string]*idempotencyRecord)
	for _, id := range []string{"a", "b", "c", "active"} {
		record, _, err := service.idempotency.begin(id, "digest-"+id)
		if err != nil {
			t.Fatal(err)
		}
		service.idempotency.markSubmitted(record, InvocationResponse{TxID: id, Status: 200, Submitted: true, Payload: "response-" + id}, end, func(context.Context) error { initialized.Add(1); return nil })
		records[id] = record
	}
	if !records["active"].tryLockFinality() {
		t.Fatal("lock active invocation")
	}
	defer records["active"].unlockFinality()
	service.reconcilePending(context.Background())
	slices.Sort(queried)
	if calls != 1 || !slices.Equal(queried, []string{"a", "b", "c"}) {
		t.Fatalf("expected one batch excluding active requests: calls=%d ids=%v", calls, queried)
	}
	for _, id := range []string{"a", "b"} {
		assertFinalityState(t, service, records[id], idempotencyCompleted)
	}
	for _, id := range []string{"c", "active"} {
		assertFinalityState(t, service, records[id], idempotencySubmitted)
	}
	a, _ := service.idempotency.waitResult(records["a"], nil)
	b, _ := service.idempotency.waitResult(records["b"], nil)
	if a.CommitStatus != "COMMITTED" || a.Payload != "response-a" || a.ChaincodeEvent == nil || b.CommitStatus != "ABORTED_MVCC_CONFLICT" || b.Payload != "response-b" || b.ChaincodeEvent != nil {
		t.Fatalf("store results were mixed: a=%+v b=%+v", a, b)
	}
	if initialized.Load() != 1 || submitter.calls.Load() != 0 {
		t.Fatal("maintenance repeated submission or finalized a noncommitted init")
	}
}

func TestLazyReconciliationQueryFailureKeepsPending(t *testing.T) {
	service, _, record, end := newFinalityTestService(t, &finalityTestQuery{get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		return nil, errors.New("query service unavailable")
	}})
	service.idempotency.markSubmitted(record, InvocationResponse{TxID: record.Key, Submitted: true}, end, nil)
	service.reconcilePending(context.Background())
	assertFinalityState(t, service, record, idempotencySubmitted)
	if !record.tryLockFinality() {
		t.Fatal("maintenance left the record locked")
	}
	record.unlockFinality()
}

func TestLazyReconciliationDoesNotBlockSubmissionOrQueueWorkers(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	service, submitter, old, end := newFinalityTestService(t, &finalityTestQuery{get: func(ctx context.Context, query *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-release:
			return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{finalityTestStatus(oldTransactionID, committerpb.Status_COMMITTED)}}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}})
	service.cfg.FinalityTimeout = 3 * time.Second
	service.idempotency.markSubmitted(old, InvocationResponse{TxID: oldTransactionID, Submitted: true}, end, nil)
	service.triggerReconciliation()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("lazy maintenance did not start")
	}
	for range 100 {
		service.triggerReconciliation()
	}
	fresh, _, err := service.idempotency.begin("new", "new-digest")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	out, err := service.submitTransaction(ctx, end, InvocationResponse{TxID: "new", Status: 200}, "new", fresh, nil)
	if err != nil || !out.Submitted || out.TxID != "new" || out.CommitStatus != "" || submitter.calls.Load() != 1 {
		t.Fatalf("old work delayed or contaminated the new submission: %+v %v", out, err)
	}
	if calls.Load() != 1 {
		t.Fatal("submissions created multiple maintenance workers")
	}
	close(release)
	waitForRecoveryPass(t, service)
	if _, found, _ := service.idempotency.existing(old.Key, old.Digest); found {
		t.Fatal("confirmed old transaction was not evicted")
	}
	time.Sleep(40 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatal("maintenance queued another pass or started periodic checks")
	}
	assertFinalityState(t, service, fresh, idempotencySubmitted)
}

const oldTransactionID = "old-submitted-transaction"

func TestLazyReconciliationStopsOnServiceClose(t *testing.T) {
	entered, stopped := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	service, _, record, end := newFinalityTestService(t, &finalityTestQuery{get: func(ctx context.Context, _ *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		calls.Add(1)
		close(entered)
		<-ctx.Done()
		close(stopped)
		return nil, ctx.Err()
	}})
	service.idempotency.markSubmitted(record, InvocationResponse{TxID: record.Key, Submitted: true}, end, nil)
	service.triggerReconciliation()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("lazy maintenance did not start")
	}
	service.stopReconciliation()
	select {
	case <-stopped:
	default:
		t.Fatal("service shutdown did not cancel and join maintenance")
	}
	service.triggerReconciliation()
	if calls.Load() != 1 {
		t.Fatal("maintenance started after service shutdown")
	}
	assertFinalityState(t, service, record, idempotencySubmitted)
}

func TestLazyReconciliationLimitsEachStatusBatch(t *testing.T) {
	service, _, _, end := newFinalityTestService(t, &finalityTestQuery{get: func(_ context.Context, query *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		if len(query.TxIds) != recoveryBatchSize {
			t.Errorf("unbounded maintenance batch: %d", len(query.TxIds))
		}
		return &committerpb.TxStatusResponse{}, nil
	}})
	for i := range recoveryBatchSize * 3 {
		key := fmt.Sprintf("pending-%d", i)
		record, _, err := service.idempotency.begin(key, key)
		if err != nil {
			t.Fatal(err)
		}
		service.idempotency.markSubmitted(record, InvocationResponse{TxID: key, Submitted: true}, end, nil)
	}
	service.triggerReconciliation()
	waitForRecoveryPass(t, service)
}

func TestLazyReconciliationExcludesTheTriggeringSubmission(t *testing.T) {
	var queries atomic.Int32
	service, _, record, end := newFinalityTestService(t, &finalityTestQuery{get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		queries.Add(1)
		return &committerpb.TxStatusResponse{}, nil
	}})
	cutoff := time.Now()
	service.idempotency.markSubmitted(record, InvocationResponse{TxID: record.Key, Submitted: true}, end, nil)
	service.reconcilePendingBefore(t.Context(), cutoff)
	if queries.Load() != 0 {
		t.Fatal("a delayed lazy pass began observing its own new submission")
	}
	service.reconcilePending(t.Context())
	if queries.Load() != 1 {
		t.Fatal("the next submission could not observe the older pending record")
	}
}

func waitForRecoveryPass(t *testing.T, service *Service) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		service.recoveryMu.Lock()
		running := service.recoveryRunning
		service.recoveryMu.Unlock()
		if !running {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("bounded recovery pass did not stop")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLazyFinalityAfterCallerCancellationRunsInitOnce(t *testing.T) {
	var observed atomic.Pointer[committerpb.TxStatus]
	service, _, record, end := newFinalityTestService(t, &finalityTestQuery{get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{observed.Load()}}, nil
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var initialized atomic.Int32
	bookkeepingErr := errors.New("initialization bookkeeping failed")
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = service.submitAndWaitFinality(ctx, end, InvocationResponse{TxID: record.Key}, record.Key, record, func(ctx context.Context) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			initialized.Add(1)
			return bookkeepingErr
		})
	}()
	<-record.submitted
	cancel()
	<-done
	assertFinalityState(t, service, record, idempotencySubmitted)
	if initialized.Load() != 0 {
		t.Fatal("canceled request finalized init")
	}
	observed.Store(finalityTestStatus(record.Key, committerpb.Status_COMMITTED))
	service.reconcilePending(context.Background())
	out, err := service.replaySubmission(context.Background(), record)
	if !errors.Is(err, bookkeepingErr) || out.CommitStatus != "COMMITTED" || out.FinalityPending || initialized.Load() != 1 {
		t.Fatalf("lazy init finality lost: response=%+v err=%v calls=%d", out, err, initialized.Load())
	}
	service.reconcilePending(context.Background())
	if initialized.Load() != 1 {
		t.Fatal("initialization repeated")
	}
	assertFinalityState(t, service, record, idempotencyCompleted)
}

func TestFinalityObservationFailuresRemainPending(t *testing.T) {
	for _, mode := range []string{"unavailable", "timeout", "rejected", "stream-closed", "query-error", "unspecified", "wrong-tx", "unknown-status"} {
		t.Run(mode, func(t *testing.T) {
			var recovered atomic.Bool
			service, submitter, record, end := newFinalityTestService(t, &finalityTestQuery{get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
				if recovered.Load() {
					return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{finalityTestStatus("tx", committerpb.Status_ABORTED_MVCC_CONFLICT)}}, nil
				}
				result := finalityTestStatus("tx", committerpb.Status_STATUS_UNSPECIFIED)
				switch mode {
				case "query-error":
					return nil, errors.New("query unavailable")
				case "wrong-tx":
					result = finalityTestStatus("other-tx", committerpb.Status_COMMITTED)
				case "unknown-status":
					result.Status = committerpb.Status(9999)
				}
				return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{result}}, nil
			}})
			service.notify = &finalityTestNotifier{mode: mode}
			var afterCommit atomic.Int32
			out, err := service.submitAndWaitFinality(context.Background(), end, InvocationResponse{TxID: record.Key}, record.Key, record, func(context.Context) error { afterCommit.Add(1); return nil })
			if err != nil || !out.FinalityPending || out.CommitStatus != "" {
				t.Fatalf("observation failure became ledger outcome: response=%+v err=%v", out, err)
			}
			assertFinalityState(t, service, record, idempotencySubmitted)
			service.reconcilePending(context.Background())
			assertFinalityState(t, service, record, idempotencySubmitted)
			recovered.Store(true)
			service.reconcilePending(context.Background())
			out, err = service.replaySubmission(context.Background(), record)
			if err != nil || out.FinalityPending || out.CommitStatus != "ABORTED_MVCC_CONFLICT" || out.ChaincodeEvent != nil || afterCommit.Load() != 0 {
				t.Fatalf("invalid finality: response=%+v err=%v", out, err)
			}
			assertFinalityState(t, service, record, idempotencyCompleted)
			if submitter.calls.Load() != 1 {
				t.Fatal("status recovery resubmitted transaction")
			}
		})
	}
}

func TestFinalityNotificationCanCompleteWithoutQueryStatus(t *testing.T) {
	service, _, record, end := newFinalityTestService(t, nil)
	service.notify = &finalityTestNotifier{mode: "committed"}
	out, err := service.submitAndWaitFinality(context.Background(), end, InvocationResponse{TxID: record.Key}, record.Key, record, nil)
	if err != nil || out.CommitStatus != "COMMITTED" || out.FinalityPending || out.ChaincodeEvent == nil {
		t.Fatalf("notification finality lost: response=%+v err=%v", out, err)
	}
}

func TestFinalitySubscriptionStopsWhenRequestEnds(t *testing.T) {
	service, _, record, end := newFinalityTestService(t, nil)
	stopped := make(chan struct{})
	service.notify = &finalityTestNotifier{mode: "wait", stopped: stopped}
	service.cfg.FinalityTimeout = 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	out, err := service.submitAndWaitFinality(ctx, end, InvocationResponse{TxID: record.Key}, record.Key, record, nil)
	if err != nil || !out.FinalityPending {
		t.Fatalf("canceled wait lost pending state: %+v %v", out, err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("subscription outlived request cancellation")
	}
	assertFinalityState(t, service, record, idempotencySubmitted)
}

func TestFinalitySubmissionFailureDoesNotCheckStatus(t *testing.T) {
	service, submitter, record, end := newFinalityTestService(t, nil)
	submitter.err = errors.New("ordering unavailable")
	out, err := service.submitAndWaitFinality(context.Background(), end, InvocationResponse{TxID: record.Key}, record.Key, record, nil)
	if err == nil || out.Submitted || out.FinalityPending {
		t.Fatalf("submission error lost: response=%+v err=%v", out, err)
	}
	service.idempotency.complete(record, out, err)
	assertFinalityState(t, service, record, idempotencyFailed)
}

func newFinalityTestService(t *testing.T, query committerpb.QueryServiceClient) (*Service, *finalityTestSubmitter, *idempotencyRecord, sdk.Endorsement) {
	t.Helper()
	org := newProposalTestMSP(t, "org-0")
	actor := org.issue(t, "peer", time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	result := endorsementTestResult(t, actor, proposalTestSigned(t, actor.signer, ""), 0, "result")
	submitter := &finalityTestSubmitter{}
	service := &Service{
		cfg:          Config{FinalityTimeout: 25 * time.Millisecond, RequestTimeout: time.Second},
		queryService: query, notify: &finalityTestNotifier{mode: "wait"}, submitter: submitter, idempotency: newIdempotencyStore(), logger: sdk.NoOpLogger{},
	}
	record, _, err := service.idempotency.begin("tx", "digest")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	return service, submitter, record, result.Endorsement
}

func assertFinalityState(t *testing.T, service *Service, record *idempotencyRecord, want idempotencyState) {
	t.Helper()
	service.idempotency.mu.Lock()
	defer service.idempotency.mu.Unlock()
	if record.State != want {
		t.Fatalf("state=%s, want %s", record.State, want)
	}
}

func finalityTestStatus(txID string, status committerpb.Status) *committerpb.TxStatus {
	return &committerpb.TxStatus{Ref: &committerpb.TxRef{TxId: txID, BlockNum: 42, TxNum: 3}, Status: status}
}

func TestWaitForTransactionStatus(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mode       string
		readyAfter int32
		deadline   time.Duration
	}{
		{name: "already final", readyAfter: 1},
		{name: "committed between query and subscription", readyAfter: 2},
		{name: "live notification", mode: "committed"},
		{name: "event lost before registration", readyAfter: 3},
		{name: "stream closes before finality", mode: "stream-closed", readyAfter: 3},
		{name: "subscription timeout is not transaction rejection", mode: "timeout", readyAfter: 3},
		{name: "subscription refusal is not transaction rejection", mode: "rejected", readyAfter: 3},
		{name: "notification service unavailable", mode: "unavailable", readyAfter: 3},
		{name: "final lookup before short deadline", readyAfter: 3, deadline: 200 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var queries atomic.Int32
			stopped := make(chan struct{})
			service := &Service{logger: sdk.NoOpLogger{}, notify: &finalityTestNotifier{mode: tc.mode, stopped: stopped}}
			service.queryService = &finalityTestQuery{get: func(ctx context.Context, query *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
				if len(query.TxIds) != 1 || query.TxIds[0] != "tx" {
					t.Errorf("queried unrelated transaction: %v", query.TxIds)
				}
				if count := queries.Add(1); tc.readyAfter > 0 && count >= tc.readyAfter {
					return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{finalityTestStatus("tx", committerpb.Status_COMMITTED)}}, nil
				}
				return &committerpb.TxStatusResponse{}, nil
			}}
			deadline := tc.deadline
			if deadline == 0 {
				deadline = 4 * time.Second
			}
			ctx, cancel := context.WithTimeout(t.Context(), deadline)
			defer cancel()
			result, err := service.waitForTransactionStatus(ctx, "tx")
			if err != nil || result.GetStatus() != committerpb.Status_COMMITTED || result.GetRef().GetBlockNum() != 42 {
				t.Fatalf("no confirmed final result: %v %v", result, err)
			}
			if tc.readyAfter > 0 && queries.Load() != tc.readyAfter {
				t.Fatalf("unexpected queries: %d", queries.Load())
			}
			if tc.readyAfter != 1 && tc.mode != "unavailable" {
				select {
				case <-stopped:
				case <-time.After(time.Second):
					t.Fatal("notification receiver survived the status request")
				}
			}
		})
	}
}

func TestWaitForTransactionStatusRejectsUnconfirmedResults(t *testing.T) {
	for _, result := range []*committerpb.TxStatus{
		nil,
		finalityTestStatus("other-tx", committerpb.Status_COMMITTED),
		finalityTestStatus("tx", committerpb.Status_STATUS_UNSPECIFIED),
		finalityTestStatus("tx", committerpb.Status(999)),
	} {
		t.Run(fmt.Sprint(result), func(t *testing.T) {
			stopped := make(chan struct{})
			service := &Service{logger: sdk.NoOpLogger{}, notify: &finalityTestNotifier{stopped: stopped}, queryService: &finalityTestQuery{
				get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
					return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{result}}, nil
				},
			}}
			ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
			defer cancel()
			if actual, err := service.waitForTransactionStatus(ctx, "tx"); actual != nil || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unconfirmed result treated as final: %v %v", actual, err)
			}
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("receiver survived deadline")
			}
		})
	}
}

func TestStatusWaitCancellationStopsQueriesAndNotifications(t *testing.T) {
	var queries atomic.Int32
	stopped, secondQuery := make(chan struct{}), make(chan struct{})
	service := &Service{logger: sdk.NoOpLogger{}, notify: &finalityTestNotifier{stopped: stopped}, queryService: &finalityTestQuery{
		get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
			if queries.Add(1) == 2 {
				close(secondQuery)
			}
			return &committerpb.TxStatusResponse{}, nil
		},
	}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := service.waitForTransactionStatus(ctx, "tx"); done <- err }()
	select {
	case <-secondQuery:
	case <-time.After(time.Second):
		t.Fatal("status wait did not start")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("notification receiver remained active")
	}
	count := queries.Load()
	time.Sleep(1100 * time.Millisecond)
	if queries.Load() != count {
		t.Fatal("status queries continued after the client request ended")
	}
}

func TestCommittedChaincodeEventsValidateOriginAndFinality(t *testing.T) {
	block := gatewayEventBlock(t, 7,
		gatewayEventFixture{id: "valid", name: "sample"},
		gatewayEventFixture{id: "rejected", name: "sample", status: committerpb.Status_ABORTED_SIGNATURE_INVALID},
		gatewayEventFixture{id: "other-channel", name: "sample", channel: "other-channel"},
		gatewayEventFixture{id: "old", name: "sample", legacy: true},
	)
	// Invalid transactions may contain undecodable bytes. Validation must be
	// checked before decoding, so these cannot enter the event stream.
	block.Data.Data[1] = []byte{0xff}
	events, err := committedChaincodeEvents(block, "channelqc4")
	if err != nil || len(events) != 1 || events[0].TxId != "valid" {
		t.Fatalf("wrong committed events: %+v %v", events, err)
	}
	for _, scenario := range []string{"missing filter", "short filter", "bad envelope", "bad namespace", "wrong signed channel", "bad SDK metadata"} {
		t.Run(scenario, func(t *testing.T) {
			block := gatewayEventBlock(t, 0, gatewayEventFixture{id: "tx", name: "sample"})
			switch scenario {
			case "missing filter":
				block.Metadata = nil
			case "short filter":
				block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = nil
			case "bad envelope":
				block.Data.Data[0] = []byte{0xff}
			default:
				envelope := &common.Envelope{}
				_ = proto.Unmarshal(block.Data.Data[0], envelope)
				payload, _ := protoutil.UnmarshalPayload(envelope.Payload)
				native := &applicationpb.Tx{}
				_ = proto.Unmarshal(payload.Data, native)
				switch scenario {
				case "bad namespace":
					native.Namespaces[0].NsId = "2"
				case "wrong signed channel":
					native.Metadata[1], _ = helper.MarshalChaincodeIdentity("other-channel", "sample", "1")
				case "bad SDK metadata":
					native.Metadata[0] = []byte{0xff}
				}
				payload.Data = protoutil.MarshalOrPanic(native)
				envelope.Payload = protoutil.MarshalOrPanic(payload)
				block.Data.Data[0] = protoutil.MarshalOrPanic(envelope)
			}
			if _, err := committedChaincodeEvents(block, "channelqc4"); err == nil {
				t.Fatal("ambiguous or corrupted committed events accepted")
			}
		})
	}
}

type finalityTestSubmitter struct {
	calls atomic.Int32
	err   error
}

func (s *finalityTestSubmitter) Submit(context.Context, sdk.Endorsement) error {
	s.calls.Add(1)
	return s.err
}

func (*finalityTestSubmitter) Close() error { return nil }

type finalityTestQuery struct {
	committerpb.QueryServiceClient
	get func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error)
}

func (q *finalityTestQuery) GetTransactionStatus(ctx context.Context, query *committerpb.TxStatusQuery, _ ...grpc.CallOption) (*committerpb.TxStatusResponse, error) {
	return q.get(ctx, query)
}

type finalityTestNotifier struct {
	committerpb.NotifierClient
	mode    string
	stopped chan struct{}
}

func (n *finalityTestNotifier) OpenNotificationStream(ctx context.Context, _ ...grpc.CallOption) (committerpb.Notifier_OpenNotificationStreamClient, error) {
	if n.mode == "unavailable" {
		return nil, errors.New("notification service unavailable")
	}
	return &finalityTestStream{ctx: ctx, mode: n.mode, stopped: n.stopped}, nil
}

type finalityTestStream struct {
	committerpb.Notifier_OpenNotificationStreamClient
	ctx     context.Context
	mode    string
	stopped chan struct{}
	txID    string
}

func (s *finalityTestStream) Send(request *committerpb.NotificationRequest) error {
	s.txID = request.TxStatusRequest.GetTxIds()[0]
	return nil
}

func (s *finalityTestStream) Recv() (*committerpb.NotificationResponse, error) {
	if s.stopped != nil {
		defer close(s.stopped)
	}
	switch s.mode {
	case "timeout":
		return &committerpb.NotificationResponse{TimeoutTxIds: []string{s.txID}}, nil
	case "rejected":
		return &committerpb.NotificationResponse{RejectedTxIds: &committerpb.RejectedTxIds{TxIds: []string{s.txID}}}, nil
	case "stream-closed":
		return nil, io.EOF
	case "committed":
		return &committerpb.NotificationResponse{TxStatusEvents: []*committerpb.TxStatus{finalityTestStatus(s.txID, committerpb.Status_COMMITTED)}}, nil
	default:
		<-s.ctx.Done()
		return nil, s.ctx.Err()
	}
}

func TestEndorsementComparisonRejectsDifferentLifecycleRevisions(t *testing.T) {
	local := testHelperExecutionResult("tx", 200, "", []byte("same"), []byte("same"))
	remote := testHelperExecutionResult("tx", 200, "", []byte("same"), []byte("same"))
	local.Binding = &LifecycleExecutionBinding{ChaincodeName: "sample", ChaincodeVersion: "1.0", Sequence: 1}
	remote.Binding = &LifecycleExecutionBinding{ChaincodeName: "sample", ChaincodeVersion: "2.0", Sequence: 2}
	if err := compareCanonicalResults(local, []helperExecutionResult{remote}); err == nil {
		t.Fatal("different active definitions endorsed together")
	}
	local.Binding = nil
	if err := compareCanonicalResults(local, []helperExecutionResult{remote}); err == nil {
		t.Fatal("missing local lifecycle revision was ignored")
	}
}

func TestFabricXSubmitterWaitsForOrderingAcknowledgment(t *testing.T) {
	for _, scenario := range []string{"accepted", "rejected", "ack-lost"} {
		t.Run(scenario, func(t *testing.T) {
			received := make(chan struct{})
			release := make(chan struct{})
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			orderer.RegisterAtomicBroadcastServer(server, &orderingTestServer{broadcast: func(stream orderer.AtomicBroadcast_BroadcastServer) error {
				if _, err := stream.Recv(); err != nil {
					return err
				}
				close(received)
				select {
				case <-release:
				case <-stream.Context().Done():
					return stream.Context().Err()
				}
				response := &orderer.BroadcastResponse{Status: common.Status_SUCCESS}
				if scenario == "rejected" {
					response.Status, response.Info = common.Status_BAD_REQUEST, "rejected by ordering"
				}
				return stream.Send(response)
			}})
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(func() { server.Stop(); _ = listener.Close() })
			submitter, err := newFabricXSubmitter(network.PeerConf{Address: listener.Addr().String(), TLS: network.TLSConfig{Mode: "none"}}, orderingTestPackager{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = submitter.Close() })
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- submitter.Submit(ctx, sdk.Endorsement{}) }()
			select {
			case <-received:
			case <-ctx.Done():
				t.Fatal("ordering did not receive the submission")
			}
			select {
			case err := <-done:
				t.Fatalf("submission returned before ordering acknowledgment: %v", err)
			default:
			}
			if scenario == "ack-lost" {
				cancel()
			} else {
				close(release)
			}
			err = <-done
			if scenario == "accepted" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var orderingError *orderingSubmissionError
			if !errors.As(err, &orderingError) || orderingError.uncertain != (scenario == "ack-lost") {
				t.Fatalf("ordering outcome was classified incorrectly: %v", err)
			}
		})
	}
}

type orderingTestServer struct {
	orderer.UnimplementedAtomicBroadcastServer
	broadcast func(orderer.AtomicBroadcast_BroadcastServer) error
}

func (s *orderingTestServer) Broadcast(stream orderer.AtomicBroadcast_BroadcastServer) error {
	return s.broadcast(stream)
}

type orderingTestPackager struct{}

func (orderingTestPackager) PackageTx(sdk.Endorsement) (*common.Envelope, error) {
	return &common.Envelope{Payload: []byte("native-transaction")}, nil
}

func TestUncertainSubmissionStaysPendingAndIsNotResent(t *testing.T) {
	query := &finalityTestQuery{get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		return &committerpb.TxStatusResponse{}, nil
	}}
	service, submitter, record, end := newFinalityTestService(t, query)
	submitter.err = &orderingSubmissionError{err: context.DeadlineExceeded, uncertain: true}
	out, err := service.submitAndWaitFinality(t.Context(), end, InvocationResponse{TxID: record.Key}, record.Key, record, nil)
	if err == nil || !out.Submitted || !out.FinalityPending {
		t.Fatalf("lost ordering acknowledgment was treated as a definite failure: %+v %v", out, err)
	}
	service.idempotency.complete(record, out, err)
	assertFinalityState(t, service, record, idempotencySubmitted)
	if err := service.replayOrderingSubmission(t.Context(), record); err == nil || submitter.calls.Load() != 1 {
		t.Fatalf("uncertain delivery was acknowledged or resent: %v", err)
	}
	query.get = func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{finalityTestStatus(record.Key, committerpb.Status_COMMITTED)}}, nil
	}
	service.reconcilePending(t.Context())
	assertFinalityState(t, service, record, idempotencyCompleted)
	if err := service.replayOrderingSubmission(t.Context(), record); err != nil || submitter.calls.Load() != 1 {
		t.Fatalf("confirmed delivery was not replayed without resubmission: %v", err)
	}
}

func TestCustomInvokePostSubmitDelayRemainsRequestBounded(t *testing.T) {
	service, _, record, end := newFinalityTestService(t, nil)
	service.cfg.WaitAfterSubmit = time.Second
	service.notify = &finalityTestNotifier{mode: "committed"}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	// A canceled request must not be sent merely because a delay was configured.
	if _, err := service.submitAndWaitFinality(ctx, end, InvocationResponse{TxID: record.Key}, record.Key, record, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation was ignored: %v", err)
	}
	service.cfg.WaitAfterSubmit = 20 * time.Millisecond
	started := time.Now()
	out, err := service.submitAndWaitFinality(t.Context(), end, InvocationResponse{TxID: record.Key}, record.Key, record, nil)
	if err != nil || out.CommitStatus != "COMMITTED" || time.Since(started) < service.cfg.WaitAfterSubmit {
		t.Fatalf("custom post-submit delay was lost: %+v %v", out, err)
	}
}
