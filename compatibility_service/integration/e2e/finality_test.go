// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"compatibility_service/pkg/lifecycle"
	"compatibility_service/pkg/orchestrator"
	gwclient "github.com/hyperledger/fabric-gateway/pkg/client"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestLazyFinalityReconcilesStoreWithoutChangingNextResponse(t *testing.T) {
	for _, cancelCaller := range []bool{false, true} {
		name, finality := "timeout", "250ms"
		if cancelCaller {
			name, finality = "caller-canceled", "25s"
		}
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, harnessOptions{name: name, requestWait: "45s", finality: finality, deferFinality: true})
			before := h.blockHeight(t)
			proposalA, txA := h.newSignedProposal(t, "compatv2", uniqueKey(t, "pending-a"), "pending-a-value", uniqueKey(t, "delete-a"))
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			var pending *orchestrator.InvocationResponse
			if cancelCaller {
				h.finalityProxy.holdNotifications.Store(true)
				callCtx, stop := context.WithCancel(ctx)
				defer stop()
				done := make(chan error, 1)
				go func() { _, err := h.invokeSigned(callCtx, proposalA); done <- err }()
				// Wait for actual ordering acknowledgement, then end this client
				// RPC while its notification wait is still active.
				logPath := filepath.Join(h.root, "runtime", "compatibility_service", "e2e", sanitize(t.Name())+"-orchestrator-org0.log")
				deadline := time.Now().Add(10 * time.Second)
				for {
					log, err := os.ReadFile(logPath)
					if err != nil {
						t.Fatal(err)
					}
					if strings.Contains(string(log), "tx="+txA+" submitted") {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("ordering acknowledgement not observed")
					}
					time.Sleep(10 * time.Millisecond)
				}
				stop()
				if err := <-done; status.Code(err) != codes.Canceled {
					t.Fatalf("expected canceled caller, got %v", err)
				}
				h.finalityProxy.holdNotifications.Store(false)
			} else {
				response, err := h.invokeSigned(ctx, proposalA)
				if err != nil || response.TxID != txA || !response.Submitted || !response.FinalityPending || response.CommitStatus != "" || response.ChaincodeEvent != nil {
					t.Fatalf("expected pending finality response: %+v err=%v", response, err)
				}
				pending = &response
			}

			// Observe the real ledger directly, without invoking the Orchestrator
			// again. Its store must remain untouched while no requests arrive.
			committedA := h.waitForLedgerStatus(t, ctx, txA)
			h.finalityProxy.visible.Store(true)
			queriesBefore := h.finalityProxy.queryCount()
			time.Sleep(500 * time.Millisecond)
			if h.finalityProxy.queryCount() != queriesBefore || h.finalityProxy.callsFor(txA) != 0 {
				t.Fatal("status checks continued after the original request ended")
			}

			// A different transaction triggers lazy store maintenance. Its client
			// response must contain only B, never the now-confirmed result of A.
			proposalB, txB := h.newSignedProposal(t, "compatv2", uniqueKey(t, "new-b"), "new-b-value", uniqueKey(t, "delete-b"))
			responseB, err := h.invokeSigned(ctx, proposalB)
			if err != nil || responseB.TxID != txB || !responseB.Submitted || responseB.IdempotentReplay {
				t.Fatalf("new request returned another transaction: %+v err=%v", responseB, err)
			}
			encoded, err := json.Marshal(responseB)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), txA) {
				t.Fatal("new response leaked an older transaction result")
			}
			if h.finalityProxy.callsFor(txA) != 1 {
				t.Fatal("new submission did not reconcile the older transaction once")
			}

			// Completed responses are not replayed from local history. Wait for the
			// asynchronous cleanup evidence, and use the ledger for old outcomes.
			logPath := filepath.Join(h.root, "runtime", "compatibility_service", "e2e", sanitize(t.Name())+"-orchestrator-org0.log")
			var log []byte
			for deadline := time.Now().Add(5 * time.Second); ; {
				log, err = os.ReadFile(logPath)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(log), "tx="+txA+" finality status=COMMITTED") {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("asynchronous cleanup did not confirm A")
				}
				time.Sleep(10 * time.Millisecond)
			}
			finalA := committedA.Ref

			committedB := h.waitForLedgerStatus(t, ctx, txB)
			if after := h.waitForHeight(t, committedB.Ref.BlockNum+1); after != before+2 {
				t.Fatalf("expected two transactions in two new blocks: before=%d after=%d", before, after)
			}
			log, err = os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			for _, txID := range []string{txA, txB} {
				if n := strings.Count(string(log), "tx="+txID+" submitting endorsed Fabric-X transaction"); n != 1 {
					t.Fatalf("transaction %s submitted %d times", txID, n)
				}
			}
			if n := strings.Count(string(log), "orchestrator calling in-process helper"); n != 2 {
				t.Fatalf("expected two executions, found %d", n)
			}
			maintenance := strings.Index(string(log), "tx="+txA+" finality status=COMMITTED")
			newSubmission := strings.Index(string(log), "tx="+txB+" submitting endorsed")
			if maintenance < 0 || newSubmission < 0 {
				t.Fatal("missing asynchronous maintenance or new submission evidence")
			}

			dir := filepath.Join(h.root, "runtime", "compatibility_service", "lazy-finality")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			evidence, err := json.MarshalIndent(map[string]any{"scenario": name, "original_response": pending, "ledger_outcome": committedA, "terminal_history_retained": false, "next_submission_response": responseB, "next_submission_block": committedB.Ref.BlockNum, "idle_status_queries": 0, "submissions_per_transaction": 1}, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, name+".json"), append(evidence, '\n'), 0o644); err != nil {
				t.Fatal(err)
			}
			t.Logf("lazy store refresh: A=%s block=%d; B=%s block=%d; B response contains only B; no idle polling", txA, finalA.BlockNum, txB, committedB.Ref.BlockNum)
		})
	}
}

func (h *e2eHarness) waitForLedgerStatus(t *testing.T, ctx context.Context, txID string) *committerpb.TxStatus {
	t.Helper()
	var query committerpb.QueryServiceClient
	if h.finalityProxy != nil {
		query = h.finalityProxy.upstream
	} else {
		query = h.queryServiceClient(t)
	}
	for ctx.Err() == nil {
		response, err := query.GetTransactionStatus(ctx, &committerpb.TxStatusQuery{TxIds: []string{txID}})
		if err != nil {
			t.Fatal(err)
		}
		for _, result := range response.Statuses {
			if result.Ref.GetTxId() == txID {
				if result.Status != committerpb.Status_COMMITTED {
					t.Fatalf("real transaction validation failed: %v", result)
				}
				return result
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("no real ledger status for tx=%s: %v", txID, ctx.Err())
	return nil
}

func TestGatewayCommitStatusMissedNotification(t *testing.T) {
	name := fmt.Sprintf("gateway_status_fallback_%d", time.Now().UnixNano())
	h := newHarness(t, harnessOptions{
		name: "gateway-status-fallback", sampleChaincode: true, requestWait: "45s", finality: "25s",
		chaincodeDefinition: &lifecycle.ChaincodeDefinition{Name: name, Version: "1.0", Sequence: 1},
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	h.deployGatewayDefinition(t, ctx, name, "1.0")
	// Reuse the finality fault-injection server, forwarding real query results
	// but deliberately withholding every notification for this instance.
	configPath := h.processes["orchestrator-org0"].cmd.Args[2]
	h.finalityProxy = h.startDeferredFinality(t, configPath)
	h.finalityProxy.visible.Store(true)
	h.finalityProxy.holdNotifications.Store(true)
	h.restartProcess(t, "orchestrator-org0")
	waitForPort(t, h.ports.org0Orch)
	client := h.gatewaySDK(t, 0, 0)
	contract := client.GetNetwork(channelID).GetContract(name + ":" + namespace)
	proposal, err := contract.NewProposal("put", gwclient.WithArguments(uniqueKey(t, "status-fallback"), "committed-despite-missed-event"))
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := proposal.EndorseWithContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	commit, err := client.NewCommit(h.gatewayStatusBytes(t, 0, proposal.TransactionID()))
	if err != nil {
		t.Fatal(err)
	}
	type statusResult struct {
		status *gwclient.Status
		err    error
	}
	done := make(chan statusResult, 1)
	go func() { result, err := commit.StatusWithContext(ctx); done <- statusResult{result, err} }()
	waitForActive := func(want int32) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if h.finalityProxy.activeSubscriptions.Load() == want {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("active notification subscriptions=%d, want %d", h.finalityProxy.activeSubscriptions.Load(), want)
	}
	waitForActive(1)
	if _, err := transaction.SubmitWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	native := h.waitForLedgerStatus(t, ctx, proposal.TransactionID())
	var result *gwclient.Status
	select {
	case outcome := <-done:
		if outcome.err != nil || !outcome.status.Successful || outcome.status.BlockNumber != native.Ref.BlockNum {
			t.Fatalf("missed event prevented finality recovery: %+v %v", outcome.status, outcome.err)
		}
		result = outcome.status
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waitForActive(0)
	if h.finalityProxy.callsFor(proposal.TransactionID()) < 3 {
		t.Fatal("test never exercised a fallback status query")
	}
	unknownID := uniqueKey(t, "unsubmitted-status")
	unknown, err := client.NewCommit(h.gatewayStatusBytes(t, 0, unknownID))
	if err != nil {
		t.Fatal(err)
	}
	short, cancelShort := context.WithTimeout(ctx, 180*time.Millisecond)
	_, err = unknown.StatusWithContext(short)
	cancelShort()
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("unknown transaction was treated as final: %v", err)
	}
	waitForActive(0)
	before := h.finalityProxy.queryCount()
	time.Sleep(1200 * time.Millisecond)
	if h.finalityProxy.queryCount() != before {
		t.Fatal("status polling continued without an active client request")
	}
	dir := filepath.Join(h.root, "runtime", "compatibility_service", "gateway-commit-status")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	report, err := json.MarshalIndent(map[string]any{
		"status": result, "notifications_deliberately_withheld": true,
		"status_queries": h.finalityProxy.callsFor(proposal.TransactionID()), "unknown_transaction_result": "DEADLINE_EXCEEDED",
		"active_subscriptions_after_requests": 0, "idle_status_queries": 0,
	}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "missed-notifications.json"), append(report, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("Gateway status recovered block=%d tx=%s with all notifications withheld; timeout left no subscriptions or idle queries", result.BlockNumber, proposal.TransactionID())
}

// deferredFinalityService forwards real state/policy queries, withholds
// transaction statuses until released, and times out notification subscriptions.
// Fault injection is confined to this test server and temporary harness config.
type deferredFinalityService struct {
	committerpb.UnimplementedQueryServiceServer
	committerpb.UnimplementedNotifierServer
	upstream            committerpb.QueryServiceClient
	visible             atomic.Bool
	holdNotifications   atomic.Bool
	activeSubscriptions atomic.Int32
	mu                  sync.Mutex
	queries             map[string]int
}

func (p *deferredFinalityService) GetRows(ctx context.Context, query *committerpb.Query) (*committerpb.Rows, error) {
	return p.upstream.GetRows(ctx, query)
}

func (p *deferredFinalityService) BeginView(ctx context.Context, params *committerpb.ViewParameters) (*committerpb.View, error) {
	return p.upstream.BeginView(ctx, params)
}

func (p *deferredFinalityService) EndView(ctx context.Context, view *committerpb.View) (*emptypb.Empty, error) {
	return p.upstream.EndView(ctx, view)
}

func (p *deferredFinalityService) GetNamespacePolicies(ctx context.Context, query *emptypb.Empty) (*applicationpb.NamespacePolicies, error) {
	return p.upstream.GetNamespacePolicies(ctx, query)
}

func (p *deferredFinalityService) GetTransactionStatus(ctx context.Context, query *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
	p.mu.Lock()
	for _, txID := range query.TxIds {
		p.queries[txID]++
	}
	p.mu.Unlock()
	if !p.visible.Load() {
		return &committerpb.TxStatusResponse{}, nil
	}
	return p.upstream.GetTransactionStatus(ctx, query)
}

func (p *deferredFinalityService) OpenNotificationStream(stream grpc.BidiStreamingServer[committerpb.NotificationRequest, committerpb.NotificationResponse]) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	p.activeSubscriptions.Add(1)
	defer p.activeSubscriptions.Add(-1)
	if p.holdNotifications.Load() {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	return stream.Send(&committerpb.NotificationResponse{TimeoutTxIds: request.TxStatusRequest.GetTxIds()})
}

func (p *deferredFinalityService) callsFor(txID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.queries[txID]
}

func (p *deferredFinalityService) queryCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	var total int
	for _, count := range p.queries {
		total += count
	}
	return total
}

func (h *e2eHarness) startDeferredFinality(t *testing.T, configPath string) *deferredFinalityService {
	t.Helper()
	upstream := h.queryServiceClient(t)
	certificate, err := tls.LoadX509KeyPair(h.orchestratorTLSCert(0), h.orchestratorTLSKey(0))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	for org := range 2 {
		rootPEM, err := os.ReadFile(h.peerTLSCA(org))
		if err != nil || !roots.AppendCertsFromPEM(rootPEM) {
			t.Fatalf("load org%d proxy client TLS CA: %v", org, err)
		}
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert,
	})))
	proxy := &deferredFinalityService{upstream: upstream, queries: make(map[string]int)}
	committerpb.RegisterQueryServiceServer(server, proxy)
	committerpb.RegisterNotifierServer(server, proxy)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	config, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	config = []byte(strings.NewReplacer("    port: 7001\n", fmt.Sprintf("    port: %d\n", port), "    port: 4001\n", fmt.Sprintf("    port: %d\n", port)).Replace(string(config)))
	if err := os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatal(err)
	}
	return proxy
}
