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

	"compatibility_service/pkg/orchestrator"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-sdk/network"
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

			// Asking for A explicitly now reads the already-updated record. A is
			// absent from subsequent status queries, proving B refreshed the store.
			finalA, err := h.invokeSigned(ctx, proposalA)
			if err != nil || finalA.TxID != txA || finalA.CommitStatus != "COMMITTED" || finalA.BlockNum != committedA.Ref.BlockNum || !finalA.IdempotentReplay || finalA.ChaincodeEvent == nil {
				t.Fatalf("older record was not refreshed: %+v err=%v", finalA, err)
			}
			if h.finalityProxy.callsFor(txA) != 1 {
				t.Fatal("older record was queried again instead of using the refreshed store")
			}
			committedB := h.waitForLedgerStatus(t, ctx, txB)
			if after := h.waitForHeight(t, committedB.Ref.BlockNum+1); after != before+2 {
				t.Fatalf("expected two transactions in two new blocks: before=%d after=%d", before, after)
			}
			logPath := filepath.Join(h.root, "runtime", "compatibility_service", "e2e", sanitize(t.Name())+"-orchestrator-org0.log")
			log, err := os.ReadFile(logPath)
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
			if maintenance < 0 || maintenance > newSubmission {
				t.Fatal("older status was not refreshed during the new submission")
			}

			dir := filepath.Join(h.root, "runtime", "compatibility_service", "lazy-finality")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			evidence, err := json.MarshalIndent(map[string]any{"scenario": name, "original_response": pending, "refreshed_record": finalA, "next_submission_response": responseB, "next_submission_block": committedB.Ref.BlockNum, "idle_status_queries": 0, "submissions_per_transaction": 1}, "", "  ")
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
	for ctx.Err() == nil {
		response, err := h.finalityProxy.upstream.GetTransactionStatus(ctx, &committerpb.TxStatusQuery{TxIds: []string{txID}})
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

// deferredFinalityService forwards real state/policy queries, withholds
// transaction statuses until released, and times out notification subscriptions.
// Fault injection is confined to this test server and temporary harness config.
type deferredFinalityService struct {
	committerpb.UnimplementedQueryServiceServer
	committerpb.UnimplementedNotifierServer
	upstream          committerpb.QueryServiceClient
	visible           atomic.Bool
	holdNotifications atomic.Bool
	mu                sync.Mutex
	queries           map[string]int
}

func (p *deferredFinalityService) GetRows(ctx context.Context, query *committerpb.Query) (*committerpb.Rows, error) {
	return p.upstream.GetRows(ctx, query)
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
	upstream, err := network.NewPeer(network.PeerConf{Address: "127.0.0.1:7001", TLS: network.TLSConfig{
		Mode: network.TLSModeMTLS, CertPath: h.orchestratorTLSCert(0), KeyPath: h.orchestratorTLSKey(0),
		CACertPaths: []string{h.peerMSPTLSCA(0)}, ServerName: "127.0.0.1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = upstream.Close() })
	certificate, err := tls.LoadX509KeyPair(h.orchestratorTLSCert(0), h.orchestratorTLSKey(0))
	if err != nil {
		t.Fatal(err)
	}
	rootPEM, err := os.ReadFile(h.peerTLSCA(0))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(rootPEM) {
		t.Fatal("load proxy client TLS CA")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert,
	})))
	proxy := &deferredFinalityService{upstream: committerpb.NewQueryServiceClient(upstream.Connection()), queries: make(map[string]int)}
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
