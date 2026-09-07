// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"compatibility_service/pkg/orchestrator"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"github.com/hyperledger/fabric-x-sdk/network"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	channelID = "channelqc4"
	namespace = "1"
)

var (
	buildOnce sync.Once
	buildErr  error
	portSeq   atomic.Int32
)

func TestMain(m *testing.M) {
	if os.Getenv("FABRIC_LOGGING_SPEC") == "" {
		_ = os.Setenv("FABRIC_LOGGING_SPEC", "error")
	}
	os.Exit(m.Run())
}

func TestDuplicateInFlightRequestSubmitsOnce(t *testing.T) {
	h := newHarness(t, harnessOptions{
		name:        "duplicate-in-flight",
		org0Sleep:   "2s",
		org1Sleep:   "2s",
		requestWait: "45s",
		finality:    "25s",
	})

	before := h.blockHeight(t)
	prop, _ := h.newSignedProposal(t, "compatv2", uniqueKey(t, "duplicate"), "value-duplicate", uniqueKey(t, "delete"))

	type callResult struct {
		response orchestrator.InvocationResponse
		err      error
	}
	results := make([]callResult, 2)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			results[index].response, results[index].err = h.invokeSigned(ctx, prop)
		}(i)
	}
	wg.Wait()

	for i, result := range results {
		if result.err != nil {
			t.Fatalf("duplicate call %d failed: %v", i, result.err)
		}
		if result.response.CommitStatus != "COMMITTED" {
			t.Fatalf("duplicate call %d status = %q", i, result.response.CommitStatus)
		}
	}
	if results[0].response.TxID != results[1].response.TxID {
		t.Fatalf("duplicate requests returned different tx IDs: %s vs %s", results[0].response.TxID, results[1].response.TxID)
	}
	if results[0].response.BlockNum != results[1].response.BlockNum {
		t.Fatalf("duplicate requests returned different blocks: %d vs %d", results[0].response.BlockNum, results[1].response.BlockNum)
	}
	if !results[0].response.IdempotentReplay && !results[1].response.IdempotentReplay {
		t.Fatal("expected one duplicate request to be served from the stored idempotency result")
	}

	after := h.waitForHeight(t, results[0].response.BlockNum+1)
	if after != before+1 {
		t.Fatalf("expected one committed block for duplicate in-flight requests, before=%d after=%d", before, after)
	}
}

func TestRemoteOrgUnavailablePreventsSubmit(t *testing.T) {
	h := newHarness(t, harnessOptions{
		name:        "remote-unavailable",
		disableOrg1: true,
		requestWait: "4s",
		finality:    "2s",
	})

	before := h.blockHeight(t)
	prop, _ := h.newSignedProposal(t, "compatv2", uniqueKey(t, "remote-down"), "value-remote-down", uniqueKey(t, "delete"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := h.invokeSigned(ctx, prop); err == nil {
		t.Fatal("expected remote unavailable invocation to fail")
	}
	assertHeightUnchanged(t, h, before)
}

func TestMismatchedRemoteResultPreventsSubmit(t *testing.T) {
	h := newHarness(t, harnessOptions{
		name:         "mismatch",
		org1Mismatch: "org1-different-result",
		requestWait:  "30s",
		finality:     "20s",
	})

	before := h.blockHeight(t)
	prop, _ := h.newSignedProposal(t, "compatv2", uniqueKey(t, "mismatch"), "value-mismatch", uniqueKey(t, "delete"))
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	_, err := h.invokeSigned(ctx, prop)
	if err == nil {
		t.Fatal("expected mismatched remote result to fail")
	}
	if !strings.Contains(err.Error(), "response payload mismatch") {
		t.Fatalf("expected response payload mismatch, got: %v", err)
	}
	assertHeightUnchanged(t, h, before)
}

func TestTimeoutPreventsSubmit(t *testing.T) {
	h := newHarness(t, harnessOptions{
		name:        "timeout",
		org0Sleep:   "3s",
		requestWait: "1s",
		finality:    "1s",
	})

	before := h.blockHeight(t)
	prop, _ := h.newSignedProposal(t, "compatv2", uniqueKey(t, "timeout"), "value-timeout", uniqueKey(t, "delete"))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	_, err := h.invokeSigned(ctx, prop)
	if err == nil {
		t.Fatal("expected timed out invocation to fail")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "deadline") && !strings.Contains(strings.ToLower(err.Error()), "timeout") {
		t.Fatalf("expected timeout/deadline error, got: %v", err)
	}
	assertHeightUnchanged(t, h, before)
}

func TestRetryAfterCompletedResultDoesNotSubmitAgain(t *testing.T) {
	h := newHarness(t, harnessOptions{
		name:        "completed-retry",
		requestWait: "45s",
		finality:    "25s",
	})

	before := h.blockHeight(t)
	prop, _ := h.newSignedProposal(t, "compatv2", uniqueKey(t, "retry"), "value-retry", uniqueKey(t, "delete"))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	first, err := h.invokeSigned(ctx, prop)
	if err != nil {
		t.Fatalf("first invoke failed: %v", err)
	}
	if first.CommitStatus != "COMMITTED" {
		t.Fatalf("first invoke status = %q", first.CommitStatus)
	}
	afterFirst := h.waitForHeight(t, first.BlockNum+1)
	if afterFirst != before+1 {
		t.Fatalf("expected first invoke to add one block, before=%d after=%d", before, afterFirst)
	}

	second, err := h.invokeSigned(ctx, prop)
	if err != nil {
		t.Fatalf("retry invoke failed: %v", err)
	}
	if !second.IdempotentReplay {
		t.Fatal("expected completed retry to be served from the stored idempotency result")
	}
	if second.TxID != first.TxID || second.BlockNum != first.BlockNum {
		t.Fatalf("retry returned different result: first=(%s,%d) second=(%s,%d)", first.TxID, first.BlockNum, second.TxID, second.BlockNum)
	}
	afterSecond := h.blockHeight(t)
	if afterSecond != afterFirst {
		t.Fatalf("retry should not commit another block, afterFirst=%d afterSecond=%d", afterFirst, afterSecond)
	}
}

func TestSameTxIDDifferentRequestConflicts(t *testing.T) {
	h := newHarness(t, harnessOptions{
		name:        "same-txid-conflict",
		requestWait: "45s",
		finality:    "25s",
	})

	before := h.blockHeight(t)
	nonce := randomNonce(t)
	key := uniqueKey(t, "conflict")
	deleteKey := uniqueKey(t, "delete")
	prop1, txID1 := h.newSignedProposalWithNonce(t, nonce, "compatv2", key, "value-one", deleteKey)
	prop2, txID2 := h.newSignedProposalWithNonce(t, nonce, "compatv2", key, "value-two", deleteKey)
	if txID1 != txID2 {
		t.Fatalf("same nonce and creator produced different tx IDs: %s vs %s", txID1, txID2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	first, err := h.invokeSigned(ctx, prop1)
	if err != nil {
		t.Fatalf("first invoke failed: %v", err)
	}
	if first.CommitStatus != "COMMITTED" {
		t.Fatalf("first invoke status = %q", first.CommitStatus)
	}
	afterFirst := h.waitForHeight(t, first.BlockNum+1)
	if afterFirst != before+1 {
		t.Fatalf("expected first invoke to add one block, before=%d after=%d", before, afterFirst)
	}

	_, err = h.invokeSigned(ctx, prop2)
	if err == nil {
		t.Fatal("expected same tx id with different args to be rejected")
	}
	if !strings.Contains(err.Error(), "idempotency conflict") {
		t.Fatalf("expected idempotency conflict, got: %v", err)
	}
	assertHeightUnchanged(t, h, afterFirst)
}

type harnessOptions struct {
	name         string
	disableOrg1  bool
	org0Sleep    string
	org1Sleep    string
	org1Mismatch string
	requestWait  string
	finality     string
}

type testPorts struct {
	org0CC     int
	org1CC     int
	org0Orch   int
	org1Orch   int
	unusedOrg1 int
}

type e2eHarness struct {
	root         string
	compatDir    string
	chaincodeDir string
	ports        testPorts
}

func newHarness(t *testing.T, opts harnessOptions) *e2eHarness {
	t.Helper()
	if opts.requestWait == "" {
		opts.requestWait = "45s"
	}
	if opts.finality == "" {
		opts.finality = "25s"
	}
	if opts.name == "" {
		opts.name = "e2e"
	}

	root := projectRoot(t)
	ensureBinaries(t, root)
	ensureNetworkReady(t, root)

	h := &e2eHarness{
		root:         root,
		compatDir:    filepath.Join(root, "compatibility_service"),
		chaincodeDir: filepath.Join(root, "sample_external_chaincode"),
		ports:        nextPorts(),
	}

	logDir := filepath.Join(root, "runtime", "compatibility_service", "e2e")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("create e2e log dir: %v", err)
	}

	cfgDir := t.TempDir()
	org0Cfg := h.writeOrchestratorConfig(t, cfgDir, 0, h.ports.org0Orch, h.ports.org0CC, 1, h.remoteOrg1Port(opts), opts.requestWait, opts.finality)
	org1Cfg := h.writeOrchestratorConfig(t, cfgDir, 1, h.ports.org1Orch, h.ports.org1CC, 0, h.ports.org0Orch, opts.requestWait, opts.finality)

	h.startProcess(t, "chaincode-org0", h.chaincodeDir, logDir, []string{envIfSet("E2E_COMPATV2_SLEEP", opts.org0Sleep)}, "./bin/e2e-chaincode", "-ccid", "0:sample", "-address", fmt.Sprintf("127.0.0.1:%d", h.ports.org0CC))
	waitForPort(t, h.ports.org0CC)

	if !opts.disableOrg1 {
		env := []string{
			envIfSet("E2E_COMPATV2_SLEEP", opts.org1Sleep),
			envIfSet("E2E_COMPATV2_MISMATCH", opts.org1Mismatch),
		}
		h.startProcess(t, "chaincode-org1", h.chaincodeDir, logDir, env, "./bin/e2e-chaincode", "-ccid", "0:sample", "-address", fmt.Sprintf("127.0.0.1:%d", h.ports.org1CC))
		waitForPort(t, h.ports.org1CC)
	}

	h.startProcess(t, "orchestrator-org0", h.compatDir, logDir, nil, "./bin/orchestrator", "-c", org0Cfg, "--log-level", "debug:grpc=error")
	waitForPort(t, h.ports.org0Orch)

	if !opts.disableOrg1 {
		h.startProcess(t, "orchestrator-org1", h.compatDir, logDir, nil, "./bin/orchestrator", "-c", org1Cfg, "--log-level", "debug:grpc=error")
		waitForPort(t, h.ports.org1Orch)
	}

	return h
}

func (h *e2eHarness) remoteOrg1Port(opts harnessOptions) int {
	if opts.disableOrg1 {
		return h.ports.unusedOrg1
	}
	return h.ports.org1Orch
}

func (h *e2eHarness) writeOrchestratorConfig(t *testing.T, dir string, org, serverPort, ccPort, remoteOrg, remotePort int, requestWait, finality string) string {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("orchestrator-org%d.yaml", org))
	data := fmt.Sprintf(`channel-id: %s
protocol: fabric-x
wait-after-submit: 0s
request-timeout: %s
finality-timeout: %s

server:
  endpoint:
    host: 127.0.0.1
    port: %d
  tls:
    mode: mtls
    cert-path: %s
    key-path: %s
    ca-cert-paths:
      - %s
      - %s

identity:
  msp-id: org-%d
  msp-dir: %s

query-service:
  endpoint:
    host: 127.0.0.1
    port: 7001
  tls:
    mode: mtls
    cert-path: %s
    key-path: %s
    ca-cert-paths:
      - %s
    server-name: 127.0.0.1

chaincode-service:
  endpoint:
    host: 127.0.0.1
    port: %d
  tls:
    mode: none

orderer:
  endpoint:
    host: 127.0.0.1
    port: 6022
  tls:
    mode: mtls
    cert-path: %s
    key-path: %s
    ca-cert-paths:
      - %s
    server-name: 127.0.0.1

notification-service:
  endpoint:
    host: 127.0.0.1
    port: 4001
  tls:
    mode: mtls
    cert-path: %s
    key-path: %s
    ca-cert-paths:
      - %s
    server-name: 127.0.0.1

remote-orchestrators:
  - msp-id: org-%d
    endpoint:
      host: 127.0.0.1
      port: %d
    tls:
      mode: mtls
      cert-path: %s
      key-path: %s
      ca-cert-paths:
        - %s
      server-name: 127.0.0.1
`,
		channelID,
		requestWait,
		finality,
		serverPort,
		h.helperTLSCert(org),
		h.helperTLSKey(org),
		h.peerTLSCA(0),
		h.peerTLSCA(1),
		org,
		h.clientMSP(org),
		h.helperTLSCert(org),
		h.helperTLSKey(org),
		h.peerMSPTLSCA(0),
		ccPort,
		h.helperTLSCert(org),
		h.helperTLSKey(org),
		h.ordererTLSCA(1),
		h.helperTLSCert(org),
		h.helperTLSKey(org),
		h.peerMSPTLSCA(0),
		remoteOrg,
		remotePort,
		h.helperTLSCert(org),
		h.helperTLSKey(org),
		h.peerTLSCA(remoteOrg),
	)
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatalf("write orchestrator config: %v", err)
	}
	return path
}

func (h *e2eHarness) startProcess(t *testing.T, name, dir, logDir string, env []string, args ...string) {
	t.Helper()
	logPath := filepath.Join(logDir, sanitize(t.Name())+"-"+name+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create log %s: %v", logPath, err)
	}

	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = dir
	cmd.Env = compactEnv(append(os.Environ(), env...))
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatalf("start %s: %v", name, err)
	}
	t.Logf("started %s pid=%d log=%s", name, cmd.Process.Pid, logPath)
	t.Cleanup(func() {
		terminateProcess(cmd)
		logFile.Close()
	})
}

func (h *e2eHarness) invokeSigned(ctx context.Context, prop *peer.SignedProposal) (orchestrator.InvocationResponse, error) {
	peerClient, err := network.NewPeer(network.PeerConf{
		Address: fmt.Sprintf("127.0.0.1:%d", h.ports.org0Orch),
		TLS: network.TLSConfig{
			Mode:        network.TLSModeMTLS,
			CertPath:    h.clientTLSCert(0),
			KeyPath:     h.clientTLSKey(0),
			CACertPaths: []string{h.peerTLSCA(0)},
			ServerName:  "127.0.0.1",
		},
	})
	if err != nil {
		return orchestrator.InvocationResponse{}, err
	}
	defer peerClient.Close() //nolint:errcheck

	ctx = metadata.AppendToOutgoingContext(ctx, orchestrator.GRPCOperationMetadata, orchestrator.GRPCOperationInvoke)
	resp, err := peerClient.ProcessProposal(ctx, prop)
	if err != nil {
		return orchestrator.InvocationResponse{}, err
	}
	if resp == nil || resp.Response == nil {
		return orchestrator.InvocationResponse{}, fmt.Errorf("orchestrator returned no response")
	}

	var out orchestrator.InvocationResponse
	if err := json.Unmarshal(resp.Response.Payload, &out); err != nil {
		return orchestrator.InvocationResponse{}, fmt.Errorf("decode orchestrator response: %w", err)
	}
	return out, nil
}

func (h *e2eHarness) newSignedProposal(t *testing.T, function string, args ...string) (*peer.SignedProposal, string) {
	t.Helper()
	return h.newSignedProposalWithNonce(t, randomNonce(t), function, args...)
}

func (h *e2eHarness) newSignedProposalWithNonce(t *testing.T, nonce []byte, function string, args ...string) (*peer.SignedProposal, string) {
	t.Helper()
	signer, err := identity.SignerFromMSP(h.clientMSP(0), "org-0")
	if err != nil {
		t.Fatalf("load client signer: %v", err)
	}
	creator, err := signer.Serialize()
	if err != nil {
		t.Fatalf("serialize client signer: %v", err)
	}

	inputArgs := make([][]byte, 0, 1+len(args))
	inputArgs = append(inputArgs, []byte(function))
	for _, arg := range args {
		inputArgs = append(inputArgs, []byte(arg))
	}

	txID := protoutil.ComputeTxID(nonce, creator)
	proposal, _, err := protoutil.CreateChaincodeProposalWithTxIDNonceAndTransient(
		txID,
		common.HeaderType_ENDORSER_TRANSACTION,
		channelID,
		&peer.ChaincodeInvocationSpec{
			ChaincodeSpec: &peer.ChaincodeSpec{
				Type: peer.ChaincodeSpec_CAR,
				ChaincodeId: &peer.ChaincodeID{
					Name:    namespace,
					Version: "1.0",
				},
				Input: &peer.ChaincodeInput{Args: inputArgs},
			},
		},
		nonce,
		creator,
		nil,
	)
	if err != nil {
		t.Fatalf("create proposal: %v", err)
	}
	prop, err := protoutil.GetSignedProposal(proposal, signer)
	if err != nil {
		t.Fatalf("sign proposal: %v", err)
	}
	return prop, txID
}

func randomNonce(t *testing.T) []byte {
	t.Helper()
	nonce := make([]byte, 24)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("create nonce: %v", err)
	}
	return nonce
}

func (h *e2eHarness) blockHeight(t *testing.T) uint64 {
	t.Helper()
	conn, err := h.blockQueryConn()
	if err != nil {
		t.Fatalf("connect block query service: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	info, err := committerpb.NewBlockQueryServiceClient(conn).GetBlockchainInfo(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("get blockchain info: %v", err)
	}
	return info.Height
}

func (h *e2eHarness) waitForHeight(t *testing.T, min uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		height := h.blockHeight(t)
		if height >= min {
			return height
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for block height >= %d", min)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (h *e2eHarness) blockQueryConn() (*grpc.ClientConn, error) {
	tlsCfg, err := (network.TLSConfig{
		Mode:        network.TLSModeMTLS,
		CertPath:    h.helperTLSCert(0),
		KeyPath:     h.helperTLSKey(0),
		CACertPaths: []string{h.peerMSPTLSCA(0)},
		ServerName:  "127.0.0.1",
	}).LoadClientTLSConfig("127.0.0.1")
	if err != nil {
		return nil, err
	}
	return grpc.NewClient("127.0.0.1:4001", grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
}

func assertHeightUnchanged(t *testing.T, h *e2eHarness, before uint64) {
	t.Helper()
	time.Sleep(6 * time.Second)
	after := h.blockHeight(t)
	if after != before {
		t.Fatalf("expected no new block, before=%d after=%d", before, after)
	}
}

func ensureBinaries(t *testing.T, root string) {
	t.Helper()
	buildOnce.Do(func() {
		buildErr = runBuild(root, filepath.Join(root, "compatibility_service"), "go", "build", "-o", "bin/orchestrator", "./cmd/orchestrator")
		if buildErr != nil {
			return
		}
		buildErr = runBuild(root, filepath.Join(root, "sample_external_chaincode"), "go", "build", "-o", "bin/e2e-chaincode", "./cmd/e2e-server")
	})
	if buildErr != nil {
		t.Fatalf("build e2e binaries: %v", buildErr)
	}
}

func runBuild(root, dir string, args ...string) error {
	if err := os.MkdirAll(filepath.Join(root, ".tmp"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(root, ".gocache"), 0o755); err != nil {
		return err
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GOCACHE="+filepath.Join(root, ".gocache"),
		"TMPDIR="+filepath.Join(root, ".tmp"),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s in %s: %w\n%s", strings.Join(args, " "), dir, err, string(out))
	}
	return nil
}

func ensureNetworkReady(t *testing.T, root string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(root, "artifacts", "config-block.pb.bin")); err != nil {
		t.Fatalf("missing generated artifacts: run scripts/build-images.sh, scripts/generate-artifacts.sh, scripts/start-network.sh, and scripts/create-namespace.sh first")
	}
	h := &e2eHarness{root: root}
	conn, err := h.blockQueryConn()
	if err != nil {
		t.Fatalf("Fabric-X block query service is not ready: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := committerpb.NewBlockQueryServiceClient(conn).GetBlockchainInfo(ctx, &emptypb.Empty{}); err != nil {
		t.Fatalf("Fabric-X network is not ready: %v", err)
	}
}

func projectRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func nextPorts() testPorts {
	base := 19000 + int(portSeq.Add(1))*100
	return testPorts{
		org0CC:     base + 1,
		org1CC:     base + 2,
		org0Orch:   base + 3,
		org1Orch:   base + 4,
		unusedOrg1: base + 44,
	}
}

func waitForPort(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
		if err == nil {
			conn.Close() //nolint:errcheck
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", addr)
}

func terminateProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}
}

func compactEnv(in []string) []string {
	out := in[:0]
	for _, value := range in {
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func envIfSet(key, value string) string {
	if value == "" {
		return ""
	}
	return key + "=" + value
}

func sanitize(value string) string {
	replacer := strings.NewReplacer("/", "_", " ", "_")
	return strings.ToLower(replacer.Replace(value))
}

func uniqueKey(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func (h *e2eHarness) artifact(parts ...string) string {
	return filepath.Join(append([]string{h.root, "artifacts"}, parts...)...)
}

func (h *e2eHarness) peerOrg(org int) string {
	return fmt.Sprintf("peer-org-%d", org)
}

func (h *e2eHarness) helperTLSCert(org int) string {
	peerOrg := h.peerOrg(org)
	return h.artifact("peerOrganizations", peerOrg, "peers", "helper."+peerOrg, "tls", "server.crt")
}

func (h *e2eHarness) helperTLSKey(org int) string {
	peerOrg := h.peerOrg(org)
	return h.artifact("peerOrganizations", peerOrg, "peers", "helper."+peerOrg, "tls", "server.key")
}

func (h *e2eHarness) clientTLSCert(org int) string {
	peerOrg := h.peerOrg(org)
	return h.artifact("peerOrganizations", peerOrg, "users", "client@"+peerOrg, "tls", "client.crt")
}

func (h *e2eHarness) clientTLSKey(org int) string {
	peerOrg := h.peerOrg(org)
	return h.artifact("peerOrganizations", peerOrg, "users", "client@"+peerOrg, "tls", "client.key")
}

func (h *e2eHarness) clientMSP(org int) string {
	peerOrg := h.peerOrg(org)
	return h.artifact("peerOrganizations", peerOrg, "users", "client@"+peerOrg, "msp")
}

func (h *e2eHarness) peerTLSCA(org int) string {
	peerOrg := h.peerOrg(org)
	return h.artifact("peerOrganizations", peerOrg, "tlsca", "tlsca."+peerOrg+"-cert.pem")
}

func (h *e2eHarness) peerMSPTLSCA(org int) string {
	peerOrg := h.peerOrg(org)
	return h.artifact("peerOrganizations", peerOrg, "msp", "tlscacerts", "tlsca."+peerOrg+"-cert.pem")
}

func (h *e2eHarness) ordererTLSCA(org int) string {
	ordererOrg := fmt.Sprintf("orderer-org-%d", org)
	return h.artifact("ordererOrganizations", ordererOrg, "msp", "tlscacerts", "tlsca."+ordererOrg+"-cert.pem")
}
