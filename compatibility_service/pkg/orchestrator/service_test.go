// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"compatibility_service/pkg/config"
	"compatibility_service/pkg/helper"
	"compatibility_service/pkg/lifecycle"

	gwclient "github.com/hyperledger/fabric-gateway/pkg/client"
	gwidentity "github.com/hyperledger/fabric-gateway/pkg/identity"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/gateway"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-committer/utils/connection"
	"github.com/hyperledger/fabric-x-committer/utils/serve"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/api/metadatapb"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	efab "github.com/hyperledger/fabric-x-sdk/endorsement/fabric"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"github.com/hyperledger/fabric-x-sdk/network"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestRequestTimeoutDefault(t *testing.T) {
	var cfg Config
	if got := cfg.requestTimeout(); got != 60*time.Second {
		t.Fatalf("expected default request timeout 60s, got %s", got)
	}
}

func TestRequestTimeoutConfigured(t *testing.T) {
	cfg := Config{RequestTimeout: 12 * time.Second}
	if got := cfg.requestTimeout(); got != 12*time.Second {
		t.Fatalf("expected configured request timeout 12s, got %s", got)
	}
}

func TestValidateRejectsInvalidTimeouts(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "negative request timeout",
			cfg:  validConfigWithTimeouts(-time.Second, 20*time.Second),
			want: "request-timeout must not be negative",
		},
		{
			name: "negative finality timeout",
			cfg:  validConfigWithTimeouts(20*time.Second, -time.Second),
			want: "finality-timeout must not be negative",
		},
		{
			name: "request timeout shorter than finality timeout",
			cfg:  validConfigWithTimeouts(5*time.Second, 20*time.Second),
			want: "request-timeout must be greater than or equal to finality-timeout",
		},
		{
			name: "configured request timeout shorter than default finality timeout",
			cfg:  validConfigWithTimeouts(20*time.Second, 0),
			want: "request-timeout must be greater than or equal to finality-timeout",
		},
		{
			name: "default request timeout shorter than configured finality timeout",
			cfg:  validConfigWithTimeouts(0, 90*time.Second),
			want: "request-timeout must be greater than or equal to finality-timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got %v", tt.want, err)
			}
		})
	}
}

func validConfigWithTimeouts(requestTimeout, finalityTimeout time.Duration) Config {
	endpoint := &config.Endpoint{Host: "127.0.0.1", Port: 1}
	return Config{
		ChannelID:       "channelqc4",
		Namespace:       "0",
		Server:          &serve.ServerConfig{Endpoint: connection.Endpoint{Host: "127.0.0.1", Port: 1}},
		Identity:        &config.IdentityConfig{MSPDir: "msp", MspID: "org-0"},
		QueryService:    config.ClientConfig{Endpoint: endpoint},
		ChaincodeSvc:    config.ChaincodeServiceConfig{Endpoint: endpoint},
		Orderer:         &config.ClientConfig{Endpoint: endpoint},
		NotificationSvc: &config.ClientConfig{Endpoint: endpoint},
		RequestTimeout:  requestTimeout,
		FinalityTimeout: finalityTimeout,
		ProposalAuth: ProposalAuthConfig{
			MSPs:  []ProposalMSPConfig{{MSPID: "org-0", MSPDir: "msp"}},
			Rules: []ProposalAuthorizationRule{{Chaincode: "*", Namespace: "*", Policy: "OR('org-0.member')"}},
		},
		LifecycleAuth: lifecycle.AuthConfig{
			Policies: map[string]string{
				"install": "OR('org-0.admin')", "queryinstalled": "OR('org-0.admin')", "approveformyorg": "OR('org-0.admin')",
				"commit": "OR('org-0.member')", "checkcommitreadiness": "OR('org-0.member')", "querycommitted": "OR('org-0.member')",
			},
			Orchestrators: []lifecycle.OrchestratorIdentity{{MSPID: "org-0", CertPath: "orchestrator-cert.pem"}},
		},
	}
}

func TestValidateRequiresLifecycleAuthorization(t *testing.T) {
	cfg := validConfigWithTimeouts(45*time.Second, 20*time.Second)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg.LifecycleAuth = lifecycle.AuthConfig{}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "lifecycle-auth") {
		t.Fatalf("unconfigured lifecycle authorization did not fail closed: %v", err)
	}
}

type gatewayExecutionProbe struct {
	mu          sync.Mutex
	proposals   []*peer.SignedProposal
	args        [][][]byte
	decorations []map[string][]byte
	transient   []map[string][]byte
	payload     []byte
}

func (p *gatewayExecutionProbe) Execute(ctx context.Context, execution *helper.ExecutionContext, inv endorsement.Invocation) (endorsement.ExecutionResult, helper.ExecutionMetadata, error) {
	original := execution.ClientSignedProposal()
	decoded, err := protoutil.UnmarshalProposal(original.ProposalBytes)
	if err != nil {
		return endorsement.ExecutionResult{}, helper.ExecutionMetadata{}, err
	}
	cpp, err := protoutil.UnmarshalChaincodeProposalPayload(decoded.Payload)
	if err != nil {
		return endorsement.ExecutionResult{}, helper.ExecutionMetadata{}, err
	}
	p.mu.Lock()
	p.proposals = append(p.proposals, original)
	p.args = append(p.args, inv.Args)
	p.decorations = append(p.decorations, execution.Decorations())
	p.transient = append(p.transient, cpp.TransientMap)
	p.mu.Unlock()
	if string(inv.Args[0]) == "wait" {
		<-ctx.Done()
		return endorsement.ExecutionResult{}, helper.ExecutionMetadata{}, ctx.Err()
	}
	execution.PutState("discarded-write", []byte("never submitted"))
	if string(inv.Args[0]) == "fail" {
		return endorsement.ExecutionResult{Status: 500, Message: "deliberate chaincode failure"}, helper.ExecutionMetadata{}, nil
	}
	return endorsement.Success(execution.Result(), "discarded-event", []byte("never ordered"), p.payload), helper.ExecutionMetadata{}, nil
}

func TestGatewayEvaluateSDKAndCustomQueryShareExecution(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	actor := org.issue(t, "client", time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	probe := &gatewayExecutionProbe{payload: []byte{0, 255, 254, 128, 'o', 'k'}}
	chaincodeHelper, err := helper.NewWithSigner(helper.ServiceConfig{
		ChannelID: "channelqc4", Protocol: "fabric-x", QueryService: network.PeerConf{Address: "127.0.0.1:1", TLS: network.TLSConfig{Mode: "none"}},
	}, actor.signer, map[string]helper.Executor{"*": probe}, sdk.NoOpLogger{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chaincodeHelper.Close() })
	store, err := lifecycle.NewMemoryStore("channelqc4")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.UpsertCommittedFromLedger(t.Context(), lifecycle.ChaincodeDefinition{Name: "sample", Version: "2.0", Sequence: 2}, true); err != nil {
		t.Fatal(err)
	}
	policyBytes, err := proto.Marshal(&applicationpb.NamespacePolicy{Rule: &applicationpb.NamespacePolicy_MspRule{MspRule: []byte("not evaluated for a query")}})
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{
		cfg:    Config{ChannelID: "channelqc4", Identity: &config.IdentityConfig{MspID: "org-0"}},
		signer: actor.signer, helper: chaincodeHelper, lifecycle: store, idempotency: newIdempotencyStore(), logger: sdk.NoOpLogger{},
		proposalAuth: proposalTestVerifier(t, []ProposalMSPConfig{org.config}, "OR('org-0.client')"),
		policies:     newNamespacePolicyResolver(&fakePolicyQueryClient{policies: &applicationpb.NamespacePolicies{Policies: []*applicationpb.PolicyItem{{Namespace: "1", Policy: policyBytes}}}}),
	}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	gateway.RegisterGatewayServer(server, service)
	peer.RegisterEndorserServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///gateway-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	id, err := gwidentity.NewX509Identity("org-0", actor.cert)
	if err != nil {
		t.Fatal(err)
	}
	sign, err := gwidentity.NewPrivateKeySign(actor.key)
	if err != nil {
		t.Fatal(err)
	}
	client, err := gwclient.Connect(id, gwclient.WithClientConnection(connection), gwclient.WithSign(sign))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	contract := client.GetNetwork("channelqc4").GetContract("sample:1")
	if _, err := contract.EvaluateTransaction("", "put", "unexpected"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty function changed argument interpretation: %v", err)
	}
	// Gateway routing is derived only from its signed chaincode name. These
	// legacy custom-API metadata values cannot turn evaluation into submission.
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(GRPCOperationMetadata, GRPCOperationInvoke, GRPCNamespaceMetadata, "2"))
	value, err := contract.EvaluateWithContext(ctx, "Contract:read", gwclient.WithBytesArguments([]byte{0, 255}), gwclient.WithTransient(map[string][]byte{"secret": {0, 254}}))
	if err != nil || !bytes.Equal(value, probe.payload) {
		t.Fatalf("Gateway changed binary response: %x %v", value, err)
	}
	probe.mu.Lock()
	if len(probe.proposals) != 1 || string(probe.args[0][0]) != "Contract:read" || !bytes.Equal(probe.args[0][1], []byte{0, 255}) || !bytes.Equal(probe.transient[0]["secret"], []byte{0, 254}) {
		t.Fatal("Gateway changed function arguments or transient data")
	}
	original := probe.proposals[0]
	decorations := probe.decorations[0]
	probe.mu.Unlock()
	if _, err := service.authenticateClientProposal(original); err != nil {
		t.Fatalf("original client signature was lost: %v", err)
	}
	parsed, err := service.authenticateClientProposal(original)
	if err != nil || parsed.invocation.Namespace != "sample:1" || parsed.invocation.ChaincodeVersion != "" {
		t.Fatalf("signed proposal was rewritten: %+v %v", parsed, err)
	}
	if string(decorations["compat.chaincode_name"]) != "sample" || string(decorations["compat.chaincode_version"]) != "2.0" || string(decorations["compat.chaincode_sequence"]) != "2" || string(decorations["compat.namespace"]) != "1" {
		t.Fatalf("incorrect internal lifecycle route: %v", decorations)
	}
	if len(service.idempotency.records) != 0 {
		t.Fatal("evaluation entered transaction tracking")
	}
	custom := proposalTestSigned(t, actor.signer, "")
	customCtx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(GRPCOperationMetadata, GRPCOperationQuery, GRPCNamespaceMetadata, "1"))
	response, err := peer.NewEndorserClient(connection).ProcessProposal(customCtx, custom)
	if err != nil {
		t.Fatal(err)
	}
	var wrapped InvocationResponse
	if err := json.Unmarshal(response.Response.Payload, &wrapped); err != nil || wrapped.Submitted || wrapped.PayloadBase64 != "AP/+gG9r" {
		t.Fatalf("custom Query response changed: %+v %v", wrapped, err)
	}
	if _, err := contract.EvaluateTransaction("fail"); status.Code(err) != codes.Unknown || !strings.Contains(err.Error(), "deliberate chaincode failure") {
		t.Fatalf("chaincode failure was returned as success: %v", err)
	}
	service.cfg.RequestTimeout = 30 * time.Millisecond
	if _, err := contract.EvaluateTransaction("wait"); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("server evaluation deadline was not propagated: %v", err)
	}
	if _, err := gateway.NewGatewayClient(connection).Endorse(t.Context(), &gateway.EndorseRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty Gateway Endorse request was accepted: %v", err)
	}
}

func TestGatewayEndorseSDKAndCustomInvokeShareExecution(t *testing.T) {
	org0, org1 := newProposalTestMSP(t, "org-0"), newProposalTestMSP(t, "org-1")
	now := time.Now()
	clientActor := org0.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	peer0 := org0.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	peer1 := org1.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	trust := []ProposalMSPConfig{org0.config, org1.config}
	policy := endorsementTestPolicy(t, "AND('org-0.peer','org-1.peer')")
	probe0, probe1 := &gatewayExecutionProbe{payload: []byte{0, 255, 128, 'o', 'k'}}, &gatewayExecutionProbe{payload: []byte{0, 255, 128, 'o', 'k'}}
	service0, connection0, _ := newGatewayTestService(t, "org-0", peer0, trust, policy, probe0)
	service1, _, address1 := newGatewayTestService(t, "org-1", peer1, trust, policy, probe1)
	endpoint, err := endpointFromAddress(address1)
	if err != nil {
		t.Fatal(err)
	}
	contact, err := newOrchestratorContact(RemoteOrchestratorConfig{MSPID: "org-1", Endpoint: endpoint, TLS: config.TLSConfig{Mode: "none"}}, sdk.NoOpLogger{})
	if err != nil {
		t.Fatal(err)
	}
	remotes := &staticRemoteOrchestratorResolver{contacts: map[string][]*OrchestratorContact{"org-1": {contact}}}
	service0.remotes = remotes
	t.Cleanup(func() { _ = remotes.Close() })
	id, err := gwidentity.NewX509Identity("org-0", clientActor.cert)
	if err != nil {
		t.Fatal(err)
	}
	sign, err := gwidentity.NewPrivateKeySign(clientActor.key)
	if err != nil {
		t.Fatal(err)
	}
	client, err := gwclient.Connect(id, gwclient.WithClientConnection(connection0), gwclient.WithSign(sign))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	contract := client.GetNetwork("channelqc4").GetContract("sample:1")
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs(GRPCOperationMetadata, GRPCOperationInvoke, GRPCNamespaceMetadata, "2"))
	for _, organizations := range [][]string{nil, {"org-0"}, {"org-1"}, {"org-1", "org-0", "org-1"}} {
		proposal, err := contract.NewProposal("Contract:write", gwclient.WithBytesArguments([]byte{0, 254}), gwclient.WithTransient(map[string][]byte{"secret": []byte("private-endorsement-input")}), gwclient.WithEndorsingOrganizations(organizations...))
		if err != nil {
			t.Fatal(err)
		}
		transaction, err := proposal.EndorseWithContext(ctx)
		if err != nil || !bytes.Equal(transaction.Result(), probe0.payload) || transaction.TransactionID() != proposal.TransactionID() {
			t.Fatalf("SDK Endorse failed for organizations=%v: %v", organizations, err)
		}
		serialized, err := transaction.Bytes()
		if err != nil {
			t.Fatal(err)
		}
		prepared := &gateway.PreparedTransaction{}
		if err := proto.Unmarshal(serialized, prepared); err != nil {
			t.Fatal(err)
		}
		parts := decodeGatewayTestTransaction(t, prepared.Envelope)
		if len(prepared.Envelope.Signature) != 0 || bytes.Contains(serialized, []byte("private-endorsement-input")) || len(parts.proposal.TransientMap) != 0 {
			t.Fatal("Endorse signed for the client or leaked transient data")
		}
		gotMSPs := make([]string, 0)
		for _, endorsement := range parts.native.Endorsements[0].EndorsementsWithIdentity {
			gotMSPs = append(gotMSPs, endorsement.Identity.MspId)
		}
		wantMSPs := organizations
		if len(wantMSPs) == 0 {
			wantMSPs = []string{"org-0", "org-1"}
		} else if len(wantMSPs) == 3 {
			wantMSPs = wantMSPs[:2]
		}
		if strings.Join(gotMSPs, ",") != strings.Join(wantMSPs, ",") {
			t.Fatalf("selected endorsements=%v want=%v", gotMSPs, wantMSPs)
		}
		if len(organizations) == 0 {
			compiled, err := newNamespaceEndorsementPolicy(policy, service0.proposalAuth.identities)
			if err != nil {
				t.Fatal(err)
			}
			if err := compiled.verifier.VerifyNs(transaction.TransactionID(), parts.native, 0); err != nil {
				t.Fatalf("prepared transaction lost its native endorsement validity: %v", err)
			}
		}
		signature, err := sign(transaction.Digest())
		if err != nil {
			t.Fatal(err)
		}
		signedTransaction, err := client.NewSignedTransaction(serialized, signature)
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
		creator, _ := clientActor.signer.Serialize()
		serializedIdentity := &msppb.Identity{}
		if err := proto.Unmarshal(creator, serializedIdentity); err != nil {
			t.Fatal(err)
		}
		identity, err := service0.proposalAuth.identities.DeserializeIdentity(serializedIdentity)
		if err != nil || identity.Verify(prepared.Envelope.Payload, prepared.Envelope.Signature) != nil {
			t.Fatalf("SDK did not sign the exact prepared envelope: %v", err)
		}
		mutated := append(append([]byte(nil), prepared.Envelope.Payload...), 0)
		if identity.Verify(mutated, prepared.Envelope.Signature) == nil {
			t.Fatal("client signature accepted a modified transaction")
		}
	}
	if len(service0.idempotency.records) != 0 || len(service1.idempotency.records) != 0 {
		t.Fatal("Gateway Endorse entered submission tracking")
	}
	probe1.mu.Lock()
	original := proto.Clone(probe1.proposals[0]).(*peer.SignedProposal)
	if !bytes.Equal(probe1.transient[0]["secret"], []byte("private-endorsement-input")) || string(probe1.decorations[0]["compat.chaincode_name"]) != "sample" {
		t.Fatal("remote endorsement lost client context or logical routing")
	}
	probe1.mu.Unlock()
	original.Signature = nil
	if _, err := contact.EndorseOnly(t.Context(), InvocationRequest{ClientSignedProposal: original, Namespace: "1"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("remote Orchestrator did not verify the original client signature: %v", err)
	}
	for _, function := range []string{"fail", "wait"} {
		if function == "wait" {
			service0.cfg.RequestTimeout = 20 * time.Millisecond
		}
		proposal, err := contract.NewProposal(function)
		if err != nil {
			t.Fatal(err)
		}
		_, err = proposal.EndorseWithContext(t.Context())
		want := codes.Unknown
		if function == "wait" {
			want = codes.DeadlineExceeded
		}
		if status.Code(err) != want {
			t.Fatalf("Endorse %s error=%v want=%s", function, err, want)
		}
	}
	service0.cfg.RequestTimeout = time.Second
	probe1.payload = []byte("different-execution-result")
	mismatch, err := contract.NewProposal("write")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mismatch.EndorseWithContext(t.Context()); status.Code(err) != codes.Aborted {
		t.Fatalf("inconsistent organizational results were endorsed: %v", err)
	}
	probe1.payload = probe0.payload
	proposal, err := contract.NewProposal("write", gwclient.WithEndorsingOrganizations("missing-org", "org-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := proposal.EndorseWithContext(t.Context()); status.Code(err) != codes.Aborted {
		t.Fatalf("missing explicitly requested endorser was ignored: %v", err)
	}
	// The existing custom API continues through submission after the same shared
	// stage, while the unavailable notifier leaves its record SUBMITTED.
	submitter := &finalityTestSubmitter{}
	service0.submitter, service0.notify = submitter, &finalityTestNotifier{mode: "unavailable"}
	custom := proposalTestSigned(t, clientActor.signer, "")
	customContext := metadata.NewIncomingContext(t.Context(), metadata.Pairs(GRPCOperationMetadata, GRPCOperationInvoke, GRPCNamespaceMetadata, "1"))
	response, err := service0.ProcessProposal(customContext, custom)
	if err != nil {
		t.Fatal(err)
	}
	var invoked InvocationResponse
	if err := json.Unmarshal(response.Response.Payload, &invoked); err != nil || !invoked.Submitted || !invoked.FinalityPending || submitter.calls.Load() != 1 || len(invoked.LifecycleBindings) != 2 {
		t.Fatalf("custom Invoke stopped using the shared endorsement stage: %+v %v", invoked, err)
	}
	service0.queryService = &finalityTestQuery{get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		t.Error("Gateway Endorse reconciled unrelated submitted transactions")
		return nil, nil
	}}
	pending, err := contract.NewProposal("write")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pending.EndorseWithContext(t.Context()); err != nil || submitter.calls.Load() != 1 {
		t.Fatalf("Gateway Endorse entered submission or finality: %v", err)
	}
}

func newGatewayTestService(t *testing.T, mspID string, signer proposalTestActor, trusted []ProposalMSPConfig, policy NamespacePolicySnapshot, executor helper.Executor) (*Service, *grpc.ClientConn, string) {
	t.Helper()
	chaincodeHelper, err := helper.NewWithSigner(helper.ServiceConfig{
		ChannelID: "channelqc4", Protocol: "fabric-x", QueryService: network.PeerConf{Address: "127.0.0.1:1", TLS: network.TLSConfig{Mode: "none"}},
	}, signer.signer, map[string]helper.Executor{"*": executor}, sdk.NoOpLogger{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = chaincodeHelper.Close() })
	store, err := lifecycle.NewMemoryStore("channelqc4")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.UpsertCommittedFromLedger(t.Context(), lifecycle.ChaincodeDefinition{Name: "sample", Version: "2.0", Sequence: 2}, true); err != nil {
		t.Fatal(err)
	}
	policyBytes, err := proto.Marshal(policy.Policy)
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{
		cfg:    Config{ChannelID: "channelqc4", Identity: &config.IdentityConfig{MspID: mspID}},
		signer: signer.signer, helper: chaincodeHelper, lifecycle: store, idempotency: newIdempotencyStore(), logger: sdk.NoOpLogger{},
		proposalAuth: proposalTestVerifier(t, trusted, "OR('org-0.client')"),
		policies:     newNamespacePolicyResolver(&fakePolicyQueryClient{policies: &applicationpb.NamespacePolicies{Policies: []*applicationpb.PolicyItem{{Namespace: policy.Namespace, Version: policy.Version, Policy: policyBytes}}}}),
	}
	t.Cleanup(service.stopReconciliation)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	gateway.RegisterGatewayServer(server, service)
	peer.RegisterEndorserServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return service, connection, listener.Addr().String()
}

func gatewayTestProposal(t *testing.T, actor proposalTestActor, route string, isInit bool) *peer.SignedProposal {
	t.Helper()
	original := proposalTestSigned(t, actor.signer, "")
	proposal, _ := protoutil.UnmarshalProposal(original.ProposalBytes)
	header, _ := protoutil.UnmarshalHeader(proposal.Header)
	channel, _ := protoutil.UnmarshalChannelHeader(header.ChannelHeader)
	channel.Extension = protoutil.MarshalOrPanic(&peer.ChaincodeHeaderExtension{ChaincodeId: &peer.ChaincodeID{Name: route}})
	header.ChannelHeader = protoutil.MarshalOrPanic(channel)
	proposal.Header = protoutil.MarshalOrPanic(header)
	payload, _ := protoutil.UnmarshalChaincodeProposalPayload(proposal.Payload)
	spec, _ := protoutil.UnmarshalChaincodeInvocationSpec(payload.Input)
	spec.ChaincodeSpec.ChaincodeId = &peer.ChaincodeID{Name: route}
	spec.ChaincodeSpec.Input.IsInit = isInit
	payload.Input = protoutil.MarshalOrPanic(spec)
	proposal.Payload = protoutil.MarshalOrPanic(payload)
	signed, err := protoutil.GetSignedProposal(proposal, actor.signer)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func TestGatewaySubmitSDKUsesSharedOrdering(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	clientActor := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	node0 := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	node0b := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	policy := endorsementTestPolicy(t, "OR('org-0.peer')")
	probe0, probe0b := &gatewayExecutionProbe{payload: []byte{0, 255, 'o', 'k'}}, &gatewayExecutionProbe{}
	origin, connection0, _ := newGatewayTestService(t, "org-0", node0, []ProposalMSPConfig{org.config}, policy, probe0)
	target, connection0b, _ := newGatewayTestService(t, "org-0", node0b, []ProposalMSPConfig{org.config}, policy, probe0b)
	client0, _ := newGatewayTestClient(t, connection0, clientActor)
	client0b, _ := newGatewayTestClient(t, connection0b, clientActor)
	proposal, err := client0.GetNetwork("channelqc4").GetContract("sample:1").NewProposal("write", gwclient.WithBytesArguments([]byte{0, 128}))
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := proposal.EndorseWithContext(t.Context())
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
	expectedNative := decodeGatewayTestTransaction(t, prepared.Envelope).action.Results
	submitter := &gatewaySubmissionProbe{entered: make(chan struct{}, 2), release: make(chan struct{})}
	notifier := &gatewayNotificationProbe{}
	target.submitter, target.notify = submitter, notifier
	target.queryService = &finalityTestQuery{get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		return &committerpb.TxStatusResponse{}, nil
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 2)
	for range 2 {
		copy, err := client0b.NewTransaction(serialized)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			commit, err := copy.SubmitWithContext(ctx)
			if err == nil && commit.TransactionID() != proposal.TransactionID() {
				err = errors.New("Submit changed the transaction ID")
			}
			done <- err
		}()
	}
	select {
	case <-submitter.entered:
	case <-ctx.Done():
		t.Fatal("Submit never reached the shared ordering stage")
	}
	select {
	case err := <-done:
		t.Fatalf("Submit returned before the ordering stage acknowledged: %v", err)
	default:
	}
	close(submitter.release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if submitter.calls.Load() != 1 || notifier.calls.Load() != 0 || len(probe0b.proposals) != 0 || len(origin.idempotency.records) != 0 {
		t.Fatal("Submit resent, executed chaincode, used the Endorse cache, or observed finality")
	}
	submitter.mu.Lock()
	ordered, err := createCompatFabricXTx(submitter.end.Proposal, submitter.end.Responses...)
	submitter.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	orderedPayload, err := protoutil.UnmarshalPayload(ordered.Payload)
	if err != nil || !bytes.Equal(orderedPayload.Data, expectedNative) {
		t.Fatalf("ordering did not receive the exact native transaction signed by the client: %v", err)
	}
	record := target.idempotency.records[proposal.TransactionID()]
	assertFinalityState(t, target, record, idempotencySubmitted)
	if record.Response.CommitStatus != "" || !record.Response.FinalityPending {
		t.Fatal("Submit manufactured a commit outcome")
	}
	// A delivery retry stays successful after a lifecycle update; it neither
	// revalidates a transaction already delivered nor submits it a second time.
	if _, err := target.lifecycle.UpsertCommittedFromLedger(t.Context(), lifecycle.ChaincodeDefinition{Name: "sample", Version: "3.0", Sequence: 3}, true); err != nil {
		t.Fatal(err)
	}
	retry, err := client0b.NewTransaction(serialized)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retry.SubmitWithContext(ctx); err != nil || submitter.calls.Load() != 1 {
		t.Fatalf("acknowledged transaction was not replayed safely: %v", err)
	}
	// A second valid native result with the same transaction ID cannot replace
	// what was already submitted, even if the client signs that new envelope.
	parsed, err := parseGatewayTransaction(prepared.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Native.Namespaces[0].BlindWrites[0].Value = []byte("different-native-result")
	resignGatewayTestNative(t, node0.signer, parsed.Request.ClientTxID, parsed.Native)
	changed := gatewayTestEnvelopeFromNative(t, parsed, parsed.Native)
	changed.Signature, err = clientActor.signer.Sign(changed.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Submit(ctx, &gateway.SubmitRequest{ChannelId: "channelqc4", TransactionId: parsed.Request.ClientTxID, PreparedTransaction: changed}); status.Code(err) != codes.AlreadyExists || submitter.calls.Load() != 1 {
		t.Fatalf("different transaction reused an acknowledged ID: %v", err)
	}
}

func TestGatewaySubmitRejectsAlteredTransactions(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	client := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	node := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	for _, tc := range []struct {
		name string
		want codes.Code
	}{
		{"unsigned", codes.InvalidArgument}, {"invalid-client-signature", codes.Unauthenticated},
		{"changed-without-client-signature", codes.Unauthenticated}, {"outer-channel", codes.InvalidArgument}, {"outer-txid", codes.InvalidArgument},
		{"native-write", codes.FailedPrecondition}, {"binding-version", codes.FailedPrecondition}, {"init-intent", codes.InvalidArgument},
		{"chaincode-name", codes.InvalidArgument}, {"response-payload", codes.InvalidArgument}, {"response-message", codes.InvalidArgument},
		{"event", codes.InvalidArgument}, {"transient", codes.InvalidArgument}, {"namespace-policy-version", codes.FailedPrecondition},
		{"native-signature", codes.FailedPrecondition}, {"duplicate-signer", codes.FailedPrecondition}, {"multiple-actions", codes.InvalidArgument},
		{"missing-proposal-binding", codes.InvalidArgument}, {"unauthorized-creator", codes.PermissionDenied}, {"stale-definition", codes.FailedPrecondition},
		{"event-identity", codes.InvalidArgument}, {"event-identity-signature", codes.FailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, _, _ := newGatewayTestService(t, "org-0", node, []ProposalMSPConfig{org.config}, endorsementTestPolicy(t, "OR('org-0.peer')"), &gatewayExecutionProbe{})
			submitter := &gatewaySubmissionProbe{}
			notifier := &gatewayNotificationProbe{}
			service.submitter, service.notify = submitter, notifier
			creator := client
			if tc.name == "unauthorized-creator" {
				creator = node
			}
			signed := gatewayTestProposal(t, creator, "sample:1", false)
			proposal, _ := protoutil.UnmarshalProposal(signed.ProposalBytes)
			result := gatewayTestEndorsement(t, node, signed, 7, "payload")
			envelope, err := createGatewayTransaction(proposal, "2.0", result.Endorsement)
			if err != nil {
				t.Fatal(err)
			}
			envelope.Signature, err = creator.signer.Sign(envelope.Payload)
			if err != nil {
				t.Fatal(err)
			}
			request := &gateway.SubmitRequest{ChannelId: "channelqc4", TransactionId: result.TxID, PreparedTransaction: envelope}
			original := proto.Clone(request).(*gateway.SubmitRequest)
			parts := decodeGatewayTestTransaction(t, envelope)
			switch tc.name {
			case "outer-channel":
				request.ChannelId = "other"
			case "outer-txid":
				request.TransactionId = "other"
			case "changed-without-client-signature", "native-write":
				parts.native.Namespaces[0].BlindWrites[0].Value = []byte("forged-write")
			case "binding-version":
				binding := &helper.GatewayProposalBinding{}
				if err := json.Unmarshal(parts.native.Metadata[1], binding); err != nil {
					t.Fatal(err)
				}
				binding.ChaincodeVersion, binding.Sequence = "3.0", 3
				parts.native.Metadata[1], err = json.Marshal(binding)
				if err != nil {
					t.Fatal(err)
				}
				parts.action.ChaincodeId.Version = "3.0"
				if _, err := service.lifecycle.UpsertCommittedFromLedger(t.Context(), lifecycle.ChaincodeDefinition{Name: "sample", Version: "3.0", Sequence: 3}, true); err != nil {
					t.Fatal(err)
				}
			case "init-intent", "chaincode-name":
				spec, _ := protoutil.UnmarshalChaincodeInvocationSpec(parts.proposal.Input)
				if tc.name == "init-intent" {
					spec.ChaincodeSpec.Input.IsInit = true
				} else {
					spec.ChaincodeSpec.ChaincodeId.Name = "other:1"
					parts.action.ChaincodeId.Name = "other:1"
					channel, _ := protoutil.UnmarshalChannelHeader(parts.payload.Header.ChannelHeader)
					channel.Extension = protoutil.MarshalOrPanic(&peer.ChaincodeHeaderExtension{ChaincodeId: spec.ChaincodeSpec.ChaincodeId})
					parts.payload.Header.ChannelHeader = protoutil.MarshalOrPanic(channel)
					service.proposalAuth.rules[0].chaincode = "*"
				}
				parts.proposal.Input = protoutil.MarshalOrPanic(spec)
				parts.response.ProposalHash, err = protoutil.GetProposalHash2(parts.payload.Header, protoutil.MarshalOrPanic(parts.proposal))
				if err != nil {
					t.Fatal(err)
				}
			case "response-payload":
				parts.action.Response.Payload = []byte("forged-response")
			case "response-message":
				parts.action.Response.Message = "forged-message"
			case "event":
				parts.action.Events = protoutil.MarshalOrPanic(&peer.ChaincodeEvent{EventName: "forged-event"})
			case "transient":
				parts.proposal.TransientMap = map[string][]byte{"secret": []byte("private")}
			case "namespace-policy-version":
				parts.native.Namespaces[0].NsVersion++
			case "native-signature":
				parts.native.Endorsements[0].EndorsementsWithIdentity[0].Endorsement = []byte("invalid-native-signature")
				parts.actionPayload.Action.Endorsements[0].Signature = []byte("invalid-native-signature")
			case "event-identity":
				identity, err := helper.ChaincodeIdentityFromMetadata(parts.native.Metadata)
				if err != nil || identity == nil {
					t.Fatalf("no event identity: %v", err)
				}
				identity.ChaincodeName = "forged"
				encoded, err := json.Marshal(identity)
				if err != nil {
					t.Fatal(err)
				}
				parts.native.Metadata[2] = encoded
			case "event-identity-signature":
				// Meaning-preserving whitespace still changes the signed native bytes.
				parts.native.Metadata[2] = append(parts.native.Metadata[2], ' ')
			case "duplicate-signer":
				duplicate := proto.Clone(parts.native.Endorsements[0].EndorsementsWithIdentity[0]).(*applicationpb.EndorsementWithIdentity)
				duplicate.Identity.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
				parts.native.Endorsements[0].EndorsementsWithIdentity = append(parts.native.Endorsements[0].EndorsementsWithIdentity, duplicate)
				parts.actionPayload.Action.Endorsements = append(parts.actionPayload.Action.Endorsements, &peer.Endorsement{Endorser: protoutil.MarshalOrPanic(duplicate.Identity), Signature: duplicate.Endorsement})
			case "multiple-actions":
				parts.transaction.Actions = append(parts.transaction.Actions, proto.Clone(parts.transaction.Actions[0]).(*peer.TransactionAction))
			case "missing-proposal-binding":
				parts.native.Metadata = parts.native.Metadata[:1]
			case "stale-definition":
				if _, err := service.lifecycle.UpsertCommittedFromLedger(t.Context(), lifecycle.ChaincodeDefinition{Name: "sample", Version: "3.0", Sequence: 3}, true); err != nil {
					t.Fatal(err)
				}
			}
			envelope.Payload = marshalGatewayTestTransaction(t, parts)
			if tc.name != "changed-without-client-signature" {
				envelope.Signature, err = creator.signer.Sign(envelope.Payload)
				if err != nil {
					t.Fatal(err)
				}
			}
			if tc.name == "unsigned" {
				envelope.Signature = nil
			} else if tc.name == "invalid-client-signature" {
				envelope.Signature = []byte("invalid-client-signature")
			}
			if _, err := service.Submit(t.Context(), request); status.Code(err) != tc.want {
				t.Fatalf("altered transaction accepted or misclassified: error=%v want=%s", err, tc.want)
			}
			if submitter.calls.Load() != 0 || notifier.calls.Load() != 0 || len(service.idempotency.records) != 0 {
				t.Fatal("invalid submission reached ordering, finality, or reserved its transaction ID")
			}
			if tc.name == "native-write" {
				if _, err := service.Submit(t.Context(), original); err != nil || submitter.calls.Load() != 1 {
					t.Fatalf("invalid submission poisoned the valid transaction ID: %v", err)
				}
			}
		})
	}
	service, _, _ := newGatewayTestService(t, "org-0", node, []ProposalMSPConfig{org.config}, endorsementTestPolicy(t, "OR('org-0.peer')"), &gatewayExecutionProbe{})
	if _, err := service.Submit(t.Context(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil Submit request accepted: %v", err)
	}
}

func TestGatewaySubmitOrderingErrorsAndInitState(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	client := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	node := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	for _, scenario := range []string{"unavailable", "rejected", "ack-lost", "init"} {
		t.Run(scenario, func(t *testing.T) {
			service, _, _ := newGatewayTestService(t, "org-0", node, []ProposalMSPConfig{org.config}, endorsementTestPolicy(t, "OR('org-0.peer')"), &gatewayExecutionProbe{})
			submitter, notifier := &gatewaySubmissionProbe{}, &gatewayNotificationProbe{}
			service.submitter, service.notify = submitter, notifier
			if scenario == "init" {
				store, err := lifecycle.NewMemoryStore("channelqc4")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = store.Close() })
				service.lifecycle = store
				if _, err := store.UpsertCommittedFromLedger(t.Context(), lifecycle.ChaincodeDefinition{Name: "sample", Version: "2.0", Sequence: 2, InitRequired: true}, false); err != nil {
					t.Fatal(err)
				}
			}
			signed := gatewayTestProposal(t, client, "sample:1", scenario == "init")
			proposal, _ := protoutil.UnmarshalProposal(signed.ProposalBytes)
			result := gatewayTestEndorsement(t, node, signed, 7, "payload")
			envelope, err := createGatewayTransaction(proposal, "2.0", result.Endorsement)
			if err != nil {
				t.Fatal(err)
			}
			envelope.Signature, err = client.signer.Sign(envelope.Payload)
			if err != nil {
				t.Fatal(err)
			}
			want := codes.OK
			switch scenario {
			case "unavailable":
				submitter.err, want = errors.New("ordering unavailable before send"), codes.Unavailable
			case "rejected":
				submitter.err, want = &orderingSubmissionError{err: errors.New("ordering rejected transaction")}, codes.Aborted
			case "ack-lost":
				submitter.err, want = &orderingSubmissionError{err: context.DeadlineExceeded, uncertain: true}, codes.DeadlineExceeded
			}
			request := &gateway.SubmitRequest{ChannelId: "channelqc4", TransactionId: result.TxID, PreparedTransaction: envelope}
			if _, err := service.Submit(t.Context(), request); status.Code(err) != want {
				t.Fatalf("unexpected ordering result: %v want=%s", err, want)
			}
			record := service.idempotency.records[result.TxID]
			if scenario == "init" || scenario == "ack-lost" {
				assertFinalityState(t, service, record, idempotencySubmitted)
			} else if record != nil {
				t.Fatal("definite ordering failure retained a finished record")
			}
			wantCalls := int32(1)
			if scenario == "unavailable" || scenario == "rejected" {
				wantCalls = 2 // No delivery occurred, so a fresh attempt is allowed.
			}
			if _, err := service.Submit(t.Context(), request); status.Code(err) != want || submitter.calls.Load() != wantCalls || notifier.calls.Load() != 0 {
				t.Fatalf("retry resent or waited for finality: %v", err)
			}
			if scenario == "init" {
				definition, _, err := service.lifecycle.GetCommitted(t.Context(), "sample")
				if err != nil || definition.Initialized || record.afterCommit == nil {
					t.Fatalf("Submit marked Init complete before finality or lost its reconciliation callback: %v", err)
				}
			} else if scenario == "ack-lost" {
				// A later confirmed ledger outcome proves delivery. Submit retries
				// acknowledge that delivery even if lifecycle bookkeeping failed.
				service.idempotency.confirmFinality(record, InvocationResponse{TxID: result.TxID, Submitted: true, CommitStatus: "COMMITTED"}, errors.New("post-commit bookkeeping failed"))
				if _, found, err := service.idempotency.existing(result.TxID, record.Digest); err != nil || found {
					t.Fatalf("completed delivery remained in the pending store: %v", err)
				}
			}
		})
	}
}

func newGatewayTestClient(t *testing.T, connection *grpc.ClientConn, actor proposalTestActor) (*gwclient.Gateway, gwidentity.Sign) {
	t.Helper()
	creator, err := actor.signer.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	serialized := &msppb.Identity{}
	if err := proto.Unmarshal(creator, serialized); err != nil {
		t.Fatal(err)
	}
	identity, err := gwidentity.NewX509Identity(serialized.MspId, actor.cert)
	if err != nil {
		t.Fatal(err)
	}
	sign, err := gwidentity.NewPrivateKeySign(actor.key)
	if err != nil {
		t.Fatal(err)
	}
	client, err := gwclient.Connect(identity, gwclient.WithClientConnection(connection), gwclient.WithSign(sign))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, sign
}

type gatewaySubmissionProbe struct {
	mu       sync.Mutex
	calls    atomic.Int32
	end      sdk.Endorsement
	entered  chan struct{}
	release  chan struct{}
	err      error
	onSubmit func(sdk.Endorsement)
}

func (s *gatewaySubmissionProbe) Submit(ctx context.Context, end sdk.Endorsement) error {
	s.calls.Add(1)
	s.mu.Lock()
	s.end = end
	s.mu.Unlock()
	if s.entered != nil {
		s.entered <- struct{}{}
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.err == nil && s.onSubmit != nil {
		s.onSubmit(end)
	}
	return s.err
}

func (*gatewaySubmissionProbe) Close() error { return nil }

type gatewayNotificationProbe struct {
	committerpb.NotifierClient
	calls atomic.Int32
}

func (n *gatewayNotificationProbe) OpenNotificationStream(context.Context, ...grpc.CallOption) (committerpb.Notifier_OpenNotificationStreamClient, error) {
	n.calls.Add(1)
	return nil, errors.New("unexpected finality subscription")
}

func TestGatewayCommitStatusSDK(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	actor := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	node := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	probe := &gatewayExecutionProbe{payload: []byte{0, 255, 'o', 'k'}}
	service, connection, _ := newGatewayTestService(t, "org-0", node, []ProposalMSPConfig{org.config}, endorsementTestPolicy(t, "OR('org-0.peer')"), probe)
	client, _ := newGatewayTestClient(t, connection, actor)
	var outcomes sync.Map
	var queries atomic.Int32
	service.queryService = &finalityTestQuery{get: func(ctx context.Context, request *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		queries.Add(1)
		if len(request.TxIds) != 1 {
			t.Error("status request queried unrelated transactions")
		}
		result := &committerpb.TxStatusResponse{}
		if value, ok := outcomes.Load(request.TxIds[0]); ok {
			result.Statuses = []*committerpb.TxStatus{value.(*committerpb.TxStatus)}
		}
		return result, nil
	}}
	notifier := &gatewayNotificationProbe{}
	service.notify = notifier
	for _, tc := range []struct {
		native committerpb.Status
		code   peer.TxValidationCode
	}{
		{committerpb.Status_COMMITTED, peer.TxValidationCode_VALID},
		{committerpb.Status_ABORTED_MVCC_CONFLICT, peer.TxValidationCode_MVCC_READ_CONFLICT},
		{committerpb.Status_ABORTED_SIGNATURE_INVALID, peer.TxValidationCode_ENDORSEMENT_POLICY_FAILURE},
		{committerpb.Status_MALFORMED_NO_WRITES, peer.TxValidationCode_INVALID_WRITESET},
	} {
		t.Run(tc.native.String(), func(t *testing.T) {
			txID := "historical-" + tc.native.String()
			outcomes.Store(txID, finalityTestStatus(txID, tc.native))
			commit, err := client.NewCommit(protoutil.MarshalOrPanic(gatewayTestStatusRequest(t, actor, txID)))
			if err != nil {
				t.Fatal(err)
			}
			result, err := commit.StatusWithContext(t.Context())
			if err != nil || result.Code != tc.code || result.Successful != (tc.code == peer.TxValidationCode_VALID) || result.BlockNumber != 42 || result.TransactionID != txID {
				t.Fatalf("SDK status did not preserve validation semantics: %+v %v", result, err)
			}
		})
	}
	if len(service.idempotency.records) != 0 || len(probe.proposals) != 0 || notifier.calls.Load() != 0 {
		t.Fatal("historical status required a local record, chaincode execution, or notification subscription")
	}
	submitter := &gatewaySubmissionProbe{onSubmit: func(end sdk.Endorsement) {
		txID, err := txIDFromEndorsement(end)
		if err != nil {
			t.Error(err)
			return
		}
		outcomes.Store(txID, finalityTestStatus(txID, committerpb.Status_COMMITTED))
	}}
	service.submitter = submitter
	result, err := client.GetNetwork("channelqc4").GetContract("sample:1").SubmitTransaction("write", "asset")
	if err != nil || !bytes.Equal(result, probe.payload) || submitter.calls.Load() != 1 || notifier.calls.Load() != 0 {
		t.Fatalf("combined SDK submit-and-wait failed: %x %v", result, err)
	}
	for _, record := range service.idempotency.records {
		assertFinalityState(t, service, record, idempotencyCompleted)
	}
	if len(probe.proposals) != 1 || queries.Load() < 5 {
		t.Fatal("SDK stages did not share the existing execution/submission path")
	}
}

func TestGatewayCommitStatusFailureDoesNotChangeSubmittedState(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	actor := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	verifier := proposalTestVerifier(t, []ProposalMSPConfig{org.config}, "OR('org-0.client')")
	for _, tc := range []struct {
		name string
		want codes.Code
	}{
		{"deadline", codes.DeadlineExceeded}, {"canceled", codes.Canceled}, {"query unavailable", codes.Unavailable},
		{"invalid signature", codes.Unauthenticated}, {"reader policy", codes.PermissionDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var queries atomic.Int32
			stopped := make(chan struct{})
			localVerifier := *verifier
			service := &Service{
				cfg: Config{ChannelID: "channelqc4", RequestTimeout: 120 * time.Millisecond}, proposalAuth: &localVerifier,
				logger: sdk.NoOpLogger{}, idempotency: newIdempotencyStore(), notify: &finalityTestNotifier{stopped: stopped},
				queryService: &finalityTestQuery{get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
					queries.Add(1)
					if tc.name == "query unavailable" {
						return nil, errors.New("query service unavailable")
					}
					return &committerpb.TxStatusResponse{}, nil
				}},
			}
			record, _, _ := service.idempotency.begin("tx", "digest")
			service.idempotency.markSubmitted(record, InvocationResponse{TxID: "tx", Submitted: true}, sdk.Endorsement{}, nil)
			request := gatewayTestStatusRequest(t, actor, "tx")
			if tc.name == "invalid signature" {
				request.Signature = []byte("forged")
			}
			if tc.name == "reader policy" {
				policy, err := verifier.trust.CompilePolicy("OR('org-0.admin')")
				if err != nil {
					t.Fatal(err)
				}
				localVerifier.statusPolicy = policy
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tc.name == "canceled" {
				cancel()
			}
			if result, err := service.CommitStatus(ctx, request); status.Code(err) != tc.want || result != nil {
				t.Fatalf("got %v %v, want %s", result, err, tc.want)
			}
			assertFinalityState(t, service, record, idempotencySubmitted)
			if record.Response.CommitStatus != "" {
				t.Fatal("failed status request manufactured a validation result")
			}
			if tc.name == "deadline" {
				select {
				case <-stopped:
				case <-time.After(time.Second):
					t.Fatal("subscription survived timeout")
				}
			} else if tc.name != "query unavailable" && queries.Load() != 0 {
				t.Fatal("unauthorized/canceled request reached backend")
			}
		})
	}
}

func TestGatewayCommitStatusRefreshesOnlyRequestedRecord(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	actor := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	var finalized atomic.Int32
	service := &Service{cfg: Config{ChannelID: "channelqc4"}, proposalAuth: proposalTestVerifier(t, []ProposalMSPConfig{org.config}, "OR('org-0.client')"), logger: sdk.NoOpLogger{}, idempotency: newIdempotencyStore()}
	requested, _, _ := service.idempotency.begin("tx", "digest")
	unrelated, _, _ := service.idempotency.begin("other", "digest")
	service.idempotency.markSubmitted(requested, InvocationResponse{TxID: "tx", Submitted: true}, sdk.Endorsement{}, func(context.Context) error { finalized.Add(1); return nil })
	service.idempotency.markSubmitted(unrelated, InvocationResponse{TxID: "other", Submitted: true}, sdk.Endorsement{}, nil)
	service.queryService = &finalityTestQuery{get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{finalityTestStatus("tx", committerpb.Status_COMMITTED)}}, nil
	}}
	for range 2 {
		if result, err := service.CommitStatus(t.Context(), gatewayTestStatusRequest(t, actor, "tx")); err != nil || result.Result != peer.TxValidationCode_VALID {
			t.Fatalf("%v %v", result, err)
		}
	}
	assertFinalityState(t, service, requested, idempotencyCompleted)
	assertFinalityState(t, service, unrelated, idempotencySubmitted)
	if finalized.Load() != 1 {
		t.Fatal("deferred Init bookkeeping was lost or repeated")
	}
}

func TestGatewayValidationCodes(t *testing.T) {
	expected := map[committerpb.Status]peer.TxValidationCode{
		committerpb.Status_COMMITTED:                                 peer.TxValidationCode_VALID,
		committerpb.Status_ABORTED_SIGNATURE_INVALID:                 peer.TxValidationCode_ENDORSEMENT_POLICY_FAILURE,
		committerpb.Status_ABORTED_MVCC_CONFLICT:                     peer.TxValidationCode_MVCC_READ_CONFLICT,
		committerpb.Status_REJECTED_DUPLICATE_TX_ID:                  peer.TxValidationCode_DUPLICATE_TXID,
		committerpb.Status_MALFORMED_BAD_ENVELOPE:                    peer.TxValidationCode_BAD_PAYLOAD,
		committerpb.Status_MALFORMED_MISSING_TX_ID:                   peer.TxValidationCode_BAD_PROPOSAL_TXID,
		committerpb.Status_MALFORMED_UNSUPPORTED_ENVELOPE_PAYLOAD:    peer.TxValidationCode_UNSUPPORTED_TX_PAYLOAD,
		committerpb.Status_MALFORMED_BAD_ENVELOPE_PAYLOAD:            peer.TxValidationCode_BAD_PAYLOAD,
		committerpb.Status_MALFORMED_TX_ID_CONFLICT:                  peer.TxValidationCode_BAD_PROPOSAL_TXID,
		committerpb.Status_MALFORMED_EMPTY_NAMESPACES:                peer.TxValidationCode_BAD_RWSET,
		committerpb.Status_MALFORMED_DUPLICATE_NAMESPACE:             peer.TxValidationCode_BAD_RWSET,
		committerpb.Status_MALFORMED_NAMESPACE_ID_INVALID:            peer.TxValidationCode_BAD_RWSET,
		committerpb.Status_MALFORMED_BLIND_WRITES_NOT_ALLOWED:        peer.TxValidationCode_INVALID_WRITESET,
		committerpb.Status_MALFORMED_NO_WRITES:                       peer.TxValidationCode_INVALID_WRITESET,
		committerpb.Status_MALFORMED_EMPTY_KEY:                       peer.TxValidationCode_BAD_RWSET,
		committerpb.Status_MALFORMED_DUPLICATE_KEY_IN_READ_WRITE_SET: peer.TxValidationCode_BAD_RWSET,
		committerpb.Status_MALFORMED_MISSING_SIGNATURE:               peer.TxValidationCode_ENDORSEMENT_POLICY_FAILURE,
		committerpb.Status_MALFORMED_NAMESPACE_POLICY_INVALID:        peer.TxValidationCode_ENDORSEMENT_POLICY_FAILURE,
		committerpb.Status_MALFORMED_CONFIG_TX_INVALID:               peer.TxValidationCode_INVALID_CONFIG_TRANSACTION,
	}
	for value := range committerpb.Status_name {
		native := committerpb.Status(value)
		if native == committerpb.Status_STATUS_UNSPECIFIED {
			continue
		}
		want, ok := expected[native]
		if !ok || gatewayValidationCode(native) != want {
			t.Fatalf("undefined or incorrect mapping for %s", native)
		}
	}
	for _, unknown := range []committerpb.Status{committerpb.Status_STATUS_UNSPECIFIED, committerpb.Status(999)} {
		if gatewayValidationCode(unknown) != peer.TxValidationCode_INVALID_OTHER_REASON {
			t.Fatal("unknown outcome mapped to success")
		}
	}
}

func TestGatewayChaincodeEventsSDK(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	actor := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	node := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	for _, scenario := range []string{"replay", "checkpoint within block", "live beyond request timeout", "future start", "offline signing"} {
		t.Run(scenario, func(t *testing.T) {
			probe := &gatewayExecutionProbe{}
			service, connection, _ := newGatewayTestService(t, "org-0", node, []ProposalMSPConfig{org.config}, endorsementTestPolicy(t, "OR('org-0.peer')"), probe)
			client, sign := newGatewayTestClient(t, connection, actor)
			source := newGatewayEventSource(t, service)
			source.append(gatewayEventBlock(t, 0,
				gatewayEventFixture{id: "tx1", name: "sample", payload: []byte{0, 255}},
				gatewayEventFixture{id: "other", name: "other"},
				gatewayEventFixture{id: "tx2", name: "sample"},
				gatewayEventFixture{id: "aborted", name: "sample", status: committerpb.Status_ABORTED_MVCC_CONFLICT},
				gatewayEventFixture{id: "old-record", name: "sample", legacy: true},
			))
			source.append(gatewayEventBlock(t, 1))
			source.append(gatewayEventBlock(t, 2, gatewayEventFixture{id: "tx3", name: "sample"}))
			// Event delivery needs neither local transaction state nor chaincode.
			service.helper, service.lifecycle, service.idempotency = nil, nil, nil
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var events <-chan *gwclient.ChaincodeEvent
			var err error
			want := []string{"tx1", "tx2", "tx3"}
			switch scenario {
			case "replay":
				events, err = client.GetNetwork("channelqc4").ChaincodeEvents(ctx, "sample:1", gwclient.WithStartBlock(0))
			case "checkpoint within block":
				checkpoint := &gwclient.InMemoryCheckpointer{}
				checkpoint.CheckpointTransaction(0, "tx1")
				events, err = client.GetNetwork("channelqc4").ChaincodeEvents(ctx, "sample:1", gwclient.WithStartBlock(100), gwclient.WithCheckpoint(checkpoint))
				want = []string{"tx2", "tx3"}
			case "offline signing":
				request, buildErr := client.GetNetwork("channelqc4").NewChaincodeEventsRequest("sample:1", gwclient.WithStartBlock(0))
				if buildErr != nil {
					t.Fatal(buildErr)
				}
				serialized, _ := request.Bytes()
				signature, _ := sign(request.Digest())
				signed, buildErr := client.NewSignedChaincodeEventsRequest(serialized, signature)
				if buildErr != nil {
					t.Fatal(buildErr)
				}
				events, err = signed.Events(ctx)
			case "live beyond request timeout":
				service.cfg.RequestTimeout = 50 * time.Millisecond
				events, err = client.GetNetwork("channelqc4").ChaincodeEvents(ctx, "sample:1")
				want = []string{"tx4"}
			case "future start":
				events, err = client.GetNetwork("channelqc4").ChaincodeEvents(ctx, "sample:1", gwclient.WithStartBlock(5))
				want = []string{"tx6"}
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case start := <-source.starts:
				if (scenario == "live beyond request timeout" || scenario == "future start") && start != 3 {
					t.Fatalf("live cursor started at %d", start)
				}
			case <-ctx.Done():
				t.Fatal("backend subscription never started")
			}
			if scenario == "live beyond request timeout" || scenario == "future start" {
				time.Sleep(100 * time.Millisecond)
				source.append(gatewayEventBlock(t, 3, gatewayEventFixture{id: "tx4", name: "sample"}))
				if scenario == "future start" {
					source.append(gatewayEventBlock(t, 4, gatewayEventFixture{id: "tx5", name: "sample"}))
					source.append(gatewayEventBlock(t, 5, gatewayEventFixture{id: "tx6", name: "sample"}))
				}
			}
			for _, id := range want {
				event := readGatewayTestEvent(t, ctx, events)
				if event.TransactionID != id || event.ChaincodeName != "sample:1" || event.EventName != "event" {
					t.Fatalf("wrong event or ordering: %+v, want %s", event, id)
				}
				if id == "tx1" && !bytes.Equal(event.Payload, []byte{0, 255}) {
					t.Fatal("binary event payload changed")
				}
			}
			cancel()
			for range events {
			}
			deadline := time.Now().Add(time.Second)
			for source.active.Load() != 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if source.active.Load() != 0 || len(probe.proposals) != 0 {
				t.Fatal("subscriber survived disconnect or executed chaincode")
			}
		})
	}
}

func TestGatewayChaincodeEventsRejectsBeforeBackendAccess(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	actor := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	node := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	service, connection, _ := newGatewayTestService(t, "org-0", node, []ProposalMSPConfig{org.config}, endorsementTestPolicy(t, "OR('org-0.peer')"), &gatewayExecutionProbe{})
	source := newGatewayEventSource(t, service)
	request := gatewayTestEventsRequest(t, actor, "sample:1")
	request.Signature = []byte("forged")
	stream, err := gateway.NewGatewayClient(connection).ChaincodeEvents(t.Context(), request)
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.Unauthenticated || source.queries.Load() != 0 || source.active.Load() != 0 {
		t.Fatalf("unauthenticated subscription reached ledger: %v", err)
	}
}

func TestGatewayChaincodeEventsDeliveryFailures(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	actor := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	node := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	for _, scenario := range []string{"bad block order", "missing validation", "old checkpoint"} {
		t.Run(scenario, func(t *testing.T) {
			service, connection, _ := newGatewayTestService(t, "org-0", node, []ProposalMSPConfig{org.config}, endorsementTestPolicy(t, "OR('org-0.peer')"), &gatewayExecutionProbe{})
			source := newGatewayEventSource(t, service)
			block := gatewayEventBlock(t, 0, gatewayEventFixture{id: "tx", name: "sample", legacy: scenario == "old checkpoint"})
			want := codes.DataLoss
			if scenario == "bad block order" {
				block.Header.Number = 2
			}
			if scenario == "missing validation" {
				block.Metadata = nil
			}
			source.append(block)
			request := gatewayTestEventsRequest(t, actor, "sample:1")
			decoded := &gateway.ChaincodeEventsRequest{}
			_ = proto.Unmarshal(request.Request, decoded)
			decoded.StartPosition = &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: 0}}}
			if scenario == "old checkpoint" {
				decoded.AfterTransactionId = "tx"
				want = codes.FailedPrecondition
			}
			request.Request = protoutil.MarshalOrPanic(decoded)
			request.Signature, _ = actor.signer.Sign(request.Request)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			stream, err := gateway.NewGatewayClient(connection).ChaincodeEvents(ctx, request)
			if err == nil {
				_, err = stream.Recv()
			}
			if status.Code(err) != want {
				t.Fatalf("got %v, want %s", err, want)
			}
		})
	}
}

func TestSharedChaincodeEventExtraction(t *testing.T) {
	block := gatewayEventBlock(t, 0, gatewayEventFixture{id: "tx", name: "sample", payload: []byte{0, 255, 128}})
	envelope := &common.Envelope{}
	_ = proto.Unmarshal(block.Data.Data[0], envelope)
	payload, _ := protoutil.UnmarshalPayload(envelope.Payload)
	native := &applicationpb.Tx{}
	_ = proto.Unmarshal(payload.Data, native)
	event, err := chaincodeEventFromTransaction(native, "tx")
	if err != nil || event.ChaincodeId != "sample:1" {
		t.Fatalf("lost logical event identity: %+v %v", event, err)
	}
	legacy := eventFromEndorsement(sdk.Endorsement{Responses: []*peer.ProposalResponse{{Payload: payload.Data}}}, "tx")
	if legacy == nil || legacy.ChaincodeID != "1" || legacy.TxID != event.TxId || legacy.EventName != event.EventName || legacy.PayloadBase64 != "AP+A" {
		t.Fatalf("custom response changed: %+v", legacy)
	}
}

func TestGatewaySubmitAcceptsPreviousEventMetadataFormat(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	client := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	node := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	policy := endorsementTestPolicy(t, "OR('org-0.peer')")
	service, _, _ := newGatewayTestService(t, "org-0", node, []ProposalMSPConfig{org.config}, policy, &gatewayExecutionProbe{})
	submitter := &gatewaySubmissionProbe{}
	service.submitter = submitter
	signed := gatewayTestProposal(t, client, "sample:1", false)
	result := gatewayTestEndorsement(t, node, signed, policy.Version, "legacy-prepared-result")
	native := &applicationpb.Tx{}
	_ = proto.Unmarshal(result.Endorsement.Responses[0].Payload, native)
	native.Metadata = native.Metadata[:2]
	data, err := native.Namespaces[0].ASN1Marshal(result.TxID, native.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	result.Endorsement.Responses[0].Endorsement.Signature, err = node.signer.Sign(data)
	if err != nil {
		t.Fatal(err)
	}
	result.Endorsement.Responses[0].Payload = protoutil.MarshalOrPanic(native)
	proposal, _ := protoutil.UnmarshalProposal(signed.ProposalBytes)
	envelope, err := createGatewayTransaction(proposal, "2.0", result.Endorsement)
	if err != nil {
		t.Fatal(err)
	}
	envelope.Signature, err = client.signer.Sign(envelope.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Submit(t.Context(), &gateway.SubmitRequest{ChannelId: "channelqc4", TransactionId: result.TxID, PreparedTransaction: envelope}); err != nil || submitter.calls.Load() != 1 {
		t.Fatalf("older signed prepared transaction rejected: %v", err)
	}
}

func readGatewayTestEvent(t *testing.T, ctx context.Context, events <-chan *gwclient.ChaincodeEvent) *gwclient.ChaincodeEvent {
	t.Helper()
	select {
	case event, ok := <-events:
		if !ok {
			t.Fatal("Gateway event stream closed unexpectedly")
		}
		return event
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return nil
	}
}

type gatewayEventFixture struct {
	id, name, channel, namespace string
	payload                      []byte
	status                       committerpb.Status
	legacy                       bool
}

func gatewayEventBlock(t *testing.T, number uint64, fixtures ...gatewayEventFixture) *common.Block {
	t.Helper()
	block := &common.Block{Header: &common.BlockHeader{Number: number}, Data: &common.BlockData{}, Metadata: &common.BlockMetadata{Metadata: make([][]byte, int(common.BlockMetadataIndex_TRANSACTIONS_FILTER)+1)}}
	for _, f := range fixtures {
		if f.channel == "" {
			f.channel = "channelqc4"
		}
		if f.namespace == "" {
			f.namespace = "1"
		}
		if f.status == 0 {
			f.status = committerpb.Status_COMMITTED
		}
		if f.payload == nil {
			f.payload = []byte("payload-" + f.id)
		}
		native := &applicationpb.Tx{Namespaces: []*applicationpb.TxNamespace{{NsId: f.namespace}}, Metadata: [][]byte{protoutil.MarshalOrPanic(&metadatapb.Metadata{EventName: "event", Event: f.payload})}}
		if !f.legacy {
			identity, err := helper.MarshalChaincodeIdentity(f.channel, f.name, f.namespace)
			if err != nil {
				t.Fatal(err)
			}
			native.Metadata = append(native.Metadata, identity)
		}
		payload := &common.Payload{Header: &common.Header{ChannelHeader: protoutil.MarshalOrPanic(&common.ChannelHeader{Type: int32(common.HeaderType_MESSAGE), ChannelId: f.channel, TxId: f.id})}, Data: protoutil.MarshalOrPanic(native)}
		block.Data.Data = append(block.Data.Data, protoutil.MarshalOrPanic(&common.Envelope{Payload: protoutil.MarshalOrPanic(payload)}))
		block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = append(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER], byte(f.status))
	}
	return block
}

type gatewayEventSource struct {
	committerpb.UnimplementedBlockQueryServiceServer
	peer.UnimplementedDeliverServer
	mu       sync.Mutex
	blocks   []*common.Block
	changed  chan struct{}
	starts   chan uint64
	active   atomic.Int32
	queries  atomic.Int32
	verifier *proposalVerifier
}

func newGatewayEventSource(t *testing.T, service *Service) *gatewayEventSource {
	t.Helper()
	source := &gatewayEventSource{changed: make(chan struct{}), starts: make(chan uint64, 8), verifier: service.proposalAuth}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	committerpb.RegisterBlockQueryServiceServer(server, source)
	peer.RegisterDeliverServer(server, source)
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient("passthrough:///events", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close(); server.Stop(); _ = listener.Close() })
	service.blockQuery, service.blockDelivery = committerpb.NewBlockQueryServiceClient(connection), peer.NewDeliverClient(connection)
	return source
}

func (s *gatewayEventSource) append(block *common.Block) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocks = append(s.blocks, block)
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *gatewayEventSource) GetBlockchainInfo(context.Context, *emptypb.Empty) (*common.BlockchainInfo, error) {
	s.queries.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	return &common.BlockchainInfo{Height: uint64(len(s.blocks))}, nil
}

func (s *gatewayEventSource) GetBlockByTxID(_ context.Context, request *committerpb.TxID) (*common.Block, error) {
	s.queries.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, block := range s.blocks {
		for _, raw := range block.Data.Data {
			envelope := &common.Envelope{}
			_ = proto.Unmarshal(raw, envelope)
			payload, err := protoutil.UnmarshalPayload(envelope.Payload)
			if err != nil || payload.Header == nil {
				continue
			}
			header, _ := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
			if header.GetTxId() == request.TxId {
				return block, nil
			}
		}
	}
	return nil, status.Error(codes.NotFound, "unknown transaction")
}

func (s *gatewayEventSource) Deliver(stream peer.Deliver_DeliverServer) error {
	envelope, err := stream.Recv()
	if err != nil {
		return err
	}
	payload, err := protoutil.UnmarshalPayload(envelope.Payload)
	if err != nil || payload.Header == nil {
		return status.Error(codes.InvalidArgument, "bad seek payload")
	}
	identity, err := protoutil.UnmarshalSignatureHeader(payload.Header.SignatureHeader)
	if err != nil {
		return err
	}
	if _, err := s.verifier.trust.Authenticate(identity.Creator, envelope.Payload, envelope.Signature, time.Now()); err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	header, _ := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if header.GetType() != int32(common.HeaderType_DELIVER_SEEK_INFO) || header.GetChannelId() != "channelqc4" {
		return status.Error(codes.InvalidArgument, "wrong seek header")
	}
	seek := &orderer.SeekInfo{}
	if err := proto.Unmarshal(payload.Data, seek); err != nil || seek.Start.GetSpecified() == nil || seek.Stop.GetSpecified().GetNumber() != ^uint64(0) || seek.Behavior != orderer.SeekInfo_BLOCK_UNTIL_READY {
		return status.Error(codes.InvalidArgument, "wrong seek range")
	}
	next := seek.Start.GetSpecified().Number
	s.active.Add(1)
	defer s.active.Add(-1)
	s.starts <- next
	for {
		s.mu.Lock()
		changed := s.changed
		if next < uint64(len(s.blocks)) {
			block := s.blocks[next]
			s.mu.Unlock()
			if err := stream.Send(&peer.DeliverResponse{Type: &peer.DeliverResponse_Block{Block: block}}); err != nil {
				return err
			}
			next++
			continue
		}
		s.mu.Unlock()
		select {
		case <-changed:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func TestGatewayEndorseAdmissionAndInit(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	client := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	node := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	probe := &gatewayExecutionProbe{payload: []byte("done")}
	service, _, _ := newGatewayTestService(t, "org-0", node, []ProposalMSPConfig{org.config}, endorsementTestPolicy(t, "OR('org-0.peer')"), probe)
	request := func(actor proposalTestActor, route string, isInit bool) *gateway.EndorseRequest {
		signed := gatewayTestProposal(t, actor, route, isInit)
		proposal, _ := protoutil.UnmarshalProposal(signed.ProposalBytes)
		txID, err := txIDFromProposal(proposal)
		if err != nil {
			t.Fatal(err)
		}
		return &gateway.EndorseRequest{ChannelId: "channelqc4", TransactionId: txID, ProposedTransaction: signed}
	}
	for _, tc := range []struct {
		name   string
		route  string
		actor  proposalTestActor
		isInit bool
		mutate func(*gateway.EndorseRequest)
		want   codes.Code
	}{
		{name: "missing signature", mutate: func(r *gateway.EndorseRequest) { r.ProposedTransaction.Signature = nil }, want: codes.Unauthenticated},
		{name: "changed arguments", mutate: func(r *gateway.EndorseRequest) { changeProposalTestArgument(t, r.ProposedTransaction) }, want: codes.Unauthenticated},
		{name: "missing proposal", mutate: func(r *gateway.EndorseRequest) { r.ProposedTransaction = nil }, want: codes.InvalidArgument},
		{name: "outer transaction mismatch", mutate: func(r *gateway.EndorseRequest) { r.TransactionId = "other" }, want: codes.InvalidArgument},
		{name: "outer channel mismatch", mutate: func(r *gateway.EndorseRequest) { r.ChannelId = "other" }, want: codes.InvalidArgument},
		{name: "unqualified name", route: "sample", want: codes.InvalidArgument},
		{name: "malformed route", route: "sample:1:2", want: codes.InvalidArgument},
		{name: "namespace authorization", route: "sample:2", want: codes.PermissionDenied},
		{name: "creator authorization", actor: node, want: codes.PermissionDenied},
		{name: "empty requested organization", mutate: func(r *gateway.EndorseRequest) { r.EndorsingOrganizations = []string{""} }, want: codes.InvalidArgument},
		{name: "whitespace organization", mutate: func(r *gateway.EndorseRequest) { r.EndorsingOrganizations = []string{" org-0"} }, want: codes.InvalidArgument},
		{name: "init not required", isInit: true, want: codes.FailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actor := tc.actor
			if actor.cert == nil {
				actor = client
			}
			route := tc.route
			if route == "" {
				route = "sample:1"
			}
			req := request(actor, route, tc.isInit)
			if tc.mutate != nil {
				tc.mutate(req)
			}
			if _, err := service.Endorse(t.Context(), req); status.Code(err) != tc.want {
				t.Fatalf("error=%v want=%s", err, tc.want)
			}
		})
	}
	if _, err := service.Endorse(t.Context(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil Endorse request was accepted: %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := service.Endorse(canceled, request(client, "sample:1", false)); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled Endorse continued: %v", err)
	}
	service.proposalAuth.rules[0].chaincode = "*"
	if _, err := service.Endorse(t.Context(), request(client, "missing:1", false)); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown chaincode reached legacy execution: %v", err)
	}
	service.proposalAuth.rules[0].chaincode = "sample"
	if len(probe.proposals) != 0 {
		t.Fatal("rejected proposals reached chaincode execution")
	}
	initDefinition := lifecycle.ChaincodeDefinition{Name: "sample", Version: "3.0", Sequence: 3, InitRequired: true}
	if _, err := service.lifecycle.UpsertCommittedFromLedger(t.Context(), initDefinition, false); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Endorse(t.Context(), request(client, "sample:1", false)); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("normal endorsement bypassed the init gate: %v", err)
	}
	for range 2 {
		if _, err := service.Endorse(t.Context(), request(client, "sample:1", true)); err != nil {
			t.Fatalf("valid Init could not be endorsed: %v", err)
		}
	}
	committed, _, err := service.lifecycle.GetCommitted(t.Context(), "sample")
	if err != nil || committed.Initialized || len(service.idempotency.records) != 0 {
		t.Fatalf("Init endorsement changed committed lifecycle state: %+v %v", committed, err)
	}
	for _, signed := range probe.proposals {
		proposal, _ := protoutil.UnmarshalProposal(signed.ProposalBytes)
		if isInit, err := helper.ProposalIsInit(proposal); err != nil || !isInit {
			t.Fatalf("signed Init intent did not reach execution: %v", err)
		}
	}
	if _, err := service.lifecycle.UpsertCommittedFromLedger(t.Context(), initDefinition, true); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Endorse(t.Context(), request(client, "sample:1", true)); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("initialized definition accepted another Init endorsement: %v", err)
	}
	forwarded := metadata.NewIncomingContext(t.Context(), metadata.Pairs(GRPCOperationMetadata, GRPCOperationEndorse, GRPCNamespaceMetadata, "2"))
	if _, err := service.ProcessProposal(forwarded, request(client, "sample:1", false).ProposedTransaction); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("remote endorsement accepted a conflicting namespace: %v", err)
	}
}

func TestRequestDigestIncludesClientTxID(t *testing.T) {
	req1 := InvocationRequest{
		ClientTxID:    "client-tx-1",
		ClientCreator: []byte("creator"),
		Function:      "compatv2",
		Args:          []string{"asset1", "value1", "delete1"},
	}
	req2 := req1
	req2.ClientTxID = "client-tx-2"

	digest1 := requestDigest("channelqc4", "0", req1, true)
	digest2 := requestDigest("channelqc4", "0", req2, true)
	if digest1 == digest2 {
		t.Fatal("different client tx ids should produce different digest")
	}

	req2.ClientTxID = req1.ClientTxID
	req2.Args[1] = "different-value"
	digest3 := requestDigest("channelqc4", "0", req2, true)
	if digest1 == digest3 {
		t.Fatal("different request args should produce different digest")
	}
}

func TestIdempotencyIdentityUsesClientTxIDAsKey(t *testing.T) {
	req1 := InvocationRequest{
		ClientTxID:    "client-tx-1",
		ClientCreator: []byte("creator"),
		Function:      "compatv2",
		Args:          []string{"asset1", "value1", "delete1"},
	}
	req2 := req1
	req2.Args = []string{"asset1", "different-value", "delete1"}

	key1, digest1 := idempotencyIdentity("channelqc4", "0", req1, true)
	key2, digest2 := idempotencyIdentity("channelqc4", "0", req2, true)
	if key1 != req1.ClientTxID || key2 != req1.ClientTxID {
		t.Fatalf("expected idempotency key to use client tx id, got %q and %q", key1, key2)
	}
	if digest1 == digest2 {
		t.Fatal("same tx id with different args should produce different request digests")
	}
}

func TestRequestDigestIncludesTransient(t *testing.T) {
	req1 := InvocationRequest{
		ClientCreator: []byte("creator"),
		Function:      "compatv2",
		Args:          []string{"asset1", "value1", "delete1"},
		ClientTransient: map[string][]byte{
			"secret": []byte("alpha"),
		},
	}
	req2 := req1
	req2.ClientTransient = map[string][]byte{
		"secret": []byte("beta"),
	}

	digest1 := requestDigest("channelqc4", "0", req1, true)
	digest2 := requestDigest("channelqc4", "0", req2, true)
	if digest1 == digest2 {
		t.Fatal("different transient values should produce different digest")
	}
}

func TestRequestDigestIncludesIsInit(t *testing.T) {
	req1 := InvocationRequest{
		ClientTxID:       "client-tx-1",
		ClientCreator:    []byte("creator"),
		ChaincodeName:    "sample",
		ChaincodeVersion: "1.0",
		Function:         "compatv2",
		Args:             []string{"asset1", "value1", "delete1"},
	}
	req2 := req1
	req2.IsInit = true

	digest1 := requestDigest("channelqc4", "0", req1, true)
	digest2 := requestDigest("channelqc4", "0", req2, true)
	if digest1 == digest2 {
		t.Fatal("is-init flag should produce a different request digest")
	}
}

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

func TestProcessProposalRejectsBeforeExecution(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	client := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	peerActor := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	verifier := proposalTestVerifier(t, []ProposalMSPConfig{org.config}, "OR('org-0.client')")
	service := &Service{cfg: Config{ChannelID: "channelqc4", Namespace: "1"}, proposalAuth: verifier, logger: sdk.NoOpLogger{}}
	// No helper, Query Service, or orderer is wired. Every case must be rejected
	// at admission, before execution can reach any of those dependencies.
	for _, operation := range []string{GRPCOperationQuery, GRPCOperationInvoke, GRPCOperationEndorse} {
		for _, tc := range []struct {
			name      string
			actor     proposalTestActor
			namespace string
			badSig    bool
			want      codes.Code
		}{
			{name: "invalid signature", actor: client, namespace: "1", badSig: true, want: codes.Unauthenticated},
			{name: "unauthorized peer", actor: peerActor, namespace: "1", want: codes.PermissionDenied},
			{name: "namespace override", actor: client, namespace: "2", want: codes.PermissionDenied},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				proposal := proposalTestSigned(t, tc.actor.signer, "")
				if tc.badSig {
					proposal.Signature = nil
				}
				ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(GRPCOperationMetadata, operation, GRPCNamespaceMetadata, tc.namespace))
				_, err := service.ProcessProposal(ctx, proposal)
				if status.Code(err) != tc.want {
					t.Fatalf("error = %v, want %s", err, tc.want)
				}
			})
		}
	}
	if _, err := service.ProcessProposal(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil proposal error: %v", err)
	}
	service.proposalAuth = nil
	proposal := proposalTestSigned(t, client.signer, "")
	if _, err := service.ProcessProposal(context.Background(), proposal); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unconfigured verifier did not fail closed: %v", err)
	}
}

func TestGatewayEvaluateRejectsBeforeExecution(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	actor := org.issue(t, "client", time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	store, err := lifecycle.NewMemoryStore("channelqc4")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	def := lifecycle.ChaincodeDefinition{Name: "sample", Version: "1.0", Sequence: 1}
	if _, err := store.UpsertCommittedFromLedger(t.Context(), def, true); err != nil {
		t.Fatal(err)
	}
	service := &Service{
		cfg: Config{ChannelID: "channelqc4", Identity: &config.IdentityConfig{MspID: "org-0"}}, lifecycle: store, logger: sdk.NoOpLogger{},
		proposalAuth: proposalTestVerifier(t, []ProposalMSPConfig{org.config}, "OR('org-0.client')"),
	}
	makeRequest := func(name string, isInit bool) *gateway.EvaluateRequest {
		original := proposalTestSigned(t, actor.signer, "")
		proposal, _ := protoutil.UnmarshalProposal(original.ProposalBytes)
		header, _ := protoutil.UnmarshalHeader(proposal.Header)
		channel, _ := protoutil.UnmarshalChannelHeader(header.ChannelHeader)
		extension := &peer.ChaincodeHeaderExtension{ChaincodeId: &peer.ChaincodeID{Name: name}}
		channel.Extension = protoutil.MarshalOrPanic(extension)
		header.ChannelHeader = protoutil.MarshalOrPanic(channel)
		proposal.Header = protoutil.MarshalOrPanic(header)
		payload, _ := protoutil.UnmarshalChaincodeProposalPayload(proposal.Payload)
		spec, _ := protoutil.UnmarshalChaincodeInvocationSpec(payload.Input)
		spec.ChaincodeSpec.ChaincodeId = &peer.ChaincodeID{Name: name}
		spec.ChaincodeSpec.Input.IsInit = isInit
		payload.Input = protoutil.MarshalOrPanic(spec)
		proposal.Payload = protoutil.MarshalOrPanic(payload)
		signed, err := protoutil.GetSignedProposal(proposal, actor.signer)
		if err != nil {
			t.Fatal(err)
		}
		return &gateway.EvaluateRequest{ChannelId: channel.ChannelId, TransactionId: channel.TxId, ProposedTransaction: signed}
	}
	for _, tc := range []struct {
		name   string
		route  string
		isInit bool
		mutate func(*gateway.EvaluateRequest)
		want   codes.Code
	}{
		{name: "missing signature", mutate: func(r *gateway.EvaluateRequest) { r.ProposedTransaction.Signature = nil }, want: codes.Unauthenticated},
		{name: "invalid signature", mutate: func(r *gateway.EvaluateRequest) { r.ProposedTransaction.Signature = []byte("invalid") }, want: codes.Unauthenticated},
		{name: "tampered signed arguments", mutate: func(r *gateway.EvaluateRequest) { changeProposalTestArgument(t, r.ProposedTransaction) }, want: codes.Unauthenticated},
		{name: "outer transaction mismatch", mutate: func(r *gateway.EvaluateRequest) { r.TransactionId = "different" }, want: codes.InvalidArgument},
		{name: "outer channel mismatch", mutate: func(r *gateway.EvaluateRequest) { r.ChannelId = "other" }, want: codes.InvalidArgument},
		{name: "namespace omitted", route: "sample", want: codes.InvalidArgument},
		{name: "extra delimiter", route: "sample:1:2", want: codes.InvalidArgument},
		{name: "namespace permission", route: "sample:2", want: codes.PermissionDenied},
		{name: "chaincode permission", route: "other:1", want: codes.PermissionDenied},
		{name: "init intent", isInit: true, want: codes.InvalidArgument},
		{name: "empty target MSP", mutate: func(r *gateway.EvaluateRequest) { r.TargetOrganizations = []string{""} }, want: codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			route := tc.route
			if route == "" {
				route = "sample:1"
			}
			req := makeRequest(route, tc.isInit)
			if tc.mutate != nil {
				tc.mutate(req)
			}
			if _, err := service.Evaluate(t.Context(), req); status.Code(err) != tc.want {
				t.Fatalf("error=%v want=%s", err, tc.want)
			}
		})
	}
	if _, err := service.Evaluate(t.Context(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil request: %v", err)
	}
	request := makeRequest("sample:1", false)
	request.TargetOrganizations = []string{"org-1"}
	localOnly := metadata.NewIncomingContext(t.Context(), metadata.Pairs(gatewayEvaluateLocalOnly, "true"))
	if _, err := service.Evaluate(localOnly, request); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("forwarded evaluation attempted another hop: %v", err)
	}
	if _, err := service.Evaluate(t.Context(), request); status.Code(err) != codes.Unavailable {
		t.Fatalf("unavailable requested organization was ignored: %v", err)
	}
	service.proposalAuth.rules[0].chaincode = "*"
	if _, err := service.Evaluate(t.Context(), makeRequest("unknown:1", false)); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown chaincode reached legacy fallback: %v", err)
	}
	def.Version, def.Sequence, def.InitRequired = "2.0", 2, true
	if _, err := store.UpsertCommittedFromLedger(t.Context(), def, false); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Evaluate(t.Context(), makeRequest("sample:1", false)); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("uninitialized definition was evaluated: %v", err)
	}
}
