// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"compatibility_service/pkg/config"
	common "github.com/hyperledger/fabric-protos-go-apiv2/common"
	mspapi "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/common/policydsl"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	efab "github.com/hyperledger/fabric-x-sdk/endorsement/fabric"
	efabx "github.com/hyperledger/fabric-x-sdk/endorsement/fabricx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

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

func TestNestedPolicySelectsExtraSignaturesInValidatorOrder(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	issue := func(role string) proposalTestActor {
		return org.issue(t, role, now.Add(-time.Hour), now.Add(time.Hour))
	}
	peer0, client, exactPeer, peer1 := issue("peer"), issue("client"), issue("peer"), issue("peer")
	trust := proposalTestVerifier(t, []ProposalMSPConfig{org.config}, "OR('org-0.member')")
	exactIdentity, err := exactPeer.signer.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	// AND(OR(OR(peer, member), member), exactPeer). Fabric's evaluator
	// consumes all successful OR branches, so four signatures are needed and
	// the exact identity must follow the three other qualifying identities.
	policy := endorsementTestEnvelope(t, &common.SignaturePolicyEnvelope{
		Rule: nOutOf(2, nOutOf(1, nOutOf(1, signedBy(0), signedBy(1)), signedBy(1)), signedBy(2)),
		Identities: []*mspapi.MSPPrincipal{
			{PrincipalClassification: mspapi.MSPPrincipal_ROLE, Principal: marshalProto(t, &mspapi.MSPRole{MspIdentifier: "org-0", Role: mspapi.MSPRole_PEER})},
			rolePrincipal(t, "org-0"),
			{PrincipalClassification: mspapi.MSPPrincipal_IDENTITY, Principal: exactIdentity},
		},
	})
	compiled, err := newNamespaceEndorsementPolicy(policy, trust.identities)
	if err != nil {
		t.Fatal(err)
	}
	proposal := proposalTestSigned(t, client.signer, "")
	var candidates []verifiedEndorsement
	for _, actor := range []proposalTestActor{peer0, client, exactPeer, peer1} {
		result := endorsementTestResult(t, actor, proposal, policy.Version, "result")
		verified, err := compiled.authenticate(result, "org-0")
		if err != nil {
			t.Fatal(err)
		}
		candidates = append(candidates, verified)
	}
	end, _, ok, err := compiled.assembleSelection(context.Background(), candidates[0].result, candidates)
	if err != nil || !ok {
		t.Fatalf("select signatures accepted by the native verifier: selected=%t error=%v", ok, err)
	}
	if len(end.Responses) != 4 {
		t.Fatalf("selected %d signatures, want four distinct signers", len(end.Responses))
	}
	if string(end.Responses[3].Endorsement.Endorser) != string(exactIdentity) {
		t.Fatal("the exact identity must be preserved for the final policy leaf")
	}
}

func TestNamespacePolicyPrincipalTypes(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	actor := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	other := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	trust := proposalTestVerifier(t, []ProposalMSPConfig{org.config}, "OR('org-0.member')")
	serialized, err := actor.signer.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	nativeIdentity := &msppb.Identity{}
	if err := proto.Unmarshal(serialized, nativeIdentity); err != nil {
		t.Fatal(err)
	}
	identity, err := trust.identities.DeserializeIdentity(nativeIdentity)
	if err != nil {
		t.Fatal(err)
	}
	role := &mspapi.MSPPrincipal{PrincipalClassification: mspapi.MSPPrincipal_ROLE, Principal: marshalProto(t, &mspapi.MSPRole{MspIdentifier: "org-0", Role: mspapi.MSPRole_PEER})}
	exact := &mspapi.MSPPrincipal{PrincipalClassification: mspapi.MSPPrincipal_IDENTITY, Principal: serialized}
	ou := &mspapi.MSPPrincipal{PrincipalClassification: mspapi.MSPPrincipal_ORGANIZATION_UNIT, Principal: marshalProto(t, &mspapi.OrganizationUnit{MspIdentifier: "org-0", OrganizationalUnitIdentifier: "peer", CertifiersIdentifier: identity.GetOrganizationalUnits()[0].CertifiersIdentifier})}
	combined := &mspapi.MSPPrincipal{PrincipalClassification: mspapi.MSPPrincipal_COMBINED, Principal: marshalProto(t, &mspapi.CombinedPrincipal{Principals: []*mspapi.MSPPrincipal{role, exact}})}
	nominal := &mspapi.MSPPrincipal{PrincipalClassification: mspapi.MSPPrincipal_ANONYMITY, Principal: marshalProto(t, &mspapi.MSPIdentityAnonymity{AnonymityType: mspapi.MSPIdentityAnonymity_NOMINAL})}
	proposal := proposalTestSigned(t, actor.signer, "")
	for _, tc := range []struct {
		name      string
		principal *mspapi.MSPPrincipal
		exactOnly bool
	}{{"role", role, false}, {"organizational unit", ou, false}, {"exact identity", exact, true}, {"combined", combined, true}, {"nominal identity", nominal, false}} {
		t.Run(tc.name, func(t *testing.T) {
			policy := endorsementTestEnvelope(t, &common.SignaturePolicyEnvelope{Rule: signedBy(0), Identities: []*mspapi.MSPPrincipal{tc.principal}})
			compiled, err := newNamespaceEndorsementPolicy(policy, trust.identities)
			if err != nil {
				t.Fatal(err)
			}
			for i, signer := range []proposalTestActor{actor, other} {
				result := endorsementTestResult(t, signer, proposal, policy.Version, "result")
				verified, err := compiled.authenticate(result, "org-0")
				if err != nil {
					t.Fatal(err)
				}
				selected, err := compiled.selectEndorsements(context.Background(), []verifiedEndorsement{verified})
				if err != nil {
					t.Fatal(err)
				}
				want := i == 0 || !tc.exactOnly
				if (len(selected) != 0) != want {
					t.Fatalf("signer %d selected=%v, want satisfied=%t", i, selected, want)
				}
			}
		})
	}
}

func TestNamespaceEndorsementAuthentication(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	actor := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	expired := org.issue(t, "peer", now.Add(-2*time.Hour), now.Add(-time.Hour))
	future := org.issue(t, "peer", now.Add(time.Hour), now.Add(2*time.Hour))
	trust := proposalTestVerifier(t, []ProposalMSPConfig{org.config}, "OR('org-0.member')")
	policy := endorsementTestPolicy(t, "OR('org-0.peer')")
	compiled, err := newNamespaceEndorsementPolicy(policy, trust.identities)
	if err != nil {
		t.Fatal(err)
	}
	proposal := proposalTestSigned(t, actor.signer, "")
	for _, tc := range []struct {
		name   string
		actor  proposalTestActor
		mutate func(*helperExecutionResult)
	}{
		{name: "expired certificate", actor: expired},
		{name: "future certificate", actor: future},
		{name: "missing signature", actor: actor, mutate: func(r *helperExecutionResult) { r.Endorsement.Responses[0].Endorsement.Signature = nil }},
		{name: "missing endorsement", actor: actor, mutate: func(r *helperExecutionResult) { r.Endorsement.Responses[0].Endorsement = nil }},
		{name: "changed transaction ID", actor: actor, mutate: func(r *helperExecutionResult) { r.TxID = "different-transaction" }},
		{name: "changed metadata", actor: actor, mutate: func(r *helperExecutionResult) {
			tx := &applicationpb.Tx{}
			if err := proto.Unmarshal(r.Endorsement.Responses[0].Payload, tx); err != nil {
				t.Fatal(err)
			}
			tx.Metadata[0] = []byte("tampered")
			r.Endorsement.Responses[0].Payload = marshalProto(t, tx)
		}},
		{name: "changed read write set", actor: actor, mutate: func(r *helperExecutionResult) {
			tx := &applicationpb.Tx{}
			if err := proto.Unmarshal(r.Endorsement.Responses[0].Payload, tx); err != nil {
				t.Fatal(err)
			}
			tx.Namespaces[0].BlindWrites[0].Value = []byte("tampered")
			r.Endorsement.Responses[0].Payload = marshalProto(t, tx)
		}},
		{name: "wrong policy version", actor: actor, mutate: func(r *helperExecutionResult) {
			*r = endorsementTestResult(t, actor, proposal, policy.Version+1, "result")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := endorsementTestResult(t, tc.actor, proposal, policy.Version, "result")
			if tc.mutate != nil {
				tc.mutate(&result)
			}
			if _, err := compiled.authenticate(result, "org-0"); err == nil {
				t.Fatal("expected invalid endorsement to be rejected")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := compiled.selectEndorsements(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled selection returned %v", err)
	}
}

func TestNamespacePolicyRejectsMalformedRules(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	trust := proposalTestVerifier(t, []ProposalMSPConfig{org.config}, "OR('org-0.member')")
	for _, tc := range []struct {
		name   string
		policy NamespacePolicySnapshot
	}{
		{name: "nil policy"},
		{name: "missing rule", policy: NamespacePolicySnapshot{Policy: &applicationpb.NamespacePolicy{}}},
		{name: "bad signed by index", policy: mspPolicySnapshot(t, signedBy(99))},
		{name: "negative threshold", policy: mspPolicySnapshot(t, nOutOf(-1, signedBy(0)))},
		{name: "impossible threshold", policy: mspPolicySnapshot(t, nOutOf(2, signedBy(0)))},
		{name: "nil signature rule", policy: mspPolicySnapshot(t, nil)},
		{name: "nil principal", policy: endorsementTestEnvelope(t, &common.SignaturePolicyEnvelope{Rule: signedBy(0), Identities: []*mspapi.MSPPrincipal{nil}})},
		{name: "unknown principal type", policy: endorsementTestEnvelope(t, &common.SignaturePolicyEnvelope{Rule: signedBy(0), Identities: []*mspapi.MSPPrincipal{{PrincipalClassification: 99}}})},
		{name: "unsupported policy envelope version", policy: endorsementTestEnvelope(t, &common.SignaturePolicyEnvelope{Version: 1, Rule: signedBy(0), Identities: []*mspapi.MSPPrincipal{rolePrincipal(t, "org-0")}})},
		{name: "nil threshold rule", policy: NamespacePolicySnapshot{Policy: &applicationpb.NamespacePolicy{Rule: &applicationpb.NamespacePolicy_ThresholdRule{}}}},
		{name: "empty threshold scheme", policy: NamespacePolicySnapshot{Policy: &applicationpb.NamespacePolicy{Rule: &applicationpb.NamespacePolicy_ThresholdRule{ThresholdRule: &applicationpb.ThresholdRule{PublicKey: []byte("key")}}}}},
		{name: "empty threshold key", policy: NamespacePolicySnapshot{Policy: &applicationpb.NamespacePolicy{Rule: &applicationpb.NamespacePolicy_ThresholdRule{ThresholdRule: &applicationpb.ThresholdRule{Scheme: "ECDSA"}}}}},
		{name: "invalid EdDSA key length", policy: NamespacePolicySnapshot{Policy: &applicationpb.NamespacePolicy{Rule: &applicationpb.NamespacePolicy_ThresholdRule{ThresholdRule: &applicationpb.ThresholdRule{Scheme: "EDDSA", PublicKey: []byte("invalid")}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newNamespaceEndorsementPolicy(tc.policy, trust.identities); err == nil {
				t.Fatal("expected malformed policy to fail")
			}
		})
	}
	if _, err := newNamespaceEndorsementPolicy(endorsementTestPolicy(t, "OR('org-0.member')"), nil); err == nil {
		t.Fatal("MSP policy without trusted MSPs was accepted")
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
	for i, signer := range []proposalTestActor{actor, other} {
		local := endorsementTestResult(t, signer, proposal, policy.Version, "result")
		_, _, err := service.collectNamespaceEndorsements(context.Background(), policy, InvocationRequest{ClientTxID: local.TxID}, local)
		if (err == nil) != (i == 0) {
			t.Fatalf("threshold signer %d error = %v", i, err)
		}
	}
}

func endorsementTestPolicy(t *testing.T, expression string) NamespacePolicySnapshot {
	t.Helper()
	envelope, err := policydsl.FromString(expression)
	if err != nil {
		t.Fatal(err)
	}
	return endorsementTestEnvelope(t, envelope)
}

func endorsementTestEnvelope(t *testing.T, envelope *common.SignaturePolicyEnvelope) NamespacePolicySnapshot {
	t.Helper()
	return NamespacePolicySnapshot{Namespace: "1", Version: 7, Policy: &applicationpb.NamespacePolicy{Rule: &applicationpb.NamespacePolicy_MspRule{MspRule: marshalProto(t, envelope)}}}
}

func endorsementTestResult(t *testing.T, actor proposalTestActor, proposal *peer.SignedProposal, namespaceVersion uint64, payload string) helperExecutionResult {
	t.Helper()
	inv, err := efab.Parse(proposal, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	inv.Namespace, inv.NsVersion = "1", namespaceVersion
	response, err := efabx.NewEndorsementBuilder(actor.signer).Endorse(inv, endorsement.Success(blocks.ReadWriteSet{Writes: []blocks.KVWrite{{Key: "asset", Value: []byte("value")}}}, "event", []byte("event-value"), []byte(payload)))
	if err != nil {
		t.Fatal(err)
	}
	return helperExecutionResult{TxID: inv.TxID, Response: response.Response, Endorsement: sdk.Endorsement{Proposal: inv.Proposal, Responses: []*peer.ProposalResponse{response}}}
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
