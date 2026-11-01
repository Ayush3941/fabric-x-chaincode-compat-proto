// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

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
	"google.golang.org/protobuf/proto"
)

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
		verified, err := compiled.authenticate(result, "org-0", false)
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
				verified, err := compiled.authenticate(result, "org-0", false)
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
			if _, err := compiled.authenticate(result, "org-0", false); err == nil {
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
