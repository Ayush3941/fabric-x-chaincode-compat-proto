// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"strings"
	"testing"

	common "github.com/hyperledger/fabric-protos-go-apiv2/common"
	mspapi "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestLoadNamespacePolicy(t *testing.T) {
	raw := marshalPolicy(t, &applicationpb.NamespacePolicy{
		Rule: &applicationpb.NamespacePolicy_MspRule{MspRule: []byte("msp-policy")},
	})
	client := &fakePolicyQueryClient{
		policies: &applicationpb.NamespacePolicies{
			Policies: []*applicationpb.PolicyItem{
				{Namespace: "other", Version: 1, Policy: raw},
				{Namespace: "0", Version: 7, Policy: raw},
			},
		},
	}

	got, err := loadNamespacePolicy(context.Background(), client, "0")
	if err != nil {
		t.Fatalf("loadNamespacePolicy returned error: %v", err)
	}

	if got.Namespace != "0" {
		t.Fatalf("expected namespace 0, got %q", got.Namespace)
	}
	if got.Version != 7 {
		t.Fatalf("expected version 7, got %d", got.Version)
	}
	if !proto.Equal(got.Policy, &applicationpb.NamespacePolicy{
		Rule: &applicationpb.NamespacePolicy_MspRule{MspRule: []byte("msp-policy")},
	}) {
		t.Fatalf("unexpected decoded policy: %s", got.Policy)
	}
	if got.RuleName() != "msp" {
		t.Fatalf("expected msp rule, got %q", got.RuleName())
	}
}

func TestLoadNamespacePolicyMissing(t *testing.T) {
	client := &fakePolicyQueryClient{
		policies: &applicationpb.NamespacePolicies{},
	}

	_, err := loadNamespacePolicy(context.Background(), client, "0")
	if err == nil || !strings.Contains(err.Error(), `namespace "0" policy not found`) {
		t.Fatalf("expected missing namespace error, got %v", err)
	}
}

func TestLoadNamespacePolicyInvalidBytes(t *testing.T) {
	client := &fakePolicyQueryClient{
		policies: &applicationpb.NamespacePolicies{
			Policies: []*applicationpb.PolicyItem{
				{Namespace: "0", Version: 1, Policy: []byte("not a proto")},
			},
		},
	}

	_, err := loadNamespacePolicy(context.Background(), client, "0")
	if err == nil || !strings.Contains(err.Error(), `decode namespace "0" policy`) {
		t.Fatalf("expected decode error, got %v", err)
	}
}

func TestNamespacePolicyResolverResolveStoresClone(t *testing.T) {
	raw := marshalPolicy(t, &applicationpb.NamespacePolicy{
		Rule: &applicationpb.NamespacePolicy_MspRule{MspRule: []byte("msp-policy")},
	})
	client := &fakePolicyQueryClient{
		policies: &applicationpb.NamespacePolicies{
			Policies: []*applicationpb.PolicyItem{
				{Namespace: "0", Version: 3, Policy: raw},
			},
		},
	}
	resolver := newNamespacePolicyResolver(client)

	got, err := resolver.Resolve(context.Background(), "0")
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}

	got.RawPolicy[0] = 0
	got.Policy.Rule = nil

	again, ok := resolver.Snapshot("0")
	if !ok {
		t.Fatal("expected cached policy snapshot")
	}
	if again.RawPolicy[0] == 0 {
		t.Fatal("RawPolicy was not cloned")
	}
	if again.Policy.GetMspRule() == nil {
		t.Fatal("Policy was not cloned")
	}
}

func TestCachedNamespacePolicy(t *testing.T) {
	raw := marshalPolicy(t, &applicationpb.NamespacePolicy{
		Rule: &applicationpb.NamespacePolicy_MspRule{MspRule: []byte("msp-policy")},
	})
	client := &fakePolicyQueryClient{
		policies: &applicationpb.NamespacePolicies{
			Policies: []*applicationpb.PolicyItem{
				{Namespace: "0", Version: 9, Policy: raw},
			},
		},
	}
	svc := &Service{policies: newNamespacePolicyResolver(client)}
	if _, ok := svc.CachedNamespacePolicy("0"); ok {
		t.Fatal("policy should not be cached before request-time resolution")
	}
	if _, err := svc.resolveNamespacePolicy(context.Background(), "0"); err != nil {
		t.Fatalf("resolveNamespacePolicy returned error: %v", err)
	}
	got, ok := svc.CachedNamespacePolicy("0")
	if !ok {
		t.Fatal("expected cached policy")
	}
	if got.Version != 9 {
		t.Fatalf("expected version 9, got %d", got.Version)
	}
}

func TestLocalMSPSatisfiesNamespacePolicyMSPRules(t *testing.T) {
	tests := []struct {
		name      string
		rule      *common.SignaturePolicy
		expected  bool
		errorText string
	}{
		{
			name:     "single local org",
			rule:     signedBy(0),
			expected: true,
		},
		{
			name: "or with local org",
			rule: nOutOf(1,
				signedBy(0),
				signedBy(1),
			),
			expected: true,
		},
		{
			name: "and needs remote org",
			rule: nOutOf(2,
				signedBy(0),
				signedBy(1),
			),
			expected: false,
		},
		{
			name: "outof needs too many orgs",
			rule: nOutOf(2,
				signedBy(0),
				signedBy(1),
				signedBy(2),
			),
			expected: false,
		},
		{
			name: "zero outof is satisfied",
			rule: nOutOf(0,
				signedBy(1),
			),
			expected: true,
		},
		{
			name: "nested policy with local branch",
			rule: nOutOf(1,
				signedBy(1),
				nOutOf(2,
					signedBy(0),
					signedBy(0),
				),
			),
			expected: true,
		},
		{
			name:      "bad signed_by index",
			rule:      signedBy(99),
			errorText: "signed_by index 99 out of range",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := mspPolicySnapshot(t, test.rule)
			got, rule, err := localMSPSatisfiesNamespacePolicy(policy, "org-0")
			if test.errorText != "" {
				if err == nil || !strings.Contains(err.Error(), test.errorText) {
					t.Fatalf("expected error containing %q, got %v", test.errorText, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("localMSPSatisfiesNamespacePolicy returned error: %v", err)
			}
			if rule != "msp" {
				t.Fatalf("expected msp rule, got %q", rule)
			}
			if got != test.expected {
				t.Fatalf("expected satisfied=%t, got %t", test.expected, got)
			}
		})
	}
}

func TestLocalMSPSatisfiesNamespacePolicyRejectsRemoteOnlyOR(t *testing.T) {
	policy := mspPolicySnapshot(t, nOutOf(1, signedBy(1), signedBy(2)))

	satisfied, rule, err := localMSPSatisfiesNamespacePolicy(policy, "org-0")
	if err != nil {
		t.Fatalf("localMSPSatisfiesNamespacePolicy returned error: %v", err)
	}
	if rule != "msp" {
		t.Fatalf("expected msp rule, got %q", rule)
	}
	if satisfied {
		t.Fatal("remote-only policy should not be satisfied by local MSP")
	}
}

func TestLocalMSPSatisfiesNamespacePolicyRejectsThresholdRule(t *testing.T) {
	policy := NamespacePolicySnapshot{
		Namespace: "0",
		Policy: &applicationpb.NamespacePolicy{
			Rule: &applicationpb.NamespacePolicy_ThresholdRule{
				ThresholdRule: &applicationpb.ThresholdRule{Scheme: "ECDSA"},
			},
		},
	}

	_, rule, err := localMSPSatisfiesNamespacePolicy(policy, "org-0")
	if rule != "threshold" {
		t.Fatalf("expected threshold rule, got %q", rule)
	}
	if err == nil || !strings.Contains(err.Error(), "threshold policy") {
		t.Fatalf("expected threshold policy error, got %v", err)
	}
}

func marshalPolicy(t *testing.T, policy *applicationpb.NamespacePolicy) []byte {
	t.Helper()
	raw, err := proto.Marshal(policy)
	if err != nil {
		t.Fatalf("marshal policy: %v", err)
	}
	return raw
}

func mspPolicySnapshot(t *testing.T, rule *common.SignaturePolicy) NamespacePolicySnapshot {
	t.Helper()
	envelope := &common.SignaturePolicyEnvelope{
		Identities: []*mspapi.MSPPrincipal{
			rolePrincipal(t, "org-0"),
			rolePrincipal(t, "org-1"),
			rolePrincipal(t, "org-2"),
		},
		Rule: rule,
	}
	return NamespacePolicySnapshot{
		Namespace: "0",
		Policy: &applicationpb.NamespacePolicy{
			Rule: &applicationpb.NamespacePolicy_MspRule{MspRule: marshalProto(t, envelope)},
		},
	}
}

func rolePrincipal(t *testing.T, mspID string) *mspapi.MSPPrincipal {
	t.Helper()
	return &mspapi.MSPPrincipal{
		PrincipalClassification: mspapi.MSPPrincipal_ROLE,
		Principal: marshalProto(t, &mspapi.MSPRole{
			MspIdentifier: mspID,
			Role:          mspapi.MSPRole_MEMBER,
		}),
	}
}

func signedBy(index int32) *common.SignaturePolicy {
	return &common.SignaturePolicy{
		Type: &common.SignaturePolicy_SignedBy{SignedBy: index},
	}
}

func nOutOf(n int32, rules ...*common.SignaturePolicy) *common.SignaturePolicy {
	return &common.SignaturePolicy{
		Type: &common.SignaturePolicy_NOutOf_{
			NOutOf: &common.SignaturePolicy_NOutOf{
				N:     n,
				Rules: rules,
			},
		},
	}
}

func marshalProto(t *testing.T, msg proto.Message) []byte {
	t.Helper()
	raw, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal proto: %v", err)
	}
	return raw
}

type fakePolicyQueryClient struct {
	committerpb.QueryServiceClient
	policies *applicationpb.NamespacePolicies
	err      error
}

func (f *fakePolicyQueryClient) GetNamespacePolicies(context.Context, *emptypb.Empty, ...grpc.CallOption) (*applicationpb.NamespacePolicies, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.policies, nil
}
