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
			rolePrincipal(t, "org-3"),
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
