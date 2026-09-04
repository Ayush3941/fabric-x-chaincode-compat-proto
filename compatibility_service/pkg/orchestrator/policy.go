// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	common "github.com/hyperledger/fabric-protos-go-apiv2/common"
	mspapi "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// NamespacePolicySnapshot is the namespace policy loaded from Fabric-X Query
// Service for an invocation namespace.
type NamespacePolicySnapshot struct {
	Namespace string
	Version   uint64
	RawPolicy []byte
	Policy    *applicationpb.NamespacePolicy
}

type namespacePolicyResolver struct {
	query committerpb.QueryServiceClient
	mu    sync.RWMutex
	seen  map[string]NamespacePolicySnapshot
}

func newNamespacePolicyResolver(query committerpb.QueryServiceClient) *namespacePolicyResolver {
	return &namespacePolicyResolver{
		query: query,
		seen:  make(map[string]NamespacePolicySnapshot),
	}
}

func (r *namespacePolicyResolver) Resolve(ctx context.Context, namespace string) (NamespacePolicySnapshot, error) {
	if r == nil {
		return NamespacePolicySnapshot{}, errors.New("namespace policy resolver is required")
	}
	snapshot, err := loadNamespacePolicy(ctx, r.query, namespace)
	if err != nil {
		return NamespacePolicySnapshot{}, err
	}

	r.mu.Lock()
	r.seen[namespace] = cloneNamespacePolicySnapshot(snapshot)
	r.mu.Unlock()

	return snapshot, nil
}

func (r *namespacePolicyResolver) Snapshot(namespace string) (NamespacePolicySnapshot, bool) {
	if r == nil {
		return NamespacePolicySnapshot{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	snapshot, ok := r.seen[namespace]
	if !ok {
		return NamespacePolicySnapshot{}, false
	}
	return cloneNamespacePolicySnapshot(snapshot), true
}

// CachedNamespacePolicy returns the latest policy snapshot observed for a
// namespace through request-time policy resolution.
func (s *Service) CachedNamespacePolicy(namespace string) (NamespacePolicySnapshot, bool) {
	if s == nil || s.policies == nil {
		return NamespacePolicySnapshot{}, false
	}
	return s.policies.Snapshot(namespace)
}

func (s *Service) resolveNamespacePolicy(ctx context.Context, namespace string) (NamespacePolicySnapshot, error) {
	if s == nil || s.policies == nil {
		return NamespacePolicySnapshot{}, errors.New("namespace policy resolver is not configured")
	}
	policy, err := s.policies.Resolve(ctx, namespace)
	if err != nil {
		return NamespacePolicySnapshot{}, err
	}
	loggerOrNoop(s.logger).Infof("namespace policy resolved namespace=%s version=%d rule=%s policy_bytes=%d",
		policy.Namespace, policy.Version, policy.RuleName(), len(policy.RawPolicy))
	return policy, nil
}

func (p NamespacePolicySnapshot) RuleName() string {
	if p.Policy == nil {
		return "unknown"
	}
	switch p.Policy.GetRule().(type) {
	case *applicationpb.NamespacePolicy_MspRule:
		return "msp"
	case *applicationpb.NamespacePolicy_ThresholdRule:
		return "threshold"
	default:
		return "unknown"
	}
}

func loadNamespacePolicy(ctx context.Context, query committerpb.QueryServiceClient, namespace string) (NamespacePolicySnapshot, error) {
	if query == nil {
		return NamespacePolicySnapshot{}, errors.New("query service client is required")
	}
	policies, err := query.GetNamespacePolicies(ctx, &emptypb.Empty{})
	if err != nil {
		return NamespacePolicySnapshot{}, err
	}
	for _, item := range policies.GetPolicies() {
		if item.GetNamespace() != namespace {
			continue
		}
		raw := append([]byte(nil), item.GetPolicy()...)
		if len(raw) == 0 {
			return NamespacePolicySnapshot{}, fmt.Errorf("namespace %q has empty policy", namespace)
		}
		policy := &applicationpb.NamespacePolicy{}
		if err := proto.Unmarshal(raw, policy); err != nil {
			return NamespacePolicySnapshot{}, fmt.Errorf("decode namespace %q policy: %w", namespace, err)
		}
		return NamespacePolicySnapshot{
			Namespace: item.GetNamespace(),
			Version:   item.GetVersion(),
			RawPolicy: raw,
			Policy:    policy,
		}, nil
	}
	return NamespacePolicySnapshot{}, fmt.Errorf("namespace %q policy not found", namespace)
}

func cloneNamespacePolicySnapshot(in NamespacePolicySnapshot) NamespacePolicySnapshot {
	out := NamespacePolicySnapshot{
		Namespace: in.Namespace,
		Version:   in.Version,
		RawPolicy: append([]byte(nil), in.RawPolicy...),
	}
	if in.Policy != nil {
		out.Policy = proto.Clone(in.Policy).(*applicationpb.NamespacePolicy)
	}
	return out
}

func localMSPSatisfiesNamespacePolicy(policy NamespacePolicySnapshot, localMSPID string) (bool, string, error) {
	if localMSPID == "" {
		return false, "", errors.New("local MSP ID is required")
	}
	if policy.Policy == nil {
		return false, "", fmt.Errorf("namespace %q policy is nil", policy.Namespace)
	}

	switch rule := policy.Policy.GetRule().(type) {
	case *applicationpb.NamespacePolicy_MspRule:
		env := &common.SignaturePolicyEnvelope{}
		if err := proto.Unmarshal(rule.MspRule, env); err != nil {
			return false, "msp", fmt.Errorf("decode namespace %q MSP policy: %w", policy.Namespace, err)
		}
		satisfied, err := signaturePolicySatisfied(env.GetRule(), env.GetIdentities(), localMSPID)
		if err != nil {
			return false, "msp", fmt.Errorf("evaluate namespace %q MSP policy: %w", policy.Namespace, err)
		}
		return satisfied, "msp", nil
	case *applicationpb.NamespacePolicy_ThresholdRule:
		scheme := ""
		if rule.ThresholdRule != nil {
			scheme = rule.ThresholdRule.GetScheme()
		}
		return false, "threshold", fmt.Errorf("namespace %q uses threshold policy %q; current orchestrator only supports MSP policy routing", policy.Namespace, scheme)
	default:
		return false, "", fmt.Errorf("namespace %q has unsupported policy rule %T", policy.Namespace, rule)
	}
}

func signaturePolicySatisfied(rule *common.SignaturePolicy, identities []*mspapi.MSPPrincipal, localMSPID string) (bool, error) {
	if rule == nil {
		return false, errors.New("policy rule is nil")
	}

	switch ruleType := rule.GetType().(type) {
	case *common.SignaturePolicy_SignedBy:
		index := int(ruleType.SignedBy)
		if index < 0 || index >= len(identities) {
			return false, fmt.Errorf("signed_by index %d out of range", ruleType.SignedBy)
		}
		return principalMatchesLocalMSP(identities[index], localMSPID)
	case *common.SignaturePolicy_NOutOf_:
		noutof := ruleType.NOutOf
		if noutof == nil {
			return false, errors.New("n_out_of rule is nil")
		}
		required := int(noutof.N)
		if required <= 0 {
			return true, nil
		}
		matched := 0
		for _, child := range noutof.Rules {
			satisfied, err := signaturePolicySatisfied(child, identities, localMSPID)
			if err != nil {
				return false, err
			}
			if satisfied {
				matched++
				if matched >= required {
					return true, nil
				}
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("unsupported signature policy rule %T", ruleType)
	}
}

func principalMatchesLocalMSP(principal *mspapi.MSPPrincipal, localMSPID string) (bool, error) {
	if principal == nil {
		return false, errors.New("principal is nil")
	}
	if principal.GetPrincipalClassification() != mspapi.MSPPrincipal_ROLE {
		return false, fmt.Errorf("unsupported principal classification %s", principal.GetPrincipalClassification())
	}

	role := &mspapi.MSPRole{}
	if err := proto.Unmarshal(principal.GetPrincipal(), role); err != nil {
		return false, fmt.Errorf("decode MSP role principal: %w", err)
	}
	return role.GetMspIdentifier() == localMSPID, nil
}
