// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"

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
