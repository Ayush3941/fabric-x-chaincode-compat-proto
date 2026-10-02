// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sort"

	"compatibility_service/pkg/lifecycle"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const GRPCLifecycleBindingMetadata = "x-compat-lifecycle-binding"

// LifecycleExecutionBinding is org-local evidence for the lifecycle package
// and endpoint used during one chaincode execution.
type LifecycleExecutionBinding struct {
	MSPID            string `json:"msp_id,omitempty"`
	Namespace        string `json:"namespace,omitempty"`
	ChaincodeName    string `json:"chaincode_name,omitempty"`
	ChaincodeVersion string `json:"chaincode_version,omitempty"`
	Sequence         int64  `json:"sequence,omitempty"`
	PackageID        string `json:"package_id,omitempty"`
	PackageAddress   string `json:"package_address,omitempty"`
}

func (s *Service) lifecycleExecutionBinding(ctx context.Context, req InvocationRequest, namespace string) *LifecycleExecutionBinding {
	if s == nil || s.lifecycle == nil || req.ChaincodeName == "" || req.ChaincodeVersion == "" {
		return nil
	}
	localMSP := ""
	if s.cfg.Identity != nil {
		localMSP = s.cfg.Identity.MspID
	}
	if localMSP == "" {
		return nil
	}

	committed, ok, err := s.lifecycle.GetCommitted(ctx, req.ChaincodeName, req.ChaincodeVersion)
	if err != nil {
		s.logger.Warnf("lifecycle binding lookup failed name=%s version=%s: %s", req.ChaincodeName, req.ChaincodeVersion, err)
		return nil
	}
	if !ok {
		return nil
	}

	def := committed.Definition
	binding := &LifecycleExecutionBinding{
		MSPID:            localMSP,
		Namespace:        namespace,
		ChaincodeName:    def.Name,
		ChaincodeVersion: def.Version,
		Sequence:         def.Sequence,
	}

	approval, ok, err := s.lifecycle.GetApproval(ctx, def.Name, def.Version, localMSP)
	if err != nil {
		s.logger.Debugf("lifecycle binding approval lookup skipped msp=%s name=%s version=%s: %s",
			localMSP, def.Name, def.Version, err)
	} else if ok && lifecycleDefinitionsMatch(approval.Definition, def) {
		binding.PackageID = approval.PackageID
		binding.PackageAddress = approval.PackageAddress
	}

	if cached, ok, err := s.lifecycle.GetResolvedConnection(ctx, localMSP, def); err != nil {
		s.logger.Debugf("lifecycle binding connection cache lookup skipped msp=%s name=%s version=%s: %s",
			localMSP, def.Name, def.Version, err)
	} else if ok {
		applyResolvedBinding(binding, cached)
	}

	if (binding.PackageID == "" || binding.PackageAddress == "") && s.resolver != nil {
		resolved, ok, err := s.resolver.Resolve(ctx, chaincodeResolverRequest{
			MSPID:      localMSP,
			Definition: def,
		})
		if err != nil {
			s.logger.Debugf("lifecycle binding resolver lookup skipped msp=%s name=%s version=%s: %s",
				localMSP, def.Name, def.Version, err)
		} else if ok {
			if cached, err := s.lifecycle.PutResolvedConnection(ctx, resolved); err == nil {
				resolved = cached
			}
			applyResolvedBinding(binding, resolved)
		}
	}

	return binding
}

func applyResolvedBinding(binding *LifecycleExecutionBinding, conn lifecycle.ResolvedChaincodeConnection) {
	if binding == nil {
		return
	}
	if binding.PackageAddress == "" {
		binding.PackageAddress = conn.Address
	}
	if binding.PackageID == "" {
		binding.PackageID = conn.PackageID
	}
}

func lifecycleDefinitionsMatch(left, right lifecycle.ChaincodeDefinition) bool {
	return left.Name == right.Name &&
		left.Version == right.Version &&
		left.Sequence == right.Sequence &&
		left.InitRequired == right.InitRequired
}

func lifecycleBindings(local helperExecutionResult, remote []helperExecutionResult) []LifecycleExecutionBinding {
	out := make([]LifecycleExecutionBinding, 0, 1+len(remote))
	if local.Binding != nil {
		out = append(out, *local.Binding)
	}
	for _, result := range remote {
		if result.Binding != nil {
			out = append(out, *result.Binding)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].MSPID < out[j].MSPID
	})
	return out
}

func setLifecycleBindingMetadata(ctx context.Context, binding *LifecycleExecutionBinding) error {
	if binding == nil {
		return nil
	}
	value, err := encodeLifecycleBinding(binding)
	if err != nil {
		return err
	}
	return grpc.SetHeader(ctx, metadata.Pairs(GRPCLifecycleBindingMetadata, value))
}

func lifecycleBindingFromMetadata(md metadata.MD) (*LifecycleExecutionBinding, error) {
	values := md.Get(GRPCLifecycleBindingMetadata)
	if len(values) == 0 {
		return nil, nil
	}
	return decodeLifecycleBinding(values[0])
}

func encodeLifecycleBinding(binding *LifecycleExecutionBinding) (string, error) {
	raw, err := json.Marshal(binding)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}

func decodeLifecycleBinding(value string) (*LifecycleExecutionBinding, error) {
	raw, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return nil, err
	}
	var binding LifecycleExecutionBinding
	if err := json.Unmarshal(raw, &binding); err != nil {
		return nil, err
	}
	return &binding, nil
}
