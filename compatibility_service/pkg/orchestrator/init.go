// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"fmt"

	"compatibility_service/pkg/lifecycle"
	"google.golang.org/grpc/metadata"
)

// The signed proposal owns the init intent. Legacy metadata may assert the
// same value, but cannot override it at either the initiating or remote node.
func validateInitMetadata(ctx context.Context, isInit bool) error {
	for _, value := range metadata.ValueFromIncomingContext(ctx, GRPCInitMetadata) {
		if value != "true" && value != "false" {
			return fmt.Errorf("invalid is-init metadata %q", value)
		}
		if (value == "true") != isInit {
			return fmt.Errorf("is-init metadata conflicts with the signed proposal")
		}
	}
	return nil
}

func (s *Service) validateInitState(ctx context.Context, req InvocationRequest, namespace, operation string) (*lifecycle.ChaincodeDefinition, error) {
	if req.IsInit && operation == GRPCOperationQuery {
		return nil, fmt.Errorf("is-init is only valid for invoke")
	}
	if s == nil || s.lifecycle == nil {
		if req.IsInit {
			return nil, fmt.Errorf("initialization requires a lifecycle definition")
		}
		return nil, nil
	}
	if req.ChaincodeName == "" {
		if req.IsInit {
			return nil, fmt.Errorf("initialization requires a chaincode name")
		}
		return nil, nil
	}

	committed, ok, err := s.lifecycle.GetCommitted(ctx, req.ChaincodeName)
	if req.definition != nil {
		committed, ok, err = *req.definition, true, nil
	}
	if err != nil {
		return nil, err
	}
	if !ok {
		if req.IsInit {
			return nil, fmt.Errorf("initialization requires a committed definition for %s:%s", req.ChaincodeName, req.ChaincodeVersion)
		}
		return nil, nil
	}
	def := committed.Definition
	if req.IsInit {
		if !def.InitRequired {
			return nil, fmt.Errorf("chaincode %s:%s does not require init", def.Name, def.Version)
		}
		if committed.Initialized {
			return nil, fmt.Errorf("chaincode %s:%s sequence %d is already initialized",
				def.Name, def.Version, def.Sequence)
		}
		return &def, nil
	}
	if def.InitRequired && !committed.Initialized {
		return nil, fmt.Errorf("chaincode %s:%s sequence %d requires init before invoke/query",
			def.Name, def.Version, def.Sequence)
	}
	_ = namespace
	return nil, nil
}

func (s *Service) markInitializedAfterCommit(ctx context.Context, def lifecycle.ChaincodeDefinition) error {
	if s == nil || s.lifecycle == nil {
		return nil
	}
	// MarkLifecycleInitialized registers the pending marker before using ctx.
	// Thus an expired application caller cannot lose unfinished lifecycle work.
	if _, err := s.MarkLifecycleInitialized(ctx, def); err != nil {
		return fmt.Errorf("mark lifecycle initialized on ledger: %w", err)
	}
	if _, err := s.FinalizeLifecycleDefinition(ctx, def, true); err != nil {
		return fmt.Errorf("reconcile lifecycle initialized mirrors: %w", err)
	}
	return nil
}
