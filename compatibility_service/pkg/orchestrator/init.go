// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"fmt"

	"compatibility_service/pkg/lifecycle"
)

func (s *Service) validateInitState(ctx context.Context, req InvocationRequest, namespace, operation string) (*lifecycle.ChaincodeDefinition, error) {
	if s == nil || s.lifecycle == nil {
		return nil, nil
	}
	if req.ChaincodeName == "" || req.ChaincodeVersion == "" {
		return nil, nil
	}
	if req.IsInit && operation == GRPCOperationQuery {
		return nil, fmt.Errorf("is-init is only valid for invoke")
	}

	committed, ok, err := s.lifecycle.GetCommitted(ctx, req.ChaincodeName, req.ChaincodeVersion)
	if err != nil {
		return nil, err
	}
	if !ok {
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
	ledger, err := s.MarkLifecycleInitialized(ctx, def)
	if err != nil {
		return fmt.Errorf("mark lifecycle initialized on ledger: %w", err)
	}
	s.logger.Infof("lifecycle initialized ledger marker committed tx=%s status=%s block=%d key=%s",
		ledger.TxID, ledger.Status, ledger.BlockNum, ledger.Key)

	committed, err := s.lifecycle.MarkInitialized(ctx, def, s.cfg.Identity.MspID)
	if err != nil {
		return fmt.Errorf("mark local lifecycle initialized: %w", err)
	}
	s.logger.Infof("lifecycle initialized locally name=%s version=%s sequence=%d initialized=%t",
		def.Name, def.Version, def.Sequence, committed.Initialized)

	req, err := lifecycle.SignRequest(s.signer, lifecycle.MarkInitializedRequest{Definition: def})
	if err != nil {
		return fmt.Errorf("sign lifecycle initialized request: %w", err)
	}
	remotes, err := s.RemotePeers(ctx, lifecycle.RemotePeerRequest{
		RequesterMSP: s.cfg.Identity.MspID,
		Definition:   def,
	})
	if err != nil {
		return fmt.Errorf("resolve remote lifecycle peers: %w", err)
	}
	for _, remote := range remotes {
		res, err := remote.MarkInitialized(ctx, req)
		if err != nil {
			return err
		}
		s.logger.Infof("lifecycle initialized remote msp=%s name=%s version=%s sequence=%d initialized=%t",
			remote.MSPID(), def.Name, def.Version, def.Sequence, res.Definition.Initialized)
	}
	return nil
}
