// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"compatibility_service/pkg/helper"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	sdk "github.com/hyperledger/fabric-x-sdk"
)

type helperExecutionResult struct {
	Endorsement sdk.Endorsement
	TxID        string
	Response    *peer.Response
}

func (s *Service) executeFresh(ctx context.Context, req InvocationRequest, namespace string, args [][]byte) (helperExecutionResult, error) {
	s.logger.Infof("orchestrator calling in-process helper namespace=%s fn=%s args=%d",
		namespace, req.Function, len(req.Args))
	clientProposal := helper.ClientProposalContext{
		Creator:        req.ClientCreator,
		Nonce:          req.ClientNonce,
		SignedProposal: req.ClientSignedProposal,
		Decorations:    compatibilityDecorations(namespace),
	}
	end, err := s.executeHelper(ctx, namespace, "1.0", args, clientProposal)
	if err != nil {
		return helperExecutionResult{}, err
	}
	txID, err := txIDFromEndorsement(end)
	if err != nil {
		return helperExecutionResult{}, err
	}
	if req.ClientTxID != "" && req.ClientTxID != txID {
		s.logger.Infof("client_tx=%s helper_tx=%s helper execution transaction id selected", req.ClientTxID, txID)
	}
	if len(end.Responses) == 0 || end.Responses[0] == nil || end.Responses[0].Response == nil {
		return helperExecutionResult{}, errors.New("helper returned no proposal response")
	}
	return helperExecutionResult{
		Endorsement: end,
		TxID:        txID,
		Response:    end.Responses[0].Response,
	}, nil
}

func (s *Service) requestRemoteOrchestratorsIfPolicyNeedsThem(
	ctx context.Context,
	policy NamespacePolicySnapshot,
	local helperExecutionResult,
) ([]helperExecutionResult, error) {
	_ = ctx
	_ = local
	localMSPID := ""
	if s != nil && s.cfg.Identity != nil {
		localMSPID = s.cfg.Identity.MspID
	}
	satisfied, rule, err := localMSPSatisfiesNamespacePolicy(policy, localMSPID)
	if err != nil {
		return nil, err
	}
	if !satisfied {
		return nil, fmt.Errorf("namespace %s policy=%s is not satisfied by local msp %s; remote orchestrator support is not configured",
			policy.Namespace, rule, localMSPID)
	}
	s.logger.Debugf("namespace=%s policy=%s satisfied by local msp=%s; remote orchestrator execution skipped",
		policy.Namespace, rule, localMSPID)
	return nil, nil
}

func compareCanonicalResults(local helperExecutionResult, remote []helperExecutionResult) error {
	if local.Response == nil {
		return errors.New("local helper response is nil")
	}
	for i, result := range remote {
		if result.Response == nil {
			return fmt.Errorf("remote helper result %d response is nil", i)
		}
		if local.Response.Status != result.Response.Status {
			return fmt.Errorf("remote helper result %d status mismatch: local=%d remote=%d",
				i, local.Response.Status, result.Response.Status)
		}
		if local.Response.Message != result.Response.Message {
			return fmt.Errorf("remote helper result %d message mismatch", i)
		}
		if !bytes.Equal(local.Response.Payload, result.Response.Payload) {
			return fmt.Errorf("remote helper result %d response payload mismatch", i)
		}
		if len(local.Endorsement.Responses) == 0 || len(result.Endorsement.Responses) == 0 {
			return fmt.Errorf("remote helper result %d missing endorsement response", i)
		}
		if !bytes.Equal(local.Endorsement.Responses[0].Payload, result.Endorsement.Responses[0].Payload) {
			return fmt.Errorf("remote helper result %d transaction payload mismatch", i)
		}
	}
	return nil
}

func mergeEndorsements(local helperExecutionResult, remote []helperExecutionResult) (sdk.Endorsement, error) {
	if local.Endorsement.Proposal == nil {
		return sdk.Endorsement{}, errors.New("local endorsement has no proposal")
	}
	responses := append([]*peer.ProposalResponse(nil), local.Endorsement.Responses...)
	for _, result := range remote {
		responses = append(responses, result.Endorsement.Responses...)
	}
	if len(responses) == 0 {
		return sdk.Endorsement{}, errors.New("no endorsement responses to merge")
	}
	return sdk.Endorsement{
		Proposal:  local.Endorsement.Proposal,
		Responses: responses,
	}, nil
}

func (s *Service) submitAndWaitFinality(
	ctx context.Context,
	end sdk.Endorsement,
	out InvocationResponse,
	txID string,
	record *idempotencyRecord,
) (InvocationResponse, error) {
	finality, err := s.subscribeFinality(ctx, txID)
	if err != nil {
		return out, fmt.Errorf("subscribe finality: %w", err)
	}
	if finality != nil {
		defer finality.Cancel()
	}

	s.logger.Infof("tx=%s submitting endorsed Fabric-X transaction", txID)
	if err := s.submitter.Submit(ctx, end); err != nil {
		return out, fmt.Errorf("submit failed: %w", err)
	}
	out.Submitted = true
	if record != nil {
		s.idempotency.markSubmitted(record, out)
	}
	s.logger.Infof("tx=%s submitted", txID)

	status, err := finality.Wait()
	if err != nil {
		s.logger.Warnf("tx=%s finality wait failed: %s", txID, err)
		return out, nil
	}
	if status != nil {
		out.CommitStatus = status.Status.String()
		out.BlockNum = status.Ref.GetBlockNum()
		out.TxNum = status.Ref.GetTxNum()
		s.logger.Infof("tx=%s finality status=%s block=%d txnum=%d",
			txID, out.CommitStatus, out.BlockNum, out.TxNum)
		if status.Status == committerpb.Status_COMMITTED {
			out.ChaincodeEvent = eventFromEndorsement(end, txID)
		}
	}

	return out, nil
}
