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
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/network"
	"google.golang.org/grpc/metadata"
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
	req InvocationRequest,
	local helperExecutionResult,
) ([]helperExecutionResult, error) {
	// local is kept for later chaincode version/hash checks during lifecycle work.
	_ = local
	localMSPID := ""
	if s != nil && s.cfg.Identity != nil {
		localMSPID = s.cfg.Identity.MspID
	}
	remotesByMSP := s.remoteOrchestratorsByMSP()
	availableRemoteMSPs := make(map[string]struct{}, len(remotesByMSP))
	for mspID := range remotesByMSP {
		availableRemoteMSPs[mspID] = struct{}{}
	}

	plan, rule, err := namespacePolicyPlan(policy, localMSPID, availableRemoteMSPs)
	if err != nil {
		return nil, err
	}
	if !plan.satisfied {
		return nil, fmt.Errorf("namespace %s policy=%s is not satisfied by local msp %s; remote orchestrator support is not configured",
			policy.Namespace, rule, localMSPID)
	}
	if len(plan.remoteMSPs) == 0 {
		s.logger.Debugf("namespace=%s policy=%s satisfied by local msp=%s; remote orchestrator execution skipped",
			policy.Namespace, rule, localMSPID)
		return nil, nil
	}
	if req.ClientSignedProposal == nil {
		return nil, errors.New("remote orchestrator execution requires the original signed proposal")
	}

	results := make([]helperExecutionResult, 0, len(plan.remoteMSPs))
	for _, mspID := range plan.remoteMSPs {
		remote, ok := remotesByMSP[mspID]
		if !ok {
			return nil, fmt.Errorf("remote orchestrator for msp %s is not configured", mspID)
		}
		result, err := s.executeRemoteOrchestrator(ctx, remote, req.ClientSignedProposal)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (s *Service) remoteOrchestratorsByMSP() map[string]RemoteOrchestratorConfig {
	out := make(map[string]RemoteOrchestratorConfig, len(s.cfg.RemoteOrgs))
	for _, remote := range s.cfg.RemoteOrgs {
		if remote.MSPID == "" {
			continue
		}
		out[remote.MSPID] = remote
	}
	return out
}

func (s *Service) executeRemoteOrchestrator(ctx context.Context, remote RemoteOrchestratorConfig, prop *peer.SignedProposal) (helperExecutionResult, error) {
	proposal, err := protoutil.UnmarshalProposal(prop.ProposalBytes)
	if err != nil {
		return helperExecutionResult{}, fmt.Errorf("unmarshal remote proposal: %w", err)
	}
	txID, err := txIDFromProposal(proposal)
	if err != nil {
		return helperExecutionResult{}, err
	}

	s.logger.Infof("tx=%s requesting remote orchestrator msp=%s endpoint=%s", txID, remote.MSPID, remote.Address())
	remotePeer, err := network.NewPeer(remote.ToPeerConf())
	if err != nil {
		return helperExecutionResult{}, fmt.Errorf("remote orchestrator %s: %w", remote.MSPID, err)
	}
	defer remotePeer.Close() //nolint:errcheck

	ctx = metadata.AppendToOutgoingContext(ctx, GRPCOperationMetadata, GRPCOperationEndorse)
	resp, err := remotePeer.ProcessProposal(ctx, prop)
	if err != nil {
		return helperExecutionResult{}, fmt.Errorf("remote orchestrator %s endorsement: %w", remote.MSPID, err)
	}
	if resp == nil || resp.Response == nil {
		return helperExecutionResult{}, fmt.Errorf("remote orchestrator %s returned no proposal response", remote.MSPID)
	}
	s.logger.Infof("tx=%s remote orchestrator msp=%s response status=%d tx_payload_bytes=%d endorsement_present=%t",
		txID, remote.MSPID, resp.Response.Status, len(resp.Payload), resp.Endorsement != nil)

	return helperExecutionResult{
		Endorsement: sdk.Endorsement{
			Proposal:  proposal,
			Responses: []*peer.ProposalResponse{resp},
		},
		TxID:     txID,
		Response: resp.Response,
	}, nil
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
