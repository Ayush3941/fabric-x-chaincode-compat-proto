// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"

	"compatibility_service/pkg/helper"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/msp"
	sdk "github.com/hyperledger/fabric-x-sdk"
)

type helperExecutionResult struct {
	Endorsement sdk.Endorsement
	TxID        string
	Response    *peer.Response
	Binding     *LifecycleExecutionBinding
}

func (s *Service) executeFresh(ctx context.Context, req InvocationRequest, namespace string, namespaceVersion uint64, args [][]byte) (helperExecutionResult, error) {
	s.logger.Infof("orchestrator calling in-process helper namespace=%s chaincode=%s:%s fn=%s args=%d",
		namespace, req.ChaincodeName, req.ChaincodeVersion, req.Function, len(req.Args))
	clientProposal := helper.ClientProposalContext{
		Creator:          req.ClientCreator,
		Nonce:            req.ClientNonce,
		SignedProposal:   req.ClientSignedProposal,
		Decorations:      compatibilityDecorations(namespace, req.ChaincodeName, req.ChaincodeVersion),
		NamespaceVersion: namespaceVersion,
		IsInit:           req.IsInit,
	}
	end, err := s.executeHelper(ctx, namespace, "1.0", args, req.ClientTransient, clientProposal)
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
	binding := s.lifecycleExecutionBinding(ctx, req, namespace)
	if binding != nil {
		s.logger.Infof("tx=%s lifecycle binding msp=%s namespace=%s chaincode=%s:%s sequence=%d package_id=%s endpoint=%s",
			txID, binding.MSPID, binding.Namespace, binding.ChaincodeName, binding.ChaincodeVersion,
			binding.Sequence, binding.PackageID, binding.PackageAddress)
	}
	return helperExecutionResult{
		Endorsement: end,
		TxID:        txID,
		Response:    end.Responses[0].Response,
		Binding:     binding,
	}, nil
}

func (s *Service) collectNamespaceEndorsements(
	ctx context.Context,
	policy NamespacePolicySnapshot,
	req InvocationRequest,
	local helperExecutionResult,
) (sdk.Endorsement, []helperExecutionResult, error) {
	localMSPID := ""
	if s != nil && s.cfg.Identity != nil {
		localMSPID = s.cfg.Identity.MspID
	}
	var deserializer msp.IdentityDeserializer
	if s.proposalAuth != nil {
		deserializer = s.proposalAuth.identities
	}
	compiled, err := newNamespaceEndorsementPolicy(policy, deserializer)
	if err != nil {
		return sdk.Endorsement{}, nil, err
	}
	if req.ClientTxID != "" && req.ClientTxID != local.TxID {
		return sdk.Endorsement{}, nil, errors.New("local execution transaction ID does not match client proposal")
	}
	verified, err := compiled.authenticate(local, localMSPID)
	if err != nil {
		return sdk.Endorsement{}, nil, fmt.Errorf("local endorsement: %w", err)
	}
	candidates := []verifiedEndorsement{verified}
	seenSigners := map[string]bool{verified.key: true}
	if end, remote, ok, err := compiled.assembleSelection(ctx, local, candidates); ok || err != nil {
		return end, remote, err
	}
	if req.ClientSignedProposal == nil {
		return sdk.Endorsement{}, nil, errors.New("remote orchestrator execution requires the original signed proposal")
	}
	var availableRemoteMSPs map[string]struct{}
	if s != nil && s.remotes != nil {
		availableRemoteMSPs = s.remotes.AvailableMSPs()
	}

	targetMSPs := append([]string(nil), compiled.targetMSPs...)
	if compiled.unscoped {
		var extra []string
		for mspID := range availableRemoteMSPs {
			extra = append(extra, mspID)
		}
		sort.Strings(extra)
		targetMSPs = append(targetMSPs, extra...)
	}
	req.Namespace = policy.Namespace
	visitedMSPs := make(map[string]bool)
	var failures []error
	for _, mspID := range targetMSPs {
		if err := ctx.Err(); err != nil {
			return sdk.Endorsement{}, nil, err
		}
		if visitedMSPs[mspID] {
			continue
		}
		visitedMSPs[mspID] = true
		if _, ok := availableRemoteMSPs[mspID]; !ok {
			continue
		}
		remotes, ok, err := s.remotes.Resolve(ctx, remoteOrchestratorResolveRequest{
			RequesterMSPID:   localMSPID,
			TargetMSPID:      mspID,
			ChannelID:        s.cfg.ChannelID,
			Namespace:        policy.Namespace,
			ChaincodeName:    req.ChaincodeName,
			ChaincodeVersion: req.ChaincodeVersion,
		})
		if err != nil {
			failures = append(failures, fmt.Errorf("resolve endorsers for %s: %w", mspID, err))
			continue
		}
		if !ok || len(remotes) == 0 {
			continue
		}
		visitedEndpoints := make(map[string]bool)
		for _, remote := range remotes {
			if err := ctx.Err(); err != nil {
				return sdk.Endorsement{}, nil, err
			}
			if remote == nil || visitedEndpoints[remote.Address()] {
				continue
			}
			visitedEndpoints[remote.Address()] = true
			result, err := remote.EndorseOnly(ctx, req)
			if err != nil {
				failures = append(failures, err)
				loggerOrNoop(s.logger).Warnf("remote orchestrator candidate failed msp=%s endpoint=%s: %s", mspID, remote.Address(), err)
				continue
			}
			if result.TxID != local.TxID {
				return sdk.Endorsement{}, nil, errors.New("remote execution transaction ID mismatch")
			}
			verified, err := compiled.authenticate(result, mspID)
			if err != nil {
				failures = append(failures, fmt.Errorf("endorser %s at %s: %w", mspID, remote.Address(), err))
				loggerOrNoop(s.logger).Warnf("remote endorsement rejected msp=%s endpoint=%s: %s", mspID, remote.Address(), err)
				continue
			}
			if err := compareCanonicalResults(local, []helperExecutionResult{result}); err != nil {
				return sdk.Endorsement{}, nil, err
			}
			if seenSigners[verified.key] {
				continue
			}
			seenSigners[verified.key] = true
			candidates = append(candidates, verified)
			if end, selected, ok, err := compiled.assembleSelection(ctx, local, candidates); ok || err != nil {
				return end, selected, err
			}
		}
	}
	failures = append([]error{fmt.Errorf("namespace %s endorsement policy is not satisfied by the available distinct signing identities", policy.Namespace)}, failures...)
	return sdk.Endorsement{}, nil, errors.Join(failures...)
}

func (p *namespaceEndorsementPolicy) assembleSelection(ctx context.Context, local helperExecutionResult, candidates []verifiedEndorsement) (sdk.Endorsement, []helperExecutionResult, bool, error) {
	indices, err := p.selectEndorsements(ctx, candidates)
	if err != nil || len(indices) == 0 {
		return sdk.Endorsement{}, nil, false, err
	}
	chosen := make([]helperExecutionResult, len(indices))
	var remote []helperExecutionResult
	for i, index := range indices {
		chosen[i] = candidates[index].result
		if index != 0 {
			remote = append(remote, chosen[i])
		}
	}
	end, err := mergeEndorsements(chosen[0], chosen[1:])
	if err != nil {
		return sdk.Endorsement{}, nil, false, err
	}
	end.Proposal = local.Endorsement.Proposal
	if err := p.verifySelection(end, local.TxID); err != nil {
		return sdk.Endorsement{}, nil, false, fmt.Errorf("verify collected namespace endorsements: %w", err)
	}
	return end, remote, true, nil
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
	afterCommit func(context.Context) error,
) (InvocationResponse, error) {
	if record == nil {
		return out, errors.New("submission requires an idempotency record")
	}
	if err := record.lockFinality(ctx); err != nil {
		return out, err
	}
	defer record.unlockFinality()

	// Only the active invocation owns this subscription. Returning or canceling
	// the request stops observation; later submissions refresh the stored record.
	finality, err := s.subscribeFinality(ctx, txID)
	if err != nil {
		s.logger.Warnf("tx=%s notification subscription unavailable; submitted status will be refreshed on a later invoke: %s", txID, err)
	}
	defer finality.Cancel()
	s.logger.Infof("tx=%s submitting endorsed Fabric-X transaction", txID)
	if err := s.submitter.Submit(ctx, end); err != nil {
		return out, fmt.Errorf("submit failed: %w", err)
	}
	out.Submitted = true
	s.idempotency.markSubmitted(record, out, end, afterCommit)
	s.logger.Infof("tx=%s submitted", txID)

	status, err := finality.Wait()
	if err != nil || !isFinalStatus(txID, status) {
		s.logger.Warnf("tx=%s finality wait ended without a confirmed outcome; transaction remains SUBMITTED: %v", txID, err)
		return s.idempotency.waitResult(record, nil)
	}
	_ = s.recordFinality(ctx, record, status)
	return s.idempotency.waitResult(record, nil)
}

func (s *Service) replaySubmission(ctx context.Context, record *idempotencyRecord) (InvocationResponse, error) {
	out, err := s.idempotency.wait(ctx, record)
	if err != nil || !out.FinalityPending {
		return out, err
	}
	// This response belongs only to the explicitly retried transaction. Share
	// its in-flight wait; completed records need no ledger RPC.
	waitCtx, cancel := context.WithTimeout(ctx, s.cfg.finalityTimeout())
	defer cancel()
	if err := record.lockFinality(waitCtx); err != nil {
		return s.idempotency.waitResult(record, err)
	}
	defer record.unlockFinality()
	return s.idempotency.waitResult(record, nil)
}

// reconcilePending updates only the internal store when another invoke arrives.
// Its results are never included in the new invocation's client response.
// An active invoke owns its finality wait; skip it rather than delaying or
// duplicating that work. Missing statuses and query failures leave records alone.
func (s *Service) reconcilePending(ctx context.Context) {
	if s.queryService == nil || ctx.Err() != nil {
		return
	}
	pending := make(map[string]*idempotencyRecord)
	var txIDs []string
	for _, record := range s.idempotency.pendingRecords() {
		if !record.tryLockFinality() {
			continue
		}
		invocation, ok := s.idempotency.pending(record)
		if !ok {
			record.unlockFinality()
			continue
		}
		txID := invocation.Response.TxID
		pending[txID] = record
		txIDs = append(txIDs, txID)
	}
	if len(txIDs) == 0 {
		return
	}
	defer func() {
		for _, record := range pending {
			record.unlockFinality()
		}
	}()
	queryCtx, cancel := context.WithTimeout(ctx, s.cfg.finalityTimeout())
	defer cancel()
	response, err := s.queryService.GetTransactionStatus(queryCtx, &committerpb.TxStatusQuery{TxIds: txIDs})
	if err != nil {
		s.logger.Debugf("pending transaction reconciliation unavailable: %s", err)
		return
	}
	for _, status := range response.GetStatuses() {
		record := pending[status.GetRef().GetTxId()]
		if record == nil || !isFinalStatus(status.GetRef().GetTxId(), status) {
			continue
		}
		if err := s.recordFinality(ctx, record, status); err != nil {
			s.logger.Warnf("tx=%s pending transaction finalization failed: %s", status.Ref.TxId, err)
		}
	}
}

// recordFinality records the outcome without returning a client response.
// It never reexecutes or resubmits that transaction.
func (s *Service) recordFinality(ctx context.Context, record *idempotencyRecord, status *committerpb.TxStatus) error {
	pending, ok := s.idempotency.pending(record)
	if !ok {
		return nil
	}
	out := pending.Response
	if !isFinalStatus(out.TxID, status) {
		return nil
	}
	var err error
	out.CommitStatus = status.Status.String()
	out.BlockNum = status.Ref.GetBlockNum()
	out.TxNum = status.Ref.GetTxNum()
	out.FinalityPending = false
	if status.Status == committerpb.Status_COMMITTED {
		out.ChaincodeEvent = eventFromEndorsement(pending.Endorsement, out.TxID)
		if pending.AfterCommit != nil {
			// A canceled request leaves initialization for a later live request rather
			// than consuming its one finalization attempt with a dead context.
			if ctx.Err() != nil {
				return nil
			}
			err = pending.AfterCommit(ctx)
		}
	}
	s.logger.Infof("tx=%s finality status=%s block=%d txnum=%d", out.TxID, out.CommitStatus, out.BlockNum, out.TxNum)
	s.idempotency.confirmFinality(record, out, err)
	return err
}

func isFinalStatus(txID string, status *committerpb.TxStatus) bool {
	if status == nil || status.Ref.GetTxId() != txID || status.Status == committerpb.Status_STATUS_UNSPECIFIED {
		return false
	}
	_, known := committerpb.Status_name[int32(status.Status)]
	return known
}
