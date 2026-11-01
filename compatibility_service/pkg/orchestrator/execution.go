// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"compatibility_service/pkg/helper"
	"compatibility_service/pkg/lifecycle"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/gateway"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/network"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type helperExecutionResult struct {
	Endorsement sdk.Endorsement
	TxID        string
	Response    *peer.Response
	Binding     *LifecycleExecutionBinding
}

// endorsedInvocation contains simulation results only. Callers own submission,
// transaction tracking, and any lifecycle update after a confirmed commit.
type endorsedInvocation struct {
	Request        InvocationRequest
	Local          helperExecutionResult
	Remote         []helperExecutionResult
	Endorsement    sdk.Endorsement
	InitDefinition *lifecycle.ChaincodeDefinition
}

// endorseInvocation is shared by custom Invoke and Gateway Endorse. Each
// participating helper owns its execution view; no view survives this stage.
func (s *Service) endorseInvocation(ctx context.Context, req InvocationRequest) (result endorsedInvocation, err error) {
	namespace, args, err := s.prepareInvocation(req)
	if err != nil {
		return result, err
	}
	req, err = s.selectLifecycleDefinition(ctx, req)
	if err != nil {
		return result, err
	}
	result.Request = req
	result.InitDefinition, err = s.validateInitState(ctx, req, namespace, GRPCOperationEndorse)
	if err != nil {
		return result, status.Error(codes.FailedPrecondition, err.Error())
	}
	policy, err := s.resolveNamespacePolicy(ctx, namespace)
	if err != nil {
		return result, fmt.Errorf("resolve namespace policy: %w", err)
	}
	result.Local, err = s.executeFresh(ctx, req, namespace, policy.Version, args)
	if err != nil {
		return result, fmt.Errorf("helper endorsement failed: %w", err)
	}
	response := result.Local.Response
	if response.Status < 200 || response.Status >= 400 {
		return result, nil
	}
	result.Endorsement, result.Remote, err = s.collectNamespaceEndorsements(ctx, policy, req, result.Local)
	return result, err
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
	if req.definition != nil {
		clientProposal.Decorations["compat.chaincode_sequence"] = []byte(strconv.FormatInt(req.definition.Definition.Sequence, 10))
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
	requireMSP := len(req.EndorsingOrganizations) != 0
	verified, err := compiled.authenticate(local, localMSPID, requireMSP)
	if err != nil {
		return sdk.Endorsement{}, nil, fmt.Errorf("local endorsement: %w", err)
	}
	candidates := []verifiedEndorsement{verified}
	seenSigners := map[string]bool{verified.key: true}
	if end, remote, ok, err := compiled.assembleSelection(ctx, local, candidates, req.EndorsingOrganizations...); ok || err != nil {
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
	if len(req.EndorsingOrganizations) != 0 {
		targetMSPs = req.EndorsingOrganizations
	} else if compiled.unscoped {
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
			verified, err := compiled.authenticate(result, mspID, requireMSP)
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
			if end, selected, ok, err := compiled.assembleSelection(ctx, local, candidates, req.EndorsingOrganizations...); ok || err != nil {
				return end, selected, err
			}
		}
	}
	if len(req.EndorsingOrganizations) != 0 {
		failures = append([]error{fmt.Errorf("could not obtain endorsements from all requested organizations: %v", req.EndorsingOrganizations)}, failures...)
	} else {
		failures = append([]error{fmt.Errorf("namespace %s endorsement policy is not satisfied by the available distinct signing identities", policy.Namespace)}, failures...)
	}
	return sdk.Endorsement{}, nil, errors.Join(failures...)
}

func (p *namespaceEndorsementPolicy) assembleSelection(ctx context.Context, local helperExecutionResult, candidates []verifiedEndorsement, requestedMSPs ...string) (sdk.Endorsement, []helperExecutionResult, bool, error) {
	var indices []int
	var err error
	if len(requestedMSPs) == 0 {
		indices, err = p.selectEndorsements(ctx, candidates)
	} else {
		// As in Fabric Gateway, explicit organizations replace automatic policy
		// planning. The client is responsible for requesting a sufficient set;
		// Fabric-X still enforces the namespace policy at validation time.
		seen := make(map[string]bool)
		for _, mspID := range requestedMSPs {
			if seen[mspID] {
				continue
			}
			seen[mspID] = true
			found := false
			for index, candidate := range candidates {
				if candidate.mspID == mspID {
					indices = append(indices, index)
					found = true
					break
				}
			}
			if !found {
				return sdk.Endorsement{}, nil, false, nil
			}
		}
		err = ctx.Err()
	}
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
	if len(requestedMSPs) == 0 {
		if err := p.verifySelection(end, local.TxID); err != nil {
			return sdk.Endorsement{}, nil, false, fmt.Errorf("verify collected namespace endorsements: %w", err)
		}
	}
	return end, remote, true, nil
}

func compareCanonicalResults(local helperExecutionResult, remote []helperExecutionResult) error {
	if local.Response == nil {
		return errors.New("local helper response is nil")
	}
	for i, result := range remote {
		if (local.Binding == nil) != (result.Binding == nil) {
			return fmt.Errorf("remote helper result %d lifecycle definition mismatch", i)
		}
		if local.Binding != nil && (result.Binding == nil ||
			local.Binding.ChaincodeName != result.Binding.ChaincodeName ||
			local.Binding.ChaincodeVersion != result.Binding.ChaincodeVersion ||
			local.Binding.Sequence != result.Binding.Sequence) {
			return fmt.Errorf("remote helper result %d lifecycle definition mismatch", i)
		}
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
	out, err = s.submitTransaction(ctx, end, out, txID, record, afterCommit)
	if err != nil {
		return out, err
	}
	// Preserve the custom Invoke setting without delaying Gateway Submit.
	if s.cfg.WaitAfterSubmit > 0 && protocolOrDefault(s.cfg.Protocol) == "fabric-x" {
		timer := time.NewTimer(s.cfg.WaitAfterSubmit)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return s.idempotency.waitResult(record, nil)
		}
	}

	status, err := finality.Wait()
	if err != nil || !isFinalStatus(txID, status) {
		s.logger.Warnf("tx=%s finality wait ended without a confirmed outcome; transaction remains SUBMITTED: %v", txID, err)
		return s.idempotency.waitResult(record, nil)
	}
	_ = s.recordFinality(ctx, record, status)
	return s.idempotency.waitResult(record, nil)
}

// submitTransaction is the shared ordering stage. Custom Invoke surrounds it
// with a finality subscription; Gateway Submit returns after this stage alone.
func (s *Service) submitTransaction(ctx context.Context, end sdk.Endorsement, out InvocationResponse, txID string, record *idempotencyRecord, afterCommit func(context.Context) error) (InvocationResponse, error) {
	if record == nil || s.submitter == nil {
		return out, errors.New("submission requires a transaction record and ordering client")
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	s.logger.Infof("tx=%s submitting endorsed Fabric-X transaction", txID)
	if err := s.submitter.Submit(ctx, end); err != nil {
		var orderingError *orderingSubmissionError
		if errors.As(err, &orderingError) && orderingError.uncertain {
			// A send may have reached ordering even if its acknowledgment was lost.
			// Preserve it for lazy reconciliation and never automatically resend it.
			out.Submitted = true
			out.FinalityPending = true
			s.idempotency.markSubmission(record, out, end, afterCommit, err)
		}
		return out, fmt.Errorf("submit failed: %w", err)
	}
	out.Submitted = true
	out.FinalityPending = true
	s.idempotency.markSubmitted(record, out, end, afterCommit)
	s.logger.Infof("tx=%s submitted", txID)
	return out, nil
}

func (s *Service) validateSubmissionEndorsements(ctx context.Context, prepared gatewayPreparedInvocation) error {
	policy, err := s.resolveNamespacePolicy(ctx, prepared.Request.Namespace)
	if err != nil {
		return err
	}
	compiled, err := newNamespaceEndorsementPolicy(policy, s.proposalAuth.identities)
	if err != nil {
		return err
	}
	seen := make(map[string]bool)
	for _, response := range prepared.Endorsement.Responses {
		identity := &msppb.Identity{}
		if err := proto.Unmarshal(response.Endorsement.Endorser, identity); err != nil {
			return err
		}
		verified, err := compiled.authenticate(helperExecutionResult{
			TxID: prepared.Request.ClientTxID, Response: response.Response,
			Endorsement: sdk.Endorsement{Proposal: prepared.Endorsement.Proposal, Responses: []*peer.ProposalResponse{response}},
		}, identity.MspId, false)
		if err != nil {
			return err
		}
		if seen[verified.key] {
			return errors.New("a signing identity cannot be counted more than once")
		}
		seen[verified.key] = true
	}
	return compiled.verifySelection(prepared.Endorsement, prepared.Request.ClientTxID)
}

func (s *Service) replayOrderingSubmission(ctx context.Context, record *idempotencyRecord) error {
	if _, err := s.idempotency.wait(ctx, record); err != nil {
		if submitted, _ := s.idempotency.submissionOutcome(record); !submitted {
			return err
		}
	}
	submitted, err := s.idempotency.submissionOutcome(record)
	if err != nil {
		return err
	}
	if !submitted {
		return errors.New("transaction has not been acknowledged by ordering")
	}
	return nil
}

type orderingSubmissionError struct {
	err       error
	uncertain bool
}

func (e *orderingSubmissionError) Error() string { return e.err.Error() }
func (e *orderingSubmissionError) Unwrap() error { return e.err }

// fabricXSubmitter reuses an SDK connection and native packager, with a
// request-scoped broadcast stream that checks the orderer's response. The SDK's
// fire-and-forget Broadcast cannot establish the acknowledgment Submit needs.
type fabricXSubmitter struct {
	peer     *network.Peer
	packager network.TxPackager
}

func newFabricXSubmitter(config network.PeerConf, packager network.TxPackager) (*fabricXSubmitter, error) {
	peer, err := network.NewPeer(config)
	if err != nil {
		return nil, err
	}
	return &fabricXSubmitter{peer: peer, packager: packager}, nil
}

func (s *fabricXSubmitter) Submit(ctx context.Context, end sdk.Endorsement) error {
	envelope, err := s.packager.PackageTx(end)
	if err != nil {
		return fmt.Errorf("package transaction: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := orderer.NewAtomicBroadcastClient(s.peer.Connection()).Broadcast(ctx)
	if err != nil {
		return &orderingSubmissionError{err: err}
	}
	defer stream.CloseSend() //nolint:errcheck
	if err := stream.Send(envelope); err != nil {
		return &orderingSubmissionError{err: err, uncertain: true}
	}
	response, err := stream.Recv()
	if err != nil {
		return &orderingSubmissionError{err: err, uncertain: true}
	}
	if response.Status != common.Status_SUCCESS {
		return &orderingSubmissionError{err: fmt.Errorf("orderer rejected transaction: %s: %s", response.Status, response.Info)}
	}
	return nil
}

func (s *fabricXSubmitter) Close() error { return s.peer.Close() }

func (s *Service) replaySubmission(ctx context.Context, record *idempotencyRecord) (InvocationResponse, error) {
	out, err := s.idempotency.wait(ctx, record)
	if err != nil || !out.FinalityPending {
		return out, err
	}
	// This response belongs only to the explicitly retried transaction. Share
	// its in-flight wait. Terminal results exist only for callers that already
	// hold this pointer; the global map keeps no historical response.
	waitCtx, cancel := context.WithTimeout(ctx, s.cfg.finalityTimeout())
	defer cancel()
	if err := record.lockFinality(waitCtx); err != nil {
		return s.idempotency.waitResult(record, err)
	}
	defer record.unlockFinality()
	return s.idempotency.waitResult(record, nil)
}

const recoveryBatchSize = 32

func (s *Service) stopReconciliation() {
	s.recoveryMu.Lock()
	s.recoveryClosed = true
	if s.recoveryCancel != nil {
		s.recoveryCancel()
	}
	s.recoveryMu.Unlock()
	s.recoveryWait.Wait()
}

// triggerReconciliation never waits for old work or uses the client's deadline.
// Repeated submissions coalesce into one pass, with no queued or periodic runs.
func (s *Service) triggerReconciliation() {
	cutoff := time.Now()
	s.recoveryMu.Lock()
	if s.recoveryClosed || s.recoveryRunning {
		s.recoveryMu.Unlock()
		return
	}
	if s.recoveryCtx == nil {
		s.recoveryCtx, s.recoveryCancel = context.WithCancel(context.Background())
	}
	ctx := s.recoveryCtx
	s.recoveryRunning = true
	s.recoveryWait.Add(1)
	s.recoveryMu.Unlock()
	go func() {
		defer s.recoveryWait.Done()
		defer func() {
			s.recoveryMu.Lock()
			s.recoveryRunning = false
			s.recoveryMu.Unlock()
		}()
		passCtx, cancel := context.WithTimeout(ctx, min(5*time.Second, s.cfg.finalityTimeout()))
		defer cancel()
		s.reconcilePendingBefore(passCtx, cutoff)
		s.reconcileLifecyclePending(passCtx)
	}()
}

// reconcilePending updates only the internal store in a bounded lazy pass.
// Its results are never included in the new invocation's client response.
// An active invoke owns its finality wait; skip it rather than delaying or
// duplicating that work. Missing statuses and query failures leave records alone.
func (s *Service) reconcilePending(ctx context.Context) {
	s.reconcilePendingBefore(ctx, time.Now())
}

func (s *Service) reconcilePendingBefore(ctx context.Context, cutoff time.Time) {
	if s.idempotency == nil || s.queryService == nil || ctx.Err() != nil {
		return
	}
	pending := make(map[string]*idempotencyRecord)
	var txIDs []string
	for _, record := range s.idempotency.pendingRecordsBefore(recoveryBatchSize, cutoff) {
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
	queryCtx, cancel := context.WithTimeout(ctx, min(time.Second, s.cfg.finalityTimeout()))
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
		out.ChaincodeEvent = pending.Event
		if pending.AfterCommit != nil {
			// The callback first registers small pending lifecycle work, even if
			// this request just expired. The finished application record can go.
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

// queryTransactionStatus only accepts a known final outcome for the requested
// ID. A missing row is unknown/pending, not a rejection or proof of non-submission.
func (s *Service) queryTransactionStatus(ctx context.Context, txID string) (*committerpb.TxStatus, error) {
	if s.queryService == nil {
		return nil, errors.New("transaction status query service is not configured")
	}
	queryCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	response, err := s.queryService.GetTransactionStatus(queryCtx, &committerpb.TxStatusQuery{TxIds: []string{txID}})
	if err != nil {
		return nil, fmt.Errorf("query transaction status: %w", err)
	}
	for _, result := range response.GetStatuses() {
		if isFinalStatus(txID, result) {
			return result, nil
		}
	}
	return nil, nil
}

// waitForTransactionStatus serves an explicit status request, including IDs not
// present in this instance's store. It owns all observation until ctx ends.
// Query -> subscribe -> query handles the usual registration race. The notifier
// has no registration acknowledgment or replay, so request-scoped status checks
// also recover events lost during registration, stream failure, or timeout.
func (s *Service) waitForTransactionStatus(ctx context.Context, txID string) (*committerpb.TxStatus, error) {
	result, err := s.queryTransactionStatus(ctx, txID)
	if err != nil || result != nil {
		return result, err
	}
	finality, err := s.subscribeFinality(ctx, txID)
	if err != nil {
		s.logger.Debugf("tx=%s notification subscription unavailable during status request: %v", txID, err)
	}
	defer finality.Cancel()
	var notifications <-chan finalityResult
	if finality != nil {
		notifications = finality.result
	}

	// These lookups exist only for this explicit CommitStatus request. No worker
	// survives its completion, and custom Invoke retains its lazy-store behavior.
	recheck := time.NewTicker(time.Second)
	defer recheck.Stop()
	var lastCheck <-chan time.Time
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		// Leave a live request context for a final lookup even when a short
		// client deadline expires before the first periodic fallback check.
		timer := time.NewTimer(max(0, remaining-min(time.Second, remaining/10)))
		defer timer.Stop()
		lastCheck = timer.C
	}
	for {
		result, err = s.queryTransactionStatus(ctx, txID)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == nil && result != nil {
			return result, nil
		}
		if err != nil {
			s.logger.Debugf("tx=%s transaction status recheck unavailable: %v", txID, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-recheck.C:
		case <-lastCheck:
			lastCheck = nil
		case notification, ok := <-notifications:
			// waitForNotification emits at most one outcome. Disable a failed or
			// closed stream, then query again rather than spinning or resubscribing.
			notifications = nil
			if ok && isFinalStatus(txID, notification.status) {
				return notification.status, nil
			}
			finality.Cancel()
		}
	}
}

// refreshObservedFinality updates only matching local records. An active Invoke
// owns its record until it returns; status requests need not wait for that owner.
func (s *Service) refreshObservedFinality(ctx context.Context, result *committerpb.TxStatus) {
	if s.idempotency == nil {
		return
	}
	for _, record := range s.idempotency.pendingRecords(0) {
		pending, ok := s.idempotency.pending(record)
		if !ok || pending.Response.TxID != result.GetRef().GetTxId() || !record.tryLockFinality() {
			continue
		}
		err := s.recordFinality(ctx, record, result)
		record.unlockFinality()
		if err != nil {
			s.logger.Warnf("tx=%s finality confirmed but local finalization failed: %v", result.Ref.TxId, err)
		}
	}
}

// streamCommittedBlocks reuses the sidecar's durable block cursor for replay
// and live delivery. Its gRPC context, cursor, and backpressure belong to the
// requesting subscriber; no worker or polling loop survives that request.
func (s *Service) streamCommittedBlocks(ctx context.Context, start uint64, visit func(*common.Block) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if s.signer == nil {
		return status.Error(codes.FailedPrecondition, "block delivery signer is not configured")
	}
	seek := &orderer.SeekInfo{
		Start:    &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: start}}},
		Stop:     &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: math.MaxUint64}}},
		Behavior: orderer.SeekInfo_BLOCK_UNTIL_READY,
	}
	envelope, err := protoutil.CreateSignedEnvelope(common.HeaderType_DELIVER_SEEK_INFO, s.cfg.ChannelID, s.signer, seek, 0, 0)
	if err != nil {
		return status.Errorf(codes.Internal, "sign block delivery request: %v", err)
	}
	delivery, err := s.blockDelivery.Deliver(ctx)
	if err != nil {
		return fmt.Errorf("open committed block delivery: %w", err)
	}
	defer delivery.CloseSend() //nolint:errcheck
	if err := delivery.Send(envelope); err != nil {
		return fmt.Errorf("send block seek: %w", err)
	}
	if err := delivery.CloseSend(); err != nil {
		return fmt.Errorf("close block seek send: %w", err)
	}
	expected := start
	for {
		response, err := delivery.Recv()
		if err != nil {
			return fmt.Errorf("receive committed block: %w", err)
		}
		block := response.GetBlock()
		if block == nil {
			return fmt.Errorf("committed block delivery ended with status %s", response.GetStatus())
		}
		if block.GetHeader() == nil || block.Header.Number != expected {
			return status.Errorf(codes.DataLoss, "committed block delivery expected block %d", expected)
		}
		if err := visit(block); err != nil {
			return err
		}
		if expected == math.MaxUint64 {
			return nil
		}
		expected++
	}
}

// committedChaincodeEvents reads only native COMMITTED entries, using the
// shared event decoder. Records without a signed logical chaincode identity
// predate event subscriptions and cannot be safely attributed by namespace.
func committedChaincodeEvents(block *common.Block, channel string) ([]*peer.ChaincodeEvent, error) {
	if block.GetHeader() == nil {
		return nil, errors.New("committed block has no header")
	}
	transactions := block.GetData().GetData()
	if len(transactions) == 0 {
		return nil, nil
	}
	index := int(common.BlockMetadataIndex_TRANSACTIONS_FILTER)
	if len(block.GetMetadata().GetMetadata()) <= index || len(block.Metadata.Metadata[index]) != len(transactions) {
		return nil, errors.New("committed block has missing or inconsistent transaction validation codes")
	}
	filter := block.Metadata.Metadata[index]
	var events []*peer.ChaincodeEvent
	for i, raw := range transactions {
		// Fabric-X COMMITTED is 1, unlike Fabric's VALID enum value of 0.
		if filter[i] != byte(committerpb.Status_COMMITTED) {
			continue
		}
		envelope := &common.Envelope{}
		if err := proto.Unmarshal(raw, envelope); err != nil {
			return nil, fmt.Errorf("decode committed envelope: %w", err)
		}
		payload, err := protoutil.UnmarshalPayload(envelope.Payload)
		if err != nil || payload.Header == nil {
			return nil, errors.New("committed transaction has invalid payload/header")
		}
		header, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			return nil, fmt.Errorf("decode committed channel header: %w", err)
		}
		if header.ChannelId != channel || header.Type != int32(common.HeaderType_MESSAGE) {
			continue
		}
		tx := &applicationpb.Tx{}
		if err := proto.Unmarshal(payload.Data, tx); err != nil {
			return nil, fmt.Errorf("decode native transaction: %w", err)
		}
		identity, err := helper.ChaincodeIdentityFromMetadata(tx.Metadata)
		if err != nil {
			return nil, err
		}
		if identity == nil {
			continue
		}
		if identity.ChannelID != channel {
			return nil, errors.New("signed event channel does not match committed envelope")
		}
		if header.TxId == "" {
			return nil, errors.New("committed event transaction has no ID")
		}
		event, err := chaincodeEventFromTransaction(tx, header.TxId)
		if err != nil {
			return nil, err
		}
		if event != nil {
			events = append(events, event)
		}
	}
	return events, nil
}

// evaluate is shared by custom Query and Gateway Evaluate. It returns the raw
// chaincode response; API adapters decide how to serialize that result. It never
// touches submission, transaction-store reconciliation or finality observation.
func (s *Service) evaluate(ctx context.Context, req InvocationRequest) (helperExecutionResult, error) {
	if err := ctx.Err(); err != nil {
		return helperExecutionResult{}, err
	}
	namespace, args, err := s.prepareInvocation(req)
	if err != nil {
		return helperExecutionResult{}, err
	}
	if req.IsInit {
		return helperExecutionResult{}, status.Error(codes.InvalidArgument, "is-init is only valid for invoke")
	}
	req, err = s.selectLifecycleDefinition(ctx, req)
	if err != nil {
		return helperExecutionResult{}, err
	}
	if _, err := s.validateInitState(ctx, req, namespace, GRPCOperationQuery); err != nil {
		return helperExecutionResult{}, status.Error(codes.FailedPrecondition, err.Error())
	}
	policy, err := s.resolveNamespacePolicy(ctx, namespace)
	if err != nil {
		return helperExecutionResult{}, fmt.Errorf("resolve namespace policy: %w", err)
	}
	result, err := s.executeFresh(ctx, req, namespace, policy.Version, args)
	if err != nil {
		return helperExecutionResult{}, fmt.Errorf("helper query failed: %w", err)
	}
	if req.ClientTxID != "" && result.TxID != req.ClientTxID {
		return helperExecutionResult{}, status.Error(codes.Internal, "query transaction ID does not match the client proposal")
	}
	s.logger.Infof("tx=%s query completed namespace=%s status=%d", result.TxID, namespace, result.Response.Status)
	return result, nil
}

func (s *Service) evaluateRemote(ctx context.Context, request *gateway.EvaluateRequest, req InvocationRequest) (*gateway.EvaluateResponse, error) {
	if s.remotes == nil {
		return nil, status.Error(codes.Unavailable, "no target Orchestrator is configured")
	}
	visited := map[string]bool{}
	var details []*gateway.ErrorDetail
	for _, target := range request.TargetOrganizations {
		if visited[target] {
			continue
		}
		visited[target] = true
		contacts, found, err := s.remotes.Resolve(ctx, remoteOrchestratorResolveRequest{
			RequesterMSPID: s.cfg.Identity.MspID, TargetMSPID: target, ChannelID: s.cfg.ChannelID,
			Namespace: req.Namespace, ChaincodeName: req.ChaincodeName,
			ChaincodeVersion: req.definition.Definition.Version, Sequence: req.definition.Definition.Sequence,
		})
		if err != nil {
			details = append(details, &gateway.ErrorDetail{MspId: target, Message: err.Error()})
			continue
		}
		if !found || len(contacts) == 0 {
			details = append(details, &gateway.ErrorDetail{MspId: target, Message: "no Orchestrator endpoint found"})
			continue
		}
		for _, contact := range contacts {
			if contact == nil || contact.peer == nil {
				continue
			}
			if err := ctx.Err(); err != nil {
				return nil, status.FromContextError(err).Err()
			}
			forwarded := &gateway.EvaluateRequest{
				TransactionId: request.TransactionId, ChannelId: request.ChannelId,
				ProposedTransaction: cloneSignedProposal(request.ProposedTransaction), TargetOrganizations: []string{target},
			}
			forwardCtx := metadata.AppendToOutgoingContext(ctx, gatewayEvaluateLocalOnly, "true")
			result, err := gateway.NewGatewayClient(contact.peer.Connection()).Evaluate(forwardCtx, forwarded, grpc.WaitForReady(false))
			if err == nil {
				if result == nil {
					return nil, status.Error(codes.Internal, "remote evaluation returned no result")
				}
				return gatewayEvaluationResponse(result.Result, target, contact.Address())
			}
			if ctx.Err() != nil {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			switch status.Code(err) {
			case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.Unimplemented:
				details = append(details, &gateway.ErrorDetail{MspId: target, Address: contact.Address(), Message: err.Error()})
			default:
				// Signature, authorization, lifecycle and chaincode errors are not
				// reasons to silently run the client's proposal somewhere else.
				return nil, err
			}
		}
	}
	errStatus := status.New(codes.Unavailable, "no requested organization could evaluate the transaction")
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	for _, detail := range details {
		if next, err := errStatus.WithDetails(detail); err == nil {
			errStatus = next
		}
	}
	return nil, errStatus.Err()
}
