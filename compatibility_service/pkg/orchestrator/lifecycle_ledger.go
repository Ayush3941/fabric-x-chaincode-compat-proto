// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"compatibility_service/pkg/lifecycle"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	efab "github.com/hyperledger/fabric-x-sdk/endorsement/fabric"
	efabx "github.com/hyperledger/fabric-x-sdk/endorsement/fabricx"
	nfab "github.com/hyperledger/fabric-x-sdk/network/fabric"
)

const lifecycleCommitFunction = "__lifecycle_commit"

var (
	errLifecyclePending    = errors.New("lifecycle transaction finality is pending; it will not be resubmitted")
	errLifecycleSuperseded = errors.New("lifecycle operation was superseded by a newer committed revision")
)

type lifecycleOperation struct {
	Name     string
	Sequence int64
	Event    string
}

// A pending lifecycle operation contains no proposal, endorsement, application
// result, or completed-transaction history. An empty TxID means an Init marker
// still needs preparing; otherwise an unknown outcome must be checked first.
type pendingLifecycle struct {
	operation   lifecycleOperation
	definition  lifecycle.ChaincodeDefinition
	result      lifecycle.LedgerCommitResult
	propagation lifecycle.DefinitionPropagation
	gate        chan struct{}
}

func (s *Service) lifecycleServer() *lifecycle.Server {
	mspID := ""
	if s.cfg.Identity != nil {
		mspID = s.cfg.Identity.MspID
	}
	return lifecycle.NewServerWithRemoteProvider(s.lifecycle,
		lifecycle.ServerConfig{ChannelID: s.cfg.ChannelID, MSPID: mspID, Auth: s.lifecycleAuth, Signer: s.signer}, s.logger, s, s)
}

func (s *Service) lifecycleOperation(ctx context.Context, def lifecycle.ChaincodeDefinition, initialized bool) (*pendingLifecycle, error) {
	event := lifecycle.LifecycleEventCommit
	if initialized {
		event = lifecycle.LifecycleEventInitialized
	}
	key := lifecycleOperation{Name: def.Name, Sequence: def.Sequence, Event: event}
	s.recoveryMu.Lock()
	if s.recoveryClosed {
		s.recoveryMu.Unlock()
		return nil, errors.New("orchestrator is closed")
	}
	if s.lifecyclePending == nil {
		s.lifecyclePending = make(map[lifecycleOperation]*pendingLifecycle)
	}
	if pending := s.lifecyclePending[key]; pending != nil {
		s.recoveryMu.Unlock()
		if pending.definition != def {
			return nil, fmt.Errorf("conflicting pending lifecycle definition for %s sequence %d", def.Name, def.Sequence)
		}
		if err := pending.lock(ctx); err != nil {
			return nil, err
		}
		return pending, nil
	}
	if err := ctx.Err(); err != nil && !initialized {
		s.recoveryMu.Unlock()
		return nil, err // A definition canceled before any send leaves no work.
	}
	pending := &pendingLifecycle{operation: key, definition: def, gate: make(chan struct{}, 1)}
	s.lifecyclePending[key] = pending
	if err := ctx.Err(); err != nil {
		s.recoveryMu.Unlock()
		return nil, err // A committed Init still needs its marker on a later pass.
	}
	// Reserve the operation before exposing it to a lazy pass or another caller.
	pending.gate <- struct{}{}
	s.recoveryMu.Unlock()
	return pending, nil
}

func (s *Service) removeLifecycleOperation(pending *pendingLifecycle) {
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	if s.lifecyclePending[pending.operation] == pending {
		delete(s.lifecyclePending, pending.operation)
	}
}

func (p *pendingLifecycle) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case p.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *pendingLifecycle) unlock() { <-p.gate }

func (s *Service) submitLifecycleDefinition(ctx context.Context, def lifecycle.ChaincodeDefinition, eventType string) (lifecycle.LedgerCommitResult, error) {
	// Creation happens even with an expired Init caller: that caller may already
	// have committed application state and must leave its missing marker pending.
	pending, err := s.lifecycleOperation(ctx, def, eventType == lifecycle.LifecycleEventInitialized)
	if err != nil {
		return lifecycle.LedgerCommitResult{}, err
	}
	defer pending.unlock()
	result, err := s.advanceLifecycleTransaction(ctx, pending, true)
	if err != nil && result.TxID == "" && eventType == lifecycle.LifecycleEventCommit {
		s.removeLifecycleOperation(pending) // A definition that was never sent can be retried normally.
	}
	return result, err
}

func (s *Service) advanceLifecycleTransaction(ctx context.Context, pending *pendingLifecycle, wait bool) (lifecycle.LedgerCommitResult, error) {
	if pending.result.Status == committerpb.Status_COMMITTED.String() {
		return pending.result, nil
	}
	if pending.result.TxID != "" {
		status, err := s.queryTransactionStatus(ctx, pending.result.TxID)
		if err != nil {
			return pending.result, err
		}
		if status == nil {
			return pending.result, errLifecyclePending
		}
		return s.observeLifecycleFinality(pending, status)
	}
	end, txID, err := s.prepareLifecycleTransaction(ctx, pending.definition, pending.operation.Event)
	if err != nil {
		if errors.Is(err, errLifecycleSuperseded) {
			s.removeLifecycleOperation(pending)
		}
		return pending.result, err
	}
	pending.result = lifecycle.LedgerCommitResult{TxID: txID, Namespace: s.cfg.lifecycleNamespace(), Key: lifecycle.LedgerKey(s.cfg.ChannelID, pending.definition.Name)}
	if txID == "" {
		pending.result.Status = committerpb.Status_COMMITTED.String()
		return pending.result, nil
	}
	var finality *finalitySubscription
	if wait {
		finality, err = s.subscribeFinality(ctx, txID)
		if err != nil {
			pending.result.TxID = "" // No send happened.
			return pending.result, fmt.Errorf("subscribe lifecycle finality: %w", err)
		}
		defer finality.Cancel()
	}
	// Save the TxID before attempting delivery. Lost ordering acknowledgments and
	// notification timeouts retain only this receipt and can never trigger a resend.
	if err := s.submitter.Submit(ctx, end); err != nil {
		var delivery *orderingSubmissionError
		if !errors.As(err, &delivery) || !delivery.uncertain {
			pending.result.TxID = ""
		}
		return pending.result, fmt.Errorf("submit lifecycle transaction: %w", err)
	}
	if !wait || finality == nil {
		return pending.result, errLifecyclePending
	}
	status, err := finality.Wait()
	if err != nil || !isFinalStatus(txID, status) {
		return pending.result, fmt.Errorf("wait lifecycle finality: %w", errors.Join(errLifecyclePending, err))
	}
	return s.observeLifecycleFinality(pending, status)
}

func (s *Service) observeLifecycleFinality(pending *pendingLifecycle, status *committerpb.TxStatus) (lifecycle.LedgerCommitResult, error) {
	result := pending.result
	result.Status, result.BlockNum, result.TxNum = status.Status.String(), status.Ref.GetBlockNum(), status.Ref.GetTxNum()
	if status.Status != committerpb.Status_COMMITTED {
		if pending.operation.Event == lifecycle.LifecycleEventInitialized {
			// Only a known rejection allows a new marker attempt on a future pass.
			pending.result = lifecycle.LedgerCommitResult{}
		} else {
			s.removeLifecycleOperation(pending)
		}
		return result, fmt.Errorf("lifecycle transaction status %s", result.Status)
	}
	pending.result = result
	return result, nil
}

// FinalizeLifecycleDefinition is shared by the lifecycle RPC, Init completion,
// and lazy recovery. It never reruns Init or resends an uncertain transaction.
func (s *Service) FinalizeLifecycleDefinition(ctx context.Context, def lifecycle.ChaincodeDefinition, initialized bool) (lifecycle.CommittedDefinition, error) {
	pending, err := s.lifecycleOperation(ctx, def, initialized)
	if err != nil {
		return lifecycle.CommittedDefinition{}, err
	}
	defer pending.unlock()
	return s.finalizeLifecycleOperation(ctx, pending)
}

func (s *Service) finalizeLifecycleOperation(ctx context.Context, pending *pendingLifecycle) (lifecycle.CommittedDefinition, error) {
	ledger, exists, err := s.ReadLifecycleDefinition(ctx, pending.definition)
	if err != nil {
		return lifecycle.CommittedDefinition{}, err
	}
	if exists && ledger.Sequence > pending.definition.Sequence {
		// A newer committed revision supersedes this operation. Never apply an old
		// Init result to the new revision or overwrite an organization's new mirror.
		s.removeLifecycleOperation(pending)
		return lifecycle.CommittedDefinition{}, nil
	}
	committed, err := s.lifecycleServer().ReconcileDefinition(ctx, pending.definition,
		pending.operation.Event == lifecycle.LifecycleEventInitialized, &pending.propagation)
	if err == nil {
		s.removeLifecycleOperation(pending)
	}
	return committed, err
}

func (s *Service) reconcileLifecyclePending(ctx context.Context) {
	s.recoveryMu.Lock()
	batch := make([]*pendingLifecycle, 0, min(recoveryBatchSize, len(s.lifecyclePending)))
	for _, pending := range s.lifecyclePending {
		batch = append(batch, pending)
		if len(batch) == recoveryBatchSize {
			break
		}
	}
	s.recoveryMu.Unlock()
	for _, pending := range batch {
		if ctx.Err() != nil {
			return
		}
		select {
		case pending.gate <- struct{}{}:
		default:
			continue
		}
		s.recoveryMu.Lock()
		current := s.lifecyclePending[pending.operation] == pending
		s.recoveryMu.Unlock()
		if !current {
			pending.unlock()
			continue
		}
		if pending.operation.Event == lifecycle.LifecycleEventCommit && pending.result.TxID == "" && pending.result.Status == "" {
			s.removeLifecycleOperation(pending) // Never submit a definition abandoned before delivery.
			pending.unlock()
			continue
		}
		_, err := s.advanceLifecycleTransaction(ctx, pending, false)
		if err == nil {
			_, err = s.finalizeLifecycleOperation(ctx, pending)
		}
		pending.unlock()
		if err != nil && !errors.Is(err, errLifecyclePending) {
			loggerOrNoop(s.logger).Debugf("lifecycle recovery name=%s sequence=%d pending: %v", pending.definition.Name, pending.definition.Sequence, err)
		}
	}
}

func (cfg Config) lifecycleNamespace() string {
	if cfg.LifecycleNamespace != "" {
		return cfg.LifecycleNamespace
	}
	return lifecycle.DefaultLedgerNamespace
}

// CommitLifecycleDefinition submits the shared lifecycle definition as a real
// Fabric-X transaction before local orchestrator stores are updated.
func (s *Service) CommitLifecycleDefinition(ctx context.Context, def lifecycle.ChaincodeDefinition) (lifecycle.LedgerCommitResult, error) {
	s.triggerReconciliation()
	return s.submitLifecycleDefinition(ctx, def, lifecycle.LifecycleEventCommit)
}

// MarkLifecycleInitialized submits the initialized lifecycle marker as a real
// Fabric-X transaction after the init invoke has committed.
func (s *Service) MarkLifecycleInitialized(ctx context.Context, def lifecycle.ChaincodeDefinition) (lifecycle.LedgerCommitResult, error) {
	if !def.InitRequired {
		return lifecycle.LedgerCommitResult{}, fmt.Errorf("definition %s does not require init", def.Name)
	}
	return s.submitLifecycleDefinition(ctx, def, lifecycle.LifecycleEventInitialized)
}

// ReadLifecycleDefinition verifies local lifecycle updates against committed state.
func (s *Service) ReadLifecycleDefinition(ctx context.Context, def lifecycle.ChaincodeDefinition) (lifecycle.LedgerDefinition, bool, error) {
	value, _, ok, err := s.lifecycleRow(ctx, s.cfg.lifecycleNamespace(), lifecycle.LedgerKey(s.cfg.ChannelID, def.Name))
	if err != nil || !ok {
		return lifecycle.LedgerDefinition{}, ok, err
	}
	record, err := lifecycle.UnmarshalLedgerDefinition(value)
	if err == nil && record.ChannelID != s.cfg.ChannelID {
		err = fmt.Errorf("lifecycle record belongs to channel %s", record.ChannelID)
	}
	return record, true, err
}

func (s *Service) prepareLifecycleTransaction(ctx context.Context, def lifecycle.ChaincodeDefinition, eventType string) (sdk.Endorsement, string, error) {
	namespace := s.cfg.lifecycleNamespace()
	key := lifecycle.LedgerKey(s.cfg.ChannelID, def.Name)
	currentValue, defVersion, exists, err := s.lifecycleRow(ctx, namespace, key)
	if err != nil {
		return sdk.Endorsement{}, "", fmt.Errorf("read lifecycle key version: %w", err)
	}
	var current *lifecycle.ChaincodeDefinition
	if exists {
		record, err := lifecycle.UnmarshalLedgerDefinition(currentValue)
		if err != nil {
			return sdk.Endorsement{}, "", err
		}
		if record.ChannelID != s.cfg.ChannelID || record.Name != def.Name {
			return sdk.Endorsement{}, "", fmt.Errorf("lifecycle definition identity mismatch")
		}
		active := lifecycle.ChaincodeDefinitionFromLedger(record)
		current = &active
		if active.Sequence > def.Sequence {
			return sdk.Endorsement{}, "", fmt.Errorf("%w: next sequence follows active sequence %d", errLifecycleSuperseded, active.Sequence)
		}
		if active == def && (eventType == lifecycle.LifecycleEventCommit || record.Initialized) {
			return sdk.Endorsement{}, "", nil // Ledger proof: no second transaction is needed.
		}
	}
	initialized := !def.InitRequired
	if eventType == lifecycle.LifecycleEventInitialized {
		if current == nil || *current != def {
			return sdk.Endorsement{}, "", fmt.Errorf("initialization marker does not match the active definition")
		}
		initialized = true
	} else if err := lifecycle.ValidateNextSequence(current, def); err != nil {
		return sdk.Endorsement{}, "", err
	}
	value, err := lifecycle.MarshalLedgerDefinition(s.cfg.ChannelID, def, initialized)
	if err != nil {
		return sdk.Endorsement{}, "", err
	}
	highKey := lifecycle.LifecycleHighWatermarkKey(s.cfg.ChannelID)
	highValue, highVersion, ok, err := s.lifecycleRow(ctx, namespace, highKey)
	if err != nil {
		return sdk.Endorsement{}, "", fmt.Errorf("read lifecycle high watermark: %w", err)
	}
	highWatermark := uint64(0)
	if ok {
		highWatermark, err = lifecycle.UnmarshalLifecycleHighWatermark(highValue)
		if err != nil {
			return sdk.Endorsement{}, "", fmt.Errorf("decode lifecycle high watermark: %w", err)
		}
	}
	nextIndex := highWatermark + 1
	indexKey := lifecycle.LifecycleIndexEntryKey(s.cfg.ChannelID, nextIndex)
	indexVersion, err := s.lifecycleKeyVersion(ctx, namespace, indexKey)
	if err != nil {
		return sdk.Endorsement{}, "", fmt.Errorf("read lifecycle index key version: %w", err)
	}
	if indexVersion != nil {
		return sdk.Endorsement{}, "", fmt.Errorf("lifecycle index key %s already exists", indexKey)
	}
	indexValue, err := lifecycle.MarshalLifecycleIndexEntry(lifecycle.NewLifecycleIndexEntry(s.cfg.ChannelID, nextIndex, eventType, def, initialized))
	if err != nil {
		return sdk.Endorsement{}, "", fmt.Errorf("marshal lifecycle index entry: %w", err)
	}

	args := [][]byte{
		[]byte(lifecycleCommitFunction),
		[]byte(def.Name),
		[]byte(def.Version),
		[]byte(strconv.FormatInt(def.Sequence, 10)),
	}
	prop, err := nfab.NewSignedProposal(s.signer, s.cfg.ChannelID, namespace, args)
	if err != nil {
		return sdk.Endorsement{}, "", fmt.Errorf("create lifecycle proposal: %w", err)
	}
	inv, err := efab.Parse(prop, time.Now())
	if err != nil {
		return sdk.Endorsement{}, "", fmt.Errorf("parse lifecycle proposal: %w", err)
	}

	rws := blocks.ReadWriteSet{
		Reads: []blocks.KVRead{
			{
				Key:     key,
				Version: defVersion,
			},
			{
				Key:     highKey,
				Version: highVersion,
			},
			{
				Key:     indexKey,
				Version: indexVersion,
			},
		},
		Writes: []blocks.KVWrite{
			{
				Key:   key,
				Value: value,
			},
			{
				Key:   indexKey,
				Value: indexValue,
			},
			{
				Key:   highKey,
				Value: lifecycle.MarshalLifecycleHighWatermark(nextIndex),
			},
		},
	}
	resp, err := efabx.NewEndorsementBuilder(s.signer).Endorse(inv, endorsement.Success(rws, "", nil, value))
	if err != nil {
		return sdk.Endorsement{}, "", fmt.Errorf("endorse lifecycle transaction: %w", err)
	}
	end := sdk.Endorsement{
		Proposal:  inv.Proposal,
		Responses: []*peer.ProposalResponse{resp},
	}

	s.logger.Infof("tx=%s preparing lifecycle definition namespace=%s key=%s index=%d name=%s version=%s sequence=%d initialized=%t",
		inv.TxID, namespace, key, nextIndex, def.Name, def.Version, def.Sequence, initialized)
	return end, inv.TxID, nil
}

func (s *Service) lifecycleKeyVersion(ctx context.Context, namespace, key string) (*blocks.Version, error) {
	_, version, ok, err := s.lifecycleRow(ctx, namespace, key)
	if err != nil || !ok {
		return nil, err
	}
	return version, nil
}

func (s *Service) lifecycleRow(ctx context.Context, namespace, key string) ([]byte, *blocks.Version, bool, error) {
	rows, err := s.queryService.GetRows(ctx, &committerpb.Query{
		Namespaces: []*committerpb.QueryNamespace{{
			NsId: namespace,
			Keys: [][]byte{[]byte(key)},
		}},
	})
	if err != nil {
		return nil, nil, false, err
	}
	for _, ns := range rows.Namespaces {
		if ns.GetNsId() != namespace {
			continue
		}
		for _, row := range ns.Rows {
			if string(row.GetKey()) == key {
				return append([]byte(nil), row.GetValue()...), &blocks.Version{BlockNum: row.GetVersion()}, true, nil
			}
		}
	}
	return nil, nil, false, nil
}
