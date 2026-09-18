// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"compatibility_service/pkg/lifecycle"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	efabx "github.com/hyperledger/fabric-x-sdk/endorsement/fabricx"
	"github.com/hyperledger/fabric-x-sdk/network"
)

const lifecycleCommitFunction = "__lifecycle_commit"

func (cfg Config) lifecycleNamespace() string {
	if cfg.LifecycleNamespace != "" {
		return cfg.LifecycleNamespace
	}
	return lifecycle.DefaultLedgerNamespace
}

// CommitLifecycleDefinition submits the shared lifecycle definition as a real
// Fabric-X transaction before local orchestrator stores are updated.
func (s *Service) CommitLifecycleDefinition(ctx context.Context, def lifecycle.ChaincodeDefinition) (lifecycle.LedgerCommitResult, error) {
	return s.submitLifecycleDefinition(ctx, def, !def.InitRequired)
}

// MarkLifecycleInitialized submits the initialized lifecycle marker as a real
// Fabric-X transaction after the init invoke has committed.
func (s *Service) MarkLifecycleInitialized(ctx context.Context, def lifecycle.ChaincodeDefinition) (lifecycle.LedgerCommitResult, error) {
	if !def.InitRequired {
		return lifecycle.LedgerCommitResult{}, fmt.Errorf("definition %s does not require init", lifecycle.DefinitionCCID(def))
	}
	return s.submitLifecycleDefinition(ctx, def, true)
}

func (s *Service) submitLifecycleDefinition(ctx context.Context, def lifecycle.ChaincodeDefinition, initialized bool) (lifecycle.LedgerCommitResult, error) {
	namespace := s.cfg.lifecycleNamespace()
	key := lifecycle.LedgerKey(def)
	value, err := lifecycle.MarshalLedgerDefinition(def, initialized)
	if err != nil {
		return lifecycle.LedgerCommitResult{}, err
	}

	version, err := s.lifecycleKeyVersion(ctx, namespace, key)
	if err != nil {
		return lifecycle.LedgerCommitResult{}, fmt.Errorf("read lifecycle key version: %w", err)
	}

	args := [][]byte{
		[]byte(lifecycleCommitFunction),
		[]byte(def.Name),
		[]byte(def.Version),
		[]byte(strconv.FormatInt(def.Sequence, 10)),
	}
	prop, err := network.NewSignedProposal(s.signer, s.cfg.ChannelID, namespace, "1.0", args)
	if err != nil {
		return lifecycle.LedgerCommitResult{}, fmt.Errorf("create lifecycle proposal: %w", err)
	}
	inv, err := endorsement.Parse(prop, time.Now())
	if err != nil {
		return lifecycle.LedgerCommitResult{}, fmt.Errorf("parse lifecycle proposal: %w", err)
	}

	rws := blocks.ReadWriteSet{
		Reads: []blocks.KVRead{{
			Key:     key,
			Version: version,
		}},
		Writes: []blocks.KVWrite{{
			Key:   key,
			Value: value,
		}},
	}
	resp, err := efabx.NewEndorsementBuilder(s.signer).Endorse(inv, endorsement.Success(rws, nil, value))
	if err != nil {
		return lifecycle.LedgerCommitResult{}, fmt.Errorf("endorse lifecycle transaction: %w", err)
	}
	end := sdk.Endorsement{
		Proposal:  inv.Proposal,
		Responses: []*peer.ProposalResponse{resp},
	}

	finality, err := s.subscribeFinality(ctx, inv.TxID)
	if err != nil {
		return lifecycle.LedgerCommitResult{}, fmt.Errorf("subscribe lifecycle finality: %w", err)
	}
	if finality != nil {
		defer finality.Cancel()
	}

	s.logger.Infof("tx=%s submitting lifecycle definition namespace=%s key=%s ccid=%s sequence=%d initialized=%t",
		inv.TxID, namespace, key, lifecycle.DefinitionCCID(def), def.Sequence, initialized)
	if err := s.submitter.Submit(ctx, end); err != nil {
		return lifecycle.LedgerCommitResult{}, fmt.Errorf("submit lifecycle transaction: %w", err)
	}

	result := lifecycle.LedgerCommitResult{
		TxID:      inv.TxID,
		Namespace: namespace,
		Key:       key,
	}
	if finality == nil {
		return result, nil
	}
	status, err := finality.Wait()
	if err != nil {
		return result, fmt.Errorf("wait lifecycle finality: %w", err)
	}
	if status == nil {
		return result, nil
	}
	result.Status = status.Status.String()
	result.BlockNum = status.Ref.GetBlockNum()
	result.TxNum = status.Ref.GetTxNum()
	s.logger.Infof("tx=%s lifecycle finality status=%s block=%d txnum=%d initialized=%t",
		inv.TxID, result.Status, result.BlockNum, result.TxNum, initialized)
	if status.Status != committerpb.Status_COMMITTED {
		return result, fmt.Errorf("lifecycle transaction status %s", status.Status.String())
	}
	return result, nil
}

func (s *Service) lifecycleKeyVersion(ctx context.Context, namespace, key string) (*blocks.Version, error) {
	rows, err := s.queryService.GetRows(ctx, &committerpb.Query{
		Namespaces: []*committerpb.QueryNamespace{{
			NsId: namespace,
			Keys: [][]byte{[]byte(key)},
		}},
	})
	if err != nil {
		return nil, err
	}
	for _, ns := range rows.Namespaces {
		if ns.GetNsId() != namespace {
			continue
		}
		for _, row := range ns.Rows {
			if string(row.GetKey()) == key {
				return &blocks.Version{BlockNum: row.GetVersion()}, nil
			}
		}
	}
	return nil, nil
}
