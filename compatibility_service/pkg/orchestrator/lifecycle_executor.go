// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"fmt"

	"compatibility_service/pkg/helper"
	"compatibility_service/pkg/lifecycle"
	"compatibility_service/pkg/shim"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// lifecycleExecutor resolves the chaincode service endpoint from committed
// lifecycle state when available, while keeping the static endpoint as a
// fallback for older prototype namespaces.
type lifecycleExecutor struct {
	store             *lifecycle.Store
	localMSPID        string
	resolver          chaincodeConnectionResolver
	fallbackConnector *shim.Connector
	logger            sdk.Logger
}

func newLifecycleExecutor(store *lifecycle.Store, localMSPID string, resolver chaincodeConnectionResolver, fallback *shim.Connector, logger sdk.Logger) *lifecycleExecutor {
	if logger == nil {
		logger = sdk.NoOpLogger{}
	}
	return &lifecycleExecutor{
		store:             store,
		localMSPID:        localMSPID,
		resolver:          resolver,
		fallbackConnector: fallback,
		logger:            logger,
	}
}

func (e *lifecycleExecutor) Execute(ctx context.Context, execCtx *helper.ExecutionContext, inv endorsement.Invocation) (endorsement.ExecutionResult, helper.ExecutionMetadata, error) {
	connector := e.fallbackConnector
	if e.store != nil {
		chaincodeName, chaincodeVersion := lifecycleIdentityFromExecution(execCtx)
		committed, ok, err := e.resolveCommittedDefinition(ctx, chaincodeName, chaincodeVersion)
		if err != nil {
			return endorsement.ExecutionResult{}, helper.ExecutionMetadata{}, err
		}
		if ok {
			conn, ok, err := e.resolveChaincodeConnection(ctx, committed.Definition)
			if err != nil {
				return endorsement.ExecutionResult{}, helper.ExecutionMetadata{}, err
			}
			if !ok {
				_, pkg, legacyOK, legacyErr := e.store.ResolveCommittedPackage(ctx, committed.Definition.Name, committed.Definition.Version, e.localMSPID)
				if legacyErr != nil {
					if e.fallbackConnector == nil {
						return endorsement.ExecutionResult{}, helper.ExecutionMetadata{}, legacyErr
					}
					e.logger.Infof("tx=%s lifecycle package resolution skipped for name=%s version=%s: %s; using configured fallback endpoint=%s",
						inv.TxID, committed.Definition.Name, committed.Definition.Version, legacyErr, e.fallbackConnector.Endpoint())
				}
				if legacyOK {
					conn = lifecycle.ResolvedChaincodeConnection{
						MSPID:    e.localMSPID,
						Name:     committed.Definition.Name,
						Version:  committed.Definition.Version,
						Sequence: committed.Definition.Sequence,
						Address:  pkg.Address,
						TLSMode:  "none",
					}
					ok = true
				}
			}
			if !ok {
				if connector == nil {
					return endorsement.ExecutionResult{}, helper.ExecutionMetadata{}, fmt.Errorf("no chaincode connection resolved for local MSP %s chaincode %s:%s sequence %d",
						e.localMSPID, committed.Definition.Name, committed.Definition.Version, committed.Definition.Sequence)
				}
				e.logger.Infof("tx=%s lifecycle using configured fallback endpoint=%s name=%s version=%s sequence=%d",
					inv.TxID, connector.Endpoint(), committed.Definition.Name, committed.Definition.Version, committed.Definition.Sequence)
				return helper.NewChaincodeServiceExecutor(connector).Execute(ctx, execCtx, inv)
			}
			if conn.TLSMode != "" && conn.TLSMode != "none" {
				return endorsement.ExecutionResult{}, helper.ExecutionMetadata{}, fmt.Errorf("chaincode connection for %s:%s uses unsupported tls mode %q",
					committed.Definition.Name, committed.Definition.Version, conn.TLSMode)
			}
			lifecycleConnector, err := shim.NewConnector(shim.Config{Endpoint: conn.Address})
			if err != nil {
				return endorsement.ExecutionResult{}, helper.ExecutionMetadata{}, fmt.Errorf("create lifecycle chaincode connector: %w", err)
			}
			lifecycleConnector.SetLogger(e.logger)
			connector = lifecycleConnector
			e.logger.Infof("tx=%s lifecycle resolved state_namespace=%s name=%s version=%s sequence=%d endpoint=%s",
				inv.TxID, execCtx.Namespace(), committed.Definition.Name, committed.Definition.Version, committed.Definition.Sequence, conn.Address)
		}
	}
	if connector == nil {
		return endorsement.ExecutionResult{}, helper.ExecutionMetadata{}, fmt.Errorf("shim connector is not configured")
	}
	return helper.NewChaincodeServiceExecutor(connector).Execute(ctx, execCtx, inv)
}

func (e *lifecycleExecutor) resolveCommittedDefinition(ctx context.Context, name, version string) (lifecycle.CommittedDefinition, bool, error) {
	if name != "" && version != "" {
		return e.store.GetCommitted(ctx, name, version)
	}
	definitions, err := e.store.ListCommitted(ctx, name, version)
	if err != nil {
		return lifecycle.CommittedDefinition{}, false, err
	}
	if len(definitions) == 0 {
		return lifecycle.CommittedDefinition{}, false, nil
	}
	if len(definitions) > 1 {
		return lifecycle.CommittedDefinition{}, false, fmt.Errorf("%d committed definitions are active; chaincode name and version are required",
			len(definitions))
	}
	return definitions[0], true, nil
}

func (e *lifecycleExecutor) resolveChaincodeConnection(ctx context.Context, def lifecycle.ChaincodeDefinition) (lifecycle.ResolvedChaincodeConnection, bool, error) {
	cached, ok, err := e.store.GetResolvedConnection(ctx, e.localMSPID, def)
	if err != nil || ok {
		if ok {
			e.logger.Infof("lifecycle connection cache hit msp=%s name=%s version=%s sequence=%d endpoint=%s",
				e.localMSPID, def.Name, def.Version, def.Sequence, cached.Address)
		}
		return cached, ok, err
	}
	if e.resolver == nil {
		return lifecycle.ResolvedChaincodeConnection{}, false, nil
	}
	resolved, ok, err := e.resolver.Resolve(ctx, chaincodeResolverRequest{
		MSPID:      e.localMSPID,
		Definition: def,
	})
	if err != nil || !ok {
		return lifecycle.ResolvedChaincodeConnection{}, ok, err
	}
	resolved, err = e.store.PutResolvedConnection(ctx, resolved)
	if err != nil {
		return lifecycle.ResolvedChaincodeConnection{}, false, err
	}
	return resolved, true, nil
}

func lifecycleIdentityFromExecution(execCtx *helper.ExecutionContext) (string, string) {
	if execCtx == nil {
		return "", ""
	}
	decorations := execCtx.Decorations()
	return string(decorations["compat.chaincode_name"]), string(decorations["compat.chaincode_version"])
}
