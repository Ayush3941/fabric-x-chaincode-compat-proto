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
	fallbackConnector *shim.Connector
	logger            sdk.Logger
}

func newLifecycleExecutor(store *lifecycle.Store, localMSPID string, fallback *shim.Connector, logger sdk.Logger) *lifecycleExecutor {
	if logger == nil {
		logger = sdk.NoOpLogger{}
	}
	return &lifecycleExecutor{
		store:             store,
		localMSPID:        localMSPID,
		fallbackConnector: fallback,
		logger:            logger,
	}
}

func (e *lifecycleExecutor) Execute(ctx context.Context, execCtx *helper.ExecutionContext, inv endorsement.Invocation) (endorsement.ExecutionResult, helper.ExecutionMetadata, error) {
	connector := e.fallbackConnector
	if e.store != nil {
		chaincodeName, chaincodeVersion := lifecycleIdentityFromExecution(execCtx)
		committed, pkg, ok, err := e.store.ResolveCommittedPackage(ctx, chaincodeName, chaincodeVersion, e.localMSPID)
		if err != nil {
			return endorsement.ExecutionResult{}, helper.ExecutionMetadata{}, err
		}
		if ok {
			lifecycleConnector, err := shim.NewConnector(shim.Config{Endpoint: pkg.Address})
			if err != nil {
				return endorsement.ExecutionResult{}, helper.ExecutionMetadata{}, fmt.Errorf("create lifecycle chaincode connector: %w", err)
			}
			lifecycleConnector.SetLogger(e.logger)
			connector = lifecycleConnector
			e.logger.Infof("tx=%s lifecycle resolved state_namespace=%s name=%s version=%s sequence=%d package_id=%s endpoint=%s",
				inv.TxID, execCtx.Namespace(), committed.Definition.Name, committed.Definition.Version, committed.Definition.Sequence, pkg.PackageID, pkg.Address)
		}
	}
	if connector == nil {
		return endorsement.ExecutionResult{}, helper.ExecutionMetadata{}, fmt.Errorf("shim connector is not configured")
	}
	return helper.NewChaincodeServiceExecutor(connector).Execute(ctx, execCtx, inv)
}

func lifecycleIdentityFromExecution(execCtx *helper.ExecutionContext) (string, string) {
	if execCtx == nil {
		return "", ""
	}
	decorations := execCtx.Decorations()
	return string(decorations["compat.chaincode_name"]), string(decorations["compat.chaincode_version"])
}
