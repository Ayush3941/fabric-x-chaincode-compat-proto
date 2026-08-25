// SPDX-License-Identifier: Apache-2.0

package helper

import (
	"context"
	"fmt"

	"compatibility_service/pkg/shim"

	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// ChaincodeServiceExecutor links the helper service to the external
// chaincode-as-a-service shim bridge.
type ChaincodeServiceExecutor struct {
	connector *shim.Connector
}

// NewChaincodeServiceExecutor creates an executor backed by one external
// chaincode service.
func NewChaincodeServiceExecutor(connector *shim.Connector) ChaincodeServiceExecutor {
	return ChaincodeServiceExecutor{connector: connector}
}

// Execute implements Executor.
func (e ChaincodeServiceExecutor) Execute(ctx context.Context, execCtx *ExecutionContext, inv endorsement.Invocation) (endorsement.ExecutionResult, ExecutionMetadata, error) {
	if e.connector == nil {
		return endorsement.ExecutionResult{}, ExecutionMetadata{}, fmt.Errorf("shim connector is not configured")
	}

	creator := inv.Creator
	if clientCreator := execCtx.ClientCreator(); len(clientCreator) > 0 {
		creator = clientCreator
	}
	nonce := inv.Nonce
	if clientNonce := execCtx.ClientNonce(); len(clientNonce) > 0 {
		nonce = clientNonce
	}

	res, err := e.connector.Execute(ctx, execCtx, shim.Invocation{
		TxID:        inv.TxID,
		ChannelID:   inv.Channel,
		Namespace:   execCtx.Namespace(),
		Args:        inv.Args,
		Creator:     creator,
		Nonce:       nonce,
		Decorations: execCtx.Decorations(),
		QueryView:   execCtx.QueryView(),
	})
	if err != nil {
		return endorsement.ExecutionResult{}, ExecutionMetadata{}, err
	}

	return endorsement.ExecutionResult{
		RWS:     execCtx.Result(),
		Event:   res.Event,
		Status:  res.Status,
		Message: res.Message,
		Payload: res.Payload,
	}, ExecutionMetadata{QueryView: res.QueryView}, nil
}
