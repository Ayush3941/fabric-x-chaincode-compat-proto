/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"context"
	"fmt"

	"chaincode_helper/pkg/api"
	"chaincode_helper/pkg/shim"

	"github.com/hyperledger/fabric-x-sdk/endorsement"
)

// ChaincodeServiceExecutor links the helper service to the chaincode-as-a-service
// shim bridge.
type ChaincodeServiceExecutor struct {
	connector *shim.Connector
}

func NewChaincodeServiceExecutor(connector *shim.Connector) ChaincodeServiceExecutor {
	return ChaincodeServiceExecutor{connector: connector}
}

// Execute implements api.Executor.
func (e ChaincodeServiceExecutor) Execute(ctx context.Context, execCtx *api.ExecutionContext, inv endorsement.Invocation) (endorsement.ExecutionResult, api.ExecutionMetadata, error) {
	if e.connector == nil {
		return endorsement.ExecutionResult{}, api.ExecutionMetadata{}, fmt.Errorf("shim connector is not configured")
	}

	res, err := e.connector.Execute(ctx, execCtx, shim.Invocation{
		TxID:      inv.TxID,
		ChannelID: inv.Channel,
		Namespace: execCtx.Namespace(),
		Args:      inv.Args,
		Creator:   inv.Creator,
		Nonce:     inv.Nonce,
		QueryView: execCtx.QueryView(),
	})
	if err != nil {
		return endorsement.ExecutionResult{}, api.ExecutionMetadata{}, err
	}

	return endorsement.ExecutionResult{
		RWS:     execCtx.Result(),
		Event:   res.Event,
		Status:  res.Status,
		Message: res.Message,
		Payload: res.Payload,
	}, api.ExecutionMetadata{QueryView: res.QueryView}, nil
}
