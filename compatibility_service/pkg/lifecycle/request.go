// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"

	sdk "github.com/hyperledger/fabric-x-sdk"
)

const (
	OperationInstall         = "install"
	OperationQueryInstalled  = "queryinstalled"
	OperationApprove         = "approveformyorg"
	OperationReadiness       = "checkcommitreadiness"
	OperationCommit          = "commit"
	OperationQueryCommitted  = "querycommitted"
	OperationMarkInitialized = "markinitialized"
	requestVersion           = 1
	requestNonceSize         = 24
	requestMaxAge            = 5 * time.Minute
)

// SignedRequest binds the caller, channel, operation, and processing mode to
// the payload. Legacy payload-only signatures are deliberately not accepted.
type SignedRequest struct {
	Version   uint32 `json:"version"`
	ChannelID string `json:"channel_id"`
	Operation string `json:"operation"`
	Internal  bool   `json:"internal,omitempty"`
	IssuedAt  int64  `json:"issued_at"`
	Nonce     []byte `json:"nonce"`
	Payload   []byte `json:"payload"`
	Creator   []byte `json:"creator"`
	Signature []byte `json:"signature"`
}

// SignRequest signs a public lifecycle operation inferred from its payload type.
func SignRequest(signer sdk.Signer, channelID string, payload any) (*SignedRequest, error) {
	return signRequest(signer, channelID, payload, false)
}

// SignInternalRequest is for authenticated Orchestrator coordination only.
func SignInternalRequest(signer sdk.Signer, channelID string, payload any) (*SignedRequest, error) {
	return signRequest(signer, channelID, payload, true)
}

func signRequest(signer sdk.Signer, channelID string, payload any, internal bool) (*SignedRequest, error) {
	if signer == nil || channelID == "" {
		return nil, fmt.Errorf("lifecycle signer and channel are required")
	}
	operation, err := operationForPayload(payload)
	if err != nil {
		return nil, err
	}
	if (internal && !internalOperation(operation)) || (!internal && operation == OperationMarkInitialized) {
		return nil, fmt.Errorf("operation %s is not available in this lifecycle request mode", operation)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal lifecycle payload: %w", err)
	}
	creator, err := signer.Serialize()
	if err != nil {
		return nil, fmt.Errorf("serialize lifecycle creator: %w", err)
	}
	req := &SignedRequest{Version: requestVersion, ChannelID: channelID, Operation: operation,
		Internal: internal, IssuedAt: time.Now().UnixNano(), Nonce: make([]byte, requestNonceSize), Payload: body, Creator: creator}
	if _, err := rand.Read(req.Nonce); err != nil {
		return nil, fmt.Errorf("create lifecycle nonce: %w", err)
	}
	message, err := req.signingBytes()
	if err != nil {
		return nil, err
	}
	req.Signature, err = signer.Sign(message)
	if err != nil {
		return nil, fmt.Errorf("sign lifecycle request: %w", err)
	}
	return req, nil
}

func (r *SignedRequest) signingBytes() ([]byte, error) {
	// A fixed field order and domain keep these signatures separate from
	// proposal signatures and other protocols using the same enrollment key.
	return json.Marshal(struct {
		Domain    string `json:"domain"`
		Version   uint32 `json:"version"`
		ChannelID string `json:"channel_id"`
		Operation string `json:"operation"`
		Internal  bool   `json:"internal"`
		IssuedAt  int64  `json:"issued_at"`
		Nonce     []byte `json:"nonce"`
		Creator   []byte `json:"creator"`
		Payload   []byte `json:"payload"`
	}{"compat.lifecycle.request", r.Version, r.ChannelID, r.Operation, r.Internal, r.IssuedAt, r.Nonce, r.Creator, r.Payload})
}

func internalOperation(operation string) bool {
	return operation == OperationReadiness || operation == OperationCommit || operation == OperationMarkInitialized
}

func operationForPayload(payload any) (string, error) {
	switch payload.(type) {
	case InstallRequest, *InstallRequest:
		return OperationInstall, nil
	case QueryInstalledRequest, *QueryInstalledRequest:
		return OperationQueryInstalled, nil
	case ApproveForMyOrgRequest, *ApproveForMyOrgRequest:
		return OperationApprove, nil
	case CheckCommitReadinessRequest, *CheckCommitReadinessRequest:
		return OperationReadiness, nil
	case CommitRequest, *CommitRequest:
		return OperationCommit, nil
	case QueryCommittedRequest, *QueryCommittedRequest:
		return OperationQueryCommitted, nil
	case MarkInitializedRequest, *MarkInitializedRequest:
		return OperationMarkInitialized, nil
	default:
		return "", fmt.Errorf("unsupported lifecycle request payload %T", payload)
	}
}
