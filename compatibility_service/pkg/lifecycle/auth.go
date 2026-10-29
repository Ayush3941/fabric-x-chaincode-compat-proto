// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"fmt"
	"os"
	"time"

	"compatibility_service/pkg/auth"
	mspapi "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/msp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var publicOperations = []string{OperationInstall, OperationQueryInstalled, OperationApprove, OperationReadiness, OperationCommit, OperationQueryCommitted}

// AuthConfig shares proposal trust roots but assigns permissions independently.
// Internal identities are public enrollment certificates, not TLS certificates.
type AuthConfig struct {
	Policies      map[string]string      `mapstructure:"policies"`
	Orchestrators []OrchestratorIdentity `mapstructure:"orchestrators"`
}

type OrchestratorIdentity struct {
	MSPID    string `mapstructure:"msp-id"`
	CertPath string `mapstructure:"cert-path"`
}

func (c AuthConfig) Validate() error {
	for _, operation := range publicOperations {
		if c.Policies[operation] == "" {
			return fmt.Errorf("lifecycle-auth.policies.%s is required", operation)
		}
	}
	if len(c.Policies) != len(publicOperations) {
		return fmt.Errorf("lifecycle-auth.policies contains an unknown operation")
	}
	if len(c.Orchestrators) == 0 {
		return fmt.Errorf("lifecycle-auth.orchestrators is required")
	}
	for _, identity := range c.Orchestrators {
		if identity.MSPID == "" || identity.CertPath == "" {
			return fmt.Errorf("lifecycle-auth.orchestrators requires msp-id and cert-path")
		}
	}
	return nil
}

type Authorizer struct {
	trust         *auth.Verifier
	channelID     string
	mspID         string
	policies      map[string]policies.Policy
	orchestrators map[string]bool
}

func NewAuthorizer(trust *auth.Verifier, channelID, mspID string, cfg AuthConfig) (*Authorizer, error) {
	if trust == nil || channelID == "" || mspID == "" {
		return nil, fmt.Errorf("lifecycle authorization requires MSP trust, channel, and local MSP ID")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	a := &Authorizer{trust: trust, channelID: channelID, mspID: mspID,
		policies: make(map[string]policies.Policy), orchestrators: make(map[string]bool)}
	for _, operation := range publicOperations {
		policy, err := trust.CompilePolicy(cfg.Policies[operation])
		if err != nil {
			return nil, fmt.Errorf("lifecycle policy %s: %w", operation, err)
		}
		a.policies[operation] = policy
	}
	for _, entry := range cfg.Orchestrators {
		cert, err := os.ReadFile(entry.CertPath)
		if err != nil {
			return nil, fmt.Errorf("read Orchestrator certificate: %w", err)
		}
		creator, err := proto.Marshal(&mspapi.SerializedIdentity{Mspid: entry.MSPID, IdBytes: cert})
		if err != nil {
			return nil, err
		}
		identity, err := trust.Identity(creator, time.Now())
		if err != nil {
			return nil, fmt.Errorf("configured Orchestrator %s: %w", entry.MSPID, err)
		}
		key := identityKey(identity)
		if a.orchestrators[key] {
			return nil, fmt.Errorf("duplicate Orchestrator enrollment identity for MSP %s", entry.MSPID)
		}
		a.orchestrators[key] = true
	}
	return a, nil
}

func identityKey(identity msp.Identity) string {
	id := identity.GetIdentifier()
	return id.Mspid + "|" + id.Id
}

// ValidateOrchestrator detects an omitted local enrollment identity at startup.
func (a *Authorizer) ValidateOrchestrator(creator []byte) error {
	identity, err := a.trust.Identity(creator, time.Now())
	if err != nil {
		return err
	}
	if !a.orchestrators[identityKey(identity)] {
		return fmt.Errorf("local signing identity is not in lifecycle-auth.orchestrators")
	}
	return nil
}

func (a *Authorizer) check(ctx context.Context, req *SignedRequest, operation string) (string, error) {
	if a == nil {
		return "", status.Error(codes.Unauthenticated, "lifecycle authorization is not configured")
	}
	if req == nil {
		return "", status.Error(codes.Unauthenticated, "signed lifecycle request is required")
	}
	message, err := req.signingBytes()
	if err != nil {
		return "", status.Error(codes.InvalidArgument, err.Error())
	}
	identity, err := a.trust.Authenticate(req.Creator, message, req.Signature, time.Now())
	if err != nil {
		return "", status.Error(codes.Unauthenticated, err.Error())
	}
	if req.Version != requestVersion || len(req.Nonce) != requestNonceSize || len(req.Payload) == 0 {
		return "", status.Error(codes.InvalidArgument, "invalid lifecycle request envelope")
	}
	if req.ChannelID != a.channelID || req.Operation != operation {
		return "", status.Error(codes.PermissionDenied, "lifecycle request channel or operation does not match")
	}
	now, issued := time.Now(), time.Unix(0, req.IssuedAt)
	if issued.Before(now.Add(-requestMaxAge)) || issued.After(now.Add(requestMaxAge)) {
		return "", status.Error(codes.Unauthenticated, "lifecycle request is outside its validity window")
	}
	for _, value := range metadata.ValueFromIncomingContext(ctx, localOnlyMetadata) {
		if (value != "true" && value != "false") || (value == "true") != req.Internal {
			return "", status.Error(codes.PermissionDenied, "local-only metadata conflicts with the signed lifecycle request mode")
		}
	}
	if req.Internal {
		if !internalOperation(operation) || !a.orchestrators[identityKey(identity)] {
			return "", status.Error(codes.PermissionDenied, "caller is not an authorized lifecycle Orchestrator")
		}
		return identity.GetMSPIdentifier(), nil
	}
	if (operation == OperationInstall || operation == OperationQueryInstalled || operation == OperationApprove) && identity.GetMSPIdentifier() != a.mspID {
		return "", status.Error(codes.PermissionDenied, "operation requires an identity from the local organization")
	}
	policy := a.policies[operation]
	if policy == nil || policy.EvaluateIdentities([]msp.Identity{identity}) != nil {
		return "", status.Errorf(codes.PermissionDenied, "caller is not authorized for lifecycle %s", operation)
	}
	return identity.GetMSPIdentifier(), nil
}
