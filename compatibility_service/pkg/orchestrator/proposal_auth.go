// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"compatibility_service/pkg/auth"
	"github.com/hyperledger/fabric-protos-go-apiv2/gateway"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/msp"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	efab "github.com/hyperledger/fabric-x-sdk/endorsement/fabric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ProposalAuthConfig defines trust and permissions for signed client requests.
// It is independent of the TLS credentials authenticating an RPC connection.
type ProposalAuthConfig struct {
	MSPs  []ProposalMSPConfig         `mapstructure:"msps"`
	Rules []ProposalAuthorizationRule `mapstructure:"rules"`
	// An empty reader policy allows valid identities from the trusted MSPs.
	CommitStatusPolicy    string `mapstructure:"commit-status-policy"`
	ChaincodeEventsPolicy string `mapstructure:"chaincode-events-policy"`
}

// ProposalMSPConfig loads a public, verifying MSP directory, without private keys.
type ProposalMSPConfig = auth.MSPConfig

// ProposalAuthorizationRule grants access to a chaincode and native namespace.
// Selectors are exact strings or "*". A request must satisfy at least one
// matching rule's Fabric signature policy, evaluated against its single creator.
type ProposalAuthorizationRule struct {
	Chaincode string `mapstructure:"chaincode"`
	Namespace string `mapstructure:"namespace"`
	Policy    string `mapstructure:"policy"`
}

func (cfg ProposalAuthConfig) validate() error {
	if len(cfg.MSPs) == 0 {
		return errors.New("proposal-auth.msps is required")
	}
	seen := make(map[string]bool, len(cfg.MSPs))
	for i, trusted := range cfg.MSPs {
		if trusted.MSPID == "" || trusted.MSPDir == "" {
			return fmt.Errorf("proposal-auth.msps[%d] requires msp-id and msp-dir", i)
		}
		if seen[trusted.MSPID] {
			return fmt.Errorf("proposal-auth has duplicate MSP %q", trusted.MSPID)
		}
		seen[trusted.MSPID] = true
	}
	if len(cfg.Rules) == 0 {
		return errors.New("proposal-auth.rules is required")
	}
	for i, rule := range cfg.Rules {
		if rule.Chaincode == "" || rule.Namespace == "" || rule.Policy == "" {
			return fmt.Errorf("proposal-auth.rules[%d] requires chaincode, namespace, and policy", i)
		}
	}
	return nil
}

type proposalPermission struct {
	chaincode string
	namespace string
	policy    policies.Policy
}

type proposalVerifier struct {
	trust        *auth.Verifier
	identities   msp.MSPManager
	rules        []proposalPermission
	statusPolicy policies.Policy
	eventPolicy  policies.Policy
}

func newProposalVerifier(cfg ProposalAuthConfig) (*proposalVerifier, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	trust, err := auth.NewVerifier(cfg.MSPs)
	if err != nil {
		return nil, err
	}
	verifier := &proposalVerifier{identities: trust.Identities(), trust: trust}
	if cfg.CommitStatusPolicy != "" {
		verifier.statusPolicy, err = trust.CompilePolicy(cfg.CommitStatusPolicy)
		if err != nil {
			return nil, fmt.Errorf("compile proposal-auth.commit-status-policy: %w", err)
		}
	}
	if cfg.ChaincodeEventsPolicy != "" {
		verifier.eventPolicy, err = trust.CompilePolicy(cfg.ChaincodeEventsPolicy)
		if err != nil {
			return nil, fmt.Errorf("compile proposal-auth.chaincode-events-policy: %w", err)
		}
	}
	for i, rule := range cfg.Rules {
		policy, err := trust.CompilePolicy(rule.Policy)
		if err != nil {
			return nil, fmt.Errorf("compile proposal-auth.rules[%d]: %w", i, err)
		}
		verifier.rules = append(verifier.rules, proposalPermission{
			chaincode: rule.Chaincode,
			namespace: rule.Namespace,
			policy:    policy,
		})
	}
	return verifier, nil
}

// authenticate verifies the original bytes received from the client. creator
// must come from parsing those same proposal bytes, never from RPC metadata.
func (v *proposalVerifier) authenticate(prop *peer.SignedProposal, creator []byte, now time.Time) (msp.Identity, error) {
	if v == nil {
		return nil, errors.New("proposal authentication is not configured")
	}
	if prop == nil || len(prop.ProposalBytes) == 0 || len(prop.Signature) == 0 {
		return nil, errors.New("signed client proposal and signature are required")
	}
	return v.trust.Authenticate(creator, prop.ProposalBytes, prop.Signature, now)
}

func validateIdentityTime(identity msp.Identity, now time.Time) error {
	return auth.ValidateIdentityTime(identity, now)
}

func (v *proposalVerifier) authorize(identity msp.Identity, chaincode, namespace string) error {
	if v == nil || identity == nil {
		return errors.New("authenticated proposal creator is required")
	}
	for _, rule := range v.rules {
		if rule.chaincode != "*" && rule.chaincode != chaincode {
			continue
		}
		if rule.namespace != "*" && rule.namespace != namespace {
			continue
		}
		if rule.policy.EvaluateIdentities([]msp.Identity{identity}) == nil {
			return nil
		}
	}
	return fmt.Errorf("proposal creator from MSP %s is not authorized for chaincode %q in namespace %q",
		identity.GetMSPIdentifier(), chaincode, namespace)
}

type authenticatedClientProposal struct {
	invocation endorsement.Invocation
	identity   msp.Identity
}

// Both client APIs authenticate the original proposal before interpreting their
// routing conventions. The parser also validates timestamp, nonce and TxID.
func (s *Service) authenticateClientProposal(prop *peer.SignedProposal) (authenticatedClientProposal, error) {
	if prop == nil || len(prop.ProposalBytes) == 0 {
		return authenticatedClientProposal{}, status.Error(codes.InvalidArgument, "client proposal is required")
	}
	now := time.Now()
	inv, err := efab.Parse(prop, now)
	if err != nil {
		return authenticatedClientProposal{}, status.Error(codes.InvalidArgument, err.Error())
	}
	if inv.Channel != s.cfg.ChannelID {
		return authenticatedClientProposal{}, status.Errorf(codes.InvalidArgument, "channel must be %s", s.cfg.ChannelID)
	}
	identity, err := s.proposalAuth.authenticate(prop, inv.Creator, now)
	if err != nil {
		return authenticatedClientProposal{}, status.Error(codes.Unauthenticated, err.Error())
	}
	// The signed header and proposal payload must describe the same chaincode.
	header, err := protoutil.UnmarshalHeader(inv.Proposal.Header)
	if err != nil {
		return authenticatedClientProposal{}, status.Error(codes.InvalidArgument, err.Error())
	}
	channel, err := protoutil.UnmarshalChannelHeader(header.ChannelHeader)
	if err != nil {
		return authenticatedClientProposal{}, status.Error(codes.InvalidArgument, err.Error())
	}
	extension := &peer.ChaincodeHeaderExtension{}
	if err := proto.Unmarshal(channel.Extension, extension); err != nil {
		return authenticatedClientProposal{}, status.Error(codes.InvalidArgument, "invalid chaincode header extension")
	}
	if extension.ChaincodeId == nil || extension.ChaincodeId.Name != inv.Namespace || extension.ChaincodeId.Version != inv.ChaincodeVersion {
		return authenticatedClientProposal{}, status.Error(codes.InvalidArgument, "chaincode header and proposal payload differ")
	}
	return authenticatedClientProposal{invocation: inv, identity: identity}, nil
}

// Routing is internal data. The signed proposal, creator and nonce are retained
// verbatim for the helper and, when requested, remote evaluation.
func (s *Service) authorizeClientInvocation(proposal authenticatedClientProposal, original *peer.SignedProposal, req InvocationRequest) (InvocationRequest, error) {
	inv := proposal.invocation
	req.ClientTxID = inv.TxID
	req.ClientCreator = append([]byte(nil), inv.Creator...)
	req.ClientNonce = append([]byte(nil), inv.Nonce...)
	req.ClientSignedProposal = cloneSignedProposal(original)
	namespace, _, err := s.prepareInvocation(req)
	if err != nil {
		return InvocationRequest{}, status.Error(codes.InvalidArgument, err.Error())
	}
	req.Namespace = namespace
	if err := s.proposalAuth.authorize(proposal.identity, req.ChaincodeName, namespace); err != nil {
		return InvocationRequest{}, status.Error(codes.PermissionDenied, err.Error())
	}
	return req, nil
}

// gatewayInvocation shares admission for Gateway Evaluate and Endorse. Routing
// is derived from the signed chaincode name, never custom RPC metadata.
func (s *Service) gatewayInvocation(ctx context.Context, channelID, txID string, signed *peer.SignedProposal) (InvocationRequest, error) {
	proposal, err := s.authenticateClientProposal(signed)
	if err != nil {
		return InvocationRequest{}, err
	}
	inv := proposal.invocation
	if channelID != inv.Channel || txID != inv.TxID {
		return InvocationRequest{}, status.Error(codes.InvalidArgument, "Gateway channel and transaction ID must match the signed proposal")
	}
	req, err := requestFromProposal(inv)
	if err != nil {
		return InvocationRequest{}, status.Error(codes.InvalidArgument, err.Error())
	}
	req.ChaincodeName, req.Namespace, err = gatewayChaincodeRoute(inv.Namespace)
	if err != nil {
		return InvocationRequest{}, status.Error(codes.InvalidArgument, err.Error())
	}
	req, err = s.authorizeClientInvocation(proposal, signed, req)
	if err != nil {
		return InvocationRequest{}, err
	}
	req, err = s.selectLifecycleDefinition(ctx, req)
	if err != nil {
		return InvocationRequest{}, invocationRPCError(ctx.Err(), err)
	}
	if req.definition == nil {
		return InvocationRequest{}, status.Errorf(codes.NotFound, "chaincode %q has no committed definition on channel %q", req.ChaincodeName, inv.Channel)
	}
	return req, nil
}

// authenticateGatewayTransaction verifies the client's authorization over the
// exact envelope payload before interpreting its native transaction contents.
func (s *Service) authenticateGatewayTransaction(request *gateway.SubmitRequest) (msp.Identity, error) {
	if request == nil || request.PreparedTransaction == nil || len(request.PreparedTransaction.Payload) == 0 {
		return nil, status.Error(codes.InvalidArgument, "a prepared transaction is required")
	}
	envelope := request.PreparedTransaction
	if len(envelope.Signature) == 0 {
		return nil, status.Error(codes.InvalidArgument, "prepared transaction must be signed")
	}
	payload, err := protoutil.UnmarshalPayload(envelope.Payload)
	if err != nil || payload.Header == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid prepared envelope payload")
	}
	channel, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid prepared channel header")
	}
	if channel.ChannelId != s.cfg.ChannelID || request.ChannelId != channel.ChannelId || request.TransactionId != channel.TxId {
		return nil, status.Error(codes.InvalidArgument, "Submit channel and transaction ID must match the signed envelope")
	}
	signature, err := protoutil.UnmarshalSignatureHeader(payload.Header.SignatureHeader)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid prepared signature header")
	}
	if s.proposalAuth == nil {
		return nil, status.Error(codes.Unauthenticated, "client authentication is not configured")
	}
	identity, err := s.proposalAuth.trust.Authenticate(signature.Creator, envelope.Payload, envelope.Signature, time.Now())
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return identity, nil
}

// CommitStatus is channel-scoped: its signed request contains no chaincode or
// namespace. By default any authenticated member of a trusted MSP may read
// status; commit-status-policy can restrict readers independently of invocation.
func (s *Service) authenticateGatewayCommitStatus(signed *gateway.SignedCommitStatusRequest) (*gateway.CommitStatusRequest, error) {
	if signed == nil || len(signed.Request) == 0 || len(signed.Signature) == 0 {
		return nil, status.Error(codes.InvalidArgument, "a signed commit status request is required")
	}
	request := &gateway.CommitStatusRequest{}
	if err := proto.Unmarshal(signed.Request, request); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid commit status request")
	}
	if request.TransactionId == "" || strings.TrimSpace(request.TransactionId) != request.TransactionId || len(request.Identity) == 0 {
		return nil, status.Error(codes.InvalidArgument, "transaction ID and requestor identity are required")
	}
	identity, err := s.authenticateGatewayReader(request.ChannelId, request.Identity, signed.Request, signed.Signature)
	if err != nil {
		return nil, err
	}
	if s.proposalAuth.statusPolicy != nil {
		if err := s.proposalAuth.statusPolicy.EvaluateIdentities([]msp.Identity{identity}); err != nil {
			return nil, status.Error(codes.PermissionDenied, "requestor is not authorized to read transaction status")
		}
	}
	return request, nil
}

func (s *Service) authenticateGatewayReader(channel string, creator, request, signature []byte) (msp.Identity, error) {
	if channel == "" || channel != s.cfg.ChannelID {
		return nil, status.Errorf(codes.InvalidArgument, "channel must be %s", s.cfg.ChannelID)
	}
	if len(creator) == 0 {
		return nil, status.Error(codes.InvalidArgument, "reader identity is required")
	}
	if s.proposalAuth == nil || s.proposalAuth.trust == nil {
		return nil, status.Error(codes.Unauthenticated, "client authentication is not configured")
	}
	identity, err := s.proposalAuth.trust.Authenticate(creator, request, signature, time.Now())
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, err.Error())
	}
	return identity, nil
}

func (s *Service) authenticateGatewayChaincodeEvents(signed *gateway.SignedChaincodeEventsRequest) (*gateway.ChaincodeEventsRequest, error) {
	if signed == nil || len(signed.Request) == 0 || len(signed.Signature) == 0 {
		return nil, status.Error(codes.InvalidArgument, "a signed chaincode events request is required")
	}
	request := &gateway.ChaincodeEventsRequest{}
	if err := proto.Unmarshal(signed.Request, request); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid chaincode events request")
	}
	if _, _, err := gatewayChaincodeRoute(request.ChaincodeId); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// Match Gateway's supported positions: omitted/next commit, or a specified
	// block. SDK checkpoints use a specified position plus AfterTransactionId.
	switch request.GetStartPosition().GetType().(type) {
	case nil, *orderer.SeekPosition_NextCommit, *orderer.SeekPosition_Specified:
	default:
		return nil, status.Error(codes.InvalidArgument, "event start position must be next commit or a specified block")
	}
	if strings.TrimSpace(request.AfterTransactionId) != request.AfterTransactionId {
		return nil, status.Error(codes.InvalidArgument, "invalid checkpoint transaction ID")
	}
	identity, err := s.authenticateGatewayReader(request.ChannelId, request.Identity, signed.Request, signed.Signature)
	if err != nil {
		return nil, err
	}
	if s.proposalAuth.eventPolicy != nil {
		if err := s.proposalAuth.eventPolicy.EvaluateIdentities([]msp.Identity{identity}); err != nil {
			return nil, status.Error(codes.PermissionDenied, "requestor is not authorized to read chaincode events")
		}
	}
	return request, nil
}

func invocationRPCError(ctxErr, err error) error {
	if ctxErr != nil {
		return status.FromContextError(ctxErr).Err()
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, fmt.Sprint(err))
}
