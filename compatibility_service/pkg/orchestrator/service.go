// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"compatibility_service/pkg/config"
	"compatibility_service/pkg/helper"
	"compatibility_service/pkg/lifecycle"
	"compatibility_service/pkg/shim"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/gateway"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-committer/utils/serve"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/api/metadatapb"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"github.com/hyperledger/fabric-x-sdk/network"
	nfab "github.com/hyperledger/fabric-x-sdk/network/fabric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	GRPCOperationMetadata = "x-compat-operation"
	GRPCNamespaceMetadata = "x-compat-namespace"
	GRPCInitMetadata      = "x-compat-is-init" // Legacy assertion; signed ChaincodeInput.IsInit is authoritative.
	GRPCOperationInvoke   = "invoke"
	GRPCOperationQuery    = "query"
	GRPCOperationEndorse  = "endorse"
)

// Config contains the orchestrator wiring.
type Config struct {
	ChannelID          string                           `mapstructure:"channel-id"`
	Namespace          string                           `mapstructure:"namespace"`
	LifecycleNamespace string                           `mapstructure:"lifecycle-namespace"`
	Protocol           string                           `mapstructure:"protocol"`
	Server             *serve.ServerConfig              `mapstructure:"server"`
	Identity           *config.IdentityConfig           `mapstructure:"identity"`
	QueryService       config.ClientConfig              `mapstructure:"query-service"`
	ChaincodeSvc       config.ChaincodeServiceConfig    `mapstructure:"chaincode-service"`
	ChaincodeResolver  ChaincodeResolverConfig          `mapstructure:"chaincode-resolver"`
	Orderer            *config.ClientConfig             `mapstructure:"orderer"`
	NotificationSvc    *config.ClientConfig             `mapstructure:"notification-service"`
	RemoteOrgs         []RemoteOrchestratorConfig       `mapstructure:"remote-orchestrators"`
	RemoteResolver     RemoteOrchestratorResolverConfig `mapstructure:"remote-orchestrator-resolver"`
	ProposalAuth       ProposalAuthConfig               `mapstructure:"proposal-auth"`
	LifecycleAuth      lifecycle.AuthConfig             `mapstructure:"lifecycle-auth"`
	WaitAfterSubmit    time.Duration                    `mapstructure:"wait-after-submit"`
	RequestTimeout     time.Duration                    `mapstructure:"request-timeout"`
	FinalityTimeout    time.Duration                    `mapstructure:"finality-timeout"`
}

// RemoteOrchestratorConfig is a static V2 routing entry for one organization.
type RemoteOrchestratorConfig struct {
	MSPID    string           `mapstructure:"msp-id"`
	Endpoint *config.Endpoint `mapstructure:"endpoint"`
	TLS      config.TLSConfig `mapstructure:"tls"`
}

func (c RemoteOrchestratorConfig) Address() string {
	if c.Endpoint == nil {
		return ""
	}
	return c.Endpoint.Address()
}

func (c RemoteOrchestratorConfig) ToPeerConf() network.PeerConf {
	return network.PeerConf{
		Address: c.Address(),
		TLS: network.TLSConfig{
			Mode:        c.TLS.Mode,
			CertPath:    c.TLS.CertPath,
			KeyPath:     c.TLS.KeyPath,
			CACertPaths: c.TLS.CACertPaths,
			ServerName:  c.TLS.ServerName,
		},
	}
}

// InvocationRequest is the orchestrator's internal normalized request after a
// signed proposal has been parsed.
type InvocationRequest struct {
	definition             *lifecycle.CommittedDefinition
	ClientTxID             string               `json:"-"`
	ClientCreator          []byte               `json:"-"`
	ClientNonce            []byte               `json:"-"`
	ClientSignedProposal   *peer.SignedProposal `json:"-"`
	ClientTransient        map[string][]byte    `json:"-"`
	EndorsingOrganizations []string             `json:"-"`
	IdempotencyKey         string               `json:"-"`
	RequestDigest          string               `json:"-"`
	Namespace              string               `json:"namespace,omitempty"`
	ChaincodeName          string               `json:"chaincode_name,omitempty"`
	ChaincodeVersion       string               `json:"chaincode_version,omitempty"`
	IsInit                 bool                 `json:"is_init,omitempty"`
	Function               string               `json:"function,omitempty"`
	Args                   []string             `json:"args,omitempty"`
}

// InvocationResponse is returned by query and invoke.
type InvocationResponse struct {
	TxID              string                      `json:"tx_id,omitempty"`
	Status            int32                       `json:"status"`
	Message           string                      `json:"message,omitempty"`
	Payload           string                      `json:"payload,omitempty"`
	PayloadBase64     string                      `json:"payload_base64,omitempty"`
	Submitted         bool                        `json:"submitted"`
	FinalityPending   bool                        `json:"finality_pending,omitempty"`
	CommitStatus      string                      `json:"commit_status,omitempty"`
	BlockNum          uint64                      `json:"block_num,omitempty"`
	TxNum             uint32                      `json:"tx_num,omitempty"`
	IdempotencyKey    string                      `json:"idempotency_key,omitempty"`
	IdempotentReplay  bool                        `json:"idempotent_replay,omitempty"`
	ChaincodeEvent    *ChaincodeEvent             `json:"chaincode_event,omitempty"`
	LifecycleBindings []LifecycleExecutionBinding `json:"lifecycle_bindings,omitempty"`
}

// ChaincodeEvent is the client-facing committed event shape.
type ChaincodeEvent struct {
	ChaincodeID   string `json:"chaincode_id,omitempty"`
	TxID          string `json:"tx_id,omitempty"`
	EventName     string `json:"event_name,omitempty"`
	Payload       string `json:"payload,omitempty"`
	PayloadBase64 string `json:"payload_base64,omitempty"`
}

type transactionSubmitter interface {
	Submit(context.Context, sdk.Endorsement) error
	Close() error
}

// Service is the orchestrator process.
type Service struct {
	gateway.UnimplementedGatewayServer
	cfg           Config
	signer        sdk.Signer
	helper        *helper.Service
	queryPeer     *network.Peer
	queryService  committerpb.QueryServiceClient
	policies      *namespacePolicyResolver
	submitter     transactionSubmitter
	notifier      *network.Peer
	notify        committerpb.NotifierClient
	blockQuery    committerpb.BlockQueryServiceClient
	blockDelivery peer.DeliverClient
	idempotency   *idempotencyStore
	lifecycle     *lifecycle.Store
	resolver      chaincodeConnectionResolver
	remotes       remoteOrchestratorResolver
	proposalAuth  *proposalVerifier
	lifecycleAuth *lifecycle.Authorizer
	logger        sdk.Logger

	// A submission can trigger one bounded maintenance pass. No timer or
	// permanent worker runs between requests, and shutdown owns cancellation.
	recoveryMu       sync.Mutex
	recoveryCtx      context.Context
	recoveryCancel   context.CancelFunc
	recoveryRunning  bool
	recoveryClosed   bool
	recoveryWait     sync.WaitGroup
	lifecyclePending map[lifecycleOperation]*pendingLifecycle
}

// Loggers separates the deployable service logs by logical component.
type Loggers struct {
	Orchestrator sdk.Logger
	Helper       sdk.Logger
	Shim         sdk.Logger
}

// New constructs the orchestrator with one logger for all logical components.
func New(ctx context.Context, cfg Config, logger sdk.Logger) (*Service, error) {
	return NewWithLoggers(ctx, cfg, Loggers{
		Orchestrator: logger,
		Helper:       logger,
		Shim:         logger,
	})
}

// NewWithLoggers constructs the orchestrator, its in-process helper path, and
// the Fabric-X orderer/notification clients.
func NewWithLoggers(ctx context.Context, cfg Config, loggers Loggers) (*Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	orchestratorLogger := loggerOrNoop(loggers.Orchestrator)
	helperLogger := loggerOrNoop(loggers.Helper)
	shimLogger := loggerOrNoop(loggers.Shim)
	proposalAuth, err := newProposalVerifier(cfg.ProposalAuth)
	if err != nil {
		return nil, fmt.Errorf("configure proposal authentication: %w", err)
	}

	signer, err := identity.SignerFromMSP(cfg.Identity.MSPDir, cfg.Identity.MspID)
	if err != nil {
		return nil, fmt.Errorf("load identity: %w", err)
	}
	lifecycleAuth, err := lifecycle.NewAuthorizer(proposalAuth.trust, cfg.ChannelID, cfg.Identity.MspID, cfg.LifecycleAuth)
	if err != nil {
		return nil, fmt.Errorf("configure lifecycle authorization: %w", err)
	}
	creator, err := signer.Serialize()
	if err != nil {
		return nil, err
	}
	if err := lifecycleAuth.ValidateOrchestrator(creator); err != nil {
		return nil, err
	}

	queryPeer, err := network.NewPeer(cfg.QueryService.ToPeerConf())
	if err != nil {
		return nil, fmt.Errorf("query service: %w", err)
	}
	queryService := committerpb.NewQueryServiceClient(queryPeer.Connection())
	policies := newNamespacePolicyResolver(queryService)

	var shimConnector *shim.Connector
	if cfg.ChaincodeSvc.Endpoint != nil {
		shimConnector, err = shim.NewConnector(shim.Config{Endpoint: cfg.ChaincodeSvc.Address()})
		if err != nil {
			queryPeer.Close() //nolint:errcheck
			return nil, fmt.Errorf("create shim connector: %w", err)
		}
		shimConnector.SetLogger(shimLogger)
	}

	lifecycleStore, err := lifecycle.NewMemoryStore(cfg.ChannelID)
	if err != nil {
		queryPeer.Close() //nolint:errcheck
		return nil, fmt.Errorf("create lifecycle store: %w", err)
	}
	resolver, err := newChaincodeConnectionResolver(cfg.ChaincodeResolver, orchestratorLogger)
	if err != nil {
		lifecycleStore.Close() //nolint:errcheck
		queryPeer.Close()      //nolint:errcheck
		return nil, fmt.Errorf("create chaincode resolver: %w", err)
	}

	helperCfg := helper.ServiceConfig{
		ChannelID:    cfg.ChannelID,
		Protocol:     cfg.Protocol,
		QueryService: cfg.QueryService.ToPeerConf(),
	}
	executors := map[string]helper.Executor{
		"*": newLifecycleExecutor(lifecycleStore, cfg.Identity.MspID, resolver, shimConnector, shimLogger),
	}
	helper, err := helper.NewWithSigner(helperCfg, signer, executors, helperLogger)
	if err != nil {
		lifecycleStore.Close() //nolint:errcheck
		queryPeer.Close()      //nolint:errcheck
		return nil, fmt.Errorf("create internal helper: %w", err)
	}

	ordererConfs := []network.OrdererConf{cfg.Orderer.ToOrdererConf()}
	var submitter transactionSubmitter
	switch cfg.Protocol {
	case "fabric":
		submitter, err = nfab.NewSubmitter(ctx, ordererConfs, signer, cfg.WaitAfterSubmit, orchestratorLogger)
	case "fabric-x", "":
		submitter, err = newFabricXSubmitter(cfg.Orderer.ToPeerConf(), compatFabricXTxPackager{})
	default:
		helper.Close()         //nolint:errcheck
		lifecycleStore.Close() //nolint:errcheck
		queryPeer.Close()      //nolint:errcheck
		return nil, fmt.Errorf("unknown protocol %q: must be \"fabric\" or \"fabric-x\"", cfg.Protocol)
	}
	if err != nil {
		helper.Close()         //nolint:errcheck
		lifecycleStore.Close() //nolint:errcheck
		queryPeer.Close()      //nolint:errcheck
		return nil, fmt.Errorf("create submitter: %w", err)
	}

	var notifier *network.Peer
	var notify committerpb.NotifierClient
	var blockQuery committerpb.BlockQueryServiceClient
	var blockDelivery peer.DeliverClient
	if cfg.NotificationSvc != nil {
		notifier, err = network.NewPeer(cfg.NotificationSvc.ToPeerConf())
		if err != nil {
			submitter.Close()      //nolint:errcheck
			helper.Close()         //nolint:errcheck
			lifecycleStore.Close() //nolint:errcheck
			queryPeer.Close()      //nolint:errcheck
			return nil, fmt.Errorf("notification service: %w", err)
		}
		notify = committerpb.NewNotifierClient(notifier.Connection())
		blockQuery = committerpb.NewBlockQueryServiceClient(notifier.Connection())
		blockDelivery = peer.NewDeliverClient(notifier.Connection())
	}

	remoteResolver, err := newRemoteOrchestratorResolver(cfg.RemoteResolver, cfg.RemoteOrgs, orchestratorLogger)
	if err != nil {
		if closer, ok := resolver.(interface{ Close() error }); ok {
			closer.Close() //nolint:errcheck
		}
		lifecycleStore.Close() //nolint:errcheck
		if notifier != nil {
			notifier.Close() //nolint:errcheck
		}
		submitter.Close() //nolint:errcheck
		helper.Close()    //nolint:errcheck
		queryPeer.Close() //nolint:errcheck
		return nil, fmt.Errorf("create remote orchestrator resolver: %w", err)
	}

	svc := &Service{
		cfg:           cfg,
		signer:        signer,
		helper:        helper,
		queryPeer:     queryPeer,
		queryService:  queryService,
		policies:      policies,
		submitter:     submitter,
		notifier:      notifier,
		notify:        notify,
		blockQuery:    blockQuery,
		blockDelivery: blockDelivery,
		idempotency:   newIdempotencyStore(),
		lifecycle:     lifecycleStore,
		resolver:      resolver,
		remotes:       remoteResolver,
		proposalAuth:  proposalAuth,
		lifecycleAuth: lifecycleAuth,
		logger:        orchestratorLogger,
	}
	svc.recoveryCtx, svc.recoveryCancel = context.WithCancel(ctx)
	if err := svc.syncLifecycleFromLedger(ctx); err != nil {
		svc.Close() //nolint:errcheck
		return nil, fmt.Errorf("sync lifecycle from ledger: %w", err)
	}

	orchestratorLogger.Infof("orchestrator initialized channel=%s default_namespace=%s protocol=%s helper=in-process request_timeout=%s finality_timeout=%s",
		cfg.ChannelID, cfg.Namespace, protocolOrDefault(cfg.Protocol), cfg.requestTimeout(), cfg.finalityTimeout())
	return svc, nil
}

// Validate checks the endpoints needed for the current single-service flow.
func (cfg Config) Validate() error {
	var errs []error
	if err := cfg.ProposalAuth.validate(); err != nil {
		errs = append(errs, err)
	}
	if err := cfg.LifecycleAuth.Validate(); err != nil {
		errs = append(errs, err)
	}
	if cfg.ChannelID == "" {
		errs = append(errs, errors.New("channel-id is required"))
	}
	if cfg.Identity == nil {
		errs = append(errs, errors.New("identity is required"))
	}
	if cfg.Server == nil {
		errs = append(errs, errors.New("server configuration is required"))
	} else if cfg.Server.Endpoint.Empty() {
		errs = append(errs, errors.New("server.endpoint is required"))
	}
	if cfg.QueryService.Endpoint == nil {
		errs = append(errs, errors.New("query-service.endpoint is required"))
	}
	if cfg.Orderer == nil || cfg.Orderer.Endpoint == nil {
		errs = append(errs, errors.New("orderer.endpoint is required"))
	}
	if cfg.NotificationSvc == nil || cfg.NotificationSvc.Endpoint == nil {
		errs = append(errs, errors.New("notification-service.endpoint is required"))
	}
	for i, remote := range cfg.RemoteOrgs {
		if remote.MSPID == "" {
			errs = append(errs, fmt.Errorf("remote-orchestrators[%d].msp-id is required", i))
		}
		if remote.Endpoint == nil || remote.Endpoint.Host == "" || remote.Endpoint.Port == 0 {
			errs = append(errs, fmt.Errorf("remote-orchestrators[%d].endpoint is required", i))
		}
	}
	if cfg.RequestTimeout < 0 {
		errs = append(errs, errors.New("request-timeout must not be negative"))
	}
	if cfg.FinalityTimeout < 0 {
		errs = append(errs, errors.New("finality-timeout must not be negative"))
	}
	if cfg.RequestTimeout >= 0 && cfg.FinalityTimeout >= 0 && cfg.requestTimeout() < cfg.finalityTimeout() {
		errs = append(errs, errors.New("request-timeout must be greater than or equal to finality-timeout"))
	}
	return errors.Join(errs...)
}

func (cfg Config) requestTimeout() time.Duration {
	if cfg.RequestTimeout > 0 {
		return cfg.RequestTimeout
	}
	return 60 * time.Second
}

func (cfg Config) finalityTimeout() time.Duration {
	if cfg.FinalityTimeout > 0 {
		return cfg.FinalityTimeout
	}
	return 30 * time.Second
}

// Run starts the orchestrator Fabric ProcessProposal gRPC API.
func (s *Service) Run(ctx context.Context) error {
	return serve.Serve(ctx, s, &serve.Config{GRPC: *s.cfg.Server})
}

// RegisterService implements serve.Registerer.
func (s *Service) RegisterService(servers serve.Servers) {
	peer.RegisterEndorserServer(servers.GRPC, s)
	gateway.RegisterGatewayServer(servers.GRPC, s)
	lifecycle.RegisterLifecycleServer(servers.GRPC, s.lifecycleServer())
	healthgrpc.RegisterHealthServer(servers.GRPC, health.NewServer())
	reflection.Register(servers.GRPC)
	s.logger.Infof("orchestrator gRPC ProcessProposal, Gateway Evaluate/Endorse/Submit/CommitStatus/ChaincodeEvents and lifecycle services registered")
}

// Close closes outbound connections held by the orchestrator.
func (s *Service) Close() error {
	s.stopReconciliation()
	var errs []error
	if s.submitter != nil {
		errs = append(errs, s.submitter.Close())
	}
	if s.helper != nil {
		errs = append(errs, s.helper.Close())
	}
	if s.queryPeer != nil {
		errs = append(errs, s.queryPeer.Close())
	}
	if s.notifier != nil {
		errs = append(errs, s.notifier.Close())
	}
	if s.lifecycle != nil {
		errs = append(errs, s.lifecycle.Close())
	}
	if closer, ok := s.resolver.(interface{ Close() error }); ok {
		errs = append(errs, closer.Close())
	}
	if s.remotes != nil {
		errs = append(errs, s.remotes.Close())
	}
	return errors.Join(errs...)
}

// RemotePeers implements lifecycle.RemotePeerProvider.
func (s *Service) RemotePeers(ctx context.Context, req lifecycle.RemotePeerRequest) ([]lifecycle.RemotePeer, error) {
	if s == nil || s.remotes == nil {
		return nil, nil
	}
	requesterMSPID := req.RequesterMSP
	if requesterMSPID == "" && s.cfg.Identity != nil {
		requesterMSPID = s.cfg.Identity.MspID
	}
	return s.remotes.RemotePeers(ctx, remoteOrchestratorResolveRequest{
		RequesterMSPID:   requesterMSPID,
		ChannelID:        s.cfg.ChannelID,
		ChaincodeName:    req.Definition.Name,
		ChaincodeVersion: req.Definition.Version,
		Sequence:         req.Definition.Sequence,
	})
}

// ProcessProposal accepts an MSP-signed Fabric proposal from the client.
func (s *Service) ProcessProposal(ctx context.Context, prop *peer.SignedProposal) (*peer.ProposalResponse, error) {
	proposal, err := s.authenticateClientProposal(prop)
	if err != nil {
		return nil, err
	}
	inv := proposal.invocation

	operation, err := operationFromContext(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	submit := operation == GRPCOperationInvoke

	req, err := requestFromProposal(inv)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// Remote Gateway endorsement carries the original qualified chaincode name
	// through ProcessProposal. Normalize routing without changing signed bytes.
	if operation == GRPCOperationEndorse && strings.Contains(req.ChaincodeName, ":") {
		req.ChaincodeName, req.Namespace, err = gatewayChaincodeRoute(req.ChaincodeName)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	if namespace, ok := namespaceFromContext(ctx); ok {
		if req.Namespace != "" && req.Namespace != namespace {
			return nil, status.Error(codes.InvalidArgument, "forwarded namespace conflicts with the signed Gateway route")
		}
		req.Namespace = namespace
	}
	if err := validateInitMetadata(ctx, req.IsInit); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if req.IsInit && operation == GRPCOperationQuery {
		return nil, status.Error(codes.InvalidArgument, "is-init is only valid for invoke")
	}
	req, err = s.authorizeClientInvocation(proposal, prop, req)
	if err != nil {
		return nil, err
	}
	namespace := req.Namespace

	s.logger.Infof("tx=%s client proposal authenticated msp=%s chaincode=%s namespace=%s operation=%s",
		inv.TxID, proposal.identity.GetMSPIdentifier(), req.ChaincodeName, namespace, operation)
	s.logger.Infof("tx=%s orchestrator proposal received operation=%s channel=%s namespace=%s chaincode=%s:%s is_init=%t fn=%s args=%d",
		inv.TxID, operation, inv.Channel, req.Namespace, req.ChaincodeName, req.ChaincodeVersion, req.IsInit, req.Function, len(req.Args))

	requestTimeout := s.cfg.requestTimeout()
	requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	s.logger.Debugf("tx=%s request deadline started timeout=%s", inv.TxID, requestTimeout)

	if operation == GRPCOperationEndorse {
		result, err := s.EndorseOnly(requestCtx, req)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				s.logger.Warnf("tx=%s request deadline exceeded timeout=%s", inv.TxID, requestTimeout)
				return nil, status.Error(codes.DeadlineExceeded, "orchestrator request deadline exceeded")
			} else if errors.Is(err, context.Canceled) {
				return nil, status.Error(codes.Canceled, "orchestrator request canceled")
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		if err := setLifecycleBindingMetadata(ctx, result.Binding); err != nil {
			s.logger.Warnf("tx=%s lifecycle binding metadata failed: %s", result.TxID, err)
		}
		if len(result.Endorsement.Responses) == 0 || result.Endorsement.Responses[0] == nil {
			return nil, status.Error(codes.Internal, "helper returned no proposal response")
		}
		return result.Endorsement.Responses[0], nil
	}

	res, err := s.Execute(requestCtx, req, submit)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.logger.Warnf("tx=%s request deadline exceeded timeout=%s", inv.TxID, requestTimeout)
			return nil, status.Error(codes.DeadlineExceeded, "orchestrator request deadline exceeded")
		} else if errors.Is(err, context.Canceled) {
			return nil, status.Error(codes.Canceled, "orchestrator request canceled")
		}

		return nil, status.Error(codes.Internal, err.Error())
	}

	payload, err := json.Marshal(res)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &peer.ProposalResponse{
		Version: 1,
		Response: &peer.Response{
			Status:  res.Status,
			Message: res.Message,
			Payload: payload,
		},
	}, nil
}

// EndorseOnly executes the local helper path and returns the helper's Fabric-X
// endorsement result without submitting to the orderer.
func (s *Service) EndorseOnly(ctx context.Context, req InvocationRequest) (helperExecutionResult, error) {
	namespace, args, err := s.prepareInvocation(req)
	if err != nil {
		return helperExecutionResult{}, err
	}
	req, err = s.selectLifecycleDefinition(ctx, req)
	if err != nil {
		return helperExecutionResult{}, err
	}
	if _, err := s.validateInitState(ctx, req, namespace, GRPCOperationEndorse); err != nil {
		return helperExecutionResult{}, err
	}
	policy, err := s.resolveNamespacePolicy(ctx, namespace)
	if err != nil {
		return helperExecutionResult{}, fmt.Errorf("resolve namespace policy: %w", err)
	}
	result, err := s.executeFresh(ctx, req, namespace, policy.Version, args)
	if err != nil {
		return helperExecutionResult{}, fmt.Errorf("helper endorsement failed: %w", err)
	}
	if len(result.Endorsement.Responses) == 0 || result.Endorsement.Responses[0] == nil {
		return helperExecutionResult{}, errors.New("helper returned no proposal response")
	}
	s.logger.Infof("tx=%s execute-only endorsement completed namespace=%s status=%d",
		result.TxID, namespace, result.Response.Status)
	return result, nil
}

// Execute runs one query or invoke through helper endorsement
// Invokes are packaged and submitted to Fabric-X.
func (s *Service) Execute(ctx context.Context, req InvocationRequest, submit bool) (out InvocationResponse, err error) {
	if !submit {
		result, err := s.evaluate(ctx, req)
		if err != nil {
			return InvocationResponse{}, err
		}
		out := responseFromPeer(result.TxID, result.Response)
		out.LifecycleBindings = lifecycleBindings(result, nil)
		return out, nil
	}
	namespace, _, err := s.prepareInvocation(req)
	if err != nil {
		return InvocationResponse{}, err
	}

	var record *idempotencyRecord
	if submit {
		s.triggerReconciliation()
		req.IdempotencyKey, req.RequestDigest = idempotencyIdentity(s.cfg.ChannelID, namespace, req, submit)
		var firstRequest bool
		record, firstRequest, err = s.idempotency.begin(req.IdempotencyKey, req.RequestDigest)
		if err != nil {
			return InvocationResponse{}, err
		}
		if !firstRequest {
			s.logger.Infof("idempotency_key=%s duplicate request detected; waiting for stored result", req.IdempotencyKey)
			out, err := s.replaySubmission(ctx, record)
			out.IdempotentReplay = true
			return out, err
		}
		defer func() {
			s.idempotency.complete(record, out, err)
		}()
	}
	// Endorse after duplicate detection: retries retain the original request
	// digest even when the active definition has since advanced.
	result, err := s.endorseInvocation(ctx, req)
	if result.Local.Response != nil {
		s.idempotency.markExecuted(record, result.Local.TxID)
		out = responseFromPeer(result.Local.TxID, result.Local.Response)
		out.IdempotencyKey = req.IdempotencyKey
		out.LifecycleBindings = lifecycleBindings(result.Local, result.Remote)
	}
	if err != nil {
		return out, err
	}
	if out.Status < 200 || out.Status >= 400 {
		return out, nil
	}

	var afterCommit func(context.Context) error
	if req.IsInit && result.InitDefinition != nil {
		def := *result.InitDefinition
		afterCommit = func(commitCtx context.Context) error {
			return s.markInitializedAfterCommit(commitCtx, def)
		}
	}
	return s.submitAndWaitFinality(ctx, result.Endorsement, out, result.Local.TxID, record, afterCommit)
}

func (s *Service) prepareInvocation(req InvocationRequest) (string, [][]byte, error) {
	namespace := req.Namespace
	// TODO: need to modify the fallback once proper lifecycle is set up.
	if namespace == "" {
		namespace = s.cfg.Namespace
	}
	if namespace == "" {
		return "", nil, errors.New("namespace is required")
	}
	args := invocationArgs(req)
	if len(args) == 0 {
		return "", nil, errors.New("function is required")
	}
	return namespace, args, nil
}

func (s *Service) executeHelper(ctx context.Context, namespace, nsVersion string, args [][]byte, transient map[string][]byte, clientProposal helper.ClientProposalContext) (sdk.Endorsement, error) {
	if s.helper == nil {
		return sdk.Endorsement{}, errors.New("internal helper is not configured")
	}
	prop, err := s.helperProposal(namespace, nsVersion, args, transient, clientProposal)
	if err != nil {
		return sdk.Endorsement{}, err
	}
	proposal, err := protoutil.UnmarshalProposal(prop.ProposalBytes)
	if err != nil {
		return sdk.Endorsement{}, fmt.Errorf("unmarshal helper proposal: %w", err)
	}
	txID, err := txIDFromProposal(proposal)
	if err != nil {
		return sdk.Endorsement{}, err
	}
	s.logger.Infof("tx=%s helper proposal created namespace=%s version=%s fn=%s args=%d",
		txID, namespace, nsVersion, argString(args, 0), len(args)-1)

	proposalContext := clientProposal
	if proposalContext.SignedProposal == nil {
		proposalContext.SignedProposal = prop
	}
	resp, err := s.helper.ProcessProposal(helper.WithClientProposal(ctx, proposalContext), prop)
	if err != nil {
		return sdk.Endorsement{}, fmt.Errorf("helper process proposal: %w", err)
	}
	if resp != nil && resp.Response != nil {
		s.logger.Infof("tx=%s helper proposal response received status=%d chaincode_payload_bytes=%d proposal_payload_bytes=%d endorsement_present=%t",
			txID, resp.Response.Status, len(resp.Response.Payload), len(resp.Payload), resp.Endorsement != nil)
	}
	return sdk.Endorsement{Proposal: proposal, Responses: []*peer.ProposalResponse{resp}}, nil
}

func (s *Service) helperProposal(namespace, nsVersion string, args [][]byte, transient map[string][]byte, clientProposal helper.ClientProposalContext) (*peer.SignedProposal, error) {
	if len(clientProposal.Creator) == 0 || len(clientProposal.Nonce) == 0 {
		prop, err := nfab.NewSignedProposal(s.signer, s.cfg.ChannelID, namespace, args)
		if err != nil {
			return nil, fmt.Errorf("create helper proposal: %w", err)
		}
		if !clientProposal.IsInit {
			return prop, nil
		}
		proposal, err := protoutil.UnmarshalProposal(prop.ProposalBytes)
		if err != nil {
			return nil, err
		}
		payload, err := protoutil.UnmarshalChaincodeProposalPayload(proposal.Payload)
		if err != nil {
			return nil, err
		}
		spec, err := protoutil.UnmarshalChaincodeInvocationSpec(payload.Input)
		if err != nil {
			return nil, err
		}
		spec.ChaincodeSpec.Input.IsInit = true
		payload.Input, err = proto.Marshal(spec)
		if err != nil {
			return nil, err
		}
		proposal.Payload, err = proto.Marshal(payload)
		if err != nil {
			return nil, err
		}
		return protoutil.GetSignedProposal(proposal, s.signer)
	}

	txID := protoutil.ComputeTxID(clientProposal.Nonce, clientProposal.Creator)
	proposal, _, err := protoutil.CreateChaincodeProposalWithTxIDNonceAndTransient(
		txID,
		common.HeaderType_ENDORSER_TRANSACTION,
		s.cfg.ChannelID,
		&peer.ChaincodeInvocationSpec{
			ChaincodeSpec: &peer.ChaincodeSpec{
				Type: peer.ChaincodeSpec_CAR,
				ChaincodeId: &peer.ChaincodeID{
					Name:    namespace,
					Version: nsVersion,
				},
				Input: &peer.ChaincodeInput{
					Args:        args,
					Decorations: cloneByteMap(clientProposal.Decorations),
					IsInit:      clientProposal.IsInit,
				},
			},
		},
		clientProposal.Nonce,
		clientProposal.Creator,
		transient,
	)
	if err != nil {
		return nil, fmt.Errorf("create helper proposal: %w", err)
	}
	proposalBytes, err := proto.Marshal(proposal)
	if err != nil {
		return nil, fmt.Errorf("marshal helper proposal: %w", err)
	}
	return &peer.SignedProposal{ProposalBytes: proposalBytes}, nil
}

type finalitySubscription struct {
	cancel context.CancelFunc
	result <-chan finalityResult
}

type finalityResult struct {
	status *committerpb.TxStatus
	err    error
}

func (s *finalitySubscription) Cancel() {
	if s != nil && s.cancel != nil {
		s.cancel()
	}
}

func (s *finalitySubscription) Wait() (*committerpb.TxStatus, error) {
	if s == nil {
		return nil, nil
	}
	res := <-s.result
	return res.status, res.err
}

func (s *Service) subscribeFinality(ctx context.Context, txID string) (*finalitySubscription, error) {
	if s.notify == nil {
		return nil, nil
	}

	timeout := s.cfg.FinalityTimeout
	if timeout <= 0 {
		timeout = s.cfg.finalityTimeout()
	}

	notifyCtx, cancel := context.WithTimeout(ctx, timeout)
	stream, err := s.notify.OpenNotificationStream(notifyCtx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open notification stream: %w", err)
	}
	if err := stream.Send(&committerpb.NotificationRequest{
		TxStatusRequest: &committerpb.TxIDsBatch{TxIds: []string{txID}},
		Timeout:         durationpb.New(timeout),
	}); err != nil {
		cancel()
		return nil, fmt.Errorf("send tx status subscription: %w", err)
	}
	s.logger.Debugf("tx=%s notification subscription opened timeout=%s", txID, timeout)

	result := make(chan finalityResult, 1)
	go waitForNotification(notifyCtx, stream, txID, result)

	return &finalitySubscription{
		cancel: cancel,
		result: result,
	}, nil
}

func waitForNotification(
	ctx context.Context,
	stream committerpb.Notifier_OpenNotificationStreamClient,
	txID string,
	out chan<- finalityResult,
) {
	defer close(out)

	for {
		res, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				out <- finalityResult{err: ctx.Err()}
				return
			}
			if errors.Is(err, io.EOF) {
				out <- finalityResult{err: errors.New("notification stream closed before transaction status")}
				return
			}
			out <- finalityResult{err: fmt.Errorf("receive notification: %w", err)}
			return
		}

		status, err, ok := matchNotification(txID, res)
		if ok {
			out <- finalityResult{status: status, err: err}
			return
		}
	}
}

func matchNotification(txID string, res *committerpb.NotificationResponse) (*committerpb.TxStatus, error, bool) {
	for _, txStatus := range res.TxStatusEvents {
		if txStatus.Ref.GetTxId() == txID {
			return txStatus, nil, true
		}
	}
	for _, timeoutTxID := range res.TimeoutTxIds {
		if timeoutTxID == txID {
			return nil, fmt.Errorf("notification timeout for tx %s", txID), true
		}
	}
	if rejected := res.GetRejectedTxIds(); rejected != nil {
		for _, rejectedTxID := range rejected.TxIds {
			if rejectedTxID == txID {
				return nil, fmt.Errorf("notification rejected tx %s: %s", txID, rejected.Reason), true
			}
		}
	}
	return nil, nil, false
}

func invocationArgs(req InvocationRequest) [][]byte {
	if req.Function == "" {
		return nil
	}
	args := make([][]byte, 0, 1+len(req.Args))
	args = append(args, []byte(req.Function))
	for _, arg := range req.Args {
		args = append(args, []byte(arg))
	}
	return args
}

func operationFromContext(ctx context.Context) (string, error) {
	values := metadata.ValueFromIncomingContext(ctx, GRPCOperationMetadata)
	if len(values) == 0 {
		return "", fmt.Errorf("missing %s metadata", GRPCOperationMetadata)
	}
	switch values[0] {
	case GRPCOperationInvoke:
		return GRPCOperationInvoke, nil
	case GRPCOperationQuery:
		return GRPCOperationQuery, nil
	case GRPCOperationEndorse:
		return GRPCOperationEndorse, nil
	default:
		return "", fmt.Errorf("unknown orchestrator operation %q", values[0])
	}
}

func namespaceFromContext(ctx context.Context) (string, bool) {
	values := metadata.ValueFromIncomingContext(ctx, GRPCNamespaceMetadata)
	if len(values) == 0 || values[0] == "" {
		return "", false
	}
	return values[0], true
}

func requestFromProposal(inv endorsement.Invocation) (InvocationRequest, error) {
	if len(inv.Args) < 1 || len(inv.Args[0]) == 0 {
		return InvocationRequest{}, errors.New("orchestrator proposal requires function")
	}

	req := InvocationRequest{
		Function:         string(inv.Args[0]),
		ChaincodeName:    inv.Namespace,
		ChaincodeVersion: inv.ChaincodeVersion,
	}
	if inv.Proposal != nil {
		cpp, err := protoutil.UnmarshalChaincodeProposalPayload(inv.Proposal.Payload)
		if err != nil {
			return InvocationRequest{}, fmt.Errorf("unmarshal proposal payload: %w", err)
		}
		req.ClientTransient = cloneByteMap(cpp.TransientMap)
		req.IsInit, err = helper.ProposalIsInit(inv.Proposal)
		if err != nil {
			return InvocationRequest{}, err
		}
	}
	for _, arg := range inv.Args[1:] {
		req.Args = append(req.Args, string(arg))
	}
	return req, nil
}

func responseFromPeer(txID string, resp *peer.Response) InvocationResponse {
	return InvocationResponse{
		TxID:          txID,
		Status:        resp.Status,
		Message:       resp.Message,
		Payload:       string(resp.Payload),
		PayloadBase64: base64.StdEncoding.EncodeToString(resp.Payload),
	}
}

func eventFromEndorsement(end sdk.Endorsement, txID string) *ChaincodeEvent {
	if len(end.Responses) == 0 || end.Responses[0] == nil || len(end.Responses[0].Payload) == 0 {
		return nil
	}

	var tx applicationpb.Tx
	if err := proto.Unmarshal(end.Responses[0].Payload, &tx); err != nil {
		return nil
	}
	event, err := chaincodeEventFromTransaction(&tx, txID)
	if err != nil || event == nil {
		return nil
	}
	chaincodeID := ""
	if len(tx.Namespaces) > 0 {
		chaincodeID = tx.Namespaces[0].NsId
	}
	return &ChaincodeEvent{
		ChaincodeID:   chaincodeID,
		TxID:          txID,
		EventName:     event.EventName,
		Payload:       string(event.Payload),
		PayloadBase64: base64.StdEncoding.EncodeToString(event.Payload),
	}
}

// Both the custom invocation response and Gateway event streams use this
// decoder. The custom adapter keeps its historical namespace-valued ID field.
func chaincodeEventFromTransaction(tx *applicationpb.Tx, txID string) (*peer.ChaincodeEvent, error) {
	if tx == nil || len(tx.Metadata) == 0 {
		return nil, nil
	}
	metadata := &metadatapb.Metadata{}
	if err := proto.Unmarshal(tx.Metadata[0], metadata); err != nil {
		return nil, fmt.Errorf("decode chaincode event metadata: %w", err)
	}
	if metadata.EventName == "" && len(metadata.Event) == 0 {
		return nil, nil
	}
	identity, err := helper.ChaincodeIdentityFromMetadata(tx.Metadata)
	if err != nil {
		return nil, err
	}
	chaincode := ""
	if len(tx.Namespaces) > 0 {
		chaincode = tx.Namespaces[0].GetNsId()
	}
	if identity != nil {
		if len(tx.Namespaces) != 1 || identity.Namespace != chaincode {
			return nil, errors.New("event identity does not match the native namespace")
		}
		chaincode = identity.ChaincodeName + ":" + identity.Namespace
	}
	return &peer.ChaincodeEvent{ChaincodeId: chaincode, TxId: txID, EventName: metadata.EventName, Payload: metadata.Event}, nil
}

func txIDFromEndorsement(end sdk.Endorsement) (string, error) {
	if end.Proposal == nil {
		return "", errors.New("endorsement has no proposal")
	}
	return txIDFromProposal(end.Proposal)
}

func txIDFromProposal(prop *peer.Proposal) (string, error) {
	if prop == nil {
		return "", errors.New("proposal is nil")
	}
	hdr, err := protoutil.UnmarshalHeader(prop.Header)
	if err != nil {
		return "", fmt.Errorf("unmarshal proposal header: %w", err)
	}
	chdr, err := protoutil.UnmarshalChannelHeader(hdr.ChannelHeader)
	if err != nil {
		return "", fmt.Errorf("unmarshal channel header: %w", err)
	}
	return chdr.TxId, nil
}

func argString(args [][]byte, index int) string {
	if index < 0 || index >= len(args) {
		return ""
	}
	return string(args[index])
}

func operationName(submit bool) string {
	if submit {
		return "invoke"
	}
	return "query"
}

func compatibilityDecorations(namespace, chaincodeName, chaincodeVersion string) map[string][]byte {
	return map[string][]byte{
		"compat.decorator":         []byte("orchestrator"),
		"compat.namespace":         []byte(namespace),
		"compat.chaincode_name":    []byte(chaincodeName),
		"compat.chaincode_version": []byte(chaincodeVersion),
	}
}

func idempotencyIdentity(channel, namespace string, req InvocationRequest, submit bool) (string, string) {
	digest := requestDigest(channel, namespace, req, submit)
	if req.ClientTxID != "" {
		return req.ClientTxID, digest
	}
	return digest, digest
}

func requestDigest(channel, namespace string, req InvocationRequest, submit bool) string {
	h := sha256.New()
	writeDigestString(h, "v2")
	writeDigestString(h, operationName(submit))
	writeDigestString(h, req.ClientTxID)
	writeDigestString(h, channel)
	writeDigestString(h, namespace)
	writeDigestString(h, req.ChaincodeName)
	writeDigestString(h, req.ChaincodeVersion)
	writeDigestString(h, fmt.Sprintf("%t", req.IsInit))
	writeDigestString(h, req.Function)
	writeDigestBytes(h, req.ClientCreator)
	for _, arg := range req.Args {
		writeDigestString(h, arg)
	}
	writeDigestByteMap(h, req.ClientTransient)
	return hex.EncodeToString(h.Sum(nil))
}

func writeDigestString(h interface{ Write([]byte) (int, error) }, value string) {
	writeDigestBytes(h, []byte(value))
}

func writeDigestBytes(h interface{ Write([]byte) (int, error) }, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(value)
}

func writeDigestByteMap(h interface{ Write([]byte) (int, error) }, values map[string][]byte) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		writeDigestString(h, key)
		writeDigestBytes(h, values[key])
	}
}

func protocolOrDefault(protocol string) string {
	if protocol == "" {
		return "fabric-x"
	}
	return protocol
}

func loggerOrNoop(logger sdk.Logger) sdk.Logger {
	if logger == nil {
		return sdk.NoOpLogger{}
	}
	return logger
}

func cloneSignedProposal(prop *peer.SignedProposal) *peer.SignedProposal {
	if prop == nil {
		return nil
	}
	return &peer.SignedProposal{
		ProposalBytes: append([]byte(nil), prop.ProposalBytes...),
		Signature:     append([]byte(nil), prop.Signature...),
	}
}

func cloneByteMap(in map[string][]byte) map[string][]byte {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(in))
	for key, value := range in {
		out[key] = append([]byte(nil), value...)
	}
	return out
}

// Forwarded evaluations may execute once or fail; this restriction grants no permissions.
const gatewayEvaluateLocalOnly = "x-compat-evaluate-local-only"

// Evaluate implements the standard Gateway protobuf API using the same execution
// path as custom Query.
func (s *Service) Evaluate(ctx context.Context, request *gateway.EvaluateRequest) (*gateway.EvaluateResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "an evaluate request is required")
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.requestTimeout())
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	req, err := s.gatewayInvocation(ctx, request.ChannelId, request.TransactionId, request.ProposedTransaction)
	if err != nil {
		return nil, err
	}
	if req.IsInit {
		return nil, status.Error(codes.InvalidArgument, "initialization cannot be evaluated")
	}
	if _, err := s.validateInitState(ctx, req, req.Namespace, GRPCOperationQuery); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	localOnly := false
	for _, value := range metadata.ValueFromIncomingContext(ctx, gatewayEvaluateLocalOnly) {
		if value != "true" {
			return nil, status.Error(codes.InvalidArgument, "invalid evaluation routing restriction")
		}
		localOnly = true
	}
	localMSP := s.cfg.Identity.MspID
	for _, target := range request.TargetOrganizations {
		if target == "" || strings.TrimSpace(target) != target {
			return nil, status.Error(codes.InvalidArgument, "target organization must be a nonempty MSP ID")
		}
	}
	if len(request.TargetOrganizations) == 0 || containsMSP(request.TargetOrganizations, localMSP) {
		result, err := s.evaluate(ctx, req)
		if err != nil {
			return nil, invocationRPCError(ctx.Err(), err)
		}
		return gatewayEvaluationResponse(result.Response, localMSP, "")
	}
	if localOnly {
		return nil, status.Error(codes.FailedPrecondition, "forwarded evaluation reached an Orchestrator outside the target organization")
	}
	return s.evaluateRemote(ctx, request, req)
}

// Endorse executes and collects endorsements without ordering or waiting for
// finality. The SDK signs the returned envelope separately before Submit.
func (s *Service) Endorse(ctx context.Context, request *gateway.EndorseRequest) (*gateway.EndorseResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "an endorse request is required")
	}
	ctx, cancel := context.WithTimeout(ctx, s.cfg.requestTimeout())
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	req, err := s.gatewayInvocation(ctx, request.ChannelId, request.TransactionId, request.ProposedTransaction)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	for _, target := range request.EndorsingOrganizations {
		if target == "" || strings.TrimSpace(target) != target {
			return nil, status.Error(codes.InvalidArgument, "endorsing organization must be a nonempty MSP ID")
		}
		if !seen[target] {
			req.EndorsingOrganizations = append(req.EndorsingOrganizations, target)
			seen[target] = true
		}
	}
	result, err := s.endorseInvocation(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Error(codes.Aborted, err.Error())
	}
	if err := gatewayResponseError(result.Local.Response, s.cfg.Identity.MspID, ""); err != nil {
		return nil, err
	}
	proposal, err := protoutil.UnmarshalProposal(request.ProposedTransaction.ProposalBytes)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	prepared, err := createGatewayTransaction(proposal, result.Request.ChaincodeVersion, result.Endorsement)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("prepare endorsed transaction: %v", err))
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &gateway.EndorseResponse{PreparedTransaction: prepared}, nil
}

// Submit verifies the signed prepared transaction and reuses the same ordering
// stage as custom Invoke. It acknowledges ordering without observing finality.
func (s *Service) Submit(ctx context.Context, request *gateway.SubmitRequest) (_ *gateway.SubmitResponse, err error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.requestTimeout())
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	identity, err := s.authenticateGatewayTransaction(request)
	if err != nil {
		return nil, err
	}
	prepared, err := parseGatewayTransaction(request.PreparedTransaction)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	req := prepared.Request
	if err := s.proposalAuth.authorize(identity, req.ChaincodeName, req.Namespace); err != nil {
		return nil, status.Error(codes.PermissionDenied, err.Error())
	}
	digestBytes := sha256.Sum256(request.PreparedTransaction.Payload)
	digest := "gateway-submit:" + hex.EncodeToString(digestBytes[:])
	s.triggerReconciliation()
	if record, found, err := s.idempotency.existing(req.ClientTxID, digest); err != nil {
		return nil, status.Error(codes.AlreadyExists, err.Error())
	} else if found {
		if err := s.replayOrderingSubmission(ctx, record); err != nil {
			return nil, submissionRPCError(ctx, err)
		}
		return &gateway.SubmitResponse{}, nil
	}
	req, err = s.selectLifecycleDefinition(ctx, req)
	if err != nil {
		return nil, invocationRPCError(ctx.Err(), err)
	}
	if req.definition == nil {
		return nil, status.Error(codes.NotFound, "submitted chaincode has no committed definition")
	}
	definition := req.definition.Definition
	if definition.Version != prepared.Binding.ChaincodeVersion || definition.Sequence != prepared.Binding.Sequence {
		return nil, status.Error(codes.FailedPrecondition, "endorsed lifecycle revision is no longer active; obtain a fresh endorsement")
	}
	initDefinition, err := s.validateInitState(ctx, req, req.Namespace, GRPCOperationEndorse)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err := s.validateSubmissionEndorsements(ctx, prepared); err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Error(codes.FailedPrecondition, fmt.Sprintf("invalid native endorsements: %v", err))
	}
	record, first, err := s.idempotency.begin(req.ClientTxID, digest)
	if err != nil {
		return nil, status.Error(codes.AlreadyExists, err.Error())
	}
	if !first {
		if err := s.replayOrderingSubmission(ctx, record); err != nil {
			return nil, submissionRPCError(ctx, err)
		}
		return &gateway.SubmitResponse{}, nil
	}
	out := responseFromPeer(req.ClientTxID, prepared.Endorsement.Responses[0].Response)
	out.IdempotencyKey = req.ClientTxID
	defer func() { s.idempotency.complete(record, out, err) }()
	s.idempotency.markExecuted(record, req.ClientTxID)
	var afterCommit func(context.Context) error
	if initDefinition != nil {
		definition := *initDefinition
		afterCommit = func(ctx context.Context) error { return s.markInitializedAfterCommit(ctx, definition) }
	}
	out, err = s.submitTransaction(ctx, prepared.Endorsement, out, req.ClientTxID, record, afterCommit)
	if err != nil {
		return nil, submissionRPCError(ctx, err)
	}
	return &gateway.SubmitResponse{}, nil
}

func submissionRPCError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return status.FromContextError(err).Err()
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	var orderingError *orderingSubmissionError
	if errors.As(err, &orderingError) && !orderingError.uncertain {
		return status.Error(codes.Aborted, err.Error())
	}
	return status.Error(codes.Unavailable, err.Error())
}

// CommitStatus observes only the requested transaction. The final result can
// come from committed state or a live notification, independently of whether
// this instance endorsed or submitted the transaction.
func (s *Service) CommitStatus(ctx context.Context, signed *gateway.SignedCommitStatusRequest) (*gateway.CommitStatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.cfg.requestTimeout())
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	request, err := s.authenticateGatewayCommitStatus(signed)
	if err != nil {
		return nil, err
	}
	result, err := s.waitForTransactionStatus(ctx, request.TransactionId)
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	s.refreshObservedFinality(ctx, result)
	return &gateway.CommitStatusResponse{
		Result:      gatewayValidationCode(result.Status),
		BlockNumber: result.Ref.BlockNum,
	}, nil
}

// ChaincodeEvents owns a committed-block subscription for this reader only.
// It needs neither local execution state nor a transaction finality observer.
func (s *Service) ChaincodeEvents(signed *gateway.SignedChaincodeEventsRequest, stream gateway.Gateway_ChaincodeEventsServer) error {
	ctx := stream.Context()
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	request, err := s.authenticateGatewayChaincodeEvents(signed)
	if err != nil {
		return err
	}
	if s.blockQuery == nil || s.blockDelivery == nil {
		return status.Error(codes.FailedPrecondition, "committed block services are not configured")
	}
	setupCtx, cancel := context.WithTimeout(ctx, s.cfg.requestTimeout())
	defer cancel()
	info, err := s.blockQuery.GetBlockchainInfo(setupCtx, &emptypb.Empty{})
	if err != nil {
		return invocationRPCError(ctx.Err(), err)
	}
	if info == nil {
		return status.Error(codes.DataLoss, "block service returned no ledger information")
	}
	start := info.GetHeight()
	if seek := request.GetStartPosition().GetSpecified(); seek != nil {
		start = seek.Number
	}
	if request.AfterTransactionId != "" {
		block, err := s.blockQuery.GetBlockByTxID(setupCtx, &committerpb.TxID{TxId: request.AfterTransactionId})
		if err == nil {
			if block.GetHeader() == nil {
				return status.Error(codes.DataLoss, "checkpoint block has no header")
			}
			start = block.Header.Number
			events, err := committedChaincodeEvents(block, request.ChannelId)
			if err != nil {
				return status.Error(codes.DataLoss, err.Error())
			}
			found := false
			for _, event := range events {
				if event.TxId == request.AfterTransactionId {
					found = true
					break
				}
			}
			if !found {
				return status.Error(codes.FailedPrecondition, "checkpoint must identify a committed event with chaincode identity metadata")
			}
		} else if status.Code(err) != codes.NotFound {
			return invocationRPCError(ctx.Err(), err)
		}
	}
	cancel()
	// Read from the current tip when the requested block is in the future. The
	// backend cursor waits for new blocks; the callback skips to the requested one.
	deliverFrom := min(start, info.GetHeight())
	s.logger.Infof("Gateway events subscribed channel=%s chaincode=%s start_block=%d", request.ChannelId, request.ChaincodeId, start)
	previous := request.AfterTransactionId
	err = s.streamCommittedBlocks(ctx, deliverFrom, func(block *common.Block) error {
		if block.Header.Number < start {
			return nil
		}
		events, err := committedChaincodeEvents(block, request.ChannelId)
		if err != nil {
			return status.Error(codes.DataLoss, err.Error())
		}
		var matching []*peer.ChaincodeEvent
		for _, event := range events {
			if previous != "" {
				if event.TxId == previous {
					previous = ""
				}
				continue
			}
			if event.ChaincodeId == request.ChaincodeId {
				matching = append(matching, event)
			}
		}
		if len(matching) == 0 {
			return nil
		}
		return stream.Send(&gateway.ChaincodeEventsResponse{BlockNumber: block.Header.Number, Events: matching})
	})
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Unavailable, fmt.Sprint(err))
}

// Native validation codes are a different enum from Fabric TxValidationCode.
// Map their meanings explicitly; no numeric cast may turn an abort into VALID.
func gatewayValidationCode(result committerpb.Status) peer.TxValidationCode {
	switch result {
	case committerpb.Status_COMMITTED:
		return peer.TxValidationCode_VALID
	case committerpb.Status_ABORTED_SIGNATURE_INVALID, committerpb.Status_MALFORMED_MISSING_SIGNATURE,
		committerpb.Status_MALFORMED_NAMESPACE_POLICY_INVALID:
		return peer.TxValidationCode_ENDORSEMENT_POLICY_FAILURE
	case committerpb.Status_ABORTED_MVCC_CONFLICT:
		return peer.TxValidationCode_MVCC_READ_CONFLICT
	case committerpb.Status_REJECTED_DUPLICATE_TX_ID:
		return peer.TxValidationCode_DUPLICATE_TXID
	case committerpb.Status_MALFORMED_BAD_ENVELOPE, committerpb.Status_MALFORMED_BAD_ENVELOPE_PAYLOAD:
		return peer.TxValidationCode_BAD_PAYLOAD
	case committerpb.Status_MALFORMED_MISSING_TX_ID, committerpb.Status_MALFORMED_TX_ID_CONFLICT:
		return peer.TxValidationCode_BAD_PROPOSAL_TXID
	case committerpb.Status_MALFORMED_UNSUPPORTED_ENVELOPE_PAYLOAD:
		return peer.TxValidationCode_UNSUPPORTED_TX_PAYLOAD
	case committerpb.Status_MALFORMED_EMPTY_NAMESPACES, committerpb.Status_MALFORMED_DUPLICATE_NAMESPACE,
		committerpb.Status_MALFORMED_NAMESPACE_ID_INVALID, committerpb.Status_MALFORMED_EMPTY_KEY,
		committerpb.Status_MALFORMED_DUPLICATE_KEY_IN_READ_WRITE_SET:
		return peer.TxValidationCode_BAD_RWSET
	case committerpb.Status_MALFORMED_BLIND_WRITES_NOT_ALLOWED, committerpb.Status_MALFORMED_NO_WRITES:
		return peer.TxValidationCode_INVALID_WRITESET
	case committerpb.Status_MALFORMED_CONFIG_TX_INVALID:
		return peer.TxValidationCode_INVALID_CONFIG_TRANSACTION
	default:
		return peer.TxValidationCode_INVALID_OTHER_REASON
	}
}

func gatewayChaincodeRoute(name string) (string, string, error) {
	chaincode, namespace, ok := strings.Cut(name, ":")
	if !ok || chaincode == "" || namespace == "" || strings.Contains(namespace, ":") ||
		strings.TrimSpace(chaincode) != chaincode || strings.TrimSpace(namespace) != namespace {
		return "", "", fmt.Errorf("Gateway chaincode name must be <name>:<namespace>")
	}
	return chaincode, namespace, nil
}

func containsMSP(msps []string, target string) bool {
	for _, msp := range msps {
		if msp == target {
			return true
		}
	}
	return false
}

func gatewayEvaluationResponse(response *peer.Response, mspID, address string) (*gateway.EvaluateResponse, error) {
	if err := gatewayResponseError(response, mspID, address); err != nil {
		return nil, err
	}
	// Protobuf bytes preserve binary payloads. No invocation JSON or string/base64
	// conversion is involved in the Gateway response.
	return &gateway.EvaluateResponse{Result: proto.Clone(response).(*peer.Response)}, nil
}

func gatewayResponseError(response *peer.Response, mspID, address string) error {
	if response == nil {
		return status.Error(codes.Internal, "execution returned no chaincode response")
	}
	if response.Status < 200 || response.Status >= 400 {
		s := status.New(codes.Unknown, fmt.Sprintf("chaincode response %d: %s", response.Status, response.Message))
		if detailed, err := s.WithDetails(&gateway.ErrorDetail{MspId: mspID, Address: address, Message: response.Message}); err == nil {
			s = detailed
		}
		return s.Err()
	}
	return nil
}
