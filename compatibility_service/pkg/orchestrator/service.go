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
	"time"

	"compatibility_service/pkg/config"
	"compatibility_service/pkg/helper"
	"compatibility_service/pkg/shim"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-committer/utils/serve"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"github.com/hyperledger/fabric-x-sdk/network"
	nfab "github.com/hyperledger/fabric-x-sdk/network/fabric"
	nfabx "github.com/hyperledger/fabric-x-sdk/network/fabricx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	GRPCOperationMetadata = "x-compat-operation"
	GRPCOperationInvoke   = "invoke"
	GRPCOperationQuery    = "query"
	GRPCOperationEndorse  = "endorse"
)

// Config contains the orchestrator wiring.
type Config struct {
	ChannelID       string                        `mapstructure:"channel-id"`
	Namespace       string                        `mapstructure:"namespace"`
	Protocol        string                        `mapstructure:"protocol"`
	Server          *serve.ServerConfig           `mapstructure:"server"`
	Identity        *config.IdentityConfig        `mapstructure:"identity"`
	QueryService    config.ClientConfig           `mapstructure:"query-service"`
	ChaincodeSvc    config.ChaincodeServiceConfig `mapstructure:"chaincode-service"`
	Orderer         *config.ClientConfig          `mapstructure:"orderer"`
	NotificationSvc *config.ClientConfig          `mapstructure:"notification-service"`
	RemoteOrgs      []RemoteOrchestratorConfig    `mapstructure:"remote-orchestrators"`
	WaitAfterSubmit time.Duration                 `mapstructure:"wait-after-submit"`
	RequestTimeout  time.Duration                 `mapstructure:"request-timeout"`
	FinalityTimeout time.Duration                 `mapstructure:"finality-timeout"`
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
	ClientTxID           string               `json:"-"`
	ClientCreator        []byte               `json:"-"`
	ClientNonce          []byte               `json:"-"`
	ClientSignedProposal *peer.SignedProposal `json:"-"`
	ClientTransient      map[string][]byte    `json:"-"`
	IdempotencyKey       string               `json:"-"`
	RequestDigest        string               `json:"-"`
	Namespace            string               `json:"namespace,omitempty"`
	Function             string               `json:"function,omitempty"`
	Args                 []string             `json:"args,omitempty"`
}

// InvocationResponse is returned by query and invoke.
type InvocationResponse struct {
	TxID             string          `json:"tx_id,omitempty"`
	Status           int32           `json:"status"`
	Message          string          `json:"message,omitempty"`
	Payload          string          `json:"payload,omitempty"`
	PayloadBase64    string          `json:"payload_base64,omitempty"`
	Submitted        bool            `json:"submitted"`
	CommitStatus     string          `json:"commit_status,omitempty"`
	BlockNum         uint64          `json:"block_num,omitempty"`
	TxNum            uint32          `json:"tx_num,omitempty"`
	IdempotencyKey   string          `json:"idempotency_key,omitempty"`
	IdempotentReplay bool            `json:"idempotent_replay,omitempty"`
	ChaincodeEvent   *ChaincodeEvent `json:"chaincode_event,omitempty"`
}

// ChaincodeEvent is the client-facing committed event shape.
type ChaincodeEvent struct {
	ChaincodeID   string `json:"chaincode_id,omitempty"`
	TxID          string `json:"tx_id,omitempty"`
	EventName     string `json:"event_name,omitempty"`
	Payload       string `json:"payload,omitempty"`
	PayloadBase64 string `json:"payload_base64,omitempty"`
}

// Service is the orchestrator process.
type Service struct {
	cfg          Config
	signer       sdk.Signer
	helper       *helper.Service
	queryPeer    *network.Peer
	queryService committerpb.QueryServiceClient
	policies     *namespacePolicyResolver
	submitter    *network.FabricSubmitter
	notifier     *network.Peer
	notify       committerpb.NotifierClient
	idempotency  *idempotencyStore
	logger       sdk.Logger
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

	signer, err := identity.SignerFromMSP(cfg.Identity.MSPDir, cfg.Identity.MspID)
	if err != nil {
		return nil, fmt.Errorf("load identity: %w", err)
	}

	queryPeer, err := network.NewPeer(cfg.QueryService.ToPeerConf())
	if err != nil {
		return nil, fmt.Errorf("query service: %w", err)
	}
	queryService := committerpb.NewQueryServiceClient(queryPeer.Connection())
	policies := newNamespacePolicyResolver(queryService)

	shimConnector, err := shim.NewConnector(shim.Config{Endpoint: cfg.ChaincodeSvc.Address()})
	if err != nil {
		queryPeer.Close() //nolint:errcheck
		return nil, fmt.Errorf("create shim connector: %w", err)
	}
	shimConnector.SetLogger(shimLogger)

	helperCfg := helper.ServiceConfig{
		ChannelID:    cfg.ChannelID,
		Protocol:     cfg.Protocol,
		QueryService: cfg.QueryService.ToPeerConf(),
	}
	executors := map[string]helper.Executor{
		"*": helper.NewChaincodeServiceExecutor(shimConnector),
	}
	helper, err := helper.NewWithSigner(helperCfg, signer, executors, helperLogger)
	if err != nil {
		queryPeer.Close() //nolint:errcheck
		return nil, fmt.Errorf("create internal helper: %w", err)
	}

	ordererConfs := []network.OrdererConf{cfg.Orderer.ToOrdererConf()}
	var submitter *network.FabricSubmitter
	switch cfg.Protocol {
	case "fabric":
		submitter, err = nfab.NewSubmitter(ctx, ordererConfs, signer, cfg.WaitAfterSubmit, orchestratorLogger)
	case "fabric-x", "":
		submitter, err = nfabx.NewSubmitter(ctx, ordererConfs, signer, cfg.WaitAfterSubmit, orchestratorLogger)
	default:
		helper.Close()    //nolint:errcheck
		queryPeer.Close() //nolint:errcheck
		return nil, fmt.Errorf("unknown protocol %q: must be \"fabric\" or \"fabric-x\"", cfg.Protocol)
	}
	if err != nil {
		helper.Close()    //nolint:errcheck
		queryPeer.Close() //nolint:errcheck
		return nil, fmt.Errorf("create submitter: %w", err)
	}

	var notifier *network.Peer
	var notify committerpb.NotifierClient
	if cfg.NotificationSvc != nil {
		notifier, err = network.NewPeer(cfg.NotificationSvc.ToPeerConf())
		if err != nil {
			submitter.Close() //nolint:errcheck
			helper.Close()    //nolint:errcheck
			queryPeer.Close() //nolint:errcheck
			return nil, fmt.Errorf("notification service: %w", err)
		}
		notify = committerpb.NewNotifierClient(notifier.Connection())
	}

	orchestratorLogger.Infof("orchestrator initialized channel=%s default_namespace=%s protocol=%s helper=in-process request_timeout=%s finality_timeout=%s",
		cfg.ChannelID, cfg.Namespace, protocolOrDefault(cfg.Protocol), cfg.requestTimeout(), cfg.finalityTimeout())
	return &Service{
		cfg:          cfg,
		signer:       signer,
		helper:       helper,
		queryPeer:    queryPeer,
		queryService: queryService,
		policies:     policies,
		submitter:    submitter,
		notifier:     notifier,
		notify:       notify,
		idempotency:  newIdempotencyStore(),
		logger:       orchestratorLogger,
	}, nil
}

// Validate checks the endpoints needed for the current single-service flow.
func (cfg Config) Validate() error {
	var errs []error
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
	if cfg.ChaincodeSvc.Endpoint == nil {
		errs = append(errs, errors.New("chaincode-service.endpoint is required"))
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
	healthgrpc.RegisterHealthServer(servers.GRPC, health.NewServer())
	reflection.Register(servers.GRPC)
	s.logger.Infof("orchestrator gRPC ProcessProposal registered")
}

// Close closes outbound connections held by the orchestrator.
func (s *Service) Close() error {
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
	return errors.Join(errs...)
}

// ProcessProposal accepts an MSP-signed Fabric proposal from the client.
func (s *Service) ProcessProposal(ctx context.Context, prop *peer.SignedProposal) (*peer.ProposalResponse, error) {
	inv, err := endorsement.Parse(prop, time.Now())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if inv.Channel != s.cfg.ChannelID {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("channel must be %s", s.cfg.ChannelID))
	}

	operation, err := operationFromContext(ctx)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	submit := operation == GRPCOperationInvoke

	req, err := requestFromProposal(inv)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	req.ClientTxID = inv.TxID
	req.ClientCreator = append([]byte(nil), inv.Creator...)
	req.ClientNonce = append([]byte(nil), inv.Nonce...)
	req.ClientSignedProposal = cloneSignedProposal(prop)
	s.logger.Infof("tx=%s orchestrator proposal received operation=%s channel=%s namespace=%s fn=%s args=%d",
		inv.TxID, operation, inv.Channel, req.Namespace, req.Function, len(req.Args))

	requestTimeout := s.cfg.requestTimeout()
	requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	s.logger.Debugf("tx=%s request deadline started timeout=%s", inv.TxID, requestTimeout)

	if operation == GRPCOperationEndorse {
		resp, err := s.EndorseOnly(requestCtx, req)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				s.logger.Warnf("tx=%s request deadline exceeded timeout=%s", inv.TxID, requestTimeout)
				return nil, status.Error(codes.DeadlineExceeded, "orchestrator request deadline exceeded")
			} else if errors.Is(err, context.Canceled) {
				return nil, status.Error(codes.Canceled, "orchestrator request canceled")
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		return resp, nil
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
// endorsement response without submitting to the orderer.
func (s *Service) EndorseOnly(ctx context.Context, req InvocationRequest) (*peer.ProposalResponse, error) {
	namespace, args, err := s.prepareInvocation(req)
	if err != nil {
		return nil, err
	}
	result, err := s.executeFresh(ctx, req, namespace, args)
	if err != nil {
		return nil, fmt.Errorf("helper endorsement failed: %w", err)
	}
	if len(result.Endorsement.Responses) == 0 || result.Endorsement.Responses[0] == nil {
		return nil, errors.New("helper returned no proposal response")
	}
	s.logger.Infof("tx=%s execute-only endorsement completed namespace=%s status=%d",
		result.TxID, namespace, result.Response.Status)
	return result.Endorsement.Responses[0], nil
}

// Execute runs one query or invoke through helper endorsement
// Invokes are packaged and submitted to Fabric-X.
func (s *Service) Execute(ctx context.Context, req InvocationRequest, submit bool) (out InvocationResponse, err error) {
	namespace, args, err := s.prepareInvocation(req)
	if err != nil {
		return InvocationResponse{}, err
	}

	var record *idempotencyRecord
	if submit {
		req.IdempotencyKey, req.RequestDigest = idempotencyIdentity(s.cfg.ChannelID, namespace, req, submit)
		var firstRequest bool
		record, firstRequest, err = s.idempotency.begin(req.IdempotencyKey, req.RequestDigest)
		if err != nil {
			return InvocationResponse{}, err
		}
		if !firstRequest {
			s.logger.Infof("idempotency_key=%s duplicate request detected; waiting for stored result", req.IdempotencyKey)
			out, err := s.idempotency.wait(ctx, record)
			out.IdempotentReplay = true
			return out, err
		}
		defer func() {
			s.idempotency.complete(record, out, err)
		}()
	}

	policy, err := s.resolveNamespacePolicy(ctx, namespace)
	if err != nil {
		return InvocationResponse{}, fmt.Errorf("resolve namespace policy: %w", err)
	}

	localResult, err := s.executeFresh(ctx, req, namespace, args)
	if err != nil {
		return InvocationResponse{}, fmt.Errorf("helper endorsement failed: %w", err)
	}
	if record != nil {
		s.idempotency.markExecuted(record, localResult.TxID, localResult.Endorsement)
	}

	resp := localResult.Response
	out = responseFromPeer(localResult.TxID, resp)
	out.IdempotencyKey = req.IdempotencyKey
	s.logger.Infof("tx=%s helper response status=%d payload_bytes=%d submit=%t",
		localResult.TxID, resp.Status, len(resp.Payload), submit)
	if resp.Status < 200 || resp.Status >= 400 {
		s.logger.Infof("tx=%s chaincode returned non-success status=%d message=%q", localResult.TxID, resp.Status, resp.Message)
		return out, nil
	}
	if !submit {
		s.logger.Infof("tx=%s query completed status=%d", localResult.TxID, resp.Status)
		return out, nil
	}

	remoteResults, err := s.requestRemoteOrchestratorsIfPolicyNeedsThem(ctx, policy, req, localResult)
	if err != nil {
		return out, err
	}
	if err := compareCanonicalResults(localResult, remoteResults); err != nil {
		return out, err
	}
	merged, err := mergeEndorsements(localResult, remoteResults)
	if err != nil {
		return out, err
	}

	out, err = s.submitAndWaitFinality(ctx, merged, out, localResult.TxID, record)
	return out, err
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

func (s *Service) executeHelper(ctx context.Context, namespace, nsVersion string, args [][]byte, clientProposal helper.ClientProposalContext) (sdk.Endorsement, error) {
	if s.helper == nil {
		return sdk.Endorsement{}, errors.New("internal helper is not configured")
	}
	prop := clientProposal.SignedProposal
	// temporary fallback for tests and stuff
	if prop == nil {
		var err error
		prop, err = network.NewSignedProposal(s.signer, s.cfg.ChannelID, namespace, nsVersion, args)
		if err != nil {
			return sdk.Endorsement{}, fmt.Errorf("create helper proposal: %w", err)
		}
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

	resp, err := s.helper.ProcessProposal(helper.WithClientProposal(ctx, clientProposal), prop)
	if err != nil {
		return sdk.Endorsement{}, fmt.Errorf("helper process proposal: %w", err)
	}
	if resp != nil && resp.Response != nil {
		s.logger.Infof("tx=%s helper proposal response received status=%d chaincode_payload_bytes=%d proposal_payload_bytes=%d endorsement_present=%t",
			txID, resp.Response.Status, len(resp.Response.Payload), len(resp.Payload), resp.Endorsement != nil)
	}
	return sdk.Endorsement{Proposal: proposal, Responses: []*peer.ProposalResponse{resp}}, nil
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

func requestFromProposal(inv endorsement.Invocation) (InvocationRequest, error) {
	if len(inv.Args) < 1 {
		return InvocationRequest{}, errors.New("orchestrator proposal requires function")
	}

	req := InvocationRequest{
		Function: string(inv.Args[0]),
	}
	if inv.CCID != nil {
		req.Namespace = inv.CCID.Name
	}
	if inv.Proposal != nil {
		cpp, err := protoutil.UnmarshalChaincodeProposalPayload(inv.Proposal.Payload)
		if err != nil {
			return InvocationRequest{}, fmt.Errorf("unmarshal proposal payload: %w", err)
		}
		req.ClientTransient = cloneByteMap(cpp.TransientMap)
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
	if len(tx.Metadata) <= 1 || len(tx.Metadata[1]) == 0 {
		return nil
	}

	event := &peer.ChaincodeEvent{}
	if err := proto.Unmarshal(tx.Metadata[1], event); err != nil {
		return nil
	}
	if event.TxId == "" {
		event.TxId = txID
	}
	return &ChaincodeEvent{
		ChaincodeID:   event.ChaincodeId,
		TxID:          event.TxId,
		EventName:     event.EventName,
		Payload:       string(event.Payload),
		PayloadBase64: base64.StdEncoding.EncodeToString(event.Payload),
	}
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

func compatibilityDecorations(namespace string) map[string][]byte {
	return map[string][]byte{
		"compat.decorator": []byte("orchestrator"),
		"compat.namespace": []byte(namespace),
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
