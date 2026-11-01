/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package helper

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-committer/utils/serve"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	efab "github.com/hyperledger/fabric-x-sdk/endorsement/fabric"
	efabx "github.com/hyperledger/fabric-x-sdk/endorsement/fabricx"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"github.com/hyperledger/fabric-x-sdk/network"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// Service implements the Fabric Endorser gRPC API without maintaining a local
// ledger or synchronized world-state database. Committed reads are delegated to
// Fabric-X Query Service and writes are captured only for the current invocation.
type Service struct {
	peer.EndorserServer
	healthcheck *health.Server
	channel     string
	queryPeer   *network.Peer
	stateReader StateReader
	executors   map[string]Executor
	builder     endorsement.Builder
	signer      sdk.Signer
	logger      sdk.Logger
}

type clientProposalContextKey struct{}

// ClientProposalContext carries the original client proposal fields needed by
// chaincode identity and binding APIs.
type ClientProposalContext struct {
	Creator        []byte
	Nonce          []byte
	SignedProposal *peer.SignedProposal
	Decorations    map[string][]byte
	IsInit         bool
	// NamespaceVersion is resolved by the Orchestrator, independently of the
	// original Fabric proposal's chaincode version.
	NamespaceVersion uint64
}

// WithClientProposal carries original client proposal fields through the
// in-process helper path so chaincode sees the invoking client's context.
func WithClientProposal(ctx context.Context, proposal ClientProposalContext) context.Context {
	if len(proposal.Creator) == 0 && len(proposal.Nonce) == 0 && proposal.SignedProposal == nil && len(proposal.Decorations) == 0 && proposal.NamespaceVersion == 0 && !proposal.IsInit {
		return ctx
	}
	return context.WithValue(ctx, clientProposalContextKey{}, ClientProposalContext{
		Creator:          append([]byte(nil), proposal.Creator...),
		Nonce:            append([]byte(nil), proposal.Nonce...),
		SignedProposal:   cloneSignedProposal(proposal.SignedProposal),
		Decorations:      cloneByteMap(proposal.Decorations),
		NamespaceVersion: proposal.NamespaceVersion,
		IsInit:           proposal.IsInit,
	})
}

func clientProposalFromContext(ctx context.Context) ClientProposalContext {
	proposal, _ := ctx.Value(clientProposalContextKey{}).(ClientProposalContext)
	return ClientProposalContext{
		Creator:          append([]byte(nil), proposal.Creator...),
		Nonce:            append([]byte(nil), proposal.Nonce...),
		SignedProposal:   cloneSignedProposal(proposal.SignedProposal),
		Decorations:      cloneByteMap(proposal.Decorations),
		NamespaceVersion: proposal.NamespaceVersion,
		IsInit:           proposal.IsInit,
	}
}

// Executor is the application execution boundary. For this prototype, the
// executor is expected to become the peer-side shim/CCAAS bridge.
type Executor interface {
	Execute(context.Context, *ExecutionContext, endorsement.Invocation) (endorsement.ExecutionResult, ExecutionMetadata, error)
}

// ExecutionMetadata is helper/orchestrator metadata produced during execution.
// It is deliberately kept outside endorsement.ExecutionResult so Fabric-X
// endorsement payloads remain clean.
type ExecutionMetadata struct {
	QueryView *committerpb.View
}

// VersionedValue is the committed value returned by the Fabric-X Query Service.
type VersionedValue struct {
	Value   []byte
	Version *blocks.Version
}

// StateReader supplies committed state and invocation-local Query Service views.
type StateReader interface {
	BeginView(context.Context) (*committerpb.View, error)
	EndView(context.Context, *committerpb.View) error
	GetState(ctx context.Context, view *committerpb.View, namespace, key string) (VersionedValue, error)
}

// QueryServiceStateReader reads committed state directly from Fabric-X Query Service.
type QueryServiceStateReader struct {
	client committerpb.QueryServiceClient
}

// NewQueryServiceStateReader creates a Query Service backed StateReader.
func NewQueryServiceStateReader(client committerpb.QueryServiceClient) *QueryServiceStateReader {
	return &QueryServiceStateReader{client: client}
}

// BeginView allocates a repeatable-read view. Query Service establishes its
// database snapshot when the first query uses the view and caps its lifetime.
func (r *QueryServiceStateReader) BeginView(ctx context.Context) (*committerpb.View, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	params := &committerpb.ViewParameters{IsoLevel: committerpb.IsoLevel_REPEATABLE_READ}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		params.TimeoutMilliseconds = uint64(max(1, remaining.Milliseconds()))
	}
	return r.client.BeginView(ctx, params)
}

// EndView releases the invocation's read-only database snapshot.
func (r *QueryServiceStateReader) EndView(ctx context.Context, view *committerpb.View) error {
	_, err := r.client.EndView(ctx, view)
	return err
}

// GetState reads one key from Query Service and converts the row version into
// the Fabric-X SDK read-version shape.
func (r *QueryServiceStateReader) GetState(ctx context.Context, view *committerpb.View, namespace, key string) (VersionedValue, error) {
	rows, err := r.client.GetRows(ctx, &committerpb.Query{
		View: view,
		Namespaces: []*committerpb.QueryNamespace{{
			NsId: namespace,
			Keys: [][]byte{[]byte(key)},
		}},
	})
	if err != nil {
		return VersionedValue{}, err
	}

	for _, ns := range rows.Namespaces {
		if ns.NsId != namespace {
			continue
		}
		for _, row := range ns.Rows {
			if string(row.Key) != key {
				continue
			}
			return VersionedValue{
				Value:   append([]byte(nil), row.Value...),
				Version: &blocks.Version{BlockNum: row.Version},
			}, nil
		}
	}

	// Missing key: Fabric-style simulation records the read with a nil version.
	return VersionedValue{}, nil
}

// ExecutionContext is the in-memory state of a single chaincode invocation. It
// is not durable storage. It records the dependencies and effects that will be
// converted into a Fabric-X transaction after execution.
type ExecutionContext struct {
	mu             sync.Mutex
	reader         StateReader
	namespace      string
	queryView      *committerpb.View
	viewOwned      bool
	closed         bool
	readErr        error
	isInit         bool
	clientCreator  []byte
	clientNonce    []byte
	signedProposal *peer.SignedProposal
	decorations    map[string][]byte
	reads          map[string]blocks.KVRead
	writes         map[string]blocks.KVWrite
}

// NewExecutionContext creates the transient context used by one invocation.
func NewExecutionContext(reader StateReader, namespace string) *ExecutionContext {
	return NewExecutionContextWithView(reader, namespace, nil)
}

// NewExecutionContextWithView creates a context using a borrowed Query Service
// view. Its caller owns that view's cleanup. Production invocations allocate
// their own views lazily through NewExecutionContext.
func NewExecutionContextWithView(reader StateReader, namespace string, view *committerpb.View) *ExecutionContext {
	return &ExecutionContext{
		reader:    reader,
		namespace: namespace,
		queryView: view,
		reads:     make(map[string]blocks.KVRead),
		writes:    make(map[string]blocks.KVWrite),
	}
}

// Namespace returns the Fabric-X namespace backing this invocation.
func (c *ExecutionContext) Namespace() string {
	return c.namespace
}

// IsInit selects the shim entry point for this invocation. It is immutable
// during execution and comes from the proposal's ChaincodeInput.IsInit field.
func (c *ExecutionContext) IsInit() bool {
	return c.isInit
}

// QueryView returns the Query Service view used for committed-state reads.
func (c *ExecutionContext) QueryView() *committerpb.View {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queryView
}

// SetQueryView attaches a borrowed view before the first state read. An
// invocation cannot replace its view after reading or after a read failure.
func (c *ExecutionContext) SetQueryView(view *committerpb.View) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.queryView != nil || c.readErr != nil {
		return errors.New("execution query view is already initialized or closed")
	}
	c.queryView = view
	return nil
}

// Err returns the first state-read failure. The helper must not endorse an
// execution that ignored a failed or expired snapshot read.
func (c *ExecutionContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readErr
}

// Close releases a view created by this context, at most once. It never creates
// a view for an invocation that did not read state.
func (c *ExecutionContext) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if !c.viewOwned {
		return nil
	}
	return c.reader.EndView(ctx, c.queryView)
}

// ClientCreator returns the serialized creator from the original client proposal.
func (c *ExecutionContext) ClientCreator() []byte {
	return append([]byte(nil), c.clientCreator...)
}

// SetClientCreator records the original client proposal creator for shim identity APIs.
func (c *ExecutionContext) SetClientCreator(creator []byte) {
	c.clientCreator = append([]byte(nil), creator...)
}

// ClientNonce returns the original client proposal nonce.
func (c *ExecutionContext) ClientNonce() []byte {
	return append([]byte(nil), c.clientNonce...)
}

// SetClientNonce records the original client proposal nonce for binding semantics.
func (c *ExecutionContext) SetClientNonce(nonce []byte) {
	c.clientNonce = append([]byte(nil), nonce...)
}

// ClientSignedProposal returns the original client-signed proposal.
func (c *ExecutionContext) ClientSignedProposal() *peer.SignedProposal {
	return cloneSignedProposal(c.signedProposal)
}

// SetClientSignedProposal records the original client-signed proposal for shim APIs.
func (c *ExecutionContext) SetClientSignedProposal(prop *peer.SignedProposal) {
	c.signedProposal = cloneSignedProposal(prop)
}

// Decorations returns peer-style proposal decorations for the chaincode stub.
func (c *ExecutionContext) Decorations() map[string][]byte {
	return cloneByteMap(c.decorations)
}

// SetDecorations records peer-style proposal decorations for the chaincode stub.
func (c *ExecutionContext) SetDecorations(decorations map[string][]byte) {
	c.decorations = cloneByteMap(decorations)
}

// GetState reads committed state and records its version for MVCC validation.
// Pending writes and deletions do not affect reads during Fabric simulation.
func (c *ExecutionContext) GetState(ctx context.Context, key string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, errors.New("execution context is closed")
	}
	if c.readErr != nil {
		return nil, c.readErr
	}
	if err := ctx.Err(); err != nil {
		c.readErr = err
		return nil, err
	}
	if c.reader == nil {
		c.readErr = errors.New("committed-state reader is not configured")
		return nil, c.readErr
	}
	if c.queryView == nil {
		view, err := c.reader.BeginView(ctx)
		if err != nil {
			c.readErr = fmt.Errorf("begin execution query view: %w", err)
			return nil, c.readErr
		}
		if view == nil || view.Id == "" {
			c.readErr = errors.New("query service returned an empty execution view")
			return nil, c.readErr
		}
		c.queryView, c.viewOwned = view, true
	}
	value, err := c.reader.GetState(ctx, c.queryView, c.namespace, key)
	if err != nil {
		c.readErr = fmt.Errorf("read committed state in execution query view: %w", err)
		return nil, c.readErr
	}
	c.reads[key] = blocks.KVRead{Key: key, Version: value.Version}
	return append([]byte(nil), value.Value...), nil
}

// PutState records a pending write for this invocation only.
func (c *ExecutionContext) PutState(key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes[key] = blocks.KVWrite{Key: key, Value: append([]byte(nil), value...)}
}

// DelState records a pending delete for this invocation only.
func (c *ExecutionContext) DelState(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes[key] = blocks.KVWrite{Key: key, IsDelete: true}
}

// Result returns the captured read/write set for endorsement.
func (c *ExecutionContext) Result() blocks.ReadWriteSet {
	c.mu.Lock()
	defer c.mu.Unlock()
	rws := blocks.ReadWriteSet{
		Reads:  make([]blocks.KVRead, 0, len(c.reads)),
		Writes: make([]blocks.KVWrite, 0, len(c.writes)),
	}
	for _, r := range c.reads {
		rws.Reads = append(rws.Reads, r)
	}
	for _, w := range c.writes {
		rws.Writes = append(rws.Writes, w)
	}
	return rws
}

// ServiceConfig is the minimal configuration needed to construct a stateless helper.
type ServiceConfig struct {
	ChannelID    string
	Protocol     string // only "fabric-x" is supported by this scaffold
	QueryService network.PeerConf
}

// New creates a new Service instance, loading the Fabric MSP identity from mspDir and mspID.
func New(cfg ServiceConfig, mspDir, mspID string, executors map[string]Executor, logger sdk.Logger) (*Service, error) {
	signer, err := identity.SignerFromMSP(mspDir, mspID)
	if err != nil {
		return nil, fmt.Errorf("failed to load signer: %w", err)
	}
	return NewWithSigner(cfg, signer, executors, logger)
}

// NewWithSigner creates a new Service with an already-constructed signer.
func NewWithSigner(cfg ServiceConfig, signer sdk.Signer, executors map[string]Executor, logger sdk.Logger) (*Service, error) {
	if cfg.Protocol != "fabric-x" {
		return nil, fmt.Errorf("protocol %q is not supported by stateless helper", cfg.Protocol)
	}

	queryPeer, err := network.NewPeer(cfg.QueryService)
	if err != nil {
		return nil, fmt.Errorf("query service: %w", err)
	}

	s := &Service{
		healthcheck: serve.DefaultHealthCheckService(),
		channel:     cfg.ChannelID,
		queryPeer:   queryPeer,
		stateReader: NewQueryServiceStateReader(committerpb.NewQueryServiceClient(queryPeer.Connection())),
		executors:   executors,
		builder:     efabx.NewEndorsementBuilder(signer),
		signer:      signer,
		logger:      logger,
	}

	logger.Infof("stateless helper initialized")
	return s, nil
}

// RegisterService implements serve.Service.
func (s *Service) RegisterService(servers serve.Servers) {
	peer.RegisterEndorserServer(servers.GRPC, s)
	healthgrpc.RegisterHealthServer(servers.GRPC, s.healthcheck)
	reflection.Register(servers.GRPC)
	s.logger.Infof("service handlers registered")
}

// Run implements serve.Service. No background world-state synchronizer is
// started because all state reads are delegated to Query Service.
func (s *Service) Run(ctx context.Context) error {
	s.logger.Infof("stateless helper running")
	<-ctx.Done()
	s.logger.Infof("stopping stateless helper")
	return s.Close()
}

// Close releases helper-owned outbound resources.
func (s *Service) Close() error {
	if s.queryPeer != nil {
		err := s.queryPeer.Close()
		s.queryPeer = nil
		return err
	}
	return nil
}

// ProcessProposal is the Fabric peer-style API for incoming requests.
func (s *Service) ProcessProposal(ctx context.Context, prop *peer.SignedProposal) (*peer.ProposalResponse, error) {
	inv, err := efab.Parse(prop, time.Now())
	if err != nil {
		s.logger.Infof("tx=%s err=%s", inv.TxID, err)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if inv.Channel != s.channel {
		s.logger.Infof("tx=%s err=wrong channel: %s", inv.TxID, inv.Channel)
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("channel must be %s", s.channel))
	}

	executor, ok := s.executors[inv.Namespace]
	if !ok {
		executor, ok = s.executors["*"]
	}
	if !ok {
		s.logger.Infof("tx=%s err=unknown namespace: %s", inv.TxID, inv.Namespace)
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("unknown namespace: %s", inv.Namespace))
	}
	if inv.ChaincodeVersion == "" {
		inv.ChaincodeVersion = "1.0"
	}

	s.logger.Infof("tx=%s helper proposal received channel=%s namespace=%s version=%s fn=%s args=%d",
		inv.TxID, inv.Channel, inv.Namespace, inv.ChaincodeVersion, argString(inv.Args, 0), len(inv.Args)-1)
	execCtx := NewExecutionContext(s.stateReader, inv.Namespace)
	execCtx.isInit, err = ProposalIsInit(inv.Proposal)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	defer func() {
		// The caller may have canceled or exhausted its deadline. Cleanup still
		// gets one bounded attempt; Query Service also expires abandoned views.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if err := execCtx.Close(cleanupCtx); err != nil {
			s.logger.Warnf("tx=%s close execution query view: %s", inv.TxID, err)
		}
	}()
	proposal := clientProposalFromContext(ctx)
	inv.NsVersion = proposal.NamespaceVersion
	creator := proposal.Creator
	if len(creator) == 0 {
		creator = inv.Creator
	}
	nonce := proposal.Nonce
	if len(nonce) == 0 {
		nonce = inv.Nonce
	}
	signedProposal := proposal.SignedProposal
	if signedProposal == nil {
		signedProposal = prop
	}
	original, err := protoutil.UnmarshalProposal(signedProposal.ProposalBytes)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	originalInit, err := ProposalIsInit(original)
	if err != nil || originalInit != execCtx.IsInit() {
		return nil, status.Error(codes.InvalidArgument, "helper init intent differs from the original proposal")
	}
	execCtx.SetClientCreator(creator)
	execCtx.SetClientNonce(nonce)
	execCtx.SetClientSignedProposal(signedProposal)
	execCtx.SetDecorations(proposal.Decorations)
	s.logger.Debugf("tx=%s original client proposal attached creator_bytes=%d nonce_bytes=%d signature_bytes=%d decorations=%d",
		inv.TxID, len(creator), len(nonce), signedProposalSignatureLen(signedProposal), len(proposal.Decorations))

	res, meta, err := executor.Execute(ctx, execCtx, inv)
	if err != nil {
		s.logger.Warnf("tx=%s chaincode execution failed: %s", inv.TxID, err)
		return nil, status.Error(codes.Internal, err.Error())
	}
	if err := execCtx.Err(); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if meta.QueryView != nil {
		s.logger.Debugf("tx=%s query-view=%s", inv.TxID, meta.QueryView.Id)
	}
	rws := execCtx.Result()
	s.logger.Infof("tx=%s chaincode execution completed status=%d reads=%d writes=%d payload_bytes=%d event_bytes=%d",
		inv.TxID, res.Status, len(rws.Reads), len(rws.Writes), len(res.Payload), len(res.Event))
	if res.Status < 200 || res.Status >= 400 {
		// A failed execution must not yield signatures over its pending writes.
		// Return its chaincode error without a Fabric-X endorsement payload.
		return &peer.ProposalResponse{Response: res.Response()}, nil
	}

	s.logger.Infof("tx=%s building Fabric-X endorsement namespace=%s reads=%d writes=%d event_bytes=%d",
		inv.TxID, inv.Namespace, len(rws.Reads), len(rws.Writes), len(res.Event))
	end, err := s.builder.Endorse(inv, res)
	if err != nil {
		s.logger.Warnf("tx=%s endorsement build failed: %s", inv.TxID, err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("endorsement: %s", err.Error()))
	}
	if err := bindChaincodeMetadata(s.signer, end, original, inv.Namespace, proposal.Decorations); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("bind chaincode metadata: %v", err))
	}

	payload := "empty"
	if len(end.Response.Payload) < 512 {
		payload = string(end.Response.Payload)
	} else {
		payload = fmt.Sprintf("(%db)", len(end.Response.Payload))
	}
	s.logger.Infof("tx=%s Fabric-X endorsement built status=%d namespace=%s fn=%s args=%d tx_payload=%s endorsement_present=%t",
		inv.TxID,
		end.Response.Status,
		inv.Namespace,
		string(inv.Args[0]),
		len(inv.Args)-1,
		payload,
		end.Endorsement != nil,
	)

	return end, nil
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

func cloneSignedProposal(prop *peer.SignedProposal) *peer.SignedProposal {
	if prop == nil {
		return nil
	}
	return &peer.SignedProposal{
		ProposalBytes: append([]byte(nil), prop.ProposalBytes...),
		Signature:     append([]byte(nil), prop.Signature...),
	}
}

func signedProposalSignatureLen(prop *peer.SignedProposal) int {
	if prop == nil {
		return 0
	}
	return len(prop.Signature)
}

func argString(args [][]byte, index int) string {
	if index < 0 || index >= len(args) {
		return ""
	}
	return string(args[index])
}

// WaitForReady implements connection.Service. There is no local catch-up state,
// so readiness is bounded to process startup and Query Service connection setup.
func (s *Service) WaitForReady(ctx context.Context) bool {
	return true
}
