/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package api

import (
	"context"
	"fmt"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-committer/utils/serve"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
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
	logger      sdk.Logger
}

// Executor is the application execution boundary. For this prototype, the
// executor is expected to become the peer-side shim/CCAAS bridge.
type Executor interface {
	Execute(context.Context, *ExecutionContext, endorsement.Invocation) (endorsement.ExecutionResult, ExecutionMetadata, error)
}

// ExecutionMetadata is helper/coordinator metadata produced during execution.
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

// StateReader is the stateless state dependency required by ExecutionContext.
type StateReader interface {
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
	reader    StateReader
	namespace string
	queryView *committerpb.View
	reads     map[string]blocks.KVRead
	writes    map[string]blocks.KVWrite
}

// NewExecutionContext creates the transient context used by one invocation.
func NewExecutionContext(reader StateReader, namespace string) *ExecutionContext {
	return NewExecutionContextWithView(reader, namespace, nil)
}

// NewExecutionContextWithView creates the transient context using a Query
// Service view supplied by a coordinator.
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

// QueryView returns the Query Service view used for committed-state reads.
func (c *ExecutionContext) QueryView() *committerpb.View {
	return c.queryView
}

// SetQueryView records the Query Service view used for this invocation.
func (c *ExecutionContext) SetQueryView(view *committerpb.View) {
	c.queryView = view
}

// GetState is the method the future shim handler should call for GetState
// messages. It checks the per-invocation overlay first, then Query Service.
func (c *ExecutionContext) GetState(ctx context.Context, key string) ([]byte, error) {
	if w, ok := c.writes[key]; ok {
		if w.IsDelete {
			return nil, nil
		}
		return append([]byte(nil), w.Value...), nil
	}

	value, err := c.reader.GetState(ctx, c.queryView, c.namespace, key)
	if err != nil {
		return nil, err
	}
	c.reads[key] = blocks.KVRead{Key: key, Version: value.Version}
	return append([]byte(nil), value.Value...), nil
}

// PutState records a pending write for this invocation only.
func (c *ExecutionContext) PutState(key string, value []byte) {
	c.writes[key] = blocks.KVWrite{Key: key, Value: append([]byte(nil), value...)}
}

// DelState records a pending delete for this invocation only.
func (c *ExecutionContext) DelState(key string) {
	c.writes[key] = blocks.KVWrite{Key: key, IsDelete: true}
}

// Result returns the captured read/write set for endorsement.
func (c *ExecutionContext) Result() blocks.ReadWriteSet {
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
	if cfg.Protocol != "" && cfg.Protocol != "fabric-x" {
		return nil, fmt.Errorf("protocol %q is not supported by stateless chaincode helper", cfg.Protocol)
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
		logger:      logger,
	}

	logger.Infof("stateless chaincode helper initialized")
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
	if s.queryPeer != nil {
		return s.queryPeer.Close()
	}
	return nil
}

// ProcessProposal is the Fabric peer-style API for incoming requests.
func (s *Service) ProcessProposal(ctx context.Context, prop *peer.SignedProposal) (*peer.ProposalResponse, error) {
	inv, err := endorsement.Parse(prop, time.Now())
	if err != nil {
		s.logger.Infof("tx=%s err=%s", inv.TxID, err)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if inv.Channel != s.channel {
		s.logger.Infof("tx=%s err=wrong channel: %s", inv.TxID, inv.Channel)
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("channel must be %s", s.channel))
	}

	executor, ok := s.executors[inv.CCID.Name]
	if !ok {
		s.logger.Infof("tx=%s err=unknown namespace: %s", inv.TxID, inv.CCID.Name)
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("unknown namespace: %s", inv.CCID.Name))
	}
	if inv.CCID.Version == "" {
		inv.CCID.Version = "1.0"
	}

	execCtx := NewExecutionContext(s.stateReader, inv.CCID.Name)
	res, meta, err := executor.Execute(ctx, execCtx, inv)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if meta.QueryView != nil {
		s.logger.Debugf("tx=%s query-view=%s", inv.TxID, meta.QueryView.Id)
	}

	end, err := s.builder.Endorse(inv, res)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("endorsement: %s", err.Error()))
	}

	var payload string
	if len(end.Response.Payload) < 512 {
		payload = string(end.Response.Payload)
	} else {
		payload = fmt.Sprintf("(%db)", len(end.Response.Payload))
	}
	s.logger.Infof("tx=%s st=%d ns=%s fn=%s args=%d res=%s",
		inv.TxID,
		end.Response.Status,
		inv.CCID.Name,
		string(inv.Args[0]),
		len(inv.Args)-1,
		payload,
	)

	return end, nil
}

// WaitForReady implements connection.Service. There is no local catch-up state,
// so readiness is bounded to process startup and Query Service connection setup.
func (s *Service) WaitForReady(ctx context.Context) bool {
	return true
}
