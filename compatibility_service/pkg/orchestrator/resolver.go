// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"fmt"
	"time"

	"compatibility_service/pkg/ccresolver"
	"compatibility_service/pkg/config"
	"compatibility_service/pkg/lifecycle"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/network"
)

const defaultResolverTimeout = 5 * time.Second

// ChaincodeResolverConfig controls lazy org-local CCAAS connection resolution.
// Static entries are checked first. If no static entry matches, dynamic
// resolution calls a user-provided endpoint and caches at the lifecycle layer.
type ChaincodeResolverConfig struct {
	// Mode is kept for older configs. Resolver behavior is structural:
	// static and dynamic can coexist and static is checked first.
	Mode    string                            `mapstructure:"mode"`
	Static  []StaticChaincodeConnectionConfig `mapstructure:"static"`
	Dynamic *DynamicChaincodeResolverConfig   `mapstructure:"dynamic"`
}

type StaticChaincodeConnectionConfig struct {
	MSPID    string           `mapstructure:"msp-id"`
	Name     string           `mapstructure:"name"`
	Version  string           `mapstructure:"version"`
	Sequence int64            `mapstructure:"sequence"`
	Address  string           `mapstructure:"address"`
	Endpoint *config.Endpoint `mapstructure:"endpoint"`
	TLS      config.TLSConfig `mapstructure:"tls"`
}

type DynamicChaincodeResolverConfig struct {
	Endpoint *config.Endpoint `mapstructure:"endpoint"`
	TLS      config.TLSConfig `mapstructure:"tls"`
	Timeout  time.Duration    `mapstructure:"timeout"`
}

type chaincodeResolverRequest struct {
	MSPID      string
	Definition lifecycle.ChaincodeDefinition
}

type chaincodeConnectionResolver interface {
	Resolve(context.Context, chaincodeResolverRequest) (lifecycle.ResolvedChaincodeConnection, bool, error)
}

func newChaincodeConnectionResolver(cfg ChaincodeResolverConfig, logger sdk.Logger) (chaincodeConnectionResolver, error) {
	if logger == nil {
		logger = sdk.NoOpLogger{}
	}
	var static *staticChaincodeResolver
	if len(cfg.Static) > 0 {
		var err error
		static, err = newStaticChaincodeResolver(cfg.Static, logger)
		if err != nil {
			return nil, err
		}
	}
	var dynamic *grpcChaincodeResolver
	if cfg.Dynamic != nil {
		var err error
		dynamic, err = newGRPCChaincodeResolver(cfg.Dynamic, logger)
		if err != nil {
			return nil, err
		}
	}
	if static == nil && dynamic == nil {
		return nil, nil
	}
	return &chaincodeConnectionResolvers{static: static, dynamic: dynamic}, nil
}

type chaincodeConnectionResolvers struct {
	static  *staticChaincodeResolver
	dynamic *grpcChaincodeResolver
}

func (r *chaincodeConnectionResolvers) Resolve(ctx context.Context, req chaincodeResolverRequest) (lifecycle.ResolvedChaincodeConnection, bool, error) {
	if r.static != nil {
		conn, ok, err := r.static.Resolve(ctx, req)
		if err != nil || ok {
			return conn, ok, err
		}
	}
	if r.dynamic != nil {
		return r.dynamic.Resolve(ctx, req)
	}
	return lifecycle.ResolvedChaincodeConnection{}, false, nil
}

func (r *chaincodeConnectionResolvers) Close() error {
	if r == nil || r.dynamic == nil {
		return nil
	}
	return r.dynamic.Close()
}

type staticChaincodeResolver struct {
	entries []StaticChaincodeConnectionConfig
	logger  sdk.Logger
}

func newStaticChaincodeResolver(entries []StaticChaincodeConnectionConfig, logger sdk.Logger) (*staticChaincodeResolver, error) {
	if logger == nil {
		logger = sdk.NoOpLogger{}
	}
	for i, entry := range entries {
		if entry.Name == "" {
			return nil, fmt.Errorf("chaincode-resolver.static[%d].name is required", i)
		}
		if entry.Version == "" {
			return nil, fmt.Errorf("chaincode-resolver.static[%d].version is required", i)
		}
		if chaincodeResolverAddress(entry) == "" {
			return nil, fmt.Errorf("chaincode-resolver.static[%d].address or endpoint is required", i)
		}
		if mode := entry.TLS.Mode; mode != "" && mode != "none" {
			return nil, fmt.Errorf("chaincode-resolver.static[%d] tls mode %q is not supported by the current CCAAS shim connector", i, mode)
		}
	}
	return &staticChaincodeResolver{entries: entries, logger: logger}, nil
}

func (r *staticChaincodeResolver) Resolve(_ context.Context, req chaincodeResolverRequest) (lifecycle.ResolvedChaincodeConnection, bool, error) {
	def := req.Definition
	for _, entry := range r.entries {
		if entry.MSPID != "" && entry.MSPID != req.MSPID {
			continue
		}
		if entry.Name != def.Name || entry.Version != def.Version {
			continue
		}
		if entry.Sequence != 0 && entry.Sequence != def.Sequence {
			continue
		}
		mode := entry.TLS.Mode
		if mode == "" {
			mode = "none"
		}
		conn := lifecycle.ResolvedChaincodeConnection{
			MSPID:    req.MSPID,
			Name:     def.Name,
			Version:  def.Version,
			Sequence: def.Sequence,
			Address:  chaincodeResolverAddress(entry),
			TLSMode:  mode,
		}
		r.logger.Infof("chaincode resolver static match msp=%s name=%s version=%s sequence=%d endpoint=%s",
			req.MSPID, def.Name, def.Version, def.Sequence, conn.Address)
		return conn, true, nil
	}
	return lifecycle.ResolvedChaincodeConnection{}, false, nil
}

func chaincodeResolverAddress(entry StaticChaincodeConnectionConfig) string {
	if entry.Address != "" {
		return entry.Address
	}
	if entry.Endpoint == nil {
		return ""
	}
	return entry.Endpoint.Address()
}

type grpcChaincodeResolver struct {
	address string
	peer    *network.Peer
	client  *ccresolver.Client
	timeout time.Duration
	logger  sdk.Logger
}

func newGRPCChaincodeResolver(cfg *DynamicChaincodeResolverConfig, logger sdk.Logger) (*grpcChaincodeResolver, error) {
	if cfg == nil || cfg.Endpoint == nil {
		return nil, fmt.Errorf("chaincode-resolver.dynamic.endpoint is required")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultResolverTimeout
	}
	address := cfg.Endpoint.Address()
	peer, err := network.NewPeer(network.PeerConf{
		Address: address,
		TLS: network.TLSConfig{
			Mode:        cfg.TLS.Mode,
			CertPath:    cfg.TLS.CertPath,
			KeyPath:     cfg.TLS.KeyPath,
			CACertPaths: cfg.TLS.CACertPaths,
			ServerName:  cfg.TLS.ServerName,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("chaincode resolver %s: %w", address, err)
	}
	return &grpcChaincodeResolver{
		address: address,
		peer:    peer,
		client:  ccresolver.NewClient(peer.Connection()),
		timeout: timeout,
		logger:  logger,
	}, nil
}

func (r *grpcChaincodeResolver) Resolve(ctx context.Context, req chaincodeResolverRequest) (lifecycle.ResolvedChaincodeConnection, bool, error) {
	def := req.Definition
	resolveCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	out, err := r.client.Resolve(resolveCtx, &ccresolver.ResolveRequest{
		Operation: ccresolver.OperationChaincodeResolution,
		MSPID:     req.MSPID,
		Name:      def.Name,
		Version:   def.Version,
		Sequence:  def.Sequence,
	})
	if err != nil {
		return lifecycle.ResolvedChaincodeConnection{}, false, fmt.Errorf("dynamic chaincode resolver grpc: %w", err)
	}
	if out == nil || !out.Found {
		return lifecycle.ResolvedChaincodeConnection{}, false, nil
	}
	if out.Address == "" {
		return lifecycle.ResolvedChaincodeConnection{}, false, fmt.Errorf("dynamic chaincode resolver response missing address")
	}
	mode := out.TLSMode
	if mode == "" {
		mode = "none"
	}
	if mode != "none" {
		return lifecycle.ResolvedChaincodeConnection{}, false, fmt.Errorf("dynamic chaincode resolver returned unsupported tls mode %q", mode)
	}
	r.logger.Infof("chaincode resolver dynamic grpc match msp=%s name=%s version=%s sequence=%d endpoint=%s resolver=%s",
		req.MSPID, def.Name, def.Version, def.Sequence, out.Address, r.address)
	return lifecycle.ResolvedChaincodeConnection{
		MSPID:    req.MSPID,
		Name:     def.Name,
		Version:  def.Version,
		Sequence: def.Sequence,
		Address:  out.Address,
		TLSMode:  mode,
	}, true, nil
}

func (r *grpcChaincodeResolver) Close() error {
	if r == nil || r.peer == nil {
		return nil
	}
	return r.peer.Close()
}
