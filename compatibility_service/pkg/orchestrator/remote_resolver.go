// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"compatibility_service/pkg/ccresolver"
	"compatibility_service/pkg/config"
	"compatibility_service/pkg/lifecycle"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/network"
)

const defaultRemoteResolverTimeout = 5 * time.Second

// RemoteOrchestratorResolverConfig controls lazy remote orchestrator discovery.
// Static entries are checked first. If no static entry matches, dynamic
// discovery calls a user-owned resolver service and caches successful contacts.
type RemoteOrchestratorResolverConfig struct {
	// Mode is kept for older configs. Resolver behavior is structural:
	// static and dynamic can coexist and static is checked first.
	Mode    string                                   `mapstructure:"mode"`
	Static  []RemoteOrchestratorConfig               `mapstructure:"static"`
	Dynamic *DynamicRemoteOrchestratorResolverConfig `mapstructure:"dynamic"`
}

type DynamicRemoteOrchestratorResolverConfig struct {
	MSPIDs     []string         `mapstructure:"msp-ids"`
	Endpoint   *config.Endpoint `mapstructure:"endpoint"`
	TLS        config.TLSConfig `mapstructure:"tls"`
	ContactTLS config.TLSConfig `mapstructure:"contact-tls"`
	Timeout    time.Duration    `mapstructure:"timeout"`
}

type remoteOrchestratorResolveRequest struct {
	RequesterMSPID   string
	TargetMSPID      string
	ChannelID        string
	Namespace        string
	ChaincodeName    string
	ChaincodeVersion string
	Sequence         int64
}

type remoteOrchestratorResolver interface {
	AvailableMSPs() map[string]struct{}
	Resolve(context.Context, remoteOrchestratorResolveRequest) (*OrchestratorContact, bool, error)
	RemotePeers(context.Context, remoteOrchestratorResolveRequest) ([]lifecycle.RemotePeer, error)
	Close() error
}

func newRemoteOrchestratorResolver(cfg RemoteOrchestratorResolverConfig, legacyStatic []RemoteOrchestratorConfig, logger sdk.Logger) (remoteOrchestratorResolver, error) {
	if logger == nil {
		logger = sdk.NoOpLogger{}
	}
	staticEntries := append([]RemoteOrchestratorConfig(nil), legacyStatic...)
	staticEntries = append(staticEntries, cfg.Static...)

	var static *staticRemoteOrchestratorResolver
	if len(staticEntries) > 0 {
		var err error
		static, err = newStaticRemoteOrchestratorResolver(staticEntries, logger)
		if err != nil {
			return nil, err
		}
	}
	var dynamic *dynamicRemoteOrchestratorResolver
	if cfg.Dynamic != nil {
		var err error
		dynamic, err = newDynamicRemoteOrchestratorResolver(cfg.Dynamic, logger)
		if err != nil {
			if static != nil {
				static.Close() //nolint:errcheck
			}
			return nil, err
		}
	}
	if static == nil && dynamic == nil {
		return newStaticRemoteOrchestratorResolver(nil, logger)
	}
	return &remoteOrchestratorResolvers{static: static, dynamic: dynamic}, nil
}

type remoteOrchestratorResolvers struct {
	static  *staticRemoteOrchestratorResolver
	dynamic *dynamicRemoteOrchestratorResolver
}

func (r *remoteOrchestratorResolvers) AvailableMSPs() map[string]struct{} {
	out := make(map[string]struct{})
	if r.static != nil {
		for mspID := range r.static.AvailableMSPs() {
			out[mspID] = struct{}{}
		}
	}
	if r.dynamic != nil {
		for mspID := range r.dynamic.AvailableMSPs() {
			out[mspID] = struct{}{}
		}
	}
	return out
}

func (r *remoteOrchestratorResolvers) Resolve(ctx context.Context, req remoteOrchestratorResolveRequest) (*OrchestratorContact, bool, error) {
	if r.static != nil {
		contact, ok, err := r.static.Resolve(ctx, req)
		if err != nil || ok {
			return contact, ok, err
		}
	}
	if r.dynamic != nil {
		return r.dynamic.Resolve(ctx, req)
	}
	return nil, false, nil
}

func (r *remoteOrchestratorResolvers) RemotePeers(ctx context.Context, req remoteOrchestratorResolveRequest) ([]lifecycle.RemotePeer, error) {
	seen := make(map[string]struct{})
	var peers []lifecycle.RemotePeer
	if r.static != nil {
		staticPeers, err := r.static.RemotePeers(ctx, req)
		if err != nil {
			return nil, err
		}
		for _, peer := range staticPeers {
			seen[peer.MSPID()] = struct{}{}
			peers = append(peers, peer)
		}
	}
	if r.dynamic != nil {
		dynamicPeers, err := r.dynamic.RemotePeers(ctx, req)
		if err != nil {
			return nil, err
		}
		for _, peer := range dynamicPeers {
			if _, ok := seen[peer.MSPID()]; ok {
				continue
			}
			peers = append(peers, peer)
		}
	}
	return peers, nil
}

func (r *remoteOrchestratorResolvers) Close() error {
	var err error
	if r.static != nil {
		err = r.static.Close()
	}
	if r.dynamic != nil {
		if dynamicErr := r.dynamic.Close(); dynamicErr != nil && err == nil {
			err = dynamicErr
		}
	}
	return err
}

type staticRemoteOrchestratorResolver struct {
	contacts map[string]*OrchestratorContact
}

func newStaticRemoteOrchestratorResolver(configs []RemoteOrchestratorConfig, logger sdk.Logger) (*staticRemoteOrchestratorResolver, error) {
	contacts, err := newOrchestratorContacts(configs, logger)
	if err != nil {
		return nil, err
	}
	return &staticRemoteOrchestratorResolver{contacts: contacts}, nil
}

func (r *staticRemoteOrchestratorResolver) AvailableMSPs() map[string]struct{} {
	out := make(map[string]struct{}, len(r.contacts))
	for mspID := range r.contacts {
		out[mspID] = struct{}{}
	}
	return out
}

func (r *staticRemoteOrchestratorResolver) Resolve(_ context.Context, req remoteOrchestratorResolveRequest) (*OrchestratorContact, bool, error) {
	contact, ok := r.contacts[req.TargetMSPID]
	return contact, ok, nil
}

func (r *staticRemoteOrchestratorResolver) RemotePeers(context.Context, remoteOrchestratorResolveRequest) ([]lifecycle.RemotePeer, error) {
	return lifecycleRemotes(r.contacts), nil
}

func (r *staticRemoteOrchestratorResolver) Close() error {
	return closeOrchestratorContacts(r.contacts)
}

type dynamicRemoteOrchestratorResolver struct {
	resolverAddress string
	resolverPeer    *network.Peer
	client          *ccresolver.Client
	timeout         time.Duration
	requesterMSPID  string
	targetMSPIDs    []string
	contactTLS      config.TLSConfig
	logger          sdk.Logger

	mu       sync.Mutex
	contacts map[string]*OrchestratorContact
}

func newDynamicRemoteOrchestratorResolver(cfg *DynamicRemoteOrchestratorResolverConfig, logger sdk.Logger) (*dynamicRemoteOrchestratorResolver, error) {
	if cfg == nil || cfg.Endpoint == nil {
		return nil, fmt.Errorf("remote-orchestrator-resolver.dynamic.endpoint is required")
	}
	if len(cfg.MSPIDs) == 0 {
		return nil, fmt.Errorf("remote-orchestrator-resolver.dynamic.msp-ids is required")
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultRemoteResolverTimeout
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
		return nil, fmt.Errorf("remote orchestrator resolver %s: %w", address, err)
	}
	return &dynamicRemoteOrchestratorResolver{
		resolverAddress: address,
		resolverPeer:    peer,
		client:          ccresolver.NewClient(peer.Connection()),
		timeout:         timeout,
		targetMSPIDs:    append([]string(nil), cfg.MSPIDs...),
		contactTLS:      cfg.ContactTLS,
		logger:          logger,
		contacts:        make(map[string]*OrchestratorContact),
	}, nil
}

func (r *dynamicRemoteOrchestratorResolver) AvailableMSPs() map[string]struct{} {
	out := make(map[string]struct{}, len(r.targetMSPIDs))
	for _, mspID := range r.targetMSPIDs {
		out[mspID] = struct{}{}
	}
	return out
}

func (r *dynamicRemoteOrchestratorResolver) Resolve(ctx context.Context, req remoteOrchestratorResolveRequest) (*OrchestratorContact, bool, error) {
	if !r.targetAllowed(req.TargetMSPID) {
		return nil, false, nil
	}
	if contact := r.cached(req.TargetMSPID); contact != nil {
		return contact, true, nil
	}

	resolveCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	out, err := r.client.Resolve(resolveCtx, &ccresolver.ResolveRequest{
		Operation:    ccresolver.OperationRemoteOrchestrator,
		RequesterMSP: req.RequesterMSPID,
		TargetMSP:    req.TargetMSPID,
		ChannelID:    req.ChannelID,
		Namespace:    req.Namespace,
		Name:         req.ChaincodeName,
		Version:      req.ChaincodeVersion,
		Sequence:     req.Sequence,
	})
	if err != nil {
		return nil, false, fmt.Errorf("dynamic remote orchestrator resolver grpc: %w", err)
	}
	if out == nil || !out.Found {
		return nil, false, nil
	}
	endpoint, err := endpointFromAddress(out.Address)
	if err != nil {
		return nil, false, fmt.Errorf("dynamic remote orchestrator resolver response: %w", err)
	}
	contact, err := newOrchestratorContact(RemoteOrchestratorConfig{
		MSPID:    req.TargetMSPID,
		Endpoint: endpoint,
		TLS:      r.contactTLS,
	}, r.logger)
	if err != nil {
		return nil, false, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if existing := r.contacts[req.TargetMSPID]; existing != nil {
		contact.Close() //nolint:errcheck
		return existing, true, nil
	}
	r.contacts[req.TargetMSPID] = contact
	r.logger.Infof("remote orchestrator resolver dynamic grpc match requester_msp=%s target_msp=%s endpoint=%s resolver=%s",
		req.RequesterMSPID, req.TargetMSPID, out.Address, r.resolverAddress)
	return contact, true, nil
}

func (r *dynamicRemoteOrchestratorResolver) RemotePeers(ctx context.Context, req remoteOrchestratorResolveRequest) ([]lifecycle.RemotePeer, error) {
	peers := make([]lifecycle.RemotePeer, 0, len(r.targetMSPIDs))
	for _, mspID := range r.targetMSPIDs {
		targetReq := req
		targetReq.TargetMSPID = mspID
		contact, ok, err := r.Resolve(ctx, targetReq)
		if err != nil {
			return nil, err
		}
		if ok {
			peers = append(peers, contact)
		}
	}
	return peers, nil
}

func (r *dynamicRemoteOrchestratorResolver) Close() error {
	r.mu.Lock()
	contacts := r.contacts
	r.contacts = nil
	r.mu.Unlock()
	if r.resolverPeer != nil {
		_ = r.resolverPeer.Close()
	}
	return closeOrchestratorContacts(contacts)
}

func (r *dynamicRemoteOrchestratorResolver) cached(mspID string) *OrchestratorContact {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.contacts[mspID]
}

func (r *dynamicRemoteOrchestratorResolver) targetAllowed(mspID string) bool {
	for _, candidate := range r.targetMSPIDs {
		if candidate == mspID {
			return true
		}
	}
	return false
}

func endpointFromAddress(address string) (*config.Endpoint, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid address %q: %w", address, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return nil, fmt.Errorf("invalid port in address %q: %w", address, err)
	}
	return &config.Endpoint{Host: host, Port: port}, nil
}
