// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"testing"

	"compatibility_service/pkg/config"
)

func TestAdditionalLocalSignersAreNotLifecyclePeers(t *testing.T) {
	resolver, err := newStaticRemoteOrchestratorResolver([]RemoteOrchestratorConfig{
		{MSPID: "org-0", Endpoint: &config.Endpoint{Host: "127.0.0.1", Port: 9103}, TLS: config.TLSConfig{Mode: "none"}},
		{MSPID: "org-1", Endpoint: &config.Endpoint{Host: "127.0.0.1", Port: 9202}, TLS: config.TLSConfig{Mode: "none"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resolver.Close() })
	contacts, found, err := resolver.Resolve(context.Background(), remoteOrchestratorResolveRequest{RequesterMSPID: "org-0", TargetMSPID: "org-0"})
	if err != nil || !found || len(contacts) != 1 {
		t.Fatalf("additional local endorsement signer not resolved: %v %v %v", contacts, found, err)
	}
	peers, err := resolver.RemotePeers(context.Background(), remoteOrchestratorResolveRequest{RequesterMSPID: "org-0"})
	if err != nil || len(peers) != 1 || peers[0].MSPID() != "org-1" {
		t.Fatalf("lifecycle peers = %v, error = %v; expected only org-1", peers, err)
	}
	// A dynamic resolver should also skip its own organization for lifecycle
	// calls. No resolver RPC is necessary for that case.
	dynamic := &dynamicRemoteOrchestratorResolver{targetMSPIDs: []string{"org-0"}}
	peers, err = dynamic.RemotePeers(context.Background(), remoteOrchestratorResolveRequest{RequesterMSPID: "org-0"})
	if err != nil || len(peers) != 0 {
		t.Fatalf("dynamic local lifecycle peers = %v, error = %v", peers, err)
	}
}
