// SPDX-License-Identifier: Apache-2.0

//go:build e2e

package e2e_test

import (
	"context"
	"testing"
	"time"

	"compatibility_service/pkg/helper"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-sdk/network"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHelperViewsUseNativeQueryServiceAndCloseIndependently(t *testing.T) {
	root := projectRoot(t)
	ensureNetworkReady(t, root)
	h := &e2eHarness{root: root}
	peer, err := network.NewPeer(network.PeerConf{Address: "127.0.0.1:7001", TLS: network.TLSConfig{
		Mode: network.TLSModeMTLS, CertPath: h.orchestratorTLSCert(0), KeyPath: h.orchestratorTLSKey(0),
		CACertPaths: []string{h.peerMSPTLSCA(0)}, ServerName: "127.0.0.1",
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close() //nolint:errcheck
	client := committerpb.NewQueryServiceClient(peer.Connection())
	reader := helper.NewQueryServiceStateReader(client)
	first := helper.NewExecutionContext(reader, namespace)
	second := helper.NewExecutionContext(reader, namespace)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = first.Close(ctx)
		_ = second.Close(ctx)
	}()
	if first.QueryView() != nil || second.QueryView() != nil {
		t.Fatal("views opened before state was read")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	key := uniqueKey(t, "view-absence")
	for _, execution := range []*helper.ExecutionContext{first, second} {
		if value, err := execution.GetState(ctx, key); err != nil || value != nil {
			t.Fatalf("native snapshot read = %q, %v; want absent key", value, err)
		}
		rws := execution.Result()
		if len(rws.Reads) != 1 || rws.Reads[0].Version != nil {
			t.Fatalf("absent key dependency was not retained: %+v", rws)
		}
	}
	firstView, secondView := first.QueryView(), second.QueryView()
	if firstView.GetId() == "" || secondView.GetId() == "" || firstView.Id == secondView.Id {
		t.Fatalf("invocations did not receive independent handles: %v %v", firstView, secondView)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	_, err = client.GetRows(ctx, &committerpb.Query{View: firstView, Namespaces: []*committerpb.QueryNamespace{{
		NsId: namespace, Keys: [][]byte{[]byte(key)},
	}}})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("closed native view remains usable: %v", err)
	}
	if value, err := second.GetState(ctx, key); err != nil || value != nil {
		t.Fatalf("closing the first invocation affected the second: value=%q err=%v", value, err)
	}
	if err := second.Close(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log("native repeatable-read views created on first read, independently usable, and released by helper contexts")
}
