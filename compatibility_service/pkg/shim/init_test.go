// SPDX-License-Identifier: Apache-2.0

package shim

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func TestInitializationDispatchesINITWithOriginalProposal(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	chaincode := &handshakeChaincodeServer{received: make(chan peer.ChaincodeMessage_Type, 2), isInit: true}
	peer.RegisterChaincodeServer(server, chaincode)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	connector := &Connector{endpoint: "bufnet", dialOptions: []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
	}}
	args := [][]byte{[]byte("transfer"), []byte("asset1")}
	decorations := map[string][]byte{"compat.decorator": []byte("orchestrator")}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := connector.Execute(ctx, newTestState(), Invocation{
		TxID: "tx1", ChannelID: "channelqc4", Namespace: "0", Args: args, IsInit: true,
		Creator: []byte("creator"), Nonce: []byte("nonce"), Decorations: decorations,
		SignedProposal: testSignedProposal(t, "tx1", "channelqc4", "0", args, decorations, []byte("creator"), []byte("nonce"), []byte("client-signature"), true),
	})
	if err != nil || result.Status != 200 || string(result.Payload) != "done" {
		t.Fatalf("INIT execution failed: result=%+v error=%v", result, err)
	}
}
