/*
SPDX-License-Identifier: Apache-2.0
*/

package shim

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func TestExecuteRunsCCAASMessageLoop(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	chaincode := &handshakeChaincodeServer{received: make(chan peer.ChaincodeMessage_Type, 2)}
	peer.RegisterChaincodeServer(server, chaincode)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	connector := &Connector{
		endpoint: "bufnet",
		dialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return listener.DialContext(ctx)
			}),
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state := newTestState()
	result, err := connector.Execute(ctx, state, Invocation{
		TxID:      "tx1",
		ChannelID: "channelqc4",
		Namespace: "0",
		Args:      [][]byte{[]byte("transfer"), []byte("asset1")},
		Creator:   []byte("creator"),
		Nonce:     []byte("nonce"),
		Decorations: map[string][]byte{
			"compat.decorator": []byte("orchestrator"),
		},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Status != http.StatusOK {
		t.Fatalf("status = %d, want %d", result.Status, http.StatusOK)
	}
	if string(result.Payload) != "done" {
		t.Fatalf("payload = %q, want done", string(result.Payload))
	}
	if string(result.Event) != "event-payload" {
		t.Fatalf("event = %q, want event-payload", string(result.Event))
	}
	if got := string(state.writes["asset2"]); got != "value2" {
		t.Fatalf("asset2 write = %q, want value2", got)
	}
	if !state.deletes["asset3"] {
		t.Fatal("asset3 delete was not captured")
	}

	want := []peer.ChaincodeMessage_Type{
		peer.ChaincodeMessage_REGISTERED,
		peer.ChaincodeMessage_READY,
	}
	for _, expected := range want {
		select {
		case actual := <-chaincode.received:
			if actual != expected {
				t.Fatalf("received message = %s, want %s", actual, expected)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s: %v", expected, ctx.Err())
		}
	}
}

type handshakeChaincodeServer struct {
	peer.UnimplementedChaincodeServer
	received chan peer.ChaincodeMessage_Type
}

func (s *handshakeChaincodeServer) Connect(stream peer.Chaincode_ConnectServer) error {
	payload, err := proto.Marshal(&peer.ChaincodeID{Name: "assetcc:hash"})
	if err != nil {
		return err
	}
	if err := stream.Send(&peer.ChaincodeMessage{
		Type:    peer.ChaincodeMessage_REGISTER,
		Payload: payload,
	}); err != nil {
		return err
	}

	for range 2 {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		s.received <- msg.Type
	}

	txMsg, err := stream.Recv()
	if err != nil {
		return err
	}
	if txMsg.Type != peer.ChaincodeMessage_TRANSACTION {
		return fmt.Errorf("message = %s, want TRANSACTION", txMsg.Type)
	}
	if txMsg.Txid != "tx1" {
		return fmt.Errorf("txid = %q, want tx1", txMsg.Txid)
	}
	if txMsg.ChannelId != "channelqc4" {
		return fmt.Errorf("channel id = %q, want channelqc4", txMsg.ChannelId)
	}
	input := &peer.ChaincodeInput{}
	if err := proto.Unmarshal(txMsg.Payload, input); err != nil {
		return err
	}
	if len(input.Args) != 2 || string(input.Args[0]) != "transfer" || string(input.Args[1]) != "asset1" {
		return fmt.Errorf("unexpected transaction args: %q", input.Args)
	}
	if string(input.Decorations["compat.decorator"]) != "orchestrator" {
		return fmt.Errorf("transaction decoration = %q, want orchestrator", string(input.Decorations["compat.decorator"]))
	}
	if txMsg.Proposal == nil {
		return fmt.Errorf("transaction proposal is nil")
	}
	proposal := &peer.Proposal{}
	if err := proto.Unmarshal(txMsg.Proposal.ProposalBytes, proposal); err != nil {
		return err
	}
	header := &common.Header{}
	if err := proto.Unmarshal(proposal.Header, header); err != nil {
		return err
	}
	signatureHeader := &common.SignatureHeader{}
	if err := proto.Unmarshal(header.SignatureHeader, signatureHeader); err != nil {
		return err
	}
	if string(signatureHeader.Creator) != "creator" {
		return fmt.Errorf("transaction creator = %q, want creator", string(signatureHeader.Creator))
	}
	if string(signatureHeader.Nonce) != "nonce" {
		return fmt.Errorf("transaction nonce = %q, want nonce", string(signatureHeader.Nonce))
	}

	if err := s.getState(stream, txMsg); err != nil {
		return err
	}
	if err := s.putState(stream, txMsg); err != nil {
		return err
	}
	if err := s.delState(stream, txMsg); err != nil {
		return err
	}

	responsePayload, err := proto.Marshal(&peer.Response{
		Status:  http.StatusOK,
		Message: http.StatusText(http.StatusOK),
		Payload: []byte("done"),
	})
	if err != nil {
		return err
	}
	if err := stream.Send(&peer.ChaincodeMessage{
		Type:    peer.ChaincodeMessage_COMPLETED,
		Payload: responsePayload,
		Txid:    txMsg.Txid,
		ChaincodeEvent: &peer.ChaincodeEvent{
			EventName: "asset-updated",
			Payload:   []byte("event-payload"),
		},
		ChannelId: txMsg.ChannelId,
	}); err != nil {
		return err
	}

	_, err = stream.Recv()
	if err == io.EOF {
		return nil
	}
	return err
}

func (s *handshakeChaincodeServer) getState(stream peer.Chaincode_ConnectServer, txMsg *peer.ChaincodeMessage) error {
	payload, err := proto.Marshal(&peer.GetState{Key: "asset1"})
	if err != nil {
		return err
	}
	if err := stream.Send(&peer.ChaincodeMessage{
		Type:      peer.ChaincodeMessage_GET_STATE,
		Payload:   payload,
		Txid:      txMsg.Txid,
		ChannelId: txMsg.ChannelId,
	}); err != nil {
		return err
	}
	resp, err := stream.Recv()
	if err != nil {
		return err
	}
	if resp.Type != peer.ChaincodeMessage_RESPONSE || string(resp.Payload) != "value1" {
		return fmt.Errorf("GET_STATE response = %s/%q, want RESPONSE/value1", resp.Type, string(resp.Payload))
	}
	return nil
}

func (s *handshakeChaincodeServer) putState(stream peer.Chaincode_ConnectServer, txMsg *peer.ChaincodeMessage) error {
	payload, err := proto.Marshal(&peer.PutState{Key: "asset2", Value: []byte("value2")})
	if err != nil {
		return err
	}
	if err := stream.Send(&peer.ChaincodeMessage{
		Type:      peer.ChaincodeMessage_PUT_STATE,
		Payload:   payload,
		Txid:      txMsg.Txid,
		ChannelId: txMsg.ChannelId,
	}); err != nil {
		return err
	}
	resp, err := stream.Recv()
	if err != nil {
		return err
	}
	if resp.Type != peer.ChaincodeMessage_RESPONSE {
		return fmt.Errorf("PUT_STATE response = %s, want RESPONSE", resp.Type)
	}
	return nil
}

func (s *handshakeChaincodeServer) delState(stream peer.Chaincode_ConnectServer, txMsg *peer.ChaincodeMessage) error {
	payload, err := proto.Marshal(&peer.DelState{Key: "asset3"})
	if err != nil {
		return err
	}
	if err := stream.Send(&peer.ChaincodeMessage{
		Type:      peer.ChaincodeMessage_DEL_STATE,
		Payload:   payload,
		Txid:      txMsg.Txid,
		ChannelId: txMsg.ChannelId,
	}); err != nil {
		return err
	}
	resp, err := stream.Recv()
	if err != nil {
		return err
	}
	if resp.Type != peer.ChaincodeMessage_RESPONSE {
		return fmt.Errorf("DEL_STATE response = %s, want RESPONSE", resp.Type)
	}
	return nil
}

type testState struct {
	values  map[string][]byte
	writes  map[string][]byte
	deletes map[string]bool
}

func newTestState() *testState {
	return &testState{
		values: map[string][]byte{
			"asset1": []byte("value1"),
		},
		writes:  make(map[string][]byte),
		deletes: make(map[string]bool),
	}
}

func (s *testState) Namespace() string {
	return "ns1"
}

func (s *testState) QueryView() *committerpb.View {
	return nil
}

func (s *testState) GetState(_ context.Context, key string) ([]byte, error) {
	return append([]byte(nil), s.values[key]...), nil
}

func (s *testState) PutState(key string, value []byte) {
	s.writes[key] = append([]byte(nil), value...)
}

func (s *testState) DelState(key string) {
	s.deletes[key] = true
}

func (s *testState) Result() blocks.ReadWriteSet {
	return blocks.ReadWriteSet{}
}
