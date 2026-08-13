/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package shim

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-sdk/blocks"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const defaultConnectTimeout = 10 * time.Second

// State is the state boundary exposed to the peer-side shim protocol handler.
// api.ExecutionContext satisfies this interface without pkg/shim importing the
// API package.
type State interface {
	Namespace() string
	QueryView() *committerpb.View
	GetState(context.Context, string) ([]byte, error)
	PutState(string, []byte)
	DelState(string)
	Result() blocks.ReadWriteSet
}

// Invocation is the Fabric-style transaction context needed by an isolated
// chaincode execution.
type Invocation struct {
	TxID      string
	ChannelID string
	Namespace string
	Args      [][]byte
	Creator   []byte
	Nonce     []byte
	QueryView *committerpb.View
}

// Result is the chaincode execution result returned by the shim bridge.
type Result struct {
	Status    int32
	Message   string
	Payload   []byte
	Event     []byte
	QueryView *committerpb.View
}

// Config contains the isolated chaincode service connection settings.
type Config struct {
	Endpoint string
}

// Connector owns the lightweight interaction with one isolated chaincode service.
type Connector struct {
	endpoint    string
	dialOptions []grpc.DialOption
}

// NewConnector creates a shim bridge connector for a chaincode-as-a-service endpoint.
func NewConnector(cfg Config) (*Connector, error) {
	if cfg.Endpoint == "" {
		return nil, errors.New("chaincode service endpoint is required")
	}
	return &Connector{endpoint: cfg.Endpoint}, nil
}

// Endpoint returns the configured chaincode service endpoint.
func (c *Connector) Endpoint() string {
	return c.endpoint
}

// Execute will drive the peer side of the Fabric CCAAS stream.
//
// Target behavior:
//   - dial the configured endpoint
//   - open peer.Chaincode/Connect
//   - complete REGISTER / REGISTERED / READY
//   - send the TRANSACTION message for inv
//   - answer GET_STATE, PUT_STATE, and DEL_STATE through State
//   - return the chaincode COMPLETED or ERROR response
func (c *Connector) Execute(ctx context.Context, state State, inv Invocation) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if state == nil {
		return Result{}, errors.New("shim state adapter is nil")
	}

	stream, closeConn, err := c.openStream(ctx)
	if err != nil {
		return Result{}, err
	}
	defer closeConn()

	ccid, err := receiveRegister(stream)
	if err != nil {
		return Result{}, err
	}

	if err := stream.Send(&peer.ChaincodeMessage{Type: peer.ChaincodeMessage_REGISTERED}); err != nil {
		return Result{}, fmt.Errorf("send REGISTERED to chaincode service %s: %w", c.endpoint, err)
	}

	if err := stream.Send(&peer.ChaincodeMessage{Type: peer.ChaincodeMessage_READY}); err != nil {
		return Result{}, fmt.Errorf("send READY to chaincode service %s: %w", c.endpoint, err)
	}

	res, err := newMessageHandler(stream, state, inv, ccid.Name).Execute(ctx)
	if closeErr := stream.CloseSend(); err == nil && closeErr != nil {
		return Result{}, fmt.Errorf("close chaincode stream to %s: %w", c.endpoint, closeErr)
	}
	return res, err
}

func (c *Connector) openStream(ctx context.Context) (peer.Chaincode_ConnectClient, func(), error) {
	dialCtx := ctx
	cancel := func() {}
	if _, ok := ctx.Deadline(); !ok {
		dialCtx, cancel = context.WithTimeout(ctx, defaultConnectTimeout)
	}

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}
	dialOptions = append(dialOptions, c.dialOptions...)

	conn, err := grpc.DialContext(dialCtx, c.endpoint, dialOptions...)
	cancel()
	if err != nil {
		return nil, nil, fmt.Errorf("connect to chaincode service %s: %w", c.endpoint, err)
	}

	stream, err := peer.NewChaincodeClient(conn).Connect(ctx)
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("open Chaincode/Connect stream to %s: %w", c.endpoint, err)
	}

	return stream, func() { _ = conn.Close() }, nil
}

func receiveRegister(stream peer.Chaincode_ConnectClient) (*peer.ChaincodeID, error) {
	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, errors.New("chaincode service closed stream before REGISTER")
			}
			return nil, fmt.Errorf("receive REGISTER from chaincode service: %w", err)
		}
		if msg == nil {
			return nil, errors.New("chaincode service sent nil message before REGISTER")
		}
		if msg.Type == peer.ChaincodeMessage_KEEPALIVE {
			continue
		}
		if msg.Type != peer.ChaincodeMessage_REGISTER {
			return nil, fmt.Errorf("expected REGISTER from chaincode service, got %s", msg.Type)
		}

		ccid := &peer.ChaincodeID{}
		if err := proto.Unmarshal(msg.Payload, ccid); err != nil {
			return nil, fmt.Errorf("unmarshal REGISTER chaincode id: %w", err)
		}
		if ccid.Name == "" {
			return nil, errors.New("REGISTER message contains empty chaincode id")
		}
		return ccid, nil
	}
}
