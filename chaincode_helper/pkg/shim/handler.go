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
	"net/http"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"google.golang.org/protobuf/proto"
)

type messageHandler struct {
	stream peer.Chaincode_ConnectClient
	state  State
	inv    Invocation
	ccid   string
}

func newMessageHandler(stream peer.Chaincode_ConnectClient, state State, inv Invocation, ccid string) *messageHandler {
	return &messageHandler{
		stream: stream,
		state:  state,
		inv:    inv,
		ccid:   ccid,
	}
}

func (h *messageHandler) Execute(ctx context.Context) (Result, error) {
	if err := h.sendTransaction(); err != nil {
		return Result{}, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}

		msg, err := h.stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return Result{}, errors.New("chaincode stream closed before COMPLETED")
			}
			return Result{}, fmt.Errorf("receive chaincode message: %w", err)
		}
		if msg == nil {
			return Result{}, errors.New("chaincode service sent nil message")
		}

		switch msg.Type {
		case peer.ChaincodeMessage_KEEPALIVE:
			continue
		case peer.ChaincodeMessage_GET_STATE:
			if err := h.handleGetState(ctx, msg); err != nil {
				return Result{}, err
			}
		case peer.ChaincodeMessage_PUT_STATE:
			if err := h.handlePutState(msg); err != nil {
				return Result{}, err
			}
		case peer.ChaincodeMessage_DEL_STATE:
			if err := h.handleDelState(msg); err != nil {
				return Result{}, err
			}
		case peer.ChaincodeMessage_COMPLETED:
			return h.completedResult(msg)
		case peer.ChaincodeMessage_ERROR:
			return Result{
				Status:    http.StatusInternalServerError,
				Message:   string(msg.Payload),
				Payload:   append([]byte(nil), msg.Payload...),
				Event:     eventPayload(msg.ChaincodeEvent),
				QueryView: h.state.QueryView(),
			}, nil
		default:
			if err := h.sendError(msg, fmt.Errorf("unsupported chaincode message type %s", msg.Type)); err != nil {
				return Result{}, err
			}
		}
	}
}

func (h *messageHandler) sendTransaction() error {
	input := &peer.ChaincodeInput{Args: h.inv.Args}
	payload, err := proto.Marshal(input)
	if err != nil {
		return fmt.Errorf("marshal transaction input: %w", err)
	}

	msg := &peer.ChaincodeMessage{
		Type:      peer.ChaincodeMessage_TRANSACTION,
		Payload:   payload,
		Txid:      h.inv.TxID,
		ChannelId: h.inv.ChannelID,
	}
	if err := h.stream.Send(msg); err != nil {
		return fmt.Errorf("send TRANSACTION to chaincode: %w", err)
	}
	return nil
}

func (h *messageHandler) handleGetState(ctx context.Context, msg *peer.ChaincodeMessage) error {
	req := &peer.GetState{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		return h.sendError(msg, fmt.Errorf("unmarshal GET_STATE: %w", err))
	}
	if req.Collection != "" {
		return h.sendError(msg, errors.New("private data collections are not supported in V1"))
	}

	value, err := h.state.GetState(ctx, req.Key)
	if err != nil {
		return h.sendError(msg, fmt.Errorf("get state %q: %w", req.Key, err))
	}
	return h.sendResponse(msg, value)
}

func (h *messageHandler) handlePutState(msg *peer.ChaincodeMessage) error {
	req := &peer.PutState{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		return h.sendError(msg, fmt.Errorf("unmarshal PUT_STATE: %w", err))
	}
	if req.Collection != "" {
		return h.sendError(msg, errors.New("private data collections are not supported in V1"))
	}

	h.state.PutState(req.Key, req.Value)
	return h.sendResponse(msg, nil)
}

func (h *messageHandler) handleDelState(msg *peer.ChaincodeMessage) error {
	req := &peer.DelState{}
	if err := proto.Unmarshal(msg.Payload, req); err != nil {
		return h.sendError(msg, fmt.Errorf("unmarshal DEL_STATE: %w", err))
	}
	if req.Collection != "" {
		return h.sendError(msg, errors.New("private data collections are not supported in V1"))
	}

	h.state.DelState(req.Key)
	return h.sendResponse(msg, nil)
}

func (h *messageHandler) completedResult(msg *peer.ChaincodeMessage) (Result, error) {
	response := &peer.Response{}
	if err := proto.Unmarshal(msg.Payload, response); err != nil {
		return Result{}, fmt.Errorf("unmarshal COMPLETED response: %w", err)
	}

	return Result{
		Status:    response.Status,
		Message:   response.Message,
		Payload:   append([]byte(nil), response.Payload...),
		Event:     eventPayload(msg.ChaincodeEvent),
		QueryView: h.state.QueryView(),
	}, nil
}

func (h *messageHandler) sendResponse(request *peer.ChaincodeMessage, payload []byte) error {
	return h.stream.Send(&peer.ChaincodeMessage{
		Type:      peer.ChaincodeMessage_RESPONSE,
		Payload:   append([]byte(nil), payload...),
		Txid:      request.Txid,
		ChannelId: request.ChannelId,
	})
}

func (h *messageHandler) sendError(request *peer.ChaincodeMessage, err error) error {
	return h.stream.Send(&peer.ChaincodeMessage{
		Type:      peer.ChaincodeMessage_ERROR,
		Payload:   []byte(err.Error()),
		Txid:      request.Txid,
		ChannelId: request.ChannelId,
	})
}

func eventPayload(event *peer.ChaincodeEvent) []byte {
	if event == nil {
		return nil
	}
	return append([]byte(nil), event.Payload...)
}
