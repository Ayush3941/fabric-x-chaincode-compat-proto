// SPDX-License-Identifier: Apache-2.0

package ccresolver

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc"
	grpcencoding "google.golang.org/grpc/encoding"
)

const (
	ServiceName                  = "compat.chaincode_resolver.Resolver"
	OperationChaincodeResolution = "CC_resolution"
	OperationRemoteOrchestrator  = "REMOTE_ORCHESTRATOR_resolution"
)

func init() {
	grpcencoding.RegisterCodec(jsonCodec{})
}

type jsonCodec struct{}

type ResolveRequest struct {
	Operation    string `json:"operation"`
	MSPID        string `json:"msp_id,omitempty"`
	RequesterMSP string `json:"requester_msp_id,omitempty"`
	TargetMSP    string `json:"target_msp_id,omitempty"`
	ChannelID    string `json:"channel_id,omitempty"`
	Namespace    string `json:"namespace,omitempty"`
	Name         string `json:"name,omitempty"`
	Version      string `json:"version,omitempty"`
	Sequence     int64  `json:"sequence,omitempty"`
}

type ResolveResponse struct {
	Found   bool   `json:"found"`
	Address string `json:"address,omitempty"`
	TLSMode string `json:"tls_mode,omitempty"`
}

type ResolverServer interface {
	Resolve(context.Context, *ResolveRequest) (*ResolveResponse, error)
}

type Client struct {
	conn grpc.ClientConnInterface
}

func (jsonCodec) Name() string {
	return "compat-json"
}

func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func NewClient(conn grpc.ClientConnInterface) *Client {
	return &Client{conn: conn}
}

func (c *Client) Resolve(ctx context.Context, req *ResolveRequest) (*ResolveResponse, error) {
	out := new(ResolveResponse)
	err := c.conn.Invoke(ctx, "/"+ServiceName+"/Resolve", req, out, grpc.ForceCodec(jsonCodec{}))
	return out, err
}

func RegisterResolverServer(registrar grpc.ServiceRegistrar, srv ResolverServer) {
	registrar.RegisterService(&grpc.ServiceDesc{
		ServiceName: ServiceName,
		HandlerType: (*ResolverServer)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Resolve",
			Handler:    resolverResolveHandler,
		}},
		Streams:  []grpc.StreamDesc{},
		Metadata: "compat_chaincode_resolver",
	}, srv)
}

func resolverResolveHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(ResolveRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ResolverServer).Resolve(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/" + ServiceName + "/Resolve",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(ResolverServer).Resolve(ctx, req.(*ResolveRequest))
	}
	return interceptor(ctx, in, info, handler)
}
