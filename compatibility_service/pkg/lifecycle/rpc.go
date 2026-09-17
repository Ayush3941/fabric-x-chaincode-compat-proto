// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"

	"github.com/hyperledger/fabric-protos-go-apiv2/msp"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcencoding "google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const lifecycleServiceName = "compat.lifecycle.Lifecycle"

func init() {
	grpcencoding.RegisterCodec(jsonCodec{})
}

type jsonCodec struct{}

func (jsonCodec) Name() string {
	return "json"
}

func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// SignedRequest is the signed lifecycle request envelope.
type SignedRequest struct {
	Payload   []byte `json:"payload"`
	Creator   []byte `json:"creator"`
	Signature []byte `json:"signature"`
}

// InstallRequest carries one lifecycle package archive.
type InstallRequest struct {
	Package []byte `json:"package"`
}

// InstallResponse returns the installed package metadata.
type InstallResponse struct {
	Package InstalledPackage `json:"package"`
}

// QueryInstalledRequest lists packages installed in the running orchestrator.
type QueryInstalledRequest struct{}

// InstalledPackageSummary is the user-facing queryinstalled result.
type InstalledPackageSummary struct {
	Label     string `json:"label"`
	PackageID string `json:"package_id"`
}

// QueryInstalledResponse returns installed packages.
type QueryInstalledResponse struct {
	Packages []InstalledPackageSummary `json:"packages"`
}

// SignRequest signs a lifecycle payload with an MSP identity.
func SignRequest(signer sdk.Signer, payload any) (*SignedRequest, error) {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal lifecycle payload: %w", err)
	}
	creator, err := signer.Serialize()
	if err != nil {
		return nil, fmt.Errorf("serialize creator: %w", err)
	}
	signature, err := signer.Sign(payloadBytes)
	if err != nil {
		return nil, fmt.Errorf("sign lifecycle payload: %w", err)
	}
	return &SignedRequest{
		Payload:   payloadBytes,
		Creator:   creator,
		Signature: signature,
	}, nil
}

// LifecycleServer is the gRPC lifecycle API.
type LifecycleServer interface {
	Install(context.Context, *SignedRequest) (*InstallResponse, error)
	QueryInstalled(context.Context, *SignedRequest) (*QueryInstalledResponse, error)
}

// Server implements lifecycle install/queryinstalled for one orchestrator.
type Server struct {
	store  *Store
	mspID  string
	logger sdk.Logger
}

// NewServer creates the lifecycle service for a running orchestrator.
func NewServer(store *Store, mspID string, logger sdk.Logger) *Server {
	if logger == nil {
		logger = sdk.NoOpLogger{}
	}
	return &Server{
		store:  store,
		mspID:  mspID,
		logger: logger,
	}
}

// RegisterLifecycleServer registers the lifecycle service on a gRPC server.
func RegisterLifecycleServer(registrar grpc.ServiceRegistrar, srv LifecycleServer) {
	registrar.RegisterService(&grpc.ServiceDesc{
		ServiceName: lifecycleServiceName,
		HandlerType: (*LifecycleServer)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "Install", Handler: lifecycleInstallHandler},
			{MethodName: "QueryInstalled", Handler: lifecycleQueryInstalledHandler},
		},
		Streams:  []grpc.StreamDesc{},
		Metadata: "compat_lifecycle",
	}, srv)
}

// Client is a lightweight lifecycle gRPC client.
type Client struct {
	conn grpc.ClientConnInterface
}

// NewClient creates a lifecycle client over an existing gRPC connection.
func NewClient(conn grpc.ClientConnInterface) *Client {
	return &Client{conn: conn}
}

// Install sends a signed install request.
func (c *Client) Install(ctx context.Context, req *SignedRequest) (*InstallResponse, error) {
	out := new(InstallResponse)
	err := c.conn.Invoke(ctx, "/"+lifecycleServiceName+"/Install", req, out, grpc.ForceCodec(jsonCodec{}))
	return out, err
}

// QueryInstalled sends a signed queryinstalled request.
func (c *Client) QueryInstalled(ctx context.Context, req *SignedRequest) (*QueryInstalledResponse, error) {
	out := new(QueryInstalledResponse)
	err := c.conn.Invoke(ctx, "/"+lifecycleServiceName+"/QueryInstalled", req, out, grpc.ForceCodec(jsonCodec{}))
	return out, err
}

// Install validates and stores a CCAAS lifecycle package in this process.
func (s *Server) Install(ctx context.Context, req *SignedRequest) (*InstallResponse, error) {
	var payload InstallRequest
	mspID, err := s.verifyAndUnmarshal(req, &payload)
	if err != nil {
		return nil, err
	}
	desc, err := InspectPackageBytes(payload.Package)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pkg := InstalledPackage{
		PackageID:      desc.PackageID,
		Label:          desc.Label,
		Type:           desc.Type,
		Address:        desc.Address,
		MetadataJSON:   desc.MetadataJSON,
		ConnectionJSON: desc.ConnectionJSON,
		PackageTGZ:     desc.PackageTGZ,
		InstalledByMSP: mspID,
	}
	if err := s.store.Install(ctx, pkg); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	installed, err := s.store.ListInstalled(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	for _, existing := range installed {
		if existing.PackageID == pkg.PackageID {
			s.logger.Infof("lifecycle install package_id=%s label=%s type=%s address=%s installed_by_msp=%s",
				existing.PackageID, existing.Label, existing.Type, existing.Address, existing.InstalledByMSP)
			return &InstallResponse{Package: existing}, nil
		}
	}
	return nil, status.Error(codes.Internal, "installed package not found after write")
}

// QueryInstalled returns packages stored by this running process.
func (s *Server) QueryInstalled(ctx context.Context, req *SignedRequest) (*QueryInstalledResponse, error) {
	mspID, err := s.verifyAndUnmarshal(req, &QueryInstalledRequest{})
	if err != nil {
		return nil, err
	}
	packages, err := s.store.ListInstalled(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	s.logger.Infof("lifecycle queryinstalled count=%d caller_msp=%s", len(packages), mspID)
	summaries := make([]InstalledPackageSummary, 0, len(packages))
	for _, pkg := range packages {
		summaries = append(summaries, InstalledPackageSummary{
			Label:     pkg.Label,
			PackageID: pkg.PackageID,
		})
	}
	return &QueryInstalledResponse{Packages: summaries}, nil
}

func (s *Server) verifyAndUnmarshal(req *SignedRequest, payload any) (string, error) {
	mspID, err := verifySignedRequest(req, s.mspID)
	if err != nil {
		return "", status.Error(codes.PermissionDenied, err.Error())
	}
	if err := json.Unmarshal(req.Payload, payload); err != nil {
		return "", status.Error(codes.InvalidArgument, err.Error())
	}
	return mspID, nil
}

func verifySignedRequest(req *SignedRequest, expectedMSP string) (string, error) {
	if req == nil {
		return "", errors.New("missing lifecycle request")
	}
	if len(req.Payload) == 0 {
		return "", errors.New("missing lifecycle payload")
	}
	if len(req.Creator) == 0 {
		return "", errors.New("missing lifecycle creator")
	}
	if len(req.Signature) == 0 {
		return "", errors.New("missing lifecycle signature")
	}

	creator := &msp.SerializedIdentity{}
	if err := proto.Unmarshal(req.Creator, creator); err != nil {
		return "", fmt.Errorf("decode creator: %w", err)
	}
	if expectedMSP != "" && creator.Mspid != expectedMSP {
		return "", fmt.Errorf("caller MSP %q does not match orchestrator MSP %q", creator.Mspid, expectedMSP)
	}
	if err := verifyECDSASignature(creator.IdBytes, req.Payload, req.Signature); err != nil {
		return "", err
	}
	return creator.Mspid, nil
}

type ecdsaSignature struct {
	R *big.Int
	S *big.Int
}

func verifyECDSASignature(certPEM, payload, signature []byte) error {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return errors.New("creator certificate is not PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("parse creator certificate: %w", err)
	}
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return errors.New("creator certificate is not ECDSA")
	}
	var sig ecdsaSignature
	if _, err := asn1.Unmarshal(signature, &sig); err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}
	if sig.R == nil || sig.S == nil {
		return errors.New("signature missing ECDSA values")
	}
	digest := sha256.Sum256(payload)
	if !ecdsa.Verify(pub, digest[:], sig.R, sig.S) {
		return errors.New("signature verification failed")
	}
	return nil
}

func lifecycleInstallHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(SignedRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LifecycleServer).Install(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/" + lifecycleServiceName + "/Install",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(LifecycleServer).Install(ctx, req.(*SignedRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func lifecycleQueryInstalledHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(SignedRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LifecycleServer).QueryInstalled(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/" + lifecycleServiceName + "/QueryInstalled",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(LifecycleServer).QueryInstalled(ctx, req.(*SignedRequest))
	}
	return interceptor(ctx, in, info, handler)
}
