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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	lifecycleServiceName = "compat.lifecycle.Lifecycle"
	localOnlyMetadata    = "x-compat-lifecycle-local-only"
)

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

// ApproveForMyOrgRequest records one local org approval for a chaincode
// definition and installed package.
type ApproveForMyOrgRequest struct {
	Definition ChaincodeDefinition `json:"definition"`
	PackageID  string              `json:"package_id"`
}

// ApproveForMyOrgResponse returns the stored approval.
type ApproveForMyOrgResponse struct {
	Approval ApprovedDefinition `json:"approval"`
}

// CheckCommitReadinessRequest checks whether orgs have approved a definition.
type CheckCommitReadinessRequest struct {
	Definition ChaincodeDefinition `json:"definition"`
}

// CheckCommitReadinessResponse aggregates readiness by MSP ID.
type CheckCommitReadinessResponse struct {
	Approvals map[string]bool `json:"approvals"`
}

// CommitRequest marks a definition active after readiness succeeds.
type CommitRequest struct {
	Definition ChaincodeDefinition `json:"definition"`
}

// CommitResponse returns the locally committed definition and readiness map.
type CommitResponse struct {
	Definition CommittedDefinition `json:"definition"`
	Approvals  map[string]bool     `json:"approvals"`
	Ledger     LedgerCommitResult  `json:"ledger,omitempty"`
}

// MarkInitializedRequest marks a committed init-required definition initialized
// after its init transaction has committed.
type MarkInitializedRequest struct {
	Definition ChaincodeDefinition `json:"definition"`
}

// MarkInitializedResponse returns the updated committed definition.
type MarkInitializedResponse struct {
	Definition CommittedDefinition `json:"definition"`
}

// QueryCommittedRequest looks up active committed definitions.
type QueryCommittedRequest struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

// QueryCommittedResponse returns committed definitions known by one
// orchestrator process.
type QueryCommittedResponse struct {
	Definitions []CommittedDefinition `json:"definitions"`
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
	ApproveForMyOrg(context.Context, *SignedRequest) (*ApproveForMyOrgResponse, error)
	CheckCommitReadiness(context.Context, *SignedRequest) (*CheckCommitReadinessResponse, error)
	Commit(context.Context, *SignedRequest) (*CommitResponse, error)
	MarkInitialized(context.Context, *SignedRequest) (*MarkInitializedResponse, error)
	QueryCommitted(context.Context, *SignedRequest) (*QueryCommittedResponse, error)
}

// RemotePeer is the small lifecycle surface needed to coordinate with another
// orchestrator. OrchestratorContact satisfies this without the lifecycle
// package importing the orchestrator package.
type RemotePeer interface {
	MSPID() string
	CheckCommitReadiness(context.Context, *SignedRequest) (*CheckCommitReadinessResponse, error)
	Commit(context.Context, *SignedRequest) (*CommitResponse, error)
	MarkInitialized(context.Context, *SignedRequest) (*MarkInitializedResponse, error)
}

// RemotePeerRequest carries the lifecycle definition context used to resolve
// remote orchestrator peers.
type RemotePeerRequest struct {
	RequesterMSP string
	Definition   ChaincodeDefinition
}

// RemotePeerProvider resolves remote orchestrator peers when lifecycle
// operations need to coordinate across organizations.
type RemotePeerProvider interface {
	RemotePeers(context.Context, RemotePeerRequest) ([]RemotePeer, error)
}

type staticRemotePeerProvider struct {
	remotes []RemotePeer
}

func (p staticRemotePeerProvider) RemotePeers(context.Context, RemotePeerRequest) ([]RemotePeer, error) {
	return append([]RemotePeer(nil), p.remotes...), nil
}

// Server implements lifecycle install/queryinstalled for one orchestrator.
type Server struct {
	store           *Store
	mspID           string
	logger          sdk.Logger
	ledgerCommitter LedgerCommitter
	remotes         RemotePeerProvider
}

// NewServer creates the lifecycle service for a running orchestrator.
func NewServer(store *Store, mspID string, logger sdk.Logger, ledgerCommitter LedgerCommitter, remotes ...RemotePeer) *Server {
	return NewServerWithRemoteProvider(store, mspID, logger, ledgerCommitter, staticRemotePeerProvider{remotes: remotes})
}

// NewServerWithRemoteProvider creates the lifecycle service with a dynamic
// remote peer provider.
func NewServerWithRemoteProvider(store *Store, mspID string, logger sdk.Logger, ledgerCommitter LedgerCommitter, remotes RemotePeerProvider) *Server {
	if logger == nil {
		logger = sdk.NoOpLogger{}
	}
	return &Server{
		store:           store,
		mspID:           mspID,
		logger:          logger,
		ledgerCommitter: ledgerCommitter,
		remotes:         remotes,
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
			{MethodName: "ApproveForMyOrg", Handler: lifecycleApproveForMyOrgHandler},
			{MethodName: "CheckCommitReadiness", Handler: lifecycleCheckCommitReadinessHandler},
			{MethodName: "Commit", Handler: lifecycleCommitHandler},
			{MethodName: "MarkInitialized", Handler: lifecycleMarkInitializedHandler},
			{MethodName: "QueryCommitted", Handler: lifecycleQueryCommittedHandler},
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

// ApproveForMyOrg sends a signed approveformyorg request.
func (c *Client) ApproveForMyOrg(ctx context.Context, req *SignedRequest) (*ApproveForMyOrgResponse, error) {
	out := new(ApproveForMyOrgResponse)
	err := c.conn.Invoke(ctx, "/"+lifecycleServiceName+"/ApproveForMyOrg", req, out, grpc.ForceCodec(jsonCodec{}))
	return out, err
}

// CheckCommitReadiness sends a signed readiness request.
func (c *Client) CheckCommitReadiness(ctx context.Context, req *SignedRequest) (*CheckCommitReadinessResponse, error) {
	out := new(CheckCommitReadinessResponse)
	err := c.conn.Invoke(ctx, "/"+lifecycleServiceName+"/CheckCommitReadiness", req, out, grpc.ForceCodec(jsonCodec{}))
	return out, err
}

// Commit sends a signed lifecycle commit request.
func (c *Client) Commit(ctx context.Context, req *SignedRequest) (*CommitResponse, error) {
	out := new(CommitResponse)
	err := c.conn.Invoke(ctx, "/"+lifecycleServiceName+"/Commit", req, out, grpc.ForceCodec(jsonCodec{}))
	return out, err
}

// MarkInitialized sends a signed mark-initialized request.
func (c *Client) MarkInitialized(ctx context.Context, req *SignedRequest) (*MarkInitializedResponse, error) {
	out := new(MarkInitializedResponse)
	err := c.conn.Invoke(ctx, "/"+lifecycleServiceName+"/MarkInitialized", req, out, grpc.ForceCodec(jsonCodec{}))
	return out, err
}

// QueryCommitted sends a signed querycommitted request.
func (c *Client) QueryCommitted(ctx context.Context, req *SignedRequest) (*QueryCommittedResponse, error) {
	out := new(QueryCommittedResponse)
	err := c.conn.Invoke(ctx, "/"+lifecycleServiceName+"/QueryCommitted", req, out, grpc.ForceCodec(jsonCodec{}))
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

// ApproveForMyOrg validates that a package is installed locally and records
// this organization's approval.
func (s *Server) ApproveForMyOrg(ctx context.Context, req *SignedRequest) (*ApproveForMyOrgResponse, error) {
	var payload ApproveForMyOrgRequest
	mspID, err := s.verifyAndUnmarshal(req, &payload)
	if err != nil {
		return nil, err
	}
	approval, err := s.store.ApproveForMyOrg(ctx, payload.Definition, payload.PackageID, mspID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.logger.Infof("lifecycle approveformyorg name=%s version=%s sequence=%d package_id=%s approving_msp=%s",
		approval.Definition.Name, approval.Definition.Version, approval.Definition.Sequence, approval.PackageID, approval.ApprovingMSP)
	return &ApproveForMyOrgResponse{Approval: approval}, nil
}

// CheckCommitReadiness checks local approval and, unless marked local-only,
// asks configured remote orchestrators for their local readiness too.
func (s *Server) CheckCommitReadiness(ctx context.Context, req *SignedRequest) (*CheckCommitReadinessResponse, error) {
	var payload CheckCommitReadinessRequest
	callerMSP, err := s.verifyAndUnmarshalAnyMSP(req, &payload)
	if err != nil {
		return nil, err
	}
	approvals, err := s.checkCommitReadiness(ctx, req, payload.Definition, !localOnly(ctx))
	if err != nil {
		return nil, err
	}
	s.logger.Infof("lifecycle checkcommitreadiness name=%s sequence=%d caller_msp=%s approvals=%v",
		payload.Definition.Name, payload.Definition.Sequence, callerMSP, approvals)
	return &CheckCommitReadinessResponse{Approvals: approvals}, nil
}

// Commit submits the shared lifecycle definition as a Fabric-X transaction,
// records it locally after finality, and propagates the same committed
// definition to configured remote orchestrators.
func (s *Server) Commit(ctx context.Context, req *SignedRequest) (*CommitResponse, error) {
	var payload CommitRequest
	callerMSP, err := s.verifyAndUnmarshalAnyMSP(req, &payload)
	if err != nil {
		return nil, err
	}
	aggregate := !localOnly(ctx)
	approvals, err := s.checkCommitReadiness(ctx, req, payload.Definition, aggregate)
	if err != nil {
		return nil, err
	}
	for mspID, approved := range approvals {
		if !approved {
			return nil, status.Errorf(codes.FailedPrecondition, "definition is not ready for %s", mspID)
		}
	}
	if err := s.store.ValidateCommitDefinition(ctx, payload.Definition, s.mspID); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}

	var ledger LedgerCommitResult
	if aggregate {
		if s.ledgerCommitter == nil {
			return nil, status.Error(codes.FailedPrecondition, "lifecycle ledger committer is not configured")
		}
		ledger, err = s.ledgerCommitter.CommitLifecycleDefinition(ctx, payload.Definition)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "commit lifecycle ledger transaction: %s", err)
		}
	}

	committed, err := s.store.CommitDefinition(ctx, payload.Definition, s.mspID)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if aggregate {
		remotes, err := s.remotePeers(ctx, RemotePeerRequest{RequesterMSP: s.mspID, Definition: payload.Definition})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "resolve remote lifecycle peers: %s", err)
		}
		remoteCtx := metadata.AppendToOutgoingContext(ctx, localOnlyMetadata, "true")
		for _, remote := range remotes {
			if _, err := remote.Commit(remoteCtx, req); err != nil {
				return nil, status.Errorf(codes.Internal, "remote commit %s: %s", remote.MSPID(), err)
			}
		}
	}
	s.logger.Infof("lifecycle commit name=%s version=%s sequence=%d caller_msp=%s local_msp=%s propagated=%t",
		payload.Definition.Name, payload.Definition.Version, payload.Definition.Sequence, callerMSP, s.mspID, aggregate)
	return &CommitResponse{Definition: committed, Approvals: approvals, Ledger: ledger}, nil
}

// MarkInitialized marks this process' committed lifecycle definition initialized
// after the init transaction has committed.
func (s *Server) MarkInitialized(ctx context.Context, req *SignedRequest) (*MarkInitializedResponse, error) {
	var payload MarkInitializedRequest
	callerMSP, err := s.verifyAndUnmarshalAnyMSP(req, &payload)
	if err != nil {
		return nil, err
	}
	committed, err := s.store.MarkInitialized(ctx, payload.Definition, s.mspID)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	s.logger.Infof("lifecycle markinitialized name=%s version=%s sequence=%d caller_msp=%s local_msp=%s initialized=%t",
		payload.Definition.Name, payload.Definition.Version, payload.Definition.Sequence, callerMSP, s.mspID, committed.Initialized)
	return &MarkInitializedResponse{Definition: committed}, nil
}

// QueryCommitted returns active committed definitions known by this process.
func (s *Server) QueryCommitted(ctx context.Context, req *SignedRequest) (*QueryCommittedResponse, error) {
	var payload QueryCommittedRequest
	mspID, err := s.verifyAndUnmarshalAnyMSP(req, &payload)
	if err != nil {
		return nil, err
	}
	definitions, err := s.store.ListCommitted(ctx, payload.Name, payload.Version)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	s.logger.Infof("lifecycle querycommitted name=%s version=%s count=%d caller_msp=%s",
		payload.Name, payload.Version, len(definitions), mspID)
	return &QueryCommittedResponse{Definitions: definitions}, nil
}

func (s *Server) checkCommitReadiness(ctx context.Context, signedReq *SignedRequest, def ChaincodeDefinition, aggregate bool) (map[string]bool, error) {
	approved, err := s.store.HasMatchingApproval(ctx, def, s.mspID)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	approvals := map[string]bool{s.mspID: approved}
	if !aggregate {
		return approvals, nil
	}
	remotes, err := s.remotePeers(ctx, RemotePeerRequest{RequesterMSP: s.mspID, Definition: def})
	if err != nil {
		return nil, err
	}
	remoteCtx := metadata.AppendToOutgoingContext(ctx, localOnlyMetadata, "true")
	for _, remote := range remotes {
		res, err := remote.CheckCommitReadiness(remoteCtx, signedReq)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "remote readiness %s: %s", remote.MSPID(), err)
		}
		for mspID, ready := range res.Approvals {
			approvals[mspID] = ready
		}
	}
	return approvals, nil
}

func (s *Server) remotePeers(ctx context.Context, req RemotePeerRequest) ([]RemotePeer, error) {
	if s.remotes == nil {
		return nil, nil
	}
	return s.remotes.RemotePeers(ctx, req)
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

func (s *Server) verifyAndUnmarshalAnyMSP(req *SignedRequest, payload any) (string, error) {
	mspID, err := verifySignedRequest(req, "")
	if err != nil {
		return "", status.Error(codes.PermissionDenied, err.Error())
	}
	if err := json.Unmarshal(req.Payload, payload); err != nil {
		return "", status.Error(codes.InvalidArgument, err.Error())
	}
	return mspID, nil
}

func localOnly(ctx context.Context) bool {
	values := metadata.ValueFromIncomingContext(ctx, localOnlyMetadata)
	for _, value := range values {
		if value == "true" {
			return true
		}
	}
	return false
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

func lifecycleApproveForMyOrgHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(SignedRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LifecycleServer).ApproveForMyOrg(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/" + lifecycleServiceName + "/ApproveForMyOrg",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(LifecycleServer).ApproveForMyOrg(ctx, req.(*SignedRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func lifecycleCheckCommitReadinessHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(SignedRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LifecycleServer).CheckCommitReadiness(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/" + lifecycleServiceName + "/CheckCommitReadiness",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(LifecycleServer).CheckCommitReadiness(ctx, req.(*SignedRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func lifecycleCommitHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(SignedRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LifecycleServer).Commit(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/" + lifecycleServiceName + "/Commit",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(LifecycleServer).Commit(ctx, req.(*SignedRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func lifecycleMarkInitializedHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(SignedRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LifecycleServer).MarkInitialized(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/" + lifecycleServiceName + "/MarkInitialized",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(LifecycleServer).MarkInitialized(ctx, req.(*SignedRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func lifecycleQueryCommittedHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(SignedRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LifecycleServer).QueryCommitted(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/" + lifecycleServiceName + "/QueryCommitted",
	}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(LifecycleServer).QueryCommitted(ctx, req.(*SignedRequest))
	}
	return interceptor(ctx, in, info, handler)
}
