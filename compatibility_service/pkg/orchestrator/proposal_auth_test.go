// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/gateway"
	mspapi "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	efab "github.com/hyperledger/fabric-x-sdk/endorsement/fabric"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestProposalAuthentication(t *testing.T) {
	org0 := newProposalTestMSP(t, "org-0")
	org1 := newProposalTestMSP(t, "org-1")
	rogue := newProposalTestMSP(t, "org-0")
	now := time.Now()
	client0 := org0.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	client1 := org1.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	rogueClient := rogue.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	expired := org0.issue(t, "client", now.Add(-2*time.Hour), now.Add(-time.Hour))
	future := org0.issue(t, "client", now.Add(time.Hour), now.Add(2*time.Hour))
	verifier := proposalTestVerifier(t, []ProposalMSPConfig{org0.config, org1.config}, "OR('org-0.client','org-1.client')")

	for _, tc := range []struct {
		name    string
		actor   proposalTestActor
		claim   string
		mutate  func(*peer.SignedProposal)
		wantErr bool
	}{
		{name: "org0 client", actor: client0},
		{name: "org1 original client", actor: client1},
		{name: "missing signature", actor: client0, mutate: func(p *peer.SignedProposal) { p.Signature = nil }, wantErr: true},
		{name: "invalid signature", actor: client0, mutate: func(p *peer.SignedProposal) { p.Signature = []byte("invalid") }, wantErr: true},
		{name: "changed arguments", actor: client0, mutate: func(p *peer.SignedProposal) { changeProposalTestArgument(t, p) }, wantErr: true},
		{name: "unknown MSP", actor: client0, claim: "unknown-org", wantErr: true},
		{name: "certificate from another MSP", actor: client1, claim: "org-0", wantErr: true},
		{name: "self issued certificate claiming trusted MSP", actor: rogueClient, wantErr: true},
		{name: "expired certificate", actor: expired, wantErr: true},
		{name: "not yet valid certificate", actor: future, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proposal := proposalTestSigned(t, tc.actor.signer, tc.claim)
			if tc.mutate != nil {
				tc.mutate(proposal)
			}
			inv, err := efab.Parse(proposal, now)
			if err != nil {
				t.Fatalf("proposal must pass structural validation to test authentication: %v", err)
			}
			_, err = verifier.authenticate(proposal, inv.Creator, now)
			if (err != nil) != tc.wantErr {
				t.Fatalf("authentication error = %v, want error %t", err, tc.wantErr)
			}
		})
	}
}

func TestProposalAuthorization(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	verifier := proposalTestVerifier(t, []ProposalMSPConfig{org.config}, "OR('org-0.client')")
	for _, role := range []string{"client", "peer"} {
		actor := org.issue(t, role, now.Add(-time.Hour), now.Add(time.Hour))
		proposal := proposalTestSigned(t, actor.signer, "")
		inv, err := efab.Parse(proposal, now)
		if err != nil {
			t.Fatal(err)
		}
		creator, err := verifier.authenticate(proposal, inv.Creator, now)
		if err != nil {
			t.Fatalf("valid %s identity should authenticate: %v", role, err)
		}
		for _, target := range []struct{ chaincode, namespace string }{{"sample", "1"}, {"other", "1"}, {"sample", "2"}} {
			err := verifier.authorize(creator, target.chaincode, target.namespace)
			wantAllowed := role == "client" && target.chaincode == "sample" && target.namespace == "1"
			if (err == nil) != wantAllowed {
				t.Errorf("role=%s target=%+v authorization error=%v", role, target, err)
			}
		}
	}
}

func TestRevokedProposalCreator(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	actor := org.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	crl, err := x509.CreateRevocationList(rand.Reader, &x509.RevocationList{
		Number: big.NewInt(1), ThisUpdate: now.Add(-time.Minute), NextUpdate: now.Add(time.Hour),
		RevokedCertificateEntries: []x509.RevocationListEntry{{SerialNumber: actor.cert.SerialNumber, RevocationTime: now.Add(-time.Minute)}},
	}, org.ca, org.key)
	if err != nil {
		t.Fatal(err)
	}
	writeProposalTestFile(t, filepath.Join(org.config.MSPDir, "crls", "revoked.pem"), pem.EncodeToMemory(&pem.Block{Type: "X509 CRL", Bytes: crl}))
	verifier := proposalTestVerifier(t, []ProposalMSPConfig{org.config}, "OR('org-0.client')")
	proposal := proposalTestSigned(t, actor.signer, "")
	inv, err := efab.Parse(proposal, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.authenticate(proposal, inv.Creator, now); err == nil {
		t.Fatal("revoked client was authenticated")
	}
}

func TestProposalAuthConfigurationFailsClosed(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	valid := ProposalAuthConfig{MSPs: []ProposalMSPConfig{org.config}, Rules: []ProposalAuthorizationRule{{Chaincode: "*", Namespace: "*", Policy: "OR('org-0.client')"}}}
	for _, tc := range []struct {
		name string
		cfg  ProposalAuthConfig
	}{
		{name: "missing trust"},
		{name: "missing permissions", cfg: ProposalAuthConfig{MSPs: valid.MSPs}},
		{name: "missing trust directory", cfg: ProposalAuthConfig{MSPs: []ProposalMSPConfig{{MSPID: "org-0", MSPDir: filepath.Join(t.TempDir(), "missing")}}, Rules: valid.Rules}},
		{name: "duplicate MSP", cfg: ProposalAuthConfig{MSPs: []ProposalMSPConfig{org.config, org.config}, Rules: valid.Rules}},
		{name: "invalid policy", cfg: ProposalAuthConfig{MSPs: valid.MSPs, Rules: []ProposalAuthorizationRule{{Chaincode: "*", Namespace: "*", Policy: "broken"}}}},
		{name: "policy references unknown MSP", cfg: ProposalAuthConfig{MSPs: valid.MSPs, Rules: []ProposalAuthorizationRule{{Chaincode: "*", Namespace: "*", Policy: "OR('unknown.member')"}}}},
		{name: "invalid status policy", cfg: ProposalAuthConfig{MSPs: valid.MSPs, Rules: valid.Rules, CommitStatusPolicy: "broken"}},
		{name: "status policy references unknown MSP", cfg: ProposalAuthConfig{MSPs: valid.MSPs, Rules: valid.Rules, CommitStatusPolicy: "OR('unknown.member')"}},
		{name: "invalid event policy", cfg: ProposalAuthConfig{MSPs: valid.MSPs, Rules: valid.Rules, ChaincodeEventsPolicy: "broken"}},
		{name: "event policy references unknown MSP", cfg: ProposalAuthConfig{MSPs: valid.MSPs, Rules: valid.Rules, ChaincodeEventsPolicy: "OR('unknown.member')"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := newProposalVerifier(tc.cfg); err == nil {
				t.Fatal("expected invalid proposal authentication configuration to fail")
			}
		})
	}
}

func TestGatewayCommitStatusAuthentication(t *testing.T) {
	org0, org1 := newProposalTestMSP(t, "org-0"), newProposalTestMSP(t, "org-1")
	rogue := newProposalTestMSP(t, "org-0")
	now := time.Now()
	client := org0.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	other := org1.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	service := &Service{cfg: Config{ChannelID: "channelqc4"}, proposalAuth: proposalTestVerifier(t, []ProposalMSPConfig{org0.config, org1.config}, "OR('org-0.client')")}
	for _, tc := range []struct {
		name   string
		actor  proposalTestActor
		mutate func(*gateway.SignedCommitStatusRequest)
		want   codes.Code
	}{
		{name: "authorized client", actor: client},
		{name: "channel reader need not have invocation permission", actor: other},
		{name: "invalid signature", actor: client, want: codes.Unauthenticated, mutate: func(r *gateway.SignedCommitStatusRequest) { r.Signature = []byte("invalid") }},
		{name: "changed signed txid", actor: client, want: codes.Unauthenticated, mutate: func(r *gateway.SignedCommitStatusRequest) {
			request := &gateway.CommitStatusRequest{}
			_ = proto.Unmarshal(r.Request, request)
			request.TransactionId = "another-transaction"
			r.Request = protoutil.MarshalOrPanic(request)
		}},
		{name: "missing signature", actor: client, want: codes.InvalidArgument, mutate: func(r *gateway.SignedCommitStatusRequest) { r.Signature = nil }},
		{name: "missing request", actor: client, want: codes.InvalidArgument, mutate: func(r *gateway.SignedCommitStatusRequest) { r.Request = nil }},
		{name: "malformed request", actor: client, want: codes.InvalidArgument, mutate: func(r *gateway.SignedCommitStatusRequest) { r.Request = []byte{0xff} }},
		{name: "wrong channel", actor: client, want: codes.InvalidArgument, mutate: func(r *gateway.SignedCommitStatusRequest) {
			request := &gateway.CommitStatusRequest{}
			_ = proto.Unmarshal(r.Request, request)
			request.ChannelId = "another-channel"
			r.Request = protoutil.MarshalOrPanic(request)
		}},
		{name: "rogue certificate", actor: rogue.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour)), want: codes.Unauthenticated},
		{name: "expired certificate", actor: org0.issue(t, "client", now.Add(-2*time.Hour), now.Add(-time.Hour)), want: codes.Unauthenticated},
		{name: "future certificate", actor: org0.issue(t, "client", now.Add(time.Hour), now.Add(2*time.Hour)), want: codes.Unauthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := gatewayTestStatusRequest(t, tc.actor, "tx")
			if tc.mutate != nil {
				tc.mutate(request)
			}
			_, err := service.authenticateGatewayCommitStatus(request)
			if status.Code(err) != tc.want {
				t.Fatalf("got %v, want %s", err, tc.want)
			}
		})
	}
	for _, txID := range []string{"", " ", " tx", "tx "} {
		if _, err := service.authenticateGatewayCommitStatus(gatewayTestStatusRequest(t, client, txID)); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("accepted missing or padded transaction ID %q: %v", txID, err)
		}
	}
	if _, err := service.authenticateGatewayCommitStatus(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	policy, err := service.proposalAuth.trust.CompilePolicy("OR('org-0.client')")
	if err != nil {
		t.Fatal(err)
	}
	service.proposalAuth.statusPolicy = policy
	if _, err := service.authenticateGatewayCommitStatus(gatewayTestStatusRequest(t, other, "tx")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("status reader policy was bypassed: %v", err)
	}
	service.proposalAuth = nil
	if _, err := service.authenticateGatewayCommitStatus(gatewayTestStatusRequest(t, client, "tx")); status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
}

func gatewayTestStatusRequest(t *testing.T, actor proposalTestActor, txID string) *gateway.SignedCommitStatusRequest {
	t.Helper()
	identity, err := actor.signer.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	request := protoutil.MarshalOrPanic(&gateway.CommitStatusRequest{ChannelId: "channelqc4", TransactionId: txID, Identity: identity})
	signature, err := actor.signer.Sign(request)
	if err != nil {
		t.Fatal(err)
	}
	return &gateway.SignedCommitStatusRequest{Request: request, Signature: signature}
}

func TestGatewayChaincodeEventsAuthentication(t *testing.T) {
	org0, org1 := newProposalTestMSP(t, "org-0"), newProposalTestMSP(t, "org-1")
	now := time.Now()
	actor := org0.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	reader := org1.issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	expired := org0.issue(t, "client", now.Add(-2*time.Hour), now.Add(-time.Hour))
	service := &Service{cfg: Config{ChannelID: "channelqc4"}, proposalAuth: proposalTestVerifier(t, []ProposalMSPConfig{org0.config, org1.config}, "OR('org-0.client')")}
	for _, tc := range []struct {
		name string
		want codes.Code
	}{
		{"valid", codes.OK}, {"reader without invocation permission", codes.OK}, {"bad signature", codes.Unauthenticated},
		{"tampered route", codes.Unauthenticated}, {"unsigned", codes.InvalidArgument}, {"malformed", codes.InvalidArgument},
		{"wrong channel", codes.InvalidArgument}, {"missing route", codes.InvalidArgument}, {"plain name", codes.InvalidArgument},
		{"unsupported seek", codes.InvalidArgument}, {"expired certificate", codes.Unauthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			signer := actor
			if tc.name == "reader without invocation permission" {
				signer = reader
			}
			if tc.name == "expired certificate" {
				signer = expired
			}
			request := gatewayTestEventsRequest(t, signer, "sample:1")
			decoded := &gateway.ChaincodeEventsRequest{}
			_ = proto.Unmarshal(request.Request, decoded)
			switch tc.name {
			case "bad signature":
				request.Signature = []byte("bad")
			case "unsigned":
				request.Signature = nil
			case "malformed":
				request.Request = []byte{0xff}
			case "tampered route":
				decoded.ChaincodeId = "other:1"
			case "wrong channel":
				decoded.ChannelId = "other-channel"
			case "missing route":
				decoded.ChaincodeId = ""
			case "plain name":
				decoded.ChaincodeId = "sample"
			case "unsupported seek":
				decoded.StartPosition = &orderer.SeekPosition{Type: &orderer.SeekPosition_Oldest{Oldest: &orderer.SeekOldest{}}}
			}
			if tc.name != "malformed" {
				request.Request = protoutil.MarshalOrPanic(decoded)
			}
			if _, err := service.authenticateGatewayChaincodeEvents(request); status.Code(err) != tc.want {
				t.Fatalf("got %v, want %s", err, tc.want)
			}
		})
	}
	policy, err := service.proposalAuth.trust.CompilePolicy("OR('org-0.client')")
	if err != nil {
		t.Fatal(err)
	}
	service.proposalAuth.eventPolicy = policy
	if _, err := service.authenticateGatewayChaincodeEvents(gatewayTestEventsRequest(t, reader, "sample:1")); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("reader policy bypassed: %v", err)
	}
	if _, err := service.authenticateGatewayChaincodeEvents(nil); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
}

func gatewayTestEventsRequest(t *testing.T, actor proposalTestActor, route string) *gateway.SignedChaincodeEventsRequest {
	t.Helper()
	creator, err := actor.signer.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	request := protoutil.MarshalOrPanic(&gateway.ChaincodeEventsRequest{ChannelId: "channelqc4", Identity: creator, ChaincodeId: route})
	signature, err := actor.signer.Sign(request)
	if err != nil {
		t.Fatal(err)
	}
	return &gateway.SignedChaincodeEventsRequest{Request: request, Signature: signature}
}

type proposalTestMSP struct {
	config ProposalMSPConfig
	ca     *x509.Certificate
	key    *ecdsa.PrivateKey
}

type proposalTestActor struct {
	signer identity.Signer
	cert   *x509.Certificate
	key    *ecdsa.PrivateKey
}

func newProposalTestMSP(t *testing.T, name string) proposalTestMSP {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: proposalTestSerial(t), Subject: pkix.Name{CommonName: name + " CA"}, NotBefore: now.Add(-24 * time.Hour), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	writeProposalTestFile(t, filepath.Join(dir, "cacerts", "ca.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	writeProposalTestFile(t, filepath.Join(dir, "config.yaml"), []byte(`NodeOUs:
  Enable: true
  ClientOUIdentifier:
    Certificate: cacerts/ca.pem
    OrganizationalUnitIdentifier: client
  PeerOUIdentifier:
    Certificate: cacerts/ca.pem
    OrganizationalUnitIdentifier: peer
  AdminOUIdentifier:
    Certificate: cacerts/ca.pem
    OrganizationalUnitIdentifier: admin
  OrdererOUIdentifier:
    Certificate: cacerts/ca.pem
    OrganizationalUnitIdentifier: orderer
`))
	return proposalTestMSP{config: ProposalMSPConfig{MSPID: name, MSPDir: dir}, ca: ca, key: key}
}

func (org proposalTestMSP) issue(t *testing.T, role string, notBefore, notAfter time.Time) proposalTestActor {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: proposalTestSerial(t), Subject: pkix.Name{CommonName: role, OrganizationalUnit: []string{role}}, NotBefore: notBefore, NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, org.ca, &key.PublicKey, org.key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	writeProposalTestFile(t, filepath.Join(dir, "keystore", "key_sk"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	writeProposalTestFile(t, filepath.Join(dir, "signcerts", "cert.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	signer, err := identity.SignerFromMSP(dir, org.config.MSPID)
	if err != nil {
		t.Fatal(err)
	}
	return proposalTestActor{signer: signer, cert: cert, key: key}
}

func proposalTestVerifier(t *testing.T, msps []ProposalMSPConfig, policy string) *proposalVerifier {
	t.Helper()
	v, err := newProposalVerifier(ProposalAuthConfig{MSPs: msps, Rules: []ProposalAuthorizationRule{{Chaincode: "sample", Namespace: "1", Policy: policy}}})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func proposalTestSigned(t *testing.T, signer identity.Signer, claimedMSP string) *peer.SignedProposal {
	t.Helper()
	creator, err := signer.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if claimedMSP != "" {
		serialized := &mspapi.SerializedIdentity{}
		if err := proto.Unmarshal(creator, serialized); err != nil {
			t.Fatal(err)
		}
		serialized.Mspid = claimedMSP
		creator, err = proto.Marshal(serialized)
		if err != nil {
			t.Fatal(err)
		}
	}
	nonce := make([]byte, 24)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	proposal, _, err := protoutil.CreateChaincodeProposalWithTxIDNonceAndTransient(protoutil.ComputeTxID(nonce, creator), common.HeaderType_ENDORSER_TRANSACTION, "channelqc4",
		&peer.ChaincodeInvocationSpec{ChaincodeSpec: &peer.ChaincodeSpec{Type: peer.ChaincodeSpec_GOLANG, ChaincodeId: &peer.ChaincodeID{Name: "sample", Version: "1.0"}, Input: &peer.ChaincodeInput{Args: [][]byte{[]byte("put"), []byte("asset"), []byte("value")}}}}, nonce, creator, nil)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := protoutil.GetSignedProposal(proposal, signer)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func changeProposalTestArgument(t *testing.T, signed *peer.SignedProposal) {
	t.Helper()
	proposal, err := protoutil.UnmarshalProposal(signed.ProposalBytes)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := protoutil.UnmarshalChaincodeProposalPayload(proposal.Payload)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := protoutil.UnmarshalChaincodeInvocationSpec(payload.Input)
	if err != nil {
		t.Fatal(err)
	}
	spec.ChaincodeSpec.Input.Args[1] = []byte("tampered-asset")
	payload.Input = protoutil.MarshalOrPanic(spec)
	proposal.Payload = protoutil.MarshalOrPanic(payload)
	signed.ProposalBytes = protoutil.MarshalOrPanic(proposal)
}

func proposalTestSerial(t *testing.T) *big.Int {
	t.Helper()
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	return n.Add(n, big.NewInt(1))
}

func writeProposalTestFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(fmt.Errorf("write %s: %w", path, err))
	}
}
