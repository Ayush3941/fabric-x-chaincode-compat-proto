// SPDX-License-Identifier: Apache-2.0

// Package auth provides the MSP trust and signature-policy checks shared by
// proposal admission and lifecycle RPCs. It does not make routing decisions.
package auth

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"time"

	mspapi "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/common/cauthdsl"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/common/policydsl"
	"github.com/hyperledger/fabric-x-common/msp"
	"google.golang.org/protobuf/proto"
)

// MSPConfig loads public MSP verification material; no private keys are needed.
type MSPConfig struct {
	MSPID  string `mapstructure:"msp-id"`
	MSPDir string `mapstructure:"msp-dir"`
}

type Verifier struct {
	identities msp.MSPManager
	known      map[string]bool
}

func NewVerifier(configs []MSPConfig) (*Verifier, error) {
	if len(configs) == 0 {
		return nil, errors.New("trusted MSPs are required")
	}
	trusted := make([]msp.MSP, 0, len(configs))
	known := make(map[string]bool, len(configs))
	for _, entry := range configs {
		if entry.MSPID == "" || entry.MSPDir == "" || known[entry.MSPID] {
			return nil, fmt.Errorf("invalid or duplicate trusted MSP %q", entry.MSPID)
		}
		provider, err := msp.LoadVerifyingMspDir(msp.DirLoadParameters{MspName: entry.MSPID, MspDir: entry.MSPDir})
		if err != nil {
			return nil, fmt.Errorf("load MSP %s: %w", entry.MSPID, err)
		}
		trusted = append(trusted, provider)
		known[entry.MSPID] = true
	}
	manager := msp.NewMSPManager()
	if err := manager.Setup(trusted); err != nil {
		return nil, fmt.Errorf("set up MSP manager: %w", err)
	}
	return &Verifier{identities: manager, known: known}, nil
}

func (v *Verifier) Identities() msp.MSPManager { return v.identities }

// Identity validates organization membership, configured revocation data, and
// current certificate validity. Serialized MSP IDs are claims until this passes.
func (v *Verifier) Identity(creator []byte, now time.Time) (msp.Identity, error) {
	if v == nil || v.identities == nil {
		return nil, errors.New("MSP verification is not configured")
	}
	serialized := &mspapi.SerializedIdentity{}
	if err := proto.Unmarshal(creator, serialized); err != nil {
		return nil, fmt.Errorf("decode creator: %w", err)
	}
	if serialized.Mspid == "" || len(serialized.IdBytes) == 0 {
		return nil, errors.New("creator requires MSP ID and certificate")
	}
	identity, err := v.identities.DeserializeIdentity(msppb.NewIdentity(serialized.Mspid, serialized.IdBytes))
	if err != nil {
		return nil, fmt.Errorf("deserialize creator: %w", err)
	}
	if err := identity.Validate(); err != nil {
		return nil, fmt.Errorf("untrusted creator: %w", err)
	}
	if err := ValidateIdentityTime(identity, now); err != nil {
		return nil, err
	}
	return identity, nil
}

func (v *Verifier) Authenticate(creator, message, signature []byte, now time.Time) (msp.Identity, error) {
	if len(message) == 0 || len(signature) == 0 {
		return nil, errors.New("signed message and signature are required")
	}
	identity, err := v.Identity(creator, now)
	if err != nil {
		return nil, err
	}
	if err := identity.Verify(message, signature); err != nil {
		return nil, fmt.Errorf("invalid request signature: %w", err)
	}
	return identity, nil
}

func (v *Verifier) CompilePolicy(expression string) (policies.Policy, error) {
	if v == nil {
		return nil, errors.New("MSP verification is not configured")
	}
	envelope, err := policydsl.FromString(expression)
	if err != nil {
		return nil, err
	}
	for _, principal := range envelope.Identities {
		role := &mspapi.MSPRole{}
		if err := proto.Unmarshal(principal.Principal, role); err != nil {
			return nil, err
		}
		if !v.known[role.MspIdentifier] {
			return nil, fmt.Errorf("policy references untrusted MSP %q", role.MspIdentifier)
		}
	}
	provider := &cauthdsl.EnvelopeBasedPolicyProvider{Deserializer: v.identities}
	return provider.NewPolicy(envelope)
}

func ValidateIdentityTime(identity msp.Identity, now time.Time) error {
	certPEM, err := identity.GetCertificatePEM()
	if err != nil {
		return fmt.Errorf("read identity certificate: %w", err)
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return errors.New("identity certificate is not PEM")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("parse identity certificate: %w", err)
	}
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return errors.New("identity certificate is outside its validity period")
	}
	return nil
}
