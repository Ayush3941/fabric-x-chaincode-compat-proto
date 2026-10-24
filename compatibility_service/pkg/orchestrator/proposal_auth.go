// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"time"

	mspapi "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/common/cauthdsl"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/common/policydsl"
	"github.com/hyperledger/fabric-x-common/msp"
	"google.golang.org/protobuf/proto"
)

// ProposalAuthConfig defines trust and permissions for original client proposals.
// It is independent of the TLS credentials authenticating an RPC connection.
type ProposalAuthConfig struct {
	MSPs  []ProposalMSPConfig         `mapstructure:"msps"`
	Rules []ProposalAuthorizationRule `mapstructure:"rules"`
}

// ProposalMSPConfig loads a public, verifying MSP directory, without private keys.
type ProposalMSPConfig struct {
	MSPID  string `mapstructure:"msp-id"`
	MSPDir string `mapstructure:"msp-dir"`
}

// ProposalAuthorizationRule grants access to a chaincode and native namespace.
// Selectors are exact strings or "*". A request must satisfy at least one
// matching rule's Fabric signature policy, evaluated against its single creator.
type ProposalAuthorizationRule struct {
	Chaincode string `mapstructure:"chaincode"`
	Namespace string `mapstructure:"namespace"`
	Policy    string `mapstructure:"policy"`
}

func (cfg ProposalAuthConfig) validate() error {
	if len(cfg.MSPs) == 0 {
		return errors.New("proposal-auth.msps is required")
	}
	seen := make(map[string]bool, len(cfg.MSPs))
	for i, trusted := range cfg.MSPs {
		if trusted.MSPID == "" || trusted.MSPDir == "" {
			return fmt.Errorf("proposal-auth.msps[%d] requires msp-id and msp-dir", i)
		}
		if seen[trusted.MSPID] {
			return fmt.Errorf("proposal-auth has duplicate MSP %q", trusted.MSPID)
		}
		seen[trusted.MSPID] = true
	}
	if len(cfg.Rules) == 0 {
		return errors.New("proposal-auth.rules is required")
	}
	for i, rule := range cfg.Rules {
		if rule.Chaincode == "" || rule.Namespace == "" || rule.Policy == "" {
			return fmt.Errorf("proposal-auth.rules[%d] requires chaincode, namespace, and policy", i)
		}
	}
	return nil
}

type proposalPermission struct {
	chaincode string
	namespace string
	policy    policies.Policy
}

type proposalVerifier struct {
	identities msp.MSPManager
	rules      []proposalPermission
}

func newProposalVerifier(cfg ProposalAuthConfig) (*proposalVerifier, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	trusted := make([]msp.MSP, 0, len(cfg.MSPs))
	known := make(map[string]bool, len(cfg.MSPs))
	for _, entry := range cfg.MSPs {
		provider, err := msp.LoadVerifyingMspDir(msp.DirLoadParameters{
			MspName: entry.MSPID,
			MspDir:  entry.MSPDir,
		})
		if err != nil {
			return nil, fmt.Errorf("load proposal MSP %s: %w", entry.MSPID, err)
		}
		trusted = append(trusted, provider)
		known[entry.MSPID] = true
	}
	manager := msp.NewMSPManager()
	if err := manager.Setup(trusted); err != nil {
		return nil, fmt.Errorf("set up proposal MSP manager: %w", err)
	}
	verifier := &proposalVerifier{identities: manager}
	policyProvider := &cauthdsl.EnvelopeBasedPolicyProvider{Deserializer: manager}
	for i, rule := range cfg.Rules {
		envelope, err := policydsl.FromString(rule.Policy)
		if err != nil {
			return nil, fmt.Errorf("proposal-auth.rules[%d] policy: %w", i, err)
		}
		for _, principal := range envelope.Identities {
			role := &mspapi.MSPRole{}
			if err := proto.Unmarshal(principal.Principal, role); err != nil {
				return nil, fmt.Errorf("proposal-auth.rules[%d] principal: %w", i, err)
			}
			if !known[role.MspIdentifier] {
				return nil, fmt.Errorf("proposal-auth.rules[%d] references untrusted MSP %q", i, role.MspIdentifier)
			}
		}
		policy, err := policyProvider.NewPolicy(envelope)
		if err != nil {
			return nil, fmt.Errorf("compile proposal-auth.rules[%d]: %w", i, err)
		}
		verifier.rules = append(verifier.rules, proposalPermission{
			chaincode: rule.Chaincode,
			namespace: rule.Namespace,
			policy:    policy,
		})
	}
	return verifier, nil
}

// authenticate verifies the original bytes received from the client. creator
// must come from parsing those same proposal bytes, never from RPC metadata.
func (v *proposalVerifier) authenticate(prop *peer.SignedProposal, creator []byte, now time.Time) (msp.Identity, error) {
	if v == nil {
		return nil, errors.New("proposal authentication is not configured")
	}
	if prop == nil || len(prop.ProposalBytes) == 0 || len(prop.Signature) == 0 {
		return nil, errors.New("signed client proposal and signature are required")
	}
	serialized := &mspapi.SerializedIdentity{}
	if err := proto.Unmarshal(creator, serialized); err != nil {
		return nil, fmt.Errorf("decode proposal creator: %w", err)
	}
	if serialized.Mspid == "" || len(serialized.IdBytes) == 0 {
		return nil, errors.New("proposal creator requires MSP ID and certificate")
	}
	identity, err := v.identities.DeserializeIdentity(msppb.NewIdentity(serialized.Mspid, serialized.IdBytes))
	if err != nil {
		return nil, fmt.Errorf("deserialize proposal creator: %w", err)
	}
	if err := identity.Validate(); err != nil {
		return nil, fmt.Errorf("untrusted proposal creator: %w", err)
	}
	if err := validateIdentityTime(identity, now); err != nil {
		return nil, err
	}
	if err := identity.Verify(prop.ProposalBytes, prop.Signature); err != nil {
		return nil, fmt.Errorf("invalid client proposal signature: %w", err)
	}
	return identity, nil
}

func validateIdentityTime(identity msp.Identity, now time.Time) error {
	// MSP validation also supports historical ledger identities. Admission of a
	// new proposal or endorsement must also require a currently valid certificate.
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

func (v *proposalVerifier) authorize(identity msp.Identity, chaincode, namespace string) error {
	if v == nil || identity == nil {
		return errors.New("authenticated proposal creator is required")
	}
	for _, rule := range v.rules {
		if rule.chaincode != "*" && rule.chaincode != chaincode {
			continue
		}
		if rule.namespace != "*" && rule.namespace != namespace {
			continue
		}
		if rule.policy.EvaluateIdentities([]msp.Identity{identity}) == nil {
			return nil
		}
	}
	return fmt.Errorf("proposal creator from MSP %s is not authorized for chaincode %q in namespace %q",
		identity.GetMSPIdentifier(), chaincode, namespace)
}
