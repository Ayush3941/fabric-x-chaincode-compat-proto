// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"errors"
	"fmt"
	"time"

	"compatibility_service/pkg/auth"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/msp"
)

// ProposalAuthConfig defines trust and permissions for original client proposals.
// It is independent of the TLS credentials authenticating an RPC connection.
type ProposalAuthConfig struct {
	MSPs  []ProposalMSPConfig         `mapstructure:"msps"`
	Rules []ProposalAuthorizationRule `mapstructure:"rules"`
}

// ProposalMSPConfig loads a public, verifying MSP directory, without private keys.
type ProposalMSPConfig = auth.MSPConfig

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
	trust      *auth.Verifier
	identities msp.MSPManager
	rules      []proposalPermission
}

func newProposalVerifier(cfg ProposalAuthConfig) (*proposalVerifier, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	trust, err := auth.NewVerifier(cfg.MSPs)
	if err != nil {
		return nil, err
	}
	verifier := &proposalVerifier{identities: trust.Identities(), trust: trust}
	for i, rule := range cfg.Rules {
		policy, err := trust.CompilePolicy(rule.Policy)
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
	return v.trust.Authenticate(creator, prop.ProposalBytes, prop.Signature, now)
}

func validateIdentityTime(identity msp.Identity, now time.Time) error {
	return auth.ValidateIdentityTime(identity, now)
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
