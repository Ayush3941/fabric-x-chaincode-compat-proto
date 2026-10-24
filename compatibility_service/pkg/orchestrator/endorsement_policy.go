// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"strings"
	"time"

	common "github.com/hyperledger/fabric-protos-go-apiv2/common"
	mspapi "github.com/hyperledger/fabric-protos-go-apiv2/msp"
	"github.com/hyperledger/fabric-x-committer/utils/signature"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/common/cauthdsl"
	"github.com/hyperledger/fabric-x-common/common/policies"
	"github.com/hyperledger/fabric-x-common/msp"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"google.golang.org/protobuf/proto"
)

// namespaceEndorsementPolicy uses the same policy evaluator as Fabric-X. MSP
// IDs are discovery hints; only authenticated signing identities satisfy rules.
type namespaceEndorsementPolicy struct {
	snapshot     NamespacePolicySnapshot
	deserializer msp.IdentityDeserializer
	envelope     *common.SignaturePolicyEnvelope
	policy       policies.Policy
	verifier     *signature.NsVerifier
	targetMSPs   []string
	unscoped     bool
}

type verifiedEndorsement struct {
	result   helperExecutionResult
	identity msp.Identity
	key      string
}

func newNamespaceEndorsementPolicy(snapshot NamespacePolicySnapshot, deserializer msp.IdentityDeserializer) (*namespaceEndorsementPolicy, error) {
	if snapshot.Policy == nil {
		return nil, fmt.Errorf("namespace %q policy is nil", snapshot.Namespace)
	}
	p := &namespaceEndorsementPolicy{snapshot: snapshot, deserializer: deserializer}
	switch rule := snapshot.Policy.GetRule().(type) {
	case *applicationpb.NamespacePolicy_MspRule:
		if deserializer == nil {
			return nil, errors.New("trusted MSPs are required to verify namespace endorsements")
		}
		p.envelope = &common.SignaturePolicyEnvelope{}
		if err := proto.Unmarshal(rule.MspRule, p.envelope); err != nil {
			return nil, fmt.Errorf("decode namespace MSP policy: %w", err)
		}
		if err := p.inspectRule(p.envelope.Rule); err != nil {
			return nil, err
		}
		var err error
		p.policy, _, err = cauthdsl.NewPolicyProvider(deserializer).NewPolicy(rule.MspRule)
		if err != nil {
			return nil, fmt.Errorf("compile namespace MSP policy: %w", err)
		}
	case *applicationpb.NamespacePolicy_ThresholdRule:
		if rule.ThresholdRule == nil {
			return nil, errors.New("namespace threshold policy is nil")
		}
		if rule.ThresholdRule.Scheme == "" {
			return nil, errors.New("namespace threshold policy scheme is empty")
		}
		if len(rule.ThresholdRule.PublicKey) == 0 && !strings.EqualFold(rule.ThresholdRule.Scheme, "NONE") {
			return nil, errors.New("namespace threshold policy public key is empty")
		}
		if strings.EqualFold(rule.ThresholdRule.Scheme, "EDDSA") && len(rule.ThresholdRule.PublicKey) != ed25519.PublicKeySize {
			return nil, errors.New("invalid EdDSA namespace public key length")
		}
	default:
		return nil, fmt.Errorf("unsupported namespace policy rule %T", rule)
	}
	var err error
	p.verifier, err = signature.NewNsVerifier(snapshot.Policy, deserializer)
	if err != nil {
		return nil, fmt.Errorf("compile namespace signature verifier: %w", err)
	}
	return p, nil
}

func (p *namespaceEndorsementPolicy) inspectRule(rule *common.SignaturePolicy) error {
	if rule == nil {
		return errors.New("empty signature policy rule")
	}
	switch r := rule.GetType().(type) {
	case *common.SignaturePolicy_SignedBy:
		if r.SignedBy < 0 || int(r.SignedBy) >= len(p.envelope.Identities) {
			return fmt.Errorf("signed_by index %d out of range", r.SignedBy)
		}
		return p.inspectPrincipal(p.envelope.Identities[r.SignedBy])
	case *common.SignaturePolicy_NOutOf_:
		if r.NOutOf == nil || r.NOutOf.N < 0 || int(r.NOutOf.N) > len(r.NOutOf.Rules) {
			return errors.New("invalid n_out_of signature policy threshold")
		}
		for _, child := range r.NOutOf.Rules {
			if err := p.inspectRule(child); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported signature policy rule %T", r)
	}
}

// inspectPrincipal extracts routing hints without replacing MSP principal
// evaluation. Combined and OU principals are checked by the MSP itself.
func (p *namespaceEndorsementPolicy) inspectPrincipal(principal *mspapi.MSPPrincipal) error {
	if principal == nil {
		return errors.New("nil MSP principal")
	}
	var mspID string
	switch principal.PrincipalClassification {
	case mspapi.MSPPrincipal_ROLE:
		role := &mspapi.MSPRole{}
		if err := proto.Unmarshal(principal.Principal, role); err != nil {
			return fmt.Errorf("decode MSP role: %w", err)
		}
		mspID = role.MspIdentifier
	case mspapi.MSPPrincipal_ORGANIZATION_UNIT:
		ou := &mspapi.OrganizationUnit{}
		if err := proto.Unmarshal(principal.Principal, ou); err != nil {
			return fmt.Errorf("decode organizational unit: %w", err)
		}
		mspID = ou.MspIdentifier
	case mspapi.MSPPrincipal_IDENTITY:
		identity := &msppb.Identity{}
		if err := proto.Unmarshal(principal.Principal, identity); err != nil {
			return fmt.Errorf("decode identity principal: %w", err)
		}
		mspID = identity.MspId
	case mspapi.MSPPrincipal_COMBINED:
		combined := &mspapi.CombinedPrincipal{}
		if err := proto.Unmarshal(principal.Principal, combined); err != nil {
			return fmt.Errorf("decode combined principal: %w", err)
		}
		if len(combined.Principals) == 0 {
			return errors.New("empty combined principal")
		}
		for _, child := range combined.Principals {
			if err := p.inspectPrincipal(child); err != nil {
				return err
			}
		}
		return nil
	case mspapi.MSPPrincipal_ANONYMITY:
		anonymity := &mspapi.MSPIdentityAnonymity{}
		if err := proto.Unmarshal(principal.Principal, anonymity); err != nil {
			return fmt.Errorf("decode anonymity principal: %w", err)
		}
		p.unscoped = true
		return nil
	default:
		return fmt.Errorf("unsupported MSP principal classification %s", principal.PrincipalClassification)
	}
	if mspID == "" {
		return errors.New("MSP principal requires an MSP ID")
	}
	for _, existing := range p.targetMSPs {
		if existing == mspID {
			return nil
		}
	}
	p.targetMSPs = append(p.targetMSPs, mspID)
	return nil
}

func (p *namespaceEndorsementPolicy) authenticate(result helperExecutionResult, expectedMSP string) (verifiedEndorsement, error) {
	if len(result.Endorsement.Responses) != 1 {
		return verifiedEndorsement{}, errors.New("expected exactly one execution endorsement")
	}
	response := result.Endorsement.Responses[0]
	if response == nil || response.Response == nil || response.Endorsement == nil || len(response.Endorsement.Signature) == 0 {
		return verifiedEndorsement{}, errors.New("execution response is missing its endorsement")
	}
	if response.Response.Status < 200 || response.Response.Status >= 400 {
		return verifiedEndorsement{}, fmt.Errorf("execution failed with status %d", response.Response.Status)
	}
	tx := &applicationpb.Tx{}
	if err := proto.Unmarshal(response.Payload, tx); err != nil {
		return verifiedEndorsement{}, fmt.Errorf("decode endorsed native transaction: %w", err)
	}
	if len(tx.Namespaces) != 1 || tx.Namespaces[0].GetNsId() != p.snapshot.Namespace || tx.Namespaces[0].GetNsVersion() != p.snapshot.Version {
		return verifiedEndorsement{}, errors.New("endorsement namespace or policy version does not match the invocation")
	}
	if p.envelope == nil {
		// A key-based namespace is verified against its configured key below.
		return verifiedEndorsement{result: result, key: string(response.Endorsement.Endorser)}, nil
	}
	serialized := &msppb.Identity{}
	if err := proto.Unmarshal(response.Endorsement.Endorser, serialized); err != nil {
		return verifiedEndorsement{}, fmt.Errorf("decode endorser identity: %w", err)
	}
	identity, err := p.deserializer.DeserializeIdentity(serialized)
	if err != nil {
		return verifiedEndorsement{}, fmt.Errorf("deserialize endorser: %w", err)
	}
	if identity.GetMSPIdentifier() != expectedMSP {
		return verifiedEndorsement{}, fmt.Errorf("endorser MSP %q does not match requested MSP %q", identity.GetMSPIdentifier(), expectedMSP)
	}
	if err := identity.Validate(); err != nil {
		return verifiedEndorsement{}, fmt.Errorf("untrusted endorser: %w", err)
	}
	if err := validateIdentityTime(identity, time.Now()); err != nil {
		return verifiedEndorsement{}, err
	}
	data, err := tx.Namespaces[0].ASN1Marshal(result.TxID, tx.Metadata)
	if err != nil {
		return verifiedEndorsement{}, err
	}
	if err := identity.Verify(data, response.Endorsement.Signature); err != nil {
		return verifiedEndorsement{}, fmt.Errorf("invalid namespace endorsement signature: %w", err)
	}
	id := identity.GetIdentifier()
	return verifiedEndorsement{result: result, identity: identity, key: id.Mspid + "\x00" + id.Id}, nil
}

// selectEndorsements searches actual identities without reusing a signer. Each
// candidate selection must also pass Fabric's evaluator in its submitted order;
// this matters when broad and specific principals overlap.
func (p *namespaceEndorsementPolicy) selectEndorsements(ctx context.Context, candidates []verifiedEndorsement) ([]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	if p.policy == nil {
		return []int{0}, nil
	}
	identities := make([]msp.Identity, len(candidates))
	all := make([]int, len(candidates))
	for i, candidate := range candidates {
		identities[i] = candidate.identity
		all[i] = i
	}
	if len(candidates) == 1 && p.policy.EvaluateIdentities(identities) == nil {
		return all, nil
	}
	matches := make([][]bool, len(p.envelope.Identities))
	for i, principal := range p.envelope.Identities {
		matches[i] = make([]bool, len(candidates))
		if principal == nil {
			continue
		}
		for j, identity := range identities {
			matches[i][j] = identity.SatisfiesPrincipal(principal) == nil
		}
	}
	var selected []int
	hasWitness := false
	used := make([]bool, len(candidates))
	var search func(*common.SignaturePolicy, []int, func([]int) bool) bool
	search = func(rule *common.SignaturePolicy, order []int, next func([]int) bool) bool {
		if ctx.Err() != nil {
			return false
		}
		switch r := rule.GetType().(type) {
		case *common.SignaturePolicy_SignedBy:
			for i, matchesPrincipal := range matches[r.SignedBy] {
				if used[i] || !matchesPrincipal {
					continue
				}
				used[i] = true
				found := next(append(order, i))
				used[i] = false
				if found {
					return true
				}
			}
		case *common.SignaturePolicy_NOutOf_:
			var choose func(int, int, []int) bool
			choose = func(index, remaining int, order []int) bool {
				if ctx.Err() != nil {
					return false
				}
				if remaining == 0 {
					return next(order)
				}
				if len(r.NOutOf.Rules)-index < remaining {
					return false
				}
				if search(r.NOutOf.Rules[index], order, func(order []int) bool { return choose(index+1, remaining-1, order) }) {
					return true
				}
				return choose(index+1, remaining, order)
			}
			return choose(0, int(r.NOutOf.N), order)
		}
		return false
	}
	search(p.envelope.Rule, nil, func(order []int) bool {
		hasWitness = true
		if len(order) == 0 {
			// Fabric-X still requires an endorsement for an unconditional policy.
			order = []int{0}
		}
		chosen := make([]msp.Identity, len(order))
		for i, index := range order {
			chosen[i] = identities[index]
		}
		if p.policy.EvaluateIdentities(chosen) != nil {
			return false
		}
		selected = append([]int(nil), order...)
		return true
	})
	if len(selected) == 0 && ctx.Err() == nil && p.policy.EvaluateIdentities(identities) == nil {
		return all, nil
	}
	if len(selected) != 0 || ctx.Err() != nil || !hasWitness {
		return selected, ctx.Err()
	}
	return p.searchSignatureOrders(ctx, identities, matches)
}

// Fabric's evaluator visits every child of an NOutOf rule, including successful
// children beyond its threshold. Nested rules can therefore need extra signers
// beyond a minimal policy witness. Search remaining subsets and orders only
// when witness-based selection fails, pruning interchangeable principal matches.
func (p *namespaceEndorsementPolicy) searchSignatureOrders(ctx context.Context, identities []msp.Identity, matches [][]bool) ([]int, error) {
	profiles := make([]string, len(identities))
	for i := range identities {
		bits := make([]byte, len(matches))
		matched := false
		for principal := range matches {
			if matches[principal][i] {
				bits[principal], matched = 1, true
			}
		}
		if matched {
			profiles[i] = string(bits)
		}
	}
	var selected []int
	order := make([]int, 0, len(identities))
	chosen := make([]msp.Identity, 0, len(identities))
	used := make([]bool, len(identities))
	var search func() bool
	search = func() bool {
		if ctx.Err() != nil {
			return false
		}
		if len(chosen) != 0 && p.policy.EvaluateIdentities(chosen) == nil {
			selected = append([]int(nil), order...)
			return true
		}
		seenProfiles := make(map[string]bool)
		for i, profile := range profiles {
			if used[i] || profile == "" || seenProfiles[profile] {
				continue
			}
			seenProfiles[profile] = true
			used[i] = true
			order, chosen = append(order, i), append(chosen, identities[i])
			if search() {
				return true
			}
			order, chosen = order[:len(order)-1], chosen[:len(chosen)-1]
			used[i] = false
		}
		return false
	}
	search()
	return selected, ctx.Err()
}

func (p *namespaceEndorsementPolicy) verifySelection(end sdk.Endorsement, txID string) error {
	if len(end.Responses) == 0 {
		return errors.New("no selected endorsements")
	}
	tx := &applicationpb.Tx{}
	if err := proto.Unmarshal(end.Responses[0].Payload, tx); err != nil {
		return err
	}
	endorsements := &applicationpb.Endorsements{}
	for _, response := range end.Responses {
		identity := &msppb.Identity{}
		if err := proto.Unmarshal(response.Endorsement.Endorser, identity); err != nil {
			return err
		}
		endorsements.EndorsementsWithIdentity = append(endorsements.EndorsementsWithIdentity, &applicationpb.EndorsementWithIdentity{
			Identity: identity, Endorsement: response.Endorsement.Signature,
		})
	}
	tx.Endorsements = []*applicationpb.Endorsements{endorsements}
	return p.verifier.VerifyNs(txID, tx, 0)
}
