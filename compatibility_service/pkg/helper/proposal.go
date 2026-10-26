// SPDX-License-Identifier: Apache-2.0

package helper

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/protoutil"
)

// ProposalIsInit reads Fabric's init flag from proposal bytes. It does not
// authenticate a signature; public admission must authenticate before using it.
func ProposalIsInit(proposal *peer.Proposal) (bool, error) {
	if proposal == nil {
		return false, fmt.Errorf("proposal is required")
	}
	payload, err := protoutil.UnmarshalChaincodeProposalPayload(proposal.Payload)
	if err != nil {
		return false, fmt.Errorf("decode proposal payload: %w", err)
	}
	spec, err := protoutil.UnmarshalChaincodeInvocationSpec(payload.Input)
	if err != nil {
		return false, fmt.Errorf("decode chaincode invocation: %w", err)
	}
	if spec.ChaincodeSpec == nil || spec.ChaincodeSpec.Input == nil {
		return false, fmt.Errorf("chaincode input is required")
	}
	return spec.ChaincodeSpec.Input.IsInit, nil
}
