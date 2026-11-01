// SPDX-License-Identifier: Apache-2.0

package helper

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"google.golang.org/protobuf/proto"
)

const chaincodeIdentityType = "fabric-x-chaincode-identity"

// ChaincodeIdentity is an application metadata record covered by the native
// namespace signatures. Unlike the proposal hash, its routing fields can be
// read from committed blocks without the original proposal or local memory.
type ChaincodeIdentity struct {
	Type          string `json:"type"`
	Version       int    `json:"version"`
	ChannelID     string `json:"channel_id"`
	ChaincodeName string `json:"chaincode_name"`
	Namespace     string `json:"namespace"`
}

func MarshalChaincodeIdentity(channel, name, namespace string) ([]byte, error) {
	if channel == "" || name == "" || namespace == "" {
		return nil, fmt.Errorf("chaincode identity requires channel, name and namespace")
	}
	return json.Marshal(ChaincodeIdentity{Type: chaincodeIdentityType, Version: 1, ChannelID: channel, ChaincodeName: name, Namespace: namespace})
}

// ChaincodeIdentityFromMetadata ignores records belonging to other consumers,
// including the SDK metadata and the existing Gateway proposal binding. Older
// transactions have no identity record; callers must not guess a chaincode name
// from a namespace that can be shared by several chaincodes.
func ChaincodeIdentityFromMetadata(metadata [][]byte) (*ChaincodeIdentity, error) {
	if len(metadata) < 2 {
		return nil, nil
	}
	var identity *ChaincodeIdentity
	for _, record := range metadata[1:] {
		var kind struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(record, &kind) != nil || kind.Type != chaincodeIdentityType {
			continue
		}
		var candidate ChaincodeIdentity
		if err := json.Unmarshal(record, &candidate); err != nil {
			return nil, fmt.Errorf("decode chaincode identity: %w", err)
		}
		if identity != nil || candidate.Version != 1 || candidate.ChannelID == "" || candidate.ChaincodeName == "" || candidate.Namespace == "" {
			return nil, fmt.Errorf("invalid or duplicate chaincode identity metadata")
		}
		identity = &candidate
	}
	return identity, nil
}

// GatewayProposalBinding is stored in native transaction metadata[1]. Native
// namespace signatures cover it alongside the SDK metadata at index zero.
// ProposalHash covers the original public proposal, including channel, creator,
// nonce, routing and Init intent. Transient values are excluded as in Fabric.
type GatewayProposalBinding struct {
	Format           int    `json:"format"`
	ProposalHash     []byte `json:"proposal_hash"`
	ChaincodeVersion string `json:"chaincode_version"`
	Sequence         int64  `json:"sequence"`
	Status           int32  `json:"status"`
	Message          string `json:"message"`
}

// MarshalGatewayProposalBinding defines the canonical version-one binding.
func MarshalGatewayProposalBinding(proposal *peer.Proposal, version string, sequence int64, response *peer.Response) ([]byte, error) {
	if proposal == nil || response == nil || version == "" || sequence < 1 {
		return nil, fmt.Errorf("Gateway binding requires a proposal, response and committed definition")
	}
	header, err := protoutil.UnmarshalHeader(proposal.Header)
	if err != nil {
		return nil, err
	}
	hash, err := protoutil.GetProposalHash1(header, proposal.Payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(GatewayProposalBinding{Format: 1, ProposalHash: hash, ChaincodeVersion: version, Sequence: sequence, Status: response.Status, Message: response.Message})
}

// bindChaincodeMetadata preserves the SDK's native RW-set conversion and signs
// the completed metadata once. Both client APIs retain a readable chaincode
// identity; Gateway also retains its existing version-one proposal binding.
func bindChaincodeMetadata(signer sdk.Signer, response *peer.ProposalResponse, original *peer.Proposal, namespace string, decorations map[string][]byte) error {
	name := string(decorations["compat.chaincode_name"])
	if name == "" {
		return nil
	}
	cpp, err := protoutil.UnmarshalChaincodeProposalPayload(original.Payload)
	if err != nil {
		return err
	}
	spec, err := protoutil.UnmarshalChaincodeInvocationSpec(cpp.Input)
	if err != nil {
		return err
	}
	native := &applicationpb.Tx{}
	if err := proto.Unmarshal(response.Payload, native); err != nil {
		return err
	}
	if len(native.Metadata) != 1 || len(native.Namespaces) != 1 || response.Endorsement == nil || signer == nil {
		return fmt.Errorf("invalid native endorsement for Gateway proposal binding")
	}
	header, err := protoutil.UnmarshalHeader(original.Header)
	if err != nil {
		return err
	}
	channel, err := protoutil.UnmarshalChannelHeader(header.ChannelHeader)
	if err != nil {
		return err
	}
	if spec.GetChaincodeSpec().GetChaincodeId().GetName() == name+":"+namespace {
		sequence, err := strconv.ParseInt(string(decorations["compat.chaincode_sequence"]), 10, 64)
		if err != nil {
			return fmt.Errorf("Gateway binding sequence: %w", err)
		}
		binding, err := MarshalGatewayProposalBinding(original, string(decorations["compat.chaincode_version"]), sequence, response.Response)
		if err != nil {
			return err
		}
		native.Metadata = append(native.Metadata, binding)
	}
	identity, err := MarshalChaincodeIdentity(channel.ChannelId, name, namespace)
	if err != nil {
		return err
	}
	native.Metadata = append(native.Metadata, identity)
	data, err := native.Namespaces[0].ASN1Marshal(channel.TxId, native.Metadata)
	if err != nil {
		return err
	}
	signature, err := signer.Sign(data)
	if err != nil {
		return err
	}
	response.Payload, err = proto.Marshal(native)
	if err != nil {
		return err
	}
	response.Endorsement.Signature = signature
	return nil
}

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
