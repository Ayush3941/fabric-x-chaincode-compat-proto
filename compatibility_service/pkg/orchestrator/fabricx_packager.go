// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"bytes"
	b64 "encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"compatibility_service/pkg/helper"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/api/metadatapb"
	"github.com/hyperledger/fabric-x-sdk/endorsement"
	"google.golang.org/protobuf/proto"
)

type compatFabricXTxPackager struct{}

func (compatFabricXTxPackager) PackageTx(end sdk.Endorsement) (*common.Envelope, error) {
	return createCompatFabricXTx(end.Proposal, end.Responses...)
}

func createCompatFabricXTx(proposal *peer.Proposal, resps ...*peer.ProposalResponse) (*common.Envelope, error) {
	if proposal == nil {
		return nil, errors.New("an endorsed proposal is required")
	}
	if len(resps) == 0 {
		return nil, errors.New("at least one proposal response is required")
	}

	hdr, err := protoutil.UnmarshalHeader(proposal.Header)
	if err != nil {
		return nil, err
	}
	shdr, err := protoutil.UnmarshalSignatureHeader(hdr.SignatureHeader)
	if err != nil {
		return nil, err
	}

	var payload []byte
	for i, resp := range resps {
		if resp == nil || resp.Response == nil {
			return nil, errors.New("proposal response is missing its chaincode response")
		}
		if resp.Response.Status < 200 || resp.Response.Status >= 400 {
			return nil, fmt.Errorf("proposal response was not successful, error code %d, msg %s", resp.Response.Status, resp.Response.Message)
		}
		if i == 0 {
			payload = resp.Payload
			continue
		}
		if !bytes.Equal(payload, resp.Payload) {
			return nil, fmt.Errorf("proposal response payloads do not match (base64): %q vs %q",
				b64.StdEncoding.EncodeToString(resp.Payload), b64.StdEncoding.EncodeToString(payload))
		}
	}

	endorsersSeen := make(map[string]struct{})
	nsEndorsements := &applicationpb.Endorsements{}
	for _, resp := range resps {
		if resp.Endorsement == nil {
			continue
		}
		key := string(resp.Endorsement.Endorser)
		if _, ok := endorsersSeen[key]; ok {
			continue
		}
		endorsersSeen[key] = struct{}{}

		identity := &msppb.Identity{}
		if err := proto.Unmarshal(resp.Endorsement.Endorser, identity); err != nil {
			return nil, fmt.Errorf("failed to unmarshal endorser identity: %w", err)
		}
		nsEndorsements.EndorsementsWithIdentity = append(nsEndorsements.EndorsementsWithIdentity,
			&applicationpb.EndorsementWithIdentity{
				Endorsement: resp.Endorsement.Signature,
				Identity:    identity,
			},
		)
	}
	if len(nsEndorsements.EndorsementsWithIdentity) == 0 {
		return nil, errors.New("no endorsements")
	}

	tx := &applicationpb.Tx{}
	if err := proto.Unmarshal(payload, tx); err != nil {
		return nil, fmt.Errorf("expected applicationpb.Tx endorsement payload: %w", err)
	}
	tx.Endorsements = make([]*applicationpb.Endorsements, len(tx.Namespaces))
	for i := range tx.Namespaces {
		tx.Endorsements[i] = nsEndorsements
	}
	txBytes, err := proto.Marshal(tx)
	if err != nil {
		return nil, fmt.Errorf("marshal transaction payload: %w", err)
	}

	chdr, err := protoutil.UnmarshalChannelHeader(hdr.ChannelHeader)
	if err != nil {
		return nil, err
	}
	chdr.Type = int32(common.HeaderType_MESSAGE)
	chdrBytes, err := proto.Marshal(chdr)
	if err != nil {
		return nil, fmt.Errorf("marshal channel header: %w", err)
	}
	shdr.Creator = nil
	shdrBytes, err := proto.Marshal(shdr)
	if err != nil {
		return nil, fmt.Errorf("marshal signature header: %w", err)
	}

	payloadBytes, err := protoutil.GetBytesPayload(&common.Payload{
		Header: &common.Header{
			ChannelHeader:   chdrBytes,
			SignatureHeader: shdrBytes,
		},
		Data: txBytes,
	})
	if err != nil {
		return nil, err
	}
	return &common.Envelope{Payload: payloadBytes}, nil
}

// createGatewayTransaction puts the endorsed native transaction into a Fabric
// transaction container understood by Gateway SDKs. ChaincodeAction.Results
// carries applicationpb.Tx, including its native namespace endorsements; these
// signatures retain Fabric-X semantics, not Fabric proposal-response semantics.
// The outer envelope remains unsigned so the client can authorize its payload.
func createGatewayTransaction(proposal *peer.Proposal, version string, end sdk.Endorsement) (*common.Envelope, error) {
	if proposal == nil {
		return nil, errors.New("the original client proposal is required")
	}
	header, err := protoutil.UnmarshalHeader(proposal.Header)
	if err != nil {
		return nil, err
	}
	channel, err := protoutil.UnmarshalChannelHeader(header.ChannelHeader)
	if err != nil {
		return nil, err
	}
	signature, err := protoutil.UnmarshalSignatureHeader(header.SignatureHeader)
	if err != nil {
		return nil, err
	}
	if channel.Type != int32(common.HeaderType_ENDORSER_TRANSACTION) || len(signature.Creator) == 0 || len(signature.Nonce) == 0 || channel.TxId != protoutil.ComputeTxID(signature.Nonce, signature.Creator) {
		return nil, errors.New("invalid client transaction header")
	}
	txID, err := txIDFromEndorsement(end)
	if err != nil || txID != channel.TxId {
		return nil, errors.New("endorsed transaction ID does not match the client proposal")
	}
	cpp, err := protoutil.UnmarshalChaincodeProposalPayload(proposal.Payload)
	if err != nil {
		return nil, err
	}
	spec, err := protoutil.UnmarshalChaincodeInvocationSpec(cpp.Input)
	if err != nil {
		return nil, err
	}
	chaincode := spec.GetChaincodeSpec().GetChaincodeId().GetName()
	_, namespace, err := gatewayChaincodeRoute(chaincode)
	if err != nil {
		return nil, err
	}
	nativeEnvelope, err := createCompatFabricXTx(end.Proposal, end.Responses...)
	if err != nil {
		return nil, err
	}
	nativePayload, err := protoutil.UnmarshalPayload(nativeEnvelope.Payload)
	if err != nil {
		return nil, err
	}
	nativeChannel, err := protoutil.UnmarshalChannelHeader(nativePayload.Header.ChannelHeader)
	if err != nil || nativeChannel.ChannelId != channel.ChannelId {
		return nil, errors.New("endorsed channel does not match the client proposal")
	}
	native := &applicationpb.Tx{}
	if err := proto.Unmarshal(nativePayload.Data, native); err != nil {
		return nil, err
	}
	if len(native.Namespaces) != 1 || native.Namespaces[0].GetNsId() != namespace || len(native.Metadata) < 2 {
		return nil, errors.New("endorsed native transaction does not match the client namespace")
	}
	binding := &helper.GatewayProposalBinding{}
	if err := json.Unmarshal(native.Metadata[1], binding); err != nil || binding.Format != 1 {
		return nil, errors.New("native transaction requires a version-one Gateway proposal binding")
	}
	expectedBinding, err := helper.MarshalGatewayProposalBinding(proposal, version, binding.Sequence, end.Responses[0].Response)
	if err != nil || !bytes.Equal(expectedBinding, native.Metadata[1]) {
		return nil, errors.New("native endorsement does not bind this proposal, response and lifecycle revision")
	}
	identity, err := helper.ChaincodeIdentityFromMetadata(native.Metadata)
	if err != nil {
		return nil, err
	}
	// Older version-one prepared transactions remain submit-compatible. If a
	// readable identity is present, it must agree with the signed proposal too.
	if identity != nil && (identity.ChannelID != channel.ChannelId || identity.ChaincodeName+":"+identity.Namespace != chaincode || identity.Namespace != namespace) {
		return nil, errors.New("native chaincode identity differs from the prepared proposal")
	}
	metadata := &metadatapb.Metadata{}
	if err := proto.Unmarshal(native.Metadata[0], metadata); err != nil {
		return nil, fmt.Errorf("decode endorsed transaction metadata: %w", err)
	}
	response := end.Responses[0].Response
	if !bytes.Equal(metadata.Payload, response.Payload) || !slices.EqualFunc(metadata.InputArgs, spec.GetChaincodeSpec().GetInput().GetArgs(), bytes.Equal) {
		return nil, errors.New("endorsed arguments or response differ from the prepared transaction")
	}
	// Transient data is sent to executors only and must never be included in a
	// prepared transaction that may later be recorded on the ledger.
	publicProposal, err := protoutil.GetBytesProposalPayloadForTx(cpp)
	if err != nil {
		return nil, err
	}
	proposalHash, err := protoutil.GetProposalHash2(header, publicProposal)
	if err != nil {
		return nil, err
	}
	var event []byte
	if metadata.EventName != "" || len(metadata.Event) != 0 {
		event, err = proto.Marshal(&peer.ChaincodeEvent{ChaincodeId: chaincode, TxId: txID, EventName: metadata.EventName, Payload: metadata.Event})
		if err != nil {
			return nil, err
		}
	}
	responsePayload, err := protoutil.GetBytesProposalResponsePayload(proposalHash, proto.Clone(response).(*peer.Response), nativePayload.Data, event, &peer.ChaincodeID{Name: chaincode, Version: version})
	if err != nil {
		return nil, err
	}
	endorsements := make([]*peer.Endorsement, len(end.Responses))
	for i, response := range end.Responses {
		if response.Endorsement == nil {
			return nil, errors.New("prepared transaction is missing a native endorsement")
		}
		endorsements[i] = proto.Clone(response.Endorsement).(*peer.Endorsement)
	}
	action, err := proto.Marshal(&peer.ChaincodeActionPayload{
		ChaincodeProposalPayload: publicProposal,
		Action:                   &peer.ChaincodeEndorsedAction{ProposalResponsePayload: responsePayload, Endorsements: endorsements},
	})
	if err != nil {
		return nil, err
	}
	transaction, err := proto.Marshal(&peer.Transaction{Actions: []*peer.TransactionAction{{Header: header.SignatureHeader, Payload: action}}})
	if err != nil {
		return nil, err
	}
	payload, err := proto.Marshal(&common.Payload{Header: header, Data: transaction})
	if err != nil {
		return nil, err
	}
	return &common.Envelope{Payload: payload}, nil
}

type gatewayPreparedInvocation struct {
	Request     InvocationRequest
	Endorsement sdk.Endorsement
	Binding     helper.GatewayProposalBinding
	Native      *applicationpb.Tx
}

// parseGatewayTransaction accepts exactly the prepared format emitted by
// Endorse. Rebuilding the public projection detects changes to any mirrored
// header, response, event or endorsement, while preserving native bytes.
// Client authentication and native signature/policy validation are separate.
func parseGatewayTransaction(envelope *common.Envelope) (result gatewayPreparedInvocation, err error) {
	if envelope == nil || len(envelope.Payload) == 0 {
		return result, errors.New("a prepared transaction is required")
	}
	payload, err := protoutil.UnmarshalPayload(envelope.Payload)
	if err != nil || payload.Header == nil {
		return result, errors.New("invalid prepared transaction payload")
	}
	channel, err := protoutil.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return result, err
	}
	signature, err := protoutil.UnmarshalSignatureHeader(payload.Header.SignatureHeader)
	if err != nil {
		return result, err
	}
	if channel.Type != int32(common.HeaderType_ENDORSER_TRANSACTION) || len(signature.Creator) == 0 || len(signature.Nonce) == 0 || channel.TxId != protoutil.ComputeTxID(signature.Nonce, signature.Creator) {
		return result, errors.New("invalid prepared client transaction header")
	}
	transaction := &peer.Transaction{}
	if err := proto.Unmarshal(payload.Data, transaction); err != nil || len(transaction.Actions) != 1 || transaction.Actions[0] == nil {
		return result, errors.New("prepared transaction must contain exactly one action")
	}
	action := transaction.Actions[0]
	if !bytes.Equal(action.Header, payload.Header.SignatureHeader) {
		return result, errors.New("action and envelope signature headers differ")
	}
	actionPayload := &peer.ChaincodeActionPayload{}
	if err := proto.Unmarshal(action.Payload, actionPayload); err != nil || actionPayload.Action == nil {
		return result, errors.New("invalid prepared chaincode action")
	}
	cpp, err := protoutil.UnmarshalChaincodeProposalPayload(actionPayload.ChaincodeProposalPayload)
	if err != nil || len(cpp.TransientMap) != 0 {
		return result, errors.New("prepared proposal must exclude transient data")
	}
	spec, err := protoutil.UnmarshalChaincodeInvocationSpec(cpp.Input)
	if err != nil || spec.GetChaincodeSpec().GetChaincodeId() == nil || spec.GetChaincodeSpec().GetInput() == nil {
		return result, errors.New("invalid prepared proposal input")
	}
	extension := &peer.ChaincodeHeaderExtension{}
	if err := proto.Unmarshal(channel.Extension, extension); err != nil || !proto.Equal(extension.ChaincodeId, spec.GetChaincodeSpec().ChaincodeId) {
		return result, errors.New("prepared header and proposal chaincode identifiers differ")
	}
	responsePayload := &peer.ProposalResponsePayload{}
	if err := proto.Unmarshal(actionPayload.Action.ProposalResponsePayload, responsePayload); err != nil {
		return result, err
	}
	execution := &peer.ChaincodeAction{}
	if err := proto.Unmarshal(responsePayload.Extension, execution); err != nil || execution.Response == nil {
		return result, errors.New("prepared transaction is missing its chaincode response")
	}
	native := &applicationpb.Tx{}
	if err := proto.Unmarshal(execution.Results, native); err != nil || len(native.Namespaces) != 1 || native.Namespaces[0] == nil || len(native.Endorsements) != 1 || native.Endorsements[0] == nil || len(native.Metadata) < 2 {
		return result, errors.New("invalid native transaction in prepared envelope")
	}
	if err := json.Unmarshal(native.Metadata[1], &result.Binding); err != nil || result.Binding.Format != 1 {
		return result, errors.New("missing version-one native proposal binding; obtain a fresh endorsement")
	}
	proposal := &peer.Proposal{Header: protoutil.MarshalOrPanic(payload.Header), Payload: actionPayload.ChaincodeProposalPayload}
	result.Endorsement.Proposal = proposal
	for _, nativeEndorsement := range native.Endorsements[0].EndorsementsWithIdentity {
		if nativeEndorsement == nil || nativeEndorsement.Identity == nil || len(nativeEndorsement.Endorsement) == 0 {
			return result, errors.New("native transaction is missing an endorsement identity or signature")
		}
		identity, err := proto.Marshal(nativeEndorsement.Identity)
		if err != nil {
			return result, err
		}
		result.Endorsement.Responses = append(result.Endorsement.Responses, &peer.ProposalResponse{
			Response: proto.Clone(execution.Response).(*peer.Response), Payload: append([]byte(nil), execution.Results...),
			Endorsement: &peer.Endorsement{Endorser: identity, Signature: append([]byte(nil), nativeEndorsement.Endorsement...)},
		})
	}
	expected, err := createGatewayTransaction(proposal, result.Binding.ChaincodeVersion, result.Endorsement)
	if err != nil {
		return result, err
	}
	if !bytes.Equal(expected.Payload, envelope.Payload) {
		return result, errors.New("prepared transaction differs from its endorsed public projection")
	}
	result.Request, err = requestFromProposal(endorsement.Invocation{Namespace: spec.ChaincodeSpec.ChaincodeId.Name, Args: spec.ChaincodeSpec.Input.Args, Proposal: proposal})
	if err != nil {
		return result, err
	}
	result.Request.ChaincodeName, result.Request.Namespace, err = gatewayChaincodeRoute(result.Request.ChaincodeName)
	if err != nil {
		return result, err
	}
	result.Request.ClientTxID = channel.TxId
	result.Request.ClientCreator = append([]byte(nil), signature.Creator...)
	result.Request.ClientNonce = append([]byte(nil), signature.Nonce...)
	result.Request.ChaincodeVersion = result.Binding.ChaincodeVersion
	result.Native = native
	return result, nil
}
