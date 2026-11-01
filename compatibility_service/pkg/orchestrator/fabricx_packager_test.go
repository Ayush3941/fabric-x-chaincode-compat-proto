// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"bytes"
	"testing"
	"time"

	"compatibility_service/pkg/helper"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"google.golang.org/protobuf/proto"
)

type gatewayTransactionTestParts struct {
	payload       *common.Payload
	proposal      *peer.ChaincodeProposalPayload
	response      *peer.ProposalResponsePayload
	action        *peer.ChaincodeAction
	native        *applicationpb.Tx
	transaction   *peer.Transaction
	actionPayload *peer.ChaincodeActionPayload
}

func decodeGatewayTestTransaction(t *testing.T, envelope *common.Envelope) gatewayTransactionTestParts {
	t.Helper()
	parts := gatewayTransactionTestParts{
		payload: &common.Payload{}, proposal: &peer.ChaincodeProposalPayload{}, response: &peer.ProposalResponsePayload{}, action: &peer.ChaincodeAction{}, native: &applicationpb.Tx{},
	}
	if err := proto.Unmarshal(envelope.Payload, parts.payload); err != nil {
		t.Fatal(err)
	}
	transaction := &peer.Transaction{}
	if err := proto.Unmarshal(parts.payload.Data, transaction); err != nil || len(transaction.Actions) != 1 {
		t.Fatalf("invalid SDK transaction container: %v", err)
	}
	if !bytes.Equal(transaction.Actions[0].Header, parts.payload.Header.SignatureHeader) {
		t.Fatal("transaction action lost the client signature header")
	}
	payload := &peer.ChaincodeActionPayload{}
	if err := proto.Unmarshal(transaction.Actions[0].Payload, payload); err != nil {
		t.Fatal(err)
	}
	parts.transaction, parts.actionPayload = transaction, payload
	if err := proto.Unmarshal(payload.ChaincodeProposalPayload, parts.proposal); err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(payload.Action.ProposalResponsePayload, parts.response); err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(parts.response.Extension, parts.action); err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(parts.action.Results, parts.native); err != nil || len(parts.native.Namespaces) != 1 || len(parts.native.Endorsements) != 1 {
		t.Fatalf("prepared envelope lost the native transaction: %v", err)
	}
	return parts
}

func TestCreateGatewayTransactionPreservesClientAndNativePayload(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	actor := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	signed := gatewayTestProposal(t, actor, "sample:1", false)
	proposal, err := protoutil.UnmarshalProposal(signed.ProposalBytes)
	if err != nil {
		t.Fatal(err)
	}
	result := gatewayTestEndorsement(t, actor, signed, 7, "payload")
	native, err := createCompatFabricXTx(result.Endorsement.Proposal, result.Endorsement.Responses...)
	if err != nil {
		t.Fatal(err)
	}
	nativePayload, err := protoutil.UnmarshalPayload(native.Payload)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := createGatewayTransaction(proposal, "2.0", result.Endorsement)
	if err != nil {
		t.Fatal(err)
	}
	parts := decodeGatewayTestTransaction(t, prepared)
	if !bytes.Equal(parts.action.Results, nativePayload.Data) || !bytes.Equal(proposal.Header, protoutil.MarshalOrPanic(parts.payload.Header)) || len(prepared.Signature) != 0 {
		t.Fatal("prepared envelope changed the endorsed native bytes or client header")
	}
	hash, err := protoutil.GetProposalHash1(parts.payload.Header, proposal.Payload)
	if err != nil || !bytes.Equal(hash, parts.response.ProposalHash) {
		t.Fatalf("prepared response is not bound to the original public proposal: %v", err)
	}
	if parts.action.ChaincodeId.Name != "sample:1" || parts.action.ChaincodeId.Version != "2.0" || string(parts.action.Response.Payload) != "payload" {
		t.Fatal("prepared transaction lost the logical chaincode, active version, or response")
	}
	event := &peer.ChaincodeEvent{}
	if err := proto.Unmarshal(parts.action.Events, event); err != nil || event.ChaincodeId != "sample:1" || event.TxId != result.TxID || event.EventName != "event" || string(event.Payload) != "event-value" {
		t.Fatalf("prepared simulation event differs from native metadata: %v %v", event, err)
	}
	compiled, err := newNamespaceEndorsementPolicy(endorsementTestPolicy(t, "OR('org-0.peer')"), proposalTestVerifier(t, []ProposalMSPConfig{org.config}, "OR('org-0.member')").identities)
	if err != nil {
		t.Fatal(err)
	}
	if err := compiled.verifier.VerifyNs(result.TxID, parts.native, 0); err != nil {
		t.Fatalf("native signature did not survive packaging: %v", err)
	}
}

func TestCreateGatewayTransactionRejectsMismatchedBindings(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	actor := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	for _, scenario := range []string{"missing-client", "transaction-id", "namespace", "arguments", "response", "missing-response", "metadata"} {
		t.Run(scenario, func(t *testing.T) {
			signed := gatewayTestProposal(t, actor, "sample:1", false)
			proposal, err := protoutil.UnmarshalProposal(signed.ProposalBytes)
			if err != nil {
				t.Fatal(err)
			}
			result := gatewayTestEndorsement(t, actor, signed, 7, "payload")
			switch scenario {
			case "missing-client":
				proposal = nil
			case "transaction-id":
				other := gatewayTestProposal(t, actor, "sample:1", false)
				result = gatewayTestEndorsement(t, actor, other, 7, "payload")
			case "namespace":
				other := gatewayTestProposal(t, actor, "sample:2", false)
				proposal, err = protoutil.UnmarshalProposal(other.ProposalBytes)
				if err != nil {
					t.Fatal(err)
				}
				// Keep this client's identity and nonce to isolate namespace binding.
				proposal.Header = result.Endorsement.Proposal.Header
			case "arguments":
				payload, _ := protoutil.UnmarshalChaincodeProposalPayload(proposal.Payload)
				spec, _ := protoutil.UnmarshalChaincodeInvocationSpec(payload.Input)
				spec.ChaincodeSpec.Input.Args[0] = []byte("different-function")
				payload.Input = protoutil.MarshalOrPanic(spec)
				proposal.Payload = protoutil.MarshalOrPanic(payload)
			case "response":
				result.Endorsement.Responses[0].Response.Payload = []byte("different-result")
			case "missing-response":
				result.Endorsement.Responses[0].Response = nil
			case "metadata":
				native := &applicationpb.Tx{}
				if err := proto.Unmarshal(result.Endorsement.Responses[0].Payload, native); err != nil {
					t.Fatal(err)
				}
				native.Metadata[0] = []byte{255}
				result.Endorsement.Responses[0].Payload = protoutil.MarshalOrPanic(native)
			}
			if _, err := createGatewayTransaction(proposal, "2.0", result.Endorsement); err == nil {
				t.Fatal("mismatched prepared transaction was accepted")
			}
		})
	}
}

func gatewayTestEndorsement(t *testing.T, actor proposalTestActor, signed *peer.SignedProposal, policyVersion uint64, payload string) helperExecutionResult {
	t.Helper()
	result := endorsementTestResult(t, actor, signed, policyVersion, payload)
	proposal, err := protoutil.UnmarshalProposal(signed.ProposalBytes)
	if err != nil {
		t.Fatal(err)
	}
	native := &applicationpb.Tx{}
	if err := proto.Unmarshal(result.Endorsement.Responses[0].Payload, native); err != nil {
		t.Fatal(err)
	}
	binding, err := helper.MarshalGatewayProposalBinding(proposal, "2.0", 2, result.Response)
	if err != nil {
		t.Fatal(err)
	}
	native.Metadata = append(native.Metadata, binding)
	identity, err := helper.MarshalChaincodeIdentity("channelqc4", "sample", native.Namespaces[0].NsId)
	if err != nil {
		t.Fatal(err)
	}
	native.Metadata = append(native.Metadata, identity)
	result.Endorsement.Responses[0].Payload = protoutil.MarshalOrPanic(native)
	data, err := native.Namespaces[0].ASN1Marshal(result.TxID, native.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	result.Endorsement.Responses[0].Endorsement.Signature, err = actor.signer.Sign(data)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestGatewayPreparedTransactionsWithoutEventIdentity(t *testing.T) {
	org := newProposalTestMSP(t, "org-0")
	now := time.Now()
	actor := org.issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	signed := gatewayTestProposal(t, actor, "sample:1", false)
	result := gatewayTestEndorsement(t, actor, signed, 7, "payload")
	native := &applicationpb.Tx{}
	_ = proto.Unmarshal(result.Endorsement.Responses[0].Payload, native)
	native.Metadata = native.Metadata[:2]
	data, err := native.Namespaces[0].ASN1Marshal(result.TxID, native.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	result.Endorsement.Responses[0].Endorsement.Signature, _ = actor.signer.Sign(data)
	result.Endorsement.Responses[0].Payload = protoutil.MarshalOrPanic(native)
	proposal, _ := protoutil.UnmarshalProposal(signed.ProposalBytes)
	envelope, err := createGatewayTransaction(proposal, "2.0", result.Endorsement)
	if err != nil {
		t.Fatalf("previous Gateway prepared format rejected: %v", err)
	}
	if _, err := parseGatewayTransaction(envelope); err != nil {
		t.Fatal(err)
	}
}

func gatewayTestEnvelopeFromNative(t *testing.T, original gatewayPreparedInvocation, native *applicationpb.Tx) *common.Envelope {
	t.Helper()
	end := original.Endorsement
	for i, response := range end.Responses {
		response.Payload = protoutil.MarshalOrPanic(native)
		response.Endorsement.Signature = append([]byte(nil), native.Endorsements[0].EndorsementsWithIdentity[i].Endorsement...)
	}
	envelope, err := createGatewayTransaction(end.Proposal, original.Binding.ChaincodeVersion, end)
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func marshalGatewayTestTransaction(t *testing.T, parts gatewayTransactionTestParts) []byte {
	t.Helper()
	parts.action.Results = protoutil.MarshalOrPanic(parts.native)
	parts.response.Extension = protoutil.MarshalOrPanic(parts.action)
	parts.actionPayload.Action.ProposalResponsePayload = protoutil.MarshalOrPanic(parts.response)
	parts.actionPayload.ChaincodeProposalPayload = protoutil.MarshalOrPanic(parts.proposal)
	parts.transaction.Actions[0].Payload = protoutil.MarshalOrPanic(parts.actionPayload)
	parts.payload.Data = protoutil.MarshalOrPanic(parts.transaction)
	return protoutil.MarshalOrPanic(parts.payload)
}

func resignGatewayTestNative(t *testing.T, signer sdk.Signer, txID string, native *applicationpb.Tx) {
	t.Helper()
	if len(native.Endorsements) == 0 {
		return
	}
	data, err := native.Namespaces[0].ASN1Marshal(txID, native.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	signature, err := signer.Sign(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, endorsement := range native.Endorsements[0].EndorsementsWithIdentity {
		endorsement.Endorsement = signature
	}
}
