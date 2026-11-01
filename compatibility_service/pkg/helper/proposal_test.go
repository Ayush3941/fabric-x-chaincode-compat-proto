// SPDX-License-Identifier: Apache-2.0

package helper

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"google.golang.org/protobuf/proto"
)

func TestGatewayProposalBindingCoversPublicIntentAndRevision(t *testing.T) {
	proposal, _, err := protoutil.CreateChaincodeProposal(common.HeaderType_ENDORSER_TRANSACTION, "channelqc4", &peer.ChaincodeInvocationSpec{
		ChaincodeSpec: &peer.ChaincodeSpec{ChaincodeId: &peer.ChaincodeID{Name: "sample:1"}, Input: &peer.ChaincodeInput{Args: [][]byte{[]byte("initialize")}}},
	}, []byte("creator"))
	if err != nil {
		t.Fatal(err)
	}
	response := &peer.Response{Status: 200, Message: "success"}
	original, err := MarshalGatewayProposalBinding(proposal, "2.0", 2, response)
	if err != nil {
		t.Fatal(err)
	}
	var binding GatewayProposalBinding
	if err := json.Unmarshal(original, &binding); err != nil || binding.Format != 1 || binding.Sequence != 2 || binding.ChaincodeVersion != "2.0" {
		t.Fatalf("invalid proposal binding: %+v %v", binding, err)
	}
	for _, scenario := range []string{"init", "arguments", "chaincode", "transient", "version", "sequence", "status", "message"} {
		t.Run(scenario, func(t *testing.T) {
			changed := proto.Clone(proposal).(*peer.Proposal)
			payload, _ := protoutil.UnmarshalChaincodeProposalPayload(changed.Payload)
			spec, _ := protoutil.UnmarshalChaincodeInvocationSpec(payload.Input)
			version, sequence := "2.0", int64(2)
			result := proto.Clone(response).(*peer.Response)
			switch scenario {
			case "init":
				spec.ChaincodeSpec.Input.IsInit = true
			case "arguments":
				spec.ChaincodeSpec.Input.Args[0] = []byte("different")
			case "chaincode":
				spec.ChaincodeSpec.ChaincodeId.Name = "other:1"
			case "transient":
				payload.TransientMap = map[string][]byte{"secret": []byte("private")}
			case "version":
				version = "3.0"
			case "sequence":
				sequence = 3
			case "status":
				result.Status = 201
			case "message":
				result.Message = "different"
			}
			payload.Input = protoutil.MarshalOrPanic(spec)
			changed.Payload = protoutil.MarshalOrPanic(payload)
			encoded, err := MarshalGatewayProposalBinding(changed, version, sequence, result)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(original, encoded) != (scenario == "transient") {
				t.Fatalf("binding changed incorrectly for %s", scenario)
			}
			if bytes.Contains(encoded, []byte("private")) {
				t.Fatal("binding leaked a transient value")
			}
		})
	}
}

func TestChaincodeIdentityMetadata(t *testing.T) {
	record, err := MarshalChaincodeIdentity("channelqc4", "sample", "1")
	if err != nil {
		t.Fatal(err)
	}
	metadata := [][]byte{[]byte("sdk metadata"), []byte(`{"format":1,"proposal_hash":"opaque"}`), record}
	identity, err := ChaincodeIdentityFromMetadata(metadata)
	if err != nil || identity == nil || identity.ChaincodeName != "sample" || identity.Namespace != "1" || identity.ChannelID != "channelqc4" {
		t.Fatalf("identity was not recoverable with other metadata records: %+v %v", identity, err)
	}
	for _, metadata := range [][][]byte{nil, {[]byte("sdk")}, {[]byte("sdk"), []byte(`{"format":1}`)}} {
		if identity, err := ChaincodeIdentityFromMetadata(metadata); identity != nil || err != nil {
			t.Fatalf("invented identity for old metadata: %+v %v", identity, err)
		}
	}
	for _, scenario := range []string{"duplicate", "version", "channel", "name", "namespace", "field-type"} {
		t.Run(scenario, func(t *testing.T) {
			changed := *identity
			switch scenario {
			case "version":
				changed.Version++
			case "channel":
				changed.ChannelID = ""
			case "name":
				changed.ChaincodeName = ""
			case "namespace":
				changed.Namespace = ""
			}
			encoded, _ := json.Marshal(changed)
			if scenario == "field-type" {
				encoded = []byte(`{"type":"fabric-x-chaincode-identity","version":"invalid"}`)
			}
			metadata := [][]byte{nil, encoded}
			if scenario == "duplicate" {
				metadata = append(metadata, record)
			}
			if _, err := ChaincodeIdentityFromMetadata(metadata); err == nil {
				t.Fatal("invalid or ambiguous identity accepted")
			}
		})
	}
	if _, err := MarshalChaincodeIdentity("", "sample", "1"); err == nil {
		t.Fatal("missing channel accepted")
	}
}
