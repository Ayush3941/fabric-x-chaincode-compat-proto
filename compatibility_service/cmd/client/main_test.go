// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"testing"

	"github.com/hyperledger/fabric-x-common/protoutil"
	"google.golang.org/protobuf/proto"
)

func TestProposalSignatureBindsInitIntent(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer := clientTestSigner{key: key}
	for _, isInit := range []bool{false, true} {
		proposal, err := newSignedProposal(signer, "channelqc4", "sample", "1.0", [][]byte{[]byte("initialize"), []byte("asset")}, nil, isInit)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(proposal.ProposalBytes)
		if !ecdsa.VerifyASN1(&key.PublicKey, digest[:], proposal.Signature) {
			t.Fatal("client proposal signature is invalid")
		}
		decoded, err := protoutil.UnmarshalProposal(proposal.ProposalBytes)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := protoutil.UnmarshalChaincodeProposalPayload(decoded.Payload)
		if err != nil {
			t.Fatal(err)
		}
		spec, err := protoutil.UnmarshalChaincodeInvocationSpec(payload.Input)
		if err != nil {
			t.Fatal(err)
		}
		if spec.ChaincodeSpec.Input.IsInit != isInit {
			t.Fatalf("signed init flag = %t, want %t", spec.ChaincodeSpec.Input.IsInit, isInit)
		}
		spec.ChaincodeSpec.Input.IsInit = !isInit
		payload.Input = protoutil.MarshalOrPanic(spec)
		decoded.Payload = protoutil.MarshalOrPanic(payload)
		tampered, err := proto.Marshal(decoded)
		if err != nil {
			t.Fatal(err)
		}
		digest = sha256.Sum256(tampered)
		if ecdsa.VerifyASN1(&key.PublicKey, digest[:], proposal.Signature) {
			t.Fatal("changing init intent preserved the original signature")
		}
	}
}

type clientTestSigner struct{ key *ecdsa.PrivateKey }

func (s clientTestSigner) Serialize() ([]byte, error) { return []byte("creator"), nil }

func (s clientTestSigner) Sign(message []byte) ([]byte, error) {
	digest := sha256.Sum256(message)
	return ecdsa.SignASN1(rand.Reader, s.key, digest[:])
}
