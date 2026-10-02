// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"bytes"
	b64 "encoding/base64"
	"errors"
	"fmt"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/msppb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"google.golang.org/protobuf/proto"
)

type compatFabricXTxPackager struct{}

func (compatFabricXTxPackager) PackageTx(end sdk.Endorsement) (*common.Envelope, error) {
	return createCompatFabricXTx(end.Proposal, end.Responses...)
}

func createCompatFabricXTx(proposal *peer.Proposal, resps ...*peer.ProposalResponse) (*common.Envelope, error) {
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
