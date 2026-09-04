// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"strings"
	"testing"

	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	sdk "github.com/hyperledger/fabric-x-sdk"
)

func TestCompareCanonicalResultsAcceptsMatchingRemoteResults(t *testing.T) {
	local := testHelperExecutionResult("tx1", 200, "ok", []byte("payload"), []byte("tx-payload"))
	remote := []helperExecutionResult{
		testHelperExecutionResult("tx1", 200, "ok", []byte("payload"), []byte("tx-payload")),
	}

	if err := compareCanonicalResults(local, remote); err != nil {
		t.Fatalf("compareCanonicalResults returned error: %v", err)
	}
}

func TestCompareCanonicalResultsRejectsPayloadMismatch(t *testing.T) {
	local := testHelperExecutionResult("tx1", 200, "ok", []byte("payload"), []byte("tx-payload"))
	remote := []helperExecutionResult{
		testHelperExecutionResult("tx1", 200, "ok", []byte("different"), []byte("tx-payload")),
	}

	err := compareCanonicalResults(local, remote)
	if err == nil || !strings.Contains(err.Error(), "response payload mismatch") {
		t.Fatalf("expected payload mismatch error, got %v", err)
	}
}

func TestMergeEndorsementsKeepsOneProposalAndAllResponses(t *testing.T) {
	local := testHelperExecutionResult("tx1", 200, "ok", []byte("payload"), []byte("tx-payload"))
	remote := []helperExecutionResult{
		testHelperExecutionResult("tx1", 200, "ok", []byte("payload"), []byte("tx-payload")),
	}

	merged, err := mergeEndorsements(local, remote)
	if err != nil {
		t.Fatalf("mergeEndorsements returned error: %v", err)
	}
	if merged.Proposal != local.Endorsement.Proposal {
		t.Fatal("expected merged endorsement to use local proposal")
	}
	if len(merged.Responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(merged.Responses))
	}
}

func testHelperExecutionResult(txID string, status int32, message string, responsePayload, txPayload []byte) helperExecutionResult {
	proposal := &peer.Proposal{Header: []byte(txID)}
	response := &peer.Response{
		Status:  status,
		Message: message,
		Payload: responsePayload,
	}
	proposalResponse := &peer.ProposalResponse{
		Response: response,
		Payload:  txPayload,
		Endorsement: &peer.Endorsement{
			Endorser:  []byte("endorser-" + txID),
			Signature: []byte("signature-" + txID),
		},
	}
	return helperExecutionResult{
		TxID:     txID,
		Response: response,
		Endorsement: sdk.Endorsement{
			Proposal:  proposal,
			Responses: []*peer.ProposalResponse{proposalResponse},
		},
	}
}
