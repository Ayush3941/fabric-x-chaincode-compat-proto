// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"errors"
	"testing"
)

func TestIdempotencyStoreReplaysCompletedResult(t *testing.T) {
	store := newIdempotencyStore()

	record, owner, err := store.begin("key1", "digest1")
	if err != nil {
		t.Fatalf("begin first: %v", err)
	}
	if !owner {
		t.Fatal("first caller should own execution")
	}

	expected := InvocationResponse{TxID: "tx1", Status: 200, CommitStatus: "COMMITTED"}
	store.complete(record, expected, nil)

	record, owner, err = store.begin("key1", "digest1")
	if err != nil {
		t.Fatalf("begin duplicate: %v", err)
	}
	if owner {
		t.Fatal("duplicate caller should not own execution")
	}

	actual, err := store.wait(context.Background(), record)
	if err != nil {
		t.Fatalf("wait duplicate: %v", err)
	}
	if actual.TxID != expected.TxID || actual.CommitStatus != expected.CommitStatus {
		t.Fatalf("unexpected replay response: %#v", actual)
	}
}

func TestIdempotencyStoreRejectsDigestConflict(t *testing.T) {
	store := newIdempotencyStore()
	if _, _, err := store.begin("key1", "digest1"); err != nil {
		t.Fatalf("begin first: %v", err)
	}
	if _, _, err := store.begin("key1", "digest2"); err == nil {
		t.Fatal("expected digest conflict")
	}
}

func TestIdempotencyStoreReplaysFailure(t *testing.T) {
	store := newIdempotencyStore()
	record, _, err := store.begin("key1", "digest1")
	if err != nil {
		t.Fatalf("begin first: %v", err)
	}
	expectedErr := errors.New("boom")
	store.complete(record, InvocationResponse{TxID: "tx1"}, expectedErr)

	record, owner, err := store.begin("key1", "digest1")
	if err != nil {
		t.Fatalf("begin duplicate: %v", err)
	}
	if owner {
		t.Fatal("duplicate caller should not own failed execution")
	}
	_, err = store.wait(context.Background(), record)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected replayed error, got %v", err)
	}
}

func TestRequestDigestIncludesClientTxID(t *testing.T) {
	req1 := InvocationRequest{
		ClientTxID:    "client-tx-1",
		ClientCreator: []byte("creator"),
		Function:      "compatv2",
		Args:          []string{"asset1", "value1", "delete1"},
	}
	req2 := req1
	req2.ClientTxID = "client-tx-2"

	digest1 := requestDigest("channelqc4", "0", req1, true)
	digest2 := requestDigest("channelqc4", "0", req2, true)
	if digest1 == digest2 {
		t.Fatal("different client tx ids should produce different digest")
	}

	req2.ClientTxID = req1.ClientTxID
	req2.Args[1] = "different-value"
	digest3 := requestDigest("channelqc4", "0", req2, true)
	if digest1 == digest3 {
		t.Fatal("different request args should produce different digest")
	}
}

func TestIdempotencyIdentityUsesClientTxIDAsKey(t *testing.T) {
	req1 := InvocationRequest{
		ClientTxID:    "client-tx-1",
		ClientCreator: []byte("creator"),
		Function:      "compatv2",
		Args:          []string{"asset1", "value1", "delete1"},
	}
	req2 := req1
	req2.Args = []string{"asset1", "different-value", "delete1"}

	key1, digest1 := idempotencyIdentity("channelqc4", "0", req1, true)
	key2, digest2 := idempotencyIdentity("channelqc4", "0", req2, true)
	if key1 != req1.ClientTxID || key2 != req1.ClientTxID {
		t.Fatalf("expected idempotency key to use client tx id, got %q and %q", key1, key2)
	}
	if digest1 == digest2 {
		t.Fatal("same tx id with different args should produce different request digests")
	}
}

func TestRequestDigestIncludesTransient(t *testing.T) {
	req1 := InvocationRequest{
		ClientCreator: []byte("creator"),
		Function:      "compatv2",
		Args:          []string{"asset1", "value1", "delete1"},
		ClientTransient: map[string][]byte{
			"secret": []byte("alpha"),
		},
	}
	req2 := req1
	req2.ClientTransient = map[string][]byte{
		"secret": []byte("beta"),
	}

	digest1 := requestDigest("channelqc4", "0", req1, true)
	digest2 := requestDigest("channelqc4", "0", req2, true)
	if digest1 == digest2 {
		t.Fatal("different transient values should produce different digest")
	}
}
