// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"errors"
	"testing"

	sdk "github.com/hyperledger/fabric-x-sdk"
)

func TestIdempotencyStoreEvictsCompletedResultWithoutLosingActiveWaiters(t *testing.T) {
	store := newIdempotencyStore()

	record, isFirstRequest, err := store.begin("key1", "digest1")
	if err != nil {
		t.Fatalf("begin first: %v", err)
	}
	if !isFirstRequest {
		t.Fatal("first caller should own execution")
	}

	expected := InvocationResponse{TxID: "tx1", Status: 200, CommitStatus: "COMMITTED"}
	waiter, isFirstRequest, err := store.begin("key1", "digest1")
	if err != nil {
		t.Fatalf("begin duplicate: %v", err)
	}
	if isFirstRequest {
		t.Fatal("duplicate caller should not own execution")
	}
	store.complete(record, expected, nil)
	if len(store.records) != 0 {
		t.Fatal("completed transaction retained in store")
	}

	actual, err := store.wait(context.Background(), waiter)
	if err != nil {
		t.Fatalf("wait duplicate: %v", err)
	}
	if actual.TxID != expected.TxID || actual.CommitStatus != expected.CommitStatus {
		t.Fatalf("unexpected replay response: %#v", actual)
	}
	replacement, first, err := store.begin("key1", "different-digest")
	if err != nil || !first || replacement == record {
		t.Fatalf("historical request still retained: first=%t error=%v", first, err)
	}
	store.complete(record, InvocationResponse{}, nil)
	if store.records["key1"] != replacement {
		t.Fatal("old cleanup removed a newer request")
	}
}

func TestIdempotencyStoreNeverAccumulatesTerminalHistory(t *testing.T) {
	store := newIdempotencyStore()
	for i := range 1000 {
		record, first, err := store.begin("reused-key", "digest")
		if err != nil || !first {
			t.Fatalf("previous terminal record retained at iteration %d: %v", i, err)
		}
		waiter, _, _ := store.begin(record.Key, record.Digest)
		if i%2 == 0 {
			store.complete(record, InvocationResponse{TxID: "never-sent"}, errors.New("execution failed"))
		} else {
			store.markSubmitted(record, InvocationResponse{TxID: "sent", Submitted: true}, sdk.Endorsement{}, nil)
			store.complete(record, InvocationResponse{}, context.DeadlineExceeded)
			if len(store.records) != 1 {
				t.Fatal("unknown finality was discarded")
			}
			store.confirmFinality(record, InvocationResponse{TxID: "sent", Submitted: true, CommitStatus: "ABORTED_MVCC_CONFLICT"}, nil)
		}
		if len(store.records) != 0 {
			t.Fatal("terminal history accumulated")
		}
		if out, _ := store.wait(t.Context(), waiter); out.TxID == "" {
			t.Fatal("eviction lost the response for an existing waiter")
		}
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

func TestIdempotencyStoreEvictsFailureWithoutLosingActiveWaiters(t *testing.T) {
	store := newIdempotencyStore()
	record, _, err := store.begin("key1", "digest1")
	if err != nil {
		t.Fatalf("begin first: %v", err)
	}
	expectedErr := errors.New("boom")
	waiter, isFirstRequest, err := store.begin("key1", "digest1")
	if err != nil {
		t.Fatalf("begin duplicate: %v", err)
	}
	if isFirstRequest {
		t.Fatal("duplicate caller should not own failed execution")
	}
	store.complete(record, InvocationResponse{TxID: "tx1"}, expectedErr)
	if len(store.records) != 0 {
		t.Fatal("failed transaction retained in store")
	}
	_, err = store.wait(context.Background(), waiter)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected replayed error, got %v", err)
	}
}
