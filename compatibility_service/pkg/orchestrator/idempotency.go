// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	sdk "github.com/hyperledger/fabric-x-sdk"
)

type idempotencyState string

const (
	idempotencyInProgress idempotencyState = "IN_PROGRESS"
	idempotencyExecuted   idempotencyState = "EXECUTED"
	idempotencySubmitted  idempotencyState = "SUBMITTED"
	idempotencyCompleted  idempotencyState = "COMPLETED"
	idempotencyFailed     idempotencyState = "FAILED"
)

type idempotencyRecord struct {
	Key         string
	Digest      string
	State       idempotencyState
	HelperTxID  string
	Endorsement sdk.Endorsement
	Response    InvocationResponse
	Err         error
	CreatedAt   time.Time
	UpdatedAt   time.Time
	done        chan struct{}
}

type idempotencyStore struct {
	mu      sync.Mutex
	records map[string]*idempotencyRecord
}

func newIdempotencyStore() *idempotencyStore {
	return &idempotencyStore{
		records: make(map[string]*idempotencyRecord),
	}
}

func (s *idempotencyStore) begin(key, digest string) (*idempotencyRecord, bool, error) {
	if key == "" || digest == "" {
		return nil, false, errors.New("idempotency key and digest are required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if record, ok := s.records[key]; ok {
		if record.Digest != digest {
			return nil, false, fmt.Errorf("idempotency conflict for key %s", key)
		}
		return record, false, nil
	}

	now := time.Now()
	record := &idempotencyRecord{
		Key:       key,
		Digest:    digest,
		State:     idempotencyInProgress,
		CreatedAt: now,
		UpdatedAt: now,
		done:      make(chan struct{}),
	}
	s.records[key] = record
	return record, true, nil
}

func (s *idempotencyStore) wait(ctx context.Context, record *idempotencyRecord) (InvocationResponse, error) {
	if record == nil {
		return InvocationResponse{}, errors.New("idempotency record is nil")
	}

	select {
	case <-record.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		out := record.Response
		out.IdempotencyKey = record.Key
		return out, record.Err
	case <-ctx.Done():
		return InvocationResponse{}, ctx.Err()
	}
}

func (s *idempotencyStore) markExecuted(record *idempotencyRecord, helperTxID string, endorsement sdk.Endorsement) {
	if record == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record.HelperTxID = helperTxID
	record.Endorsement = endorsement
	record.State = idempotencyExecuted
	record.UpdatedAt = time.Now()
}

func (s *idempotencyStore) markSubmitted(record *idempotencyRecord, response InvocationResponse) {
	if record == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record.Response = response
	record.State = idempotencySubmitted
	record.UpdatedAt = time.Now()
}

func (s *idempotencyStore) complete(record *idempotencyRecord, response InvocationResponse, err error) {
	if record == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record.Response = response
	record.Err = err
	if err != nil {
		record.State = idempotencyFailed
	} else {
		record.State = idempotencyCompleted
	}
	record.UpdatedAt = time.Now()
	close(record.done)
}
