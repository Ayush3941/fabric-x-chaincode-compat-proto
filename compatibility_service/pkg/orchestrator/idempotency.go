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
	Key           string
	Digest        string
	State         idempotencyState
	HelperTxID    string
	Response      InvocationResponse
	Err           error
	CreatedAt     time.Time
	UpdatedAt     time.Time
	submitted     chan struct{}
	done          chan struct{}
	finalityCheck chan struct{}
	afterCommit   func(context.Context) error
	event         *ChaincodeEvent
}

type submittedInvocation struct {
	Response    InvocationResponse
	Event       *ChaincodeEvent
	AfterCommit func(context.Context) error
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

// existing checks retries without reserving a new ID for an invalid submission.
func (s *idempotencyStore) existing(key, digest string) (*idempotencyRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[key]
	if ok && record.Digest != digest {
		return nil, true, fmt.Errorf("idempotency conflict for key %s", key)
	}
	return record, ok, nil
}

// begin returns firstRequest=true only for the caller that created the record
// and should execute the transaction. Duplicate callers wait on the record.
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
		Key:           key,
		Digest:        digest,
		State:         idempotencyInProgress,
		CreatedAt:     now,
		UpdatedAt:     now,
		submitted:     make(chan struct{}),
		done:          make(chan struct{}),
		finalityCheck: make(chan struct{}, 1),
	}
	s.records[key] = record
	return record, true, nil
}

// wait shares execution until the record is either submitted or finished.
// Terminal records remain accessible to callers already holding the pointer,
// but are never retained in the store for historical replay.
func (s *idempotencyStore) wait(ctx context.Context, record *idempotencyRecord) (InvocationResponse, error) {
	if record == nil {
		return InvocationResponse{}, errors.New("idempotency record is nil")
	}

	select {
	case <-record.done:
		return s.waitResult(record, nil)
	case <-record.submitted:
		return s.waitResult(record, nil)
	case <-ctx.Done():
		return s.waitResult(record, ctx.Err())
	}
}

func (s *idempotencyStore) waitResult(record *idempotencyRecord, waitErr error) (InvocationResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := record.Response
	out.IdempotencyKey = record.Key
	if record.State == idempotencySubmitted {
		out.FinalityPending = true
		return out, nil
	}
	if record.State == idempotencyCompleted || record.State == idempotencyFailed {
		return out, record.Err
	}
	return out, waitErr
}

func (s *idempotencyStore) markExecuted(record *idempotencyRecord, helperTxID string) {
	if record == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record.HelperTxID = helperTxID
	record.State = idempotencyExecuted
	record.UpdatedAt = time.Now()
}

func (s *idempotencyStore) markSubmitted(record *idempotencyRecord, response InvocationResponse, end sdk.Endorsement, afterCommit func(context.Context) error) {
	s.markSubmission(record, response, end, afterCommit, nil)
}

func (s *idempotencyStore) markSubmission(record *idempotencyRecord, response InvocationResponse, end sdk.Endorsement, afterCommit func(context.Context) error, submitErr error) {
	if record == nil {
		return
	}
	event := eventFromEndorsement(end, response.TxID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if record.State == idempotencySubmitted || record.State == idempotencyCompleted || record.State == idempotencyFailed {
		return
	}
	record.Response = response
	record.Response.FinalityPending = true
	record.Err = submitErr
	// Retain only the response/event needed by active callers, never the signed
	// proposal (which can contain transient data), endorsements, or read/write set.
	record.event = event
	record.afterCommit = afterCommit
	record.State = idempotencySubmitted
	record.UpdatedAt = time.Now()
	close(record.submitted)
}

// submissionOutcome distinguishes a successful ordering acknowledgment from an
// uncertain delivery. A later final status also proves delivery, independently
// of transaction validity or post-commit lifecycle bookkeeping errors.
func (s *idempotencyStore) submissionOutcome(record *idempotencyRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if record.State == idempotencySubmitted {
		return record.Response.Submitted, record.Err
	}
	if record.Response.Submitted {
		return true, nil
	}
	return false, record.Err
}

// complete ends execution before submission. Once submitted, only an observed
// final validation result may complete the record; an RPC timeout cannot do so.
func (s *idempotencyStore) complete(record *idempotencyRecord, response InvocationResponse, err error) {
	if record == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if record.State == idempotencySubmitted || record.State == idempotencyCompleted || record.State == idempotencyFailed {
		return
	}
	record.Response = response
	record.Err = err
	if err != nil {
		record.State = idempotencyFailed
	} else {
		record.State = idempotencyCompleted
	}
	record.UpdatedAt = time.Now()
	close(record.done)
	s.remove(record)
}

func (s *idempotencyStore) pending(record *idempotencyRecord) (submittedInvocation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if record == nil || record.State != idempotencySubmitted {
		return submittedInvocation{}, false
	}
	return submittedInvocation{Response: record.Response, Event: record.event, AfterCommit: record.afterCommit}, true
}

func (s *idempotencyStore) pendingRecords(limit int) []*idempotencyRecord {
	return s.pendingRecordsBefore(limit, time.Time{})
}

func (s *idempotencyStore) pendingRecordsBefore(limit int, cutoff time.Time) []*idempotencyRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	var pending []*idempotencyRecord
	for _, record := range s.records {
		if record.State == idempotencySubmitted && (cutoff.IsZero() || !record.UpdatedAt.After(cutoff)) {
			pending = append(pending, record)
			if limit > 0 && len(pending) == limit {
				break
			}
		}
	}
	return pending
}

// Serialize active finality waits and bounded lazy maintenance passes.
func (r *idempotencyRecord) lockFinality(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case r.finalityCheck <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *idempotencyRecord) unlockFinality() {
	<-r.finalityCheck
}

func (r *idempotencyRecord) tryLockFinality() bool {
	select {
	case r.finalityCheck <- struct{}{}:
		return true
	default:
		return false
	}
}

// confirmFinality hands the known outcome to existing callers, then evicts it.
// Unfinished lifecycle work is kept separately from this completed transaction.
func (s *idempotencyStore) confirmFinality(record *idempotencyRecord, response InvocationResponse, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if record == nil || record.State != idempotencySubmitted || response.CommitStatus == "" {
		return
	}
	response.FinalityPending = false
	record.Response, record.Err = response, err
	record.State = idempotencyCompleted
	record.UpdatedAt = time.Now()
	record.afterCommit, record.event = nil, nil
	close(record.done)
	s.remove(record)
}

// Caller holds s.mu. An old caller's deferred cleanup must not remove a newer
// request that reused the same key after the original record was evicted.
func (s *idempotencyStore) remove(record *idempotencyRecord) {
	if s.records[record.Key] == record {
		delete(s.records, record.Key)
	}
}
