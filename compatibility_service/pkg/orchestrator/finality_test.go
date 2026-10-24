// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/fabric-x-common/api/committerpb"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"google.golang.org/grpc"
)

func TestFinalityTimeoutRemainsSubmittedWithoutBackgroundChecks(t *testing.T) {
	var observed atomic.Pointer[committerpb.TxStatus]
	var queries atomic.Int32
	service, submitter, record, end := newFinalityTestService(t, &finalityTestQuery{get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		queries.Add(1)
		return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{observed.Load()}}, nil
	}})
	out, err := service.submitAndWaitFinality(context.Background(), end, InvocationResponse{TxID: record.Key, Status: 200}, record.Key, record, nil)
	if err != nil || !out.Submitted || !out.FinalityPending || out.CommitStatus != "" || out.ChaincodeEvent != nil {
		t.Fatalf("timeout must return pending submission: response=%+v err=%v", out, err)
	}
	service.idempotency.complete(record, out, nil)
	service.idempotency.complete(record, out, context.DeadlineExceeded)
	assertFinalityState(t, service, record, idempotencySubmitted)
	observed.Store(finalityTestStatus(record.Key, committerpb.Status_COMMITTED))
	before := queries.Load()
	// Even a now-visible commit must not start polling after the RPC ended.
	time.Sleep(200 * time.Millisecond)
	if queries.Load() != before {
		t.Fatal("status checks continued after the request returned")
	}
	assertFinalityState(t, service, record, idempotencySubmitted)

	const retries = 8
	var wg sync.WaitGroup
	results := make([]InvocationResponse, retries)
	retryErrors := make([]error, retries)
	for i := range results {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			service.reconcilePending(ctx)
			results[index], retryErrors[index] = service.replaySubmission(ctx, record)
		}(i)
	}
	wg.Wait()
	for i, result := range results {
		if retryErrors[i] != nil || result.FinalityPending || result.CommitStatus != "COMMITTED" || result.BlockNum != 42 || result.TxNum != 3 {
			t.Fatalf("retry %d did not recover finality: response=%+v err=%v", i, result, retryErrors[i])
		}
		if result.ChaincodeEvent == nil || result.ChaincodeEvent.EventName != "event" || result.ChaincodeEvent.Payload != "event-value" {
			t.Fatalf("retry %d lost committed event: %+v", i, result.ChaincodeEvent)
		}
	}
	if submitter.calls.Load() != 1 || queries.Load() != before+1 {
		t.Fatalf("duplicate work: submissions=%d status queries=%d", submitter.calls.Load(), queries.Load())
	}
	assertFinalityState(t, service, record, idempotencyCompleted)
	service.idempotency.complete(record, InvocationResponse{}, context.Canceled)
	replayed, err := service.replaySubmission(context.Background(), record)
	if err != nil || replayed.CommitStatus != "COMMITTED" {
		t.Fatalf("known finality changed: response=%+v err=%v", replayed, err)
	}
}

func TestLazyReconciliationBatchesPendingRecords(t *testing.T) {
	var queried []string
	var calls int
	service, submitter, _, end := newFinalityTestService(t, &finalityTestQuery{get: func(_ context.Context, query *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		calls++
		queried = append([]string(nil), query.TxIds...)
		return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{
			finalityTestStatus("a", committerpb.Status_COMMITTED),
			finalityTestStatus("b", committerpb.Status_ABORTED_MVCC_CONFLICT),
			finalityTestStatus("c", committerpb.Status_STATUS_UNSPECIFIED),
			finalityTestStatus("unrequested", committerpb.Status_COMMITTED), nil,
		}}, nil
	}})
	var initialized atomic.Int32
	records := make(map[string]*idempotencyRecord)
	for _, id := range []string{"a", "b", "c", "active"} {
		record, _, err := service.idempotency.begin(id, "digest-"+id)
		if err != nil {
			t.Fatal(err)
		}
		service.idempotency.markSubmitted(record, InvocationResponse{TxID: id, Status: 200, Submitted: true, Payload: "response-" + id}, end, func(context.Context) error { initialized.Add(1); return nil })
		records[id] = record
	}
	if !records["active"].tryLockFinality() {
		t.Fatal("lock active invocation")
	}
	defer records["active"].unlockFinality()
	service.reconcilePending(context.Background())
	slices.Sort(queried)
	if calls != 1 || !slices.Equal(queried, []string{"a", "b", "c"}) {
		t.Fatalf("expected one batch excluding active requests: calls=%d ids=%v", calls, queried)
	}
	for _, id := range []string{"a", "b"} {
		assertFinalityState(t, service, records[id], idempotencyCompleted)
	}
	for _, id := range []string{"c", "active"} {
		assertFinalityState(t, service, records[id], idempotencySubmitted)
	}
	a, _ := service.idempotency.waitResult(records["a"], nil)
	b, _ := service.idempotency.waitResult(records["b"], nil)
	if a.CommitStatus != "COMMITTED" || a.Payload != "response-a" || a.ChaincodeEvent == nil || b.CommitStatus != "ABORTED_MVCC_CONFLICT" || b.Payload != "response-b" || b.ChaincodeEvent != nil {
		t.Fatalf("store results were mixed: a=%+v b=%+v", a, b)
	}
	if initialized.Load() != 1 || submitter.calls.Load() != 0 {
		t.Fatal("maintenance repeated submission or finalized a noncommitted init")
	}
}

func TestLazyReconciliationQueryFailureKeepsPending(t *testing.T) {
	service, _, record, end := newFinalityTestService(t, &finalityTestQuery{get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		return nil, errors.New("query service unavailable")
	}})
	service.idempotency.markSubmitted(record, InvocationResponse{TxID: record.Key, Submitted: true}, end, nil)
	service.reconcilePending(context.Background())
	assertFinalityState(t, service, record, idempotencySubmitted)
	if !record.tryLockFinality() {
		t.Fatal("maintenance left the record locked")
	}
	record.unlockFinality()
}

func TestLazyFinalityAfterCallerCancellationRunsInitOnce(t *testing.T) {
	var observed atomic.Pointer[committerpb.TxStatus]
	service, _, record, end := newFinalityTestService(t, &finalityTestQuery{get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
		return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{observed.Load()}}, nil
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var initialized atomic.Int32
	bookkeepingErr := errors.New("initialization bookkeeping failed")
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = service.submitAndWaitFinality(ctx, end, InvocationResponse{TxID: record.Key}, record.Key, record, func(ctx context.Context) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			initialized.Add(1)
			return bookkeepingErr
		})
	}()
	<-record.submitted
	cancel()
	<-done
	assertFinalityState(t, service, record, idempotencySubmitted)
	if initialized.Load() != 0 {
		t.Fatal("canceled request finalized init")
	}
	observed.Store(finalityTestStatus(record.Key, committerpb.Status_COMMITTED))
	service.reconcilePending(context.Background())
	out, err := service.replaySubmission(context.Background(), record)
	if !errors.Is(err, bookkeepingErr) || out.CommitStatus != "COMMITTED" || out.FinalityPending || initialized.Load() != 1 {
		t.Fatalf("lazy init finality lost: response=%+v err=%v calls=%d", out, err, initialized.Load())
	}
	service.reconcilePending(context.Background())
	if initialized.Load() != 1 {
		t.Fatal("initialization repeated")
	}
	assertFinalityState(t, service, record, idempotencyCompleted)
}

func TestFinalityObservationFailuresRemainPending(t *testing.T) {
	for _, mode := range []string{"unavailable", "timeout", "rejected", "stream-closed", "query-error", "unspecified", "wrong-tx", "unknown-status"} {
		t.Run(mode, func(t *testing.T) {
			var recovered atomic.Bool
			service, submitter, record, end := newFinalityTestService(t, &finalityTestQuery{get: func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error) {
				if recovered.Load() {
					return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{finalityTestStatus("tx", committerpb.Status_ABORTED_MVCC_CONFLICT)}}, nil
				}
				result := finalityTestStatus("tx", committerpb.Status_STATUS_UNSPECIFIED)
				switch mode {
				case "query-error":
					return nil, errors.New("query unavailable")
				case "wrong-tx":
					result = finalityTestStatus("other-tx", committerpb.Status_COMMITTED)
				case "unknown-status":
					result.Status = committerpb.Status(9999)
				}
				return &committerpb.TxStatusResponse{Statuses: []*committerpb.TxStatus{result}}, nil
			}})
			service.notify = &finalityTestNotifier{mode: mode}
			var afterCommit atomic.Int32
			out, err := service.submitAndWaitFinality(context.Background(), end, InvocationResponse{TxID: record.Key}, record.Key, record, func(context.Context) error { afterCommit.Add(1); return nil })
			if err != nil || !out.FinalityPending || out.CommitStatus != "" {
				t.Fatalf("observation failure became ledger outcome: response=%+v err=%v", out, err)
			}
			assertFinalityState(t, service, record, idempotencySubmitted)
			service.reconcilePending(context.Background())
			assertFinalityState(t, service, record, idempotencySubmitted)
			recovered.Store(true)
			service.reconcilePending(context.Background())
			out, err = service.replaySubmission(context.Background(), record)
			if err != nil || out.FinalityPending || out.CommitStatus != "ABORTED_MVCC_CONFLICT" || out.ChaincodeEvent != nil || afterCommit.Load() != 0 {
				t.Fatalf("invalid finality: response=%+v err=%v", out, err)
			}
			assertFinalityState(t, service, record, idempotencyCompleted)
			if submitter.calls.Load() != 1 {
				t.Fatal("status recovery resubmitted transaction")
			}
		})
	}
}

func TestFinalityNotificationCanCompleteWithoutQueryStatus(t *testing.T) {
	service, _, record, end := newFinalityTestService(t, nil)
	service.notify = &finalityTestNotifier{mode: "committed"}
	out, err := service.submitAndWaitFinality(context.Background(), end, InvocationResponse{TxID: record.Key}, record.Key, record, nil)
	if err != nil || out.CommitStatus != "COMMITTED" || out.FinalityPending || out.ChaincodeEvent == nil {
		t.Fatalf("notification finality lost: response=%+v err=%v", out, err)
	}
}

func TestFinalitySubscriptionStopsWhenRequestEnds(t *testing.T) {
	service, _, record, end := newFinalityTestService(t, nil)
	stopped := make(chan struct{})
	service.notify = &finalityTestNotifier{mode: "wait", stopped: stopped}
	service.cfg.FinalityTimeout = 3 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	out, err := service.submitAndWaitFinality(ctx, end, InvocationResponse{TxID: record.Key}, record.Key, record, nil)
	if err != nil || !out.FinalityPending {
		t.Fatalf("canceled wait lost pending state: %+v %v", out, err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("subscription outlived request cancellation")
	}
	assertFinalityState(t, service, record, idempotencySubmitted)
}

func TestFinalitySubmissionFailureDoesNotCheckStatus(t *testing.T) {
	service, submitter, record, end := newFinalityTestService(t, nil)
	submitter.err = errors.New("ordering unavailable")
	out, err := service.submitAndWaitFinality(context.Background(), end, InvocationResponse{TxID: record.Key}, record.Key, record, nil)
	if err == nil || out.Submitted || out.FinalityPending {
		t.Fatalf("submission error lost: response=%+v err=%v", out, err)
	}
	service.idempotency.complete(record, out, err)
	assertFinalityState(t, service, record, idempotencyFailed)
}

func newFinalityTestService(t *testing.T, query committerpb.QueryServiceClient) (*Service, *finalityTestSubmitter, *idempotencyRecord, sdk.Endorsement) {
	t.Helper()
	org := newProposalTestMSP(t, "org-0")
	actor := org.issue(t, "peer", time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	result := endorsementTestResult(t, actor, proposalTestSigned(t, actor.signer, ""), 0, "result")
	submitter := &finalityTestSubmitter{}
	service := &Service{
		cfg:          Config{FinalityTimeout: 25 * time.Millisecond, RequestTimeout: time.Second},
		queryService: query, notify: &finalityTestNotifier{mode: "wait"}, submitter: submitter, idempotency: newIdempotencyStore(), logger: sdk.NoOpLogger{},
	}
	record, _, err := service.idempotency.begin("tx", "digest")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	return service, submitter, record, result.Endorsement
}

func assertFinalityState(t *testing.T, service *Service, record *idempotencyRecord, want idempotencyState) {
	t.Helper()
	service.idempotency.mu.Lock()
	defer service.idempotency.mu.Unlock()
	if record.State != want {
		t.Fatalf("state=%s, want %s", record.State, want)
	}
}

func finalityTestStatus(txID string, status committerpb.Status) *committerpb.TxStatus {
	return &committerpb.TxStatus{Ref: &committerpb.TxRef{TxId: txID, BlockNum: 42, TxNum: 3}, Status: status}
}

type finalityTestSubmitter struct {
	calls atomic.Int32
	err   error
}

func (s *finalityTestSubmitter) Submit(context.Context, sdk.Endorsement) error {
	s.calls.Add(1)
	return s.err
}
func (*finalityTestSubmitter) Close() error { return nil }

type finalityTestQuery struct {
	committerpb.QueryServiceClient
	get func(context.Context, *committerpb.TxStatusQuery) (*committerpb.TxStatusResponse, error)
}

func (q *finalityTestQuery) GetTransactionStatus(ctx context.Context, query *committerpb.TxStatusQuery, _ ...grpc.CallOption) (*committerpb.TxStatusResponse, error) {
	return q.get(ctx, query)
}

type finalityTestNotifier struct {
	committerpb.NotifierClient
	mode    string
	stopped chan struct{}
}

func (n *finalityTestNotifier) OpenNotificationStream(ctx context.Context, _ ...grpc.CallOption) (committerpb.Notifier_OpenNotificationStreamClient, error) {
	if n.mode == "unavailable" {
		return nil, errors.New("notification service unavailable")
	}
	return &finalityTestStream{ctx: ctx, mode: n.mode, stopped: n.stopped}, nil
}

type finalityTestStream struct {
	committerpb.Notifier_OpenNotificationStreamClient
	ctx     context.Context
	mode    string
	stopped chan struct{}
}

func (*finalityTestStream) Send(*committerpb.NotificationRequest) error { return nil }
func (s *finalityTestStream) Recv() (*committerpb.NotificationResponse, error) {
	if s.stopped != nil {
		defer close(s.stopped)
	}
	switch s.mode {
	case "timeout":
		return &committerpb.NotificationResponse{TimeoutTxIds: []string{"tx"}}, nil
	case "rejected":
		return &committerpb.NotificationResponse{RejectedTxIds: &committerpb.RejectedTxIds{TxIds: []string{"tx"}}}, nil
	case "stream-closed":
		return nil, io.EOF
	case "committed":
		return &committerpb.NotificationResponse{TxStatusEvents: []*committerpb.TxStatus{finalityTestStatus("tx", committerpb.Status_COMMITTED)}}, nil
	default:
		<-s.ctx.Done()
		return nil, s.ctx.Err()
	}
}
