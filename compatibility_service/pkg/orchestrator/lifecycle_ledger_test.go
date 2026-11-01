// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"compatibility_service/pkg/config"
	"compatibility_service/pkg/lifecycle"

	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type lifecycleDefinitionQuery struct {
	committerpb.QueryServiceClient
	value []byte
}

// This ledger applies the actual prepared Fabric-X lifecycle writes but can
// withhold final status. It exercises recovery without retaining signed inputs.
type lifecycleRecoveryLedger struct {
	committerpb.QueryServiceClient
	mu       sync.Mutex
	rows     map[string]*committerpb.Row
	statuses map[string]*committerpb.TxStatus
	visible  bool
	sends    int
	sendErr  error
}

func (l *lifecycleRecoveryLedger) GetRows(_ context.Context, query *committerpb.Query, _ ...grpc.CallOption) (*committerpb.Rows, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := &committerpb.Rows{}
	for _, ns := range query.Namespaces {
		rows := &committerpb.RowsNamespace{NsId: ns.NsId}
		for _, key := range ns.Keys {
			if row := l.rows[string(key)]; row != nil {
				rows.Rows = append(rows.Rows, proto.Clone(row).(*committerpb.Row))
			}
		}
		result.Namespaces = append(result.Namespaces, rows)
	}
	return result, nil
}

func (l *lifecycleRecoveryLedger) GetTransactionStatus(_ context.Context, query *committerpb.TxStatusQuery, _ ...grpc.CallOption) (*committerpb.TxStatusResponse, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := &committerpb.TxStatusResponse{}
	if l.visible {
		for _, id := range query.TxIds {
			if outcome := l.statuses[id]; outcome != nil {
				result.Statuses = append(result.Statuses, proto.Clone(outcome).(*committerpb.TxStatus))
			}
		}
	}
	return result, nil
}

func (l *lifecycleRecoveryLedger) Submit(_ context.Context, end sdk.Endorsement) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sends++
	txID, err := txIDFromEndorsement(end)
	if err != nil {
		return err
	}
	native := &applicationpb.Tx{}
	if err := proto.Unmarshal(end.Responses[0].Payload, native); err != nil {
		return err
	}
	put := func(key, value []byte) {
		l.rows[string(key)] = &committerpb.Row{Key: append([]byte(nil), key...), Value: append([]byte(nil), value...), Version: uint64(l.sends)}
	}
	for _, ns := range native.Namespaces {
		for _, write := range ns.ReadWrites {
			put(write.Key, write.Value)
		}
		for _, write := range ns.BlindWrites {
			put(write.Key, write.Value)
		}
	}
	l.statuses[txID] = finalityTestStatus(txID, committerpb.Status_COMMITTED)
	return l.sendErr
}

func (*lifecycleRecoveryLedger) Close() error { return nil }

func newLifecycleRecoveryService(t *testing.T, def lifecycle.ChaincodeDefinition, existing bool) (*Service, *lifecycleRecoveryLedger) {
	t.Helper()
	org := newProposalTestMSP(t, "org-0")
	actor := org.issue(t, "peer", time.Now().Add(-time.Hour), time.Now().Add(time.Hour))
	store, err := lifecycle.NewMemoryStore("channelqc4")
	if err != nil {
		t.Fatal(err)
	}
	ledger := &lifecycleRecoveryLedger{rows: map[string]*committerpb.Row{}, statuses: map[string]*committerpb.TxStatus{}}
	service := &Service{
		cfg:    Config{ChannelID: "channelqc4", Identity: &config.IdentityConfig{MspID: "org-0"}, FinalityTimeout: time.Second},
		signer: actor.signer, lifecycle: store, queryService: ledger, submitter: ledger,
		notify: &finalityTestNotifier{mode: "timeout"}, idempotency: newIdempotencyStore(), logger: sdk.NoOpLogger{},
	}
	if existing {
		value, err := lifecycle.MarshalLedgerDefinition("channelqc4", def, !def.InitRequired)
		if err != nil {
			t.Fatal(err)
		}
		key := lifecycle.LedgerKey("channelqc4", def.Name)
		ledger.rows[key] = &committerpb.Row{Key: []byte(key), Value: value, Version: 1}
		if _, err := store.UpsertCommittedFromLedger(t.Context(), def, !def.InitRequired); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = service.Close() })
	return service, ledger
}

func TestLifecycleRecoveryRetainsOnlyPendingWorkAfterMissedFinality(t *testing.T) {
	for _, event := range []string{lifecycle.LifecycleEventCommit, lifecycle.LifecycleEventInitialized} {
		for _, lostAck := range []bool{false, true} {
			t.Run(event+map[bool]string{false: "/notification-timeout", true: "/acknowledgment-lost"}[lostAck], func(t *testing.T) {
				def := lifecycle.ChaincodeDefinition{Name: "sample", Version: "1.0", Sequence: 1, InitRequired: event == lifecycle.LifecycleEventInitialized}
				service, ledger := newLifecycleRecoveryService(t, def, def.InitRequired)
				if lostAck {
					ledger.sendErr = &orderingSubmissionError{err: context.DeadlineExceeded, uncertain: true}
				}
				result, err := service.submitLifecycleDefinition(t.Context(), def, event)
				if err == nil || result.TxID == "" || len(service.lifecyclePending) != 1 {
					t.Fatalf("unconfirmed lifecycle send was forgotten: %+v %v", result, err)
				}
				for range 2 {
					service.triggerReconciliation()
					waitForRecoveryPass(t, service)
				}
				if ledger.sends != 1 || len(service.lifecyclePending) != 1 {
					t.Fatal("unknown lifecycle transaction was resubmitted or discarded")
				}
				ledger.mu.Lock()
				ledger.visible = true
				ledger.mu.Unlock()
				service.triggerReconciliation()
				waitForRecoveryPass(t, service)
				committed, ok, err := service.lifecycle.GetCommitted(t.Context(), def.Name)
				if err != nil || !ok || committed.Definition != def || !committed.Initialized {
					t.Fatalf("committed lifecycle state not reconstructed: %+v %v", committed, err)
				}
				if ledger.sends != 1 || len(service.lifecyclePending) != 0 || len(service.idempotency.records) != 0 {
					t.Fatal("completed lifecycle work remained in memory or caused another send")
				}
			})
		}
	}
}

func TestLifecycleRecoveryKeepsMarkerAfterApplicationRecordEviction(t *testing.T) {
	def := lifecycle.ChaincodeDefinition{Name: "sample", Version: "1.0", Sequence: 1, InitRequired: true}
	service, ledger := newLifecycleRecoveryService(t, def, true)
	record, _, _ := service.idempotency.begin("init-application", "public-digest")
	service.idempotency.markSubmitted(record, InvocationResponse{TxID: record.Key, Submitted: true}, sdk.Endorsement{}, func(ctx context.Context) error {
		return service.markInitializedAfterCommit(ctx, def)
	})
	// The application Init is definitively committed, but its caller's deadline
	// expires before a marker can even be prepared. Only the small task survives.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := service.recordFinality(ctx, record, finalityTestStatus(record.Key, committerpb.Status_COMMITTED)); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled bookkeeping: %v", err)
	}
	if len(service.idempotency.records) != 0 || len(service.lifecyclePending) != 1 || ledger.sends != 0 {
		t.Fatal("finished application record retained or pending marker lost")
	}
	service.triggerReconciliation()
	waitForRecoveryPass(t, service)
	if ledger.sends != 1 || len(service.lifecyclePending) != 1 {
		t.Fatal("missing marker was not submitted exactly once")
	}
	ledger.mu.Lock()
	ledger.visible = true
	ledger.mu.Unlock()
	service.triggerReconciliation()
	waitForRecoveryPass(t, service)
	committed, _, _ := service.lifecycle.GetCommitted(t.Context(), def.Name)
	if !committed.Initialized || ledger.sends != 1 || len(service.lifecyclePending) != 0 {
		t.Fatal("marker finality did not finish initialization and release pending work")
	}
}

func TestLifecycleRecoveryUsesExistingMarkerAndDoesNotInitializeNewRevision(t *testing.T) {
	for _, upgraded := range []bool{false, true} {
		t.Run(map[bool]string{false: "already-initialized", true: "superseded"}[upgraded], func(t *testing.T) {
			def := lifecycle.ChaincodeDefinition{Name: "sample", Version: "1.0", Sequence: 1, InitRequired: true}
			service, ledger := newLifecycleRecoveryService(t, def, true)
			pending, err := service.lifecycleOperation(t.Context(), def, true)
			if err != nil {
				t.Fatal(err)
			}
			pending.unlock()
			active := def
			if upgraded {
				active.Sequence, active.Version = 2, "2.0"
			}
			value, _ := lifecycle.MarshalLedgerDefinition("channelqc4", active, !upgraded)
			ledger.rows[lifecycle.LedgerKey("channelqc4", def.Name)].Value = value
			service.triggerReconciliation()
			waitForRecoveryPass(t, service)
			if ledger.sends != 0 || len(service.lifecyclePending) != 0 {
				t.Fatal("recovery rewrote an existing marker or retained obsolete initialization")
			}
		})
	}
}

func TestLifecycleRecoveryDoesNotKeepDefinitionCanceledBeforeSubmission(t *testing.T) {
	def := lifecycle.ChaincodeDefinition{Name: "sample", Version: "1.0", Sequence: 1}
	service, ledger := newLifecycleRecoveryService(t, def, false)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := service.CommitLifecycleDefinition(ctx, def); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled definition reached submission: %v", err)
	}
	service.triggerReconciliation()
	waitForRecoveryPass(t, service)
	if ledger.sends != 0 || len(service.lifecyclePending) != 0 {
		t.Fatal("maintenance submitted or retained a definition canceled before delivery")
	}
}

func TestLifecycleRecoveryConcurrentRetriesSharePendingReceipt(t *testing.T) {
	def := lifecycle.ChaincodeDefinition{Name: "sample", Version: "1.0", Sequence: 1}
	service, ledger := newLifecycleRecoveryService(t, def, false)
	const callers = 8
	results := make([]lifecycle.LedgerCommitResult, callers)
	errors := make([]error, callers)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	var group sync.WaitGroup
	for i := range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			results[i], errors[i] = service.submitLifecycleDefinition(ctx, def, lifecycle.LifecycleEventCommit)
		}()
	}
	group.Wait()
	for i, result := range results {
		if errors[i] == nil || result.TxID == "" || result.TxID != results[0].TxID {
			t.Fatalf("retry %d did not share the original pending receipt: %+v %v", i, result, errors[i])
		}
	}
	if ledger.sends != 1 || len(service.lifecyclePending) != 1 {
		t.Fatal("concurrent retries prepared or sent multiple lifecycle transactions")
	}
	conflict := def
	conflict.Version = "conflicting-label"
	if _, err := service.submitLifecycleDefinition(ctx, conflict, lifecycle.LifecycleEventCommit); err == nil {
		t.Fatal("a different definition replaced an uncertain lifecycle transaction")
	}
	ledger.mu.Lock()
	ledger.visible = true
	ledger.mu.Unlock()
	service.triggerReconciliation()
	waitForRecoveryPass(t, service)
	if ledger.sends != 1 || len(service.lifecyclePending) != 0 {
		t.Fatal("receipt was resubmitted or retained after confirmation")
	}
}

func (q lifecycleDefinitionQuery) GetRows(_ context.Context, query *committerpb.Query, _ ...grpc.CallOption) (*committerpb.Rows, error) {
	ns := query.Namespaces[0]
	return &committerpb.Rows{Namespaces: []*committerpb.RowsNamespace{{NsId: ns.NsId, Rows: []*committerpb.Row{{Key: ns.Keys[0], Value: q.value, Version: 7}}}}}, nil
}

func TestLifecycleSubmissionChecksSequenceAgainstLedger(t *testing.T) {
	active := lifecycle.ChaincodeDefinition{Name: "sample", Version: "2.0", Sequence: 2, InitRequired: true}
	value, err := lifecycle.MarshalLedgerDefinition("channelqc4", active, false)
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{cfg: Config{ChannelID: "channelqc4"}, queryService: lifecycleDefinitionQuery{value: value}}
	t.Cleanup(service.stopReconciliation)
	for _, sequence := range []int64{1, 2, 4} {
		requested := active
		requested.Version, requested.Sequence = "new-version", sequence
		if _, err := service.CommitLifecycleDefinition(t.Context(), requested); err == nil || !strings.Contains(err.Error(), "next sequence") {
			t.Fatalf("invalid revision reached submission: sequence=%d error=%v", sequence, err)
		}
	}
	stale := active
	stale.Version, stale.Sequence = "1.0", 1
	if _, err := service.MarkLifecycleInitialized(t.Context(), stale); err == nil {
		t.Fatal("stale initialization marker overwrote the active upgrade")
	}
}
