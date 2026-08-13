/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package coordinator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"chaincode_helper/pkg/config"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/api/applicationpb"
	"github.com/hyperledger/fabric-x-common/api/committerpb"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"github.com/hyperledger/fabric-x-sdk/network"
	nfab "github.com/hyperledger/fabric-x-sdk/network/fabric"
	nfabx "github.com/hyperledger/fabric-x-sdk/network/fabricx"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Config contains the V1 coordinator wiring. The coordinator receives a small
// invocation request, sends a real signed proposal to the helper, and submits
// the helper's endorsed Fabric-X transaction to the orderer.
type Config struct {
	ChannelID       string                 `mapstructure:"channel-id"`
	Namespace       string                 `mapstructure:"namespace"`
	Protocol        string                 `mapstructure:"protocol"`
	Server          config.ClientConfig    `mapstructure:"server"`
	Identity        *config.IdentityConfig `mapstructure:"identity"`
	Helpers         []config.ClientConfig  `mapstructure:"helpers"`
	Orderer         *config.ClientConfig   `mapstructure:"orderer"`
	NotificationSvc *config.ClientConfig   `mapstructure:"notification-service"`
	WaitAfterSubmit time.Duration          `mapstructure:"wait-after-submit"`
	FinalityTimeout time.Duration          `mapstructure:"finality-timeout"`
}

// InvocationRequest is the client-facing V1 request. It deliberately carries
// business input, not a Fabric SignedProposal.
type InvocationRequest struct {
	Namespace string   `json:"namespace,omitempty"`
	Function  string   `json:"function,omitempty"`
	Args      []string `json:"args,omitempty"`
}

// InvocationResponse is returned by query and invoke.
type InvocationResponse struct {
	TxID           string          `json:"tx_id,omitempty"`
	Status         int32           `json:"status"`
	Message        string          `json:"message,omitempty"`
	Payload        string          `json:"payload,omitempty"`
	PayloadBase64  string          `json:"payload_base64,omitempty"`
	Submitted      bool            `json:"submitted"`
	CommitStatus   string          `json:"commit_status,omitempty"`
	BlockNum       uint64          `json:"block_num,omitempty"`
	TxNum          uint32          `json:"tx_num,omitempty"`
	ChaincodeEvent *ChaincodeEvent `json:"chaincode_event,omitempty"`
}

// ChaincodeEvent is the client-facing committed event shape.
type ChaincodeEvent struct {
	ChaincodeID   string `json:"chaincode_id,omitempty"`
	TxID          string `json:"tx_id,omitempty"`
	EventName     string `json:"event_name,omitempty"`
	Payload       string `json:"payload,omitempty"`
	PayloadBase64 string `json:"payload_base64,omitempty"`
}

// Service is the coordinator process.
type Service struct {
	cfg       Config
	signer    sdk.Signer
	endorsers *network.EndorsementClient
	submitter *network.FabricSubmitter
	notifier  *network.Peer
	notify    committerpb.NotifierClient
	logger    sdk.Logger
}

// New constructs the coordinator and dials the helper, orderer, and
// notification endpoints.
func New(ctx context.Context, cfg Config, logger sdk.Logger) (*Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	signer, err := identity.SignerFromMSP(cfg.Identity.MSPDir, cfg.Identity.MspID)
	if err != nil {
		return nil, fmt.Errorf("load identity: %w", err)
	}

	helperConfs := make([]network.PeerConf, len(cfg.Helpers))
	for i := range cfg.Helpers {
		helperConfs[i] = cfg.Helpers[i].ToPeerConf()
	}
	endorsers, err := network.NewEndorsementClient(helperConfs, signer, cfg.ChannelID, cfg.Namespace, "1.0")
	if err != nil {
		return nil, fmt.Errorf("create helper client: %w", err)
	}

	ordererConfs := []network.OrdererConf{cfg.Orderer.ToOrdererConf()}
	var submitter *network.FabricSubmitter
	switch cfg.Protocol {
	case "fabric":
		submitter, err = nfab.NewSubmitter(ctx, ordererConfs, signer, cfg.WaitAfterSubmit, logger)
	case "fabric-x", "":
		submitter, err = nfabx.NewSubmitter(ctx, ordererConfs, signer, cfg.WaitAfterSubmit, logger)
	default:
		return nil, fmt.Errorf("unknown protocol %q: must be \"fabric\" or \"fabric-x\"", cfg.Protocol)
	}
	if err != nil {
		endorsers.Close() //nolint:errcheck
		return nil, fmt.Errorf("create submitter: %w", err)
	}

	var notifier *network.Peer
	var notify committerpb.NotifierClient
	if cfg.NotificationSvc != nil {
		notifier, err = network.NewPeer(cfg.NotificationSvc.ToPeerConf())
		if err != nil {
			submitter.Close() //nolint:errcheck
			endorsers.Close() //nolint:errcheck
			return nil, fmt.Errorf("notification service: %w", err)
		}
		notify = committerpb.NewNotifierClient(notifier.Connection())
	}

	return &Service{
		cfg:       cfg,
		signer:    signer,
		endorsers: endorsers,
		submitter: submitter,
		notifier:  notifier,
		notify:    notify,
		logger:    logger,
	}, nil
}

// Validate checks only the endpoints needed for the V1 flow.
func (cfg Config) Validate() error {
	var errs []error
	if cfg.ChannelID == "" {
		errs = append(errs, errors.New("channel-id is required"))
	}
	if cfg.Namespace == "" {
		errs = append(errs, errors.New("namespace is required"))
	}
	if cfg.Identity == nil {
		errs = append(errs, errors.New("identity is required"))
	}
	if cfg.Server.Endpoint == nil {
		errs = append(errs, errors.New("server.endpoint is required"))
	}
	if cfg.Server.TLS.Mode != "" && cfg.Server.TLS.Mode != network.TLSModeNone {
		errs = append(errs, errors.New("coordinator server currently supports only tls.mode none"))
	}
	if len(cfg.Helpers) == 0 {
		errs = append(errs, errors.New("at least one helper is required"))
	}
	if cfg.Orderer == nil || cfg.Orderer.Endpoint == nil {
		errs = append(errs, errors.New("orderer.endpoint is required"))
	}
	if cfg.NotificationSvc == nil || cfg.NotificationSvc.Endpoint == nil {
		errs = append(errs, errors.New("notification-service.endpoint is required"))
	}
	return errors.Join(errs...)
}

// Run starts the coordinator HTTP API.
func (s *Service) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("POST /v1/query", s.handleQuery)
	mux.HandleFunc("POST /v1/invoke", s.handleInvoke)

	addr := s.cfg.Server.Endpoint.Address()
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	defer listener.Close() //nolint:errcheck

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		s.logger.Infof("coordinator listening on %s", addr)
		errCh <- server.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		return ctx.Err()
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// Close closes outbound connections held by the coordinator.
func (s *Service) Close() error {
	var errs []error
	if s.submitter != nil {
		errs = append(errs, s.submitter.Close())
	}
	if s.endorsers != nil {
		errs = append(errs, s.endorsers.Close())
	}
	if s.notifier != nil {
		errs = append(errs, s.notifier.Close())
	}
	return errors.Join(errs...)
}

func (s *Service) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (s *Service) handleQuery(w http.ResponseWriter, r *http.Request) {
	s.handleOperation(w, r, false)
}

func (s *Service) handleInvoke(w http.ResponseWriter, r *http.Request) {
	s.handleOperation(w, r, true)
}

func (s *Service) handleOperation(w http.ResponseWriter, r *http.Request, submit bool) {
	if r.Method != http.MethodPost {
		http.Error(w, "method must be POST", http.StatusMethodNotAllowed)
		return
	}

	var req InvocationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}

	res, err := s.Execute(r.Context(), req, submit)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

// Execute runs one query or invoke through helper endorsement. Invokes are also
// packaged and submitted to Fabric-X.
func (s *Service) Execute(ctx context.Context, req InvocationRequest, submit bool) (InvocationResponse, error) {
	namespace := req.Namespace
	if namespace == "" {
		namespace = s.cfg.Namespace
	}
	args := invocationArgs(req)
	if len(args) == 0 {
		return InvocationResponse{}, errors.New("function is required")
	}

	end, err := s.endorsers.ExecuteTransaction(ctx, namespace, "1.0", args)
	if err != nil {
		return InvocationResponse{}, fmt.Errorf("helper endorsement failed: %w", err)
	}
	txID, err := txIDFromEndorsement(end)
	if err != nil {
		return InvocationResponse{}, err
	}
	if len(end.Responses) == 0 || end.Responses[0] == nil || end.Responses[0].Response == nil {
		return InvocationResponse{}, errors.New("helper returned no proposal response")
	}

	resp := end.Responses[0].Response
	out := responseFromPeer(txID, resp)
	if resp.Status < 200 || resp.Status >= 400 {
		return out, nil
	}
	if !submit {
		out.ChaincodeEvent = eventFromEndorsement(end, txID)
		s.logger.Infof("tx=%s query completed status=%d", txID, resp.Status)
		return out, nil
	}

	finality, err := s.subscribeFinality(ctx, txID)
	if err != nil {
		return out, fmt.Errorf("subscribe finality: %w", err)
	}
	if finality != nil {
		defer finality.Cancel()
	}

	if err := s.submitter.Submit(ctx, end); err != nil {
		return out, fmt.Errorf("submit failed: %w", err)
	}
	out.Submitted = true
	s.logger.Infof("tx=%s submitted", txID)

	status, err := finality.Wait()
	if err != nil {
		s.logger.Warnf("tx=%s finality wait failed: %s", txID, err)
		return out, nil
	}
	if status != nil {
		out.CommitStatus = status.Status.String()
		out.BlockNum = status.Ref.GetBlockNum()
		out.TxNum = status.Ref.GetTxNum()
		if status.Status == committerpb.Status_COMMITTED {
			out.ChaincodeEvent = eventFromEndorsement(end, txID)
		}
	}

	return out, nil
}

type finalitySubscription struct {
	cancel context.CancelFunc
	result <-chan finalityResult
}

type finalityResult struct {
	status *committerpb.TxStatus
	err    error
}

func (s *finalitySubscription) Cancel() {
	if s != nil && s.cancel != nil {
		s.cancel()
	}
}

func (s *finalitySubscription) Wait() (*committerpb.TxStatus, error) {
	if s == nil {
		return nil, nil
	}
	res := <-s.result
	return res.status, res.err
}

func (s *Service) subscribeFinality(ctx context.Context, txID string) (*finalitySubscription, error) {
	if s.notify == nil {
		return nil, nil
	}

	timeout := s.cfg.FinalityTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	notifyCtx, cancel := context.WithTimeout(ctx, timeout)
	stream, err := s.notify.OpenNotificationStream(notifyCtx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open notification stream: %w", err)
	}
	if err := stream.Send(&committerpb.NotificationRequest{
		TxStatusRequest: &committerpb.TxIDsBatch{TxIds: []string{txID}},
		Timeout:         durationpb.New(timeout),
	}); err != nil {
		cancel()
		return nil, fmt.Errorf("send tx status subscription: %w", err)
	}

	result := make(chan finalityResult, 1)
	go waitForNotification(notifyCtx, stream, txID, result)

	return &finalitySubscription{
		cancel: cancel,
		result: result,
	}, nil
}

func waitForNotification(
	ctx context.Context,
	stream committerpb.Notifier_OpenNotificationStreamClient,
	txID string,
	out chan<- finalityResult,
) {
	defer close(out)

	for {
		res, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				out <- finalityResult{err: ctx.Err()}
				return
			}
			if errors.Is(err, io.EOF) {
				out <- finalityResult{err: errors.New("notification stream closed before transaction status")}
				return
			}
			out <- finalityResult{err: fmt.Errorf("receive notification: %w", err)}
			return
		}

		status, err, ok := matchNotification(txID, res)
		if ok {
			out <- finalityResult{status: status, err: err}
			return
		}
	}
}

func matchNotification(txID string, res *committerpb.NotificationResponse) (*committerpb.TxStatus, error, bool) {
	for _, txStatus := range res.TxStatusEvents {
		if txStatus.Ref.GetTxId() == txID {
			return txStatus, nil, true
		}
	}
	for _, timeoutTxID := range res.TimeoutTxIds {
		if timeoutTxID == txID {
			return nil, fmt.Errorf("notification timeout for tx %s", txID), true
		}
	}
	if rejected := res.GetRejectedTxIds(); rejected != nil {
		for _, rejectedTxID := range rejected.TxIds {
			if rejectedTxID == txID {
				return nil, fmt.Errorf("notification rejected tx %s: %s", txID, rejected.Reason), true
			}
		}
	}
	return nil, nil, false
}

func invocationArgs(req InvocationRequest) [][]byte {
	if req.Function == "" {
		return nil
	}
	args := make([][]byte, 0, 1+len(req.Args))
	args = append(args, []byte(req.Function))
	for _, arg := range req.Args {
		args = append(args, []byte(arg))
	}
	return args
}

func responseFromPeer(txID string, resp *peer.Response) InvocationResponse {
	return InvocationResponse{
		TxID:          txID,
		Status:        resp.Status,
		Message:       resp.Message,
		Payload:       string(resp.Payload),
		PayloadBase64: base64.StdEncoding.EncodeToString(resp.Payload),
	}
}

func eventFromEndorsement(end sdk.Endorsement, txID string) *ChaincodeEvent {
	if len(end.Responses) == 0 || end.Responses[0] == nil || len(end.Responses[0].Payload) == 0 {
		return nil
	}

	var tx applicationpb.Tx
	if err := proto.Unmarshal(end.Responses[0].Payload, &tx); err != nil {
		return nil
	}
	if len(tx.Metadata) <= 1 || len(tx.Metadata[1]) == 0 {
		return nil
	}

	event := &peer.ChaincodeEvent{}
	if err := proto.Unmarshal(tx.Metadata[1], event); err != nil {
		return nil
	}
	if event.TxId == "" {
		event.TxId = txID
	}
	return &ChaincodeEvent{
		ChaincodeID:   event.ChaincodeId,
		TxID:          event.TxId,
		EventName:     event.EventName,
		Payload:       string(event.Payload),
		PayloadBase64: base64.StdEncoding.EncodeToString(event.Payload),
	}
}

func txIDFromEndorsement(end sdk.Endorsement) (string, error) {
	if end.Proposal == nil {
		return "", errors.New("endorsement has no proposal")
	}
	hdr, err := protoutil.UnmarshalHeader(end.Proposal.Header)
	if err != nil {
		return "", fmt.Errorf("unmarshal proposal header: %w", err)
	}
	chdr, err := protoutil.UnmarshalChannelHeader(hdr.ChannelHeader)
	if err != nil {
		return "", fmt.Errorf("unmarshal channel header: %w", err)
	}
	return chdr.TxId, nil
}

func writeError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
