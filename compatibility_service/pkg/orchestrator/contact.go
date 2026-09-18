// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"errors"
	"fmt"

	"compatibility_service/pkg/lifecycle"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/protoutil"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/hyperledger/fabric-x-sdk/network"
	"google.golang.org/grpc/metadata"
)

// OrchestratorContact is the runtime client for one remote organization
// orchestrator. Today it is used for remote execute-only endorsement; lifecycle
// readiness checks can attach to the same object.
type OrchestratorContact struct {
	mspID   string
	address string
	peer    *network.Peer
	logger  sdk.Logger
}

func newOrchestratorContacts(configs []RemoteOrchestratorConfig, logger sdk.Logger) (map[string]*OrchestratorContact, error) {
	contacts := make(map[string]*OrchestratorContact, len(configs))
	for _, cfg := range configs {
		contact, err := newOrchestratorContact(cfg, logger)
		if err != nil {
			closeOrchestratorContacts(contacts) //nolint:errcheck
			return nil, err
		}
		if _, exists := contacts[contact.MSPID()]; exists {
			contact.Close()                     //nolint:errcheck
			closeOrchestratorContacts(contacts) //nolint:errcheck
			return nil, fmt.Errorf("duplicate remote orchestrator msp-id %q", contact.MSPID())
		}
		contacts[contact.MSPID()] = contact
	}
	return contacts, nil
}

func newOrchestratorContact(cfg RemoteOrchestratorConfig, logger sdk.Logger) (*OrchestratorContact, error) {
	remotePeer, err := network.NewPeer(cfg.ToPeerConf())
	if err != nil {
		return nil, fmt.Errorf("remote orchestrator %s: %w", cfg.MSPID, err)
	}
	if logger == nil {
		logger = sdk.NoOpLogger{}
	}
	return &OrchestratorContact{
		mspID:   cfg.MSPID,
		address: cfg.Address(),
		peer:    remotePeer,
		logger:  logger,
	}, nil
}

func (c *OrchestratorContact) MSPID() string {
	if c == nil {
		return ""
	}
	return c.mspID
}

func (c *OrchestratorContact) Address() string {
	if c == nil {
		return ""
	}
	return c.address
}

func (c *OrchestratorContact) EndorseOnly(ctx context.Context, req InvocationRequest) (helperExecutionResult, error) {
	if c == nil || c.peer == nil {
		return helperExecutionResult{}, errors.New("remote orchestrator contact is not configured")
	}
	prop := req.ClientSignedProposal
	if prop == nil {
		return helperExecutionResult{}, errors.New("remote orchestrator endorsement requires the original signed proposal")
	}
	proposal, err := protoutil.UnmarshalProposal(prop.ProposalBytes)
	if err != nil {
		return helperExecutionResult{}, fmt.Errorf("unmarshal remote proposal: %w", err)
	}
	txID, err := txIDFromProposal(proposal)
	if err != nil {
		return helperExecutionResult{}, err
	}

	c.logger.Infof("tx=%s requesting remote orchestrator msp=%s endpoint=%s namespace=%s chaincode=%s:%s",
		txID, c.MSPID(), c.Address(), req.Namespace, req.ChaincodeName, req.ChaincodeVersion)
	ctx = metadata.AppendToOutgoingContext(
		ctx,
		GRPCOperationMetadata, GRPCOperationEndorse,
		GRPCNamespaceMetadata, req.Namespace,
		GRPCInitMetadata, fmt.Sprintf("%t", req.IsInit),
	)
	resp, err := c.peer.ProcessProposal(ctx, prop)
	if err != nil {
		return helperExecutionResult{}, fmt.Errorf("remote orchestrator %s endorsement: %w", c.MSPID(), err)
	}
	if resp == nil || resp.Response == nil {
		return helperExecutionResult{}, fmt.Errorf("remote orchestrator %s returned no proposal response", c.MSPID())
	}
	c.logger.Infof("tx=%s remote orchestrator msp=%s response status=%d tx_payload_bytes=%d endorsement_present=%t",
		txID, c.MSPID(), resp.Response.Status, len(resp.Payload), resp.Endorsement != nil)

	return helperExecutionResult{
		Endorsement: sdk.Endorsement{
			Proposal:  proposal,
			Responses: []*peer.ProposalResponse{resp},
		},
		TxID:     txID,
		Response: resp.Response,
	}, nil
}

// CheckCommitReadiness queries the remote orchestrator's lifecycle service.
func (c *OrchestratorContact) CheckCommitReadiness(ctx context.Context, req *lifecycle.SignedRequest) (*lifecycle.CheckCommitReadinessResponse, error) {
	if c == nil || c.peer == nil {
		return nil, errors.New("remote orchestrator contact is not configured")
	}
	c.logger.Infof("requesting remote lifecycle readiness msp=%s endpoint=%s", c.MSPID(), c.Address())
	res, err := lifecycle.NewClient(c.peer.Connection()).CheckCommitReadiness(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("remote orchestrator %s readiness: %w", c.MSPID(), err)
	}
	return res, nil
}

// Commit asks the remote orchestrator to record the committed lifecycle
// definition in its local store.
func (c *OrchestratorContact) Commit(ctx context.Context, req *lifecycle.SignedRequest) (*lifecycle.CommitResponse, error) {
	if c == nil || c.peer == nil {
		return nil, errors.New("remote orchestrator contact is not configured")
	}
	c.logger.Infof("requesting remote lifecycle commit msp=%s endpoint=%s", c.MSPID(), c.Address())
	res, err := lifecycle.NewClient(c.peer.Connection()).Commit(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("remote orchestrator %s commit: %w", c.MSPID(), err)
	}
	return res, nil
}

// MarkInitialized asks the remote orchestrator to mark the committed lifecycle
// definition initialized in its local store.
func (c *OrchestratorContact) MarkInitialized(ctx context.Context, req *lifecycle.SignedRequest) (*lifecycle.MarkInitializedResponse, error) {
	if c == nil || c.peer == nil {
		return nil, errors.New("remote orchestrator contact is not configured")
	}
	c.logger.Infof("requesting remote lifecycle mark initialized msp=%s endpoint=%s", c.MSPID(), c.Address())
	res, err := lifecycle.NewClient(c.peer.Connection()).MarkInitialized(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("remote orchestrator %s mark initialized: %w", c.MSPID(), err)
	}
	return res, nil
}

func (c *OrchestratorContact) Close() error {
	if c == nil || c.peer == nil {
		return nil
	}
	return c.peer.Close()
}

func closeOrchestratorContacts(contacts map[string]*OrchestratorContact) error {
	var errs []error
	for _, contact := range contacts {
		errs = append(errs, contact.Close())
	}
	return errors.Join(errs...)
}

func lifecycleRemotes(contacts map[string]*OrchestratorContact) []lifecycle.RemotePeer {
	remotes := make([]lifecycle.RemotePeer, 0, len(contacts))
	for _, contact := range contacts {
		remotes = append(remotes, contact)
	}
	return remotes
}
