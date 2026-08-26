/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"compatibility_service/pkg/config"
	"compatibility_service/pkg/orchestrator"
	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-protos-go-apiv2/peer"
	"github.com/hyperledger/fabric-x-common/common/viperutil"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"github.com/hyperledger/fabric-x-sdk/network"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/metadata"
)

// Config holds all configuration for the client.
type Config struct {
	// ChannelID is the channel to submit to.
	ChannelID string `mapstructure:"channel-id"`

	// Namespace is the chaincode name or Fabric-X namespace to invoke.
	Namespace string `mapstructure:"namespace"`

	// Protocol selects the network protocol: "fabric" or "fabric-x".
	// Defaults to "fabric-x".
	Protocol string `mapstructure:"protocol"`

	// Identity is the MSP identity used for signing the proposal and the transaction.
	Identity *config.IdentityConfig `mapstructure:"identity"`

	// Orchestrator is the V1 client-facing ProcessProposal endpoint.
	Orchestrator *config.ClientConfig `mapstructure:"orchestrator"`
}

// txInput is the JSON format for the transaction argument.
// It follows the Fabric peer CLI convention: function name plus arguments.
type txInput struct {
	Function  string            `json:"Function"`
	Args      []string          `json:"Args"`
	Transient map[string]string `json:"Transient"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd := &cobra.Command{
		Use:   "client",
		Short: "Client - Example Fabric-X helper client",
		Long: `Client sends invocations to the orchestrator gRPC endpoint.

  query  — endorse only; prints the response payload (read-only)
  invoke — runs the write path and prints the orchestrator result`,
	}
	cmd.PersistentFlags().StringP("config", "c", "", "Path to configuration file")
	cmd.PersistentFlags().String("namespace", "", "Namespace to invoke (overrides config)")
	cmd.MarkPersistentFlagRequired("config")

	cmd.AddCommand(newQueryCmd(), newInvokeCmd())

	if err := cmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func newQueryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   `query '{"Args":[]}'`,
		Short: "Send a read-only proposal and print the response payload",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, _, txArgs, transient, err := prepare(cmd, args[0])
			if err != nil {
				return err
			}
			ns := namespaceOrDefault(cmd, cfg.Namespace)
			res, err := callOrchestrator(cmd.Context(), cfg, ns, "query", txArgs, transient)
			if err != nil {
				return err
			}
			if res.Status < 200 || res.Status >= 400 {
				return fmt.Errorf("orchestrator returned error status %d: %s", res.Status, res.Message)
			}
			if len(res.Payload) > 0 {
				fmt.Fprintln(os.Stdout, res.Payload)
			}
			return nil
		},
	}
	return cmd
}

func newInvokeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   `invoke '{"function":"...","Args":[]}'`,
		Short: "Invoke through the orchestrator and wait for finality",
		Long: `Invoke through the orchestrator. The orchestrator calls the embedded
helper execution path, submits the Fabric-X transaction, waits for Notification
Service finality, and returns the final status.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, _, txArgs, transient, err := prepare(cmd, args[0])
			if err != nil {
				return err
			}
			ns := namespaceOrDefault(cmd, cfg.Namespace)
			res, err := callOrchestrator(cmd.Context(), cfg, ns, "invoke", txArgs, transient)
			if err != nil {
				return err
			}
			out, err := json.MarshalIndent(res, "", "  ")
			if err != nil {
				return err
			}
			fmt.Fprintln(os.Stdout, string(out))
			return nil
		},
	}
	return cmd
}

// prepare loads config and parses the transaction JSON — shared by query and invoke.
func prepare(cmd *cobra.Command, txJSON string) (Config, txInput, [][]byte, map[string][]byte, error) {
	cfg, err := loadConfig(cmd)
	if err != nil {
		return Config{}, txInput{}, nil, nil, err
	}
	tx, txArgs, transient, err := parseTxArgs(txJSON)
	if err != nil {
		return Config{}, txInput{}, nil, nil, err
	}
	return cfg, tx, txArgs, transient, nil
}

func loadConfig(cmd *cobra.Command) (Config, error) {
	configFile, _ := cmd.Flags().GetString("config")
	f, err := os.Open(configFile)
	if err != nil {
		return Config{}, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	parser := viperutil.New()
	if err := parser.ReadConfig(f); err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := parser.EnhancedExactUnmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("invalid config: %w", err)
	}
	if err := validate(cfg); err != nil {
		return Config{}, fmt.Errorf("invalid config: %w", err)
	}
	return cfg, nil
}

func parseTxArgs(txJSON string) (txInput, [][]byte, map[string][]byte, error) {
	var tx txInput
	if err := json.Unmarshal([]byte(txJSON), &tx); err != nil {
		return txInput{}, nil, nil, fmt.Errorf("invalid transaction JSON: %w", err)
	}
	txArgs := make([][]byte, 0, 1+len(tx.Args))
	if tx.Function != "" {
		txArgs = append(txArgs, []byte(tx.Function))
	}
	for _, a := range tx.Args {
		txArgs = append(txArgs, []byte(a))
	}
	transient := make(map[string][]byte, len(tx.Transient))
	for key, value := range tx.Transient {
		transient[key] = []byte(value)
	}
	if len(transient) == 0 {
		transient = nil
	}
	return tx, txArgs, transient, nil
}

func validate(cfg Config) error {
	if cfg.ChannelID == "" {
		return fmt.Errorf("channel-id is required")
	}
	if cfg.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if cfg.Orchestrator == nil || cfg.Orchestrator.Endpoint == nil {
		return fmt.Errorf("orchestrator.endpoint is required")
	}
	if cfg.Identity == nil {
		return fmt.Errorf("identity is required for orchestrator transport")
	}
	return nil
}

func callOrchestrator(ctx context.Context, cfg Config, namespace, operation string, txArgs [][]byte, transient map[string][]byte) (orchestrator.InvocationResponse, error) {
	signer, err := identity.SignerFromMSP(cfg.Identity.MSPDir, cfg.Identity.MspID)
	if err != nil {
		return orchestrator.InvocationResponse{}, fmt.Errorf("load identity: %w", err)
	}

	if operation != orchestrator.GRPCOperationInvoke && operation != orchestrator.GRPCOperationQuery {
		return orchestrator.InvocationResponse{}, fmt.Errorf("unknown orchestrator operation %q", operation)
	}

	prop, err := newSignedProposal(signer, cfg.ChannelID, namespace, "1.0", txArgs, transient)
	if err != nil {
		return orchestrator.InvocationResponse{}, fmt.Errorf("create signed proposal: %w", err)
	}

	orchestratorPeer, err := network.NewPeer(cfg.Orchestrator.ToPeerConf())
	if err != nil {
		return orchestrator.InvocationResponse{}, fmt.Errorf("create orchestrator grpc client: %w", err)
	}
	defer orchestratorPeer.Close() //nolint:errcheck

	ctx = metadata.AppendToOutgoingContext(ctx, orchestrator.GRPCOperationMetadata, operation)
	resp, err := orchestratorPeer.ProcessProposal(ctx, prop)
	if err != nil {
		return orchestrator.InvocationResponse{}, fmt.Errorf("orchestrator grpc call failed: %w", err)
	}
	if resp == nil || resp.Response == nil {
		return orchestrator.InvocationResponse{}, fmt.Errorf("orchestrator returned no response")
	}

	var out orchestrator.InvocationResponse
	if err := json.Unmarshal(resp.Response.Payload, &out); err != nil {
		return orchestrator.InvocationResponse{}, fmt.Errorf("decode orchestrator grpc response: %w", err)
	}
	return out, nil
}

func newSignedProposal(
	signer interface {
		Sign([]byte) ([]byte, error)
		Serialize() ([]byte, error)
	},
	channel,
	namespace,
	nsVersion string,
	args [][]byte,
	transient map[string][]byte,
) (*peer.SignedProposal, error) {
	creator, err := signer.Serialize()
	if err != nil {
		return nil, err
	}

	nonce, err := newNonce()
	if err != nil {
		return nil, err
	}

	proposal, _, err := protoutil.CreateChaincodeProposalWithTxIDNonceAndTransient(
		protoutil.ComputeTxID(nonce, creator),
		common.HeaderType_ENDORSER_TRANSACTION,
		channel,
		&peer.ChaincodeInvocationSpec{
			ChaincodeSpec: &peer.ChaincodeSpec{
				Type: peer.ChaincodeSpec_CAR,
				ChaincodeId: &peer.ChaincodeID{
					Name:    namespace,
					Version: nsVersion,
				},
				Input: &peer.ChaincodeInput{
					Args: args,
				},
			},
		},
		nonce,
		creator,
		transient,
	)
	if err != nil {
		return nil, err
	}
	return protoutil.GetSignedProposal(proposal, signer)
}

func newNonce() ([]byte, error) {
	nonce := make([]byte, 24)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return nonce, nil
}

func namespaceOrDefault(cmd *cobra.Command, cfgNamespace string) string {
	if ns, _ := cmd.Flags().GetString("namespace"); ns != "" {
		return ns
	}
	return cfgNamespace
}
