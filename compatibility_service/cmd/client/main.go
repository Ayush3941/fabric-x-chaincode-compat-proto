/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"compatibility_service/pkg/config"
	"compatibility_service/pkg/orchestrator"
	"github.com/hyperledger/fabric-x-common/common/viperutil"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"github.com/hyperledger/fabric-x-sdk/network"
	"github.com/spf13/cobra"
)

// Config holds all configuration for the helper client.
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
	Function string   `json:"Function"`
	Args     []string `json:"Args"`
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
			cfg, _, txArgs, err := prepare(cmd, args[0])
			if err != nil {
				return err
			}
			ns := namespaceOrDefault(cmd, cfg.Namespace)
			res, err := callOrchestrator(cmd.Context(), cfg, ns, "query", txArgs)
			if err != nil {
				return err
			}
			if res.Status < 200 || res.Status >= 400 {
				return fmt.Errorf("orchestrator returned error status %d: %s", res.Status, res.Message)
			}
			cmd.Print(res.Payload)
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
			cfg, _, txArgs, err := prepare(cmd, args[0])
			if err != nil {
				return err
			}
			ns := namespaceOrDefault(cmd, cfg.Namespace)
			res, err := callOrchestrator(cmd.Context(), cfg, ns, "invoke", txArgs)
			if err != nil {
				return err
			}
			out, err := json.MarshalIndent(res, "", "  ")
			if err != nil {
				return err
			}
			cmd.Print(string(out))
			return nil
		},
	}
	return cmd
}

// prepare loads config and parses the transaction JSON — shared by query and invoke.
func prepare(cmd *cobra.Command, txJSON string) (Config, txInput, [][]byte, error) {
	cfg, err := loadConfig(cmd)
	if err != nil {
		return Config{}, txInput{}, nil, err
	}
	tx, txArgs, err := parseTxArgs(txJSON)
	if err != nil {
		return Config{}, txInput{}, nil, err
	}
	return cfg, tx, txArgs, nil
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

func parseTxArgs(txJSON string) (txInput, [][]byte, error) {
	var tx txInput
	if err := json.Unmarshal([]byte(txJSON), &tx); err != nil {
		return txInput{}, nil, fmt.Errorf("invalid transaction JSON: %w", err)
	}
	txArgs := make([][]byte, 0, 1+len(tx.Args))
	if tx.Function != "" {
		txArgs = append(txArgs, []byte(tx.Function))
	}
	for _, a := range tx.Args {
		txArgs = append(txArgs, []byte(a))
	}
	return tx, txArgs, nil
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

func callOrchestrator(ctx context.Context, cfg Config, namespace, operation string, txArgs [][]byte) (orchestrator.InvocationResponse, error) {
	signer, err := identity.SignerFromMSP(cfg.Identity.MSPDir, cfg.Identity.MspID)
	if err != nil {
		return orchestrator.InvocationResponse{}, fmt.Errorf("load identity: %w", err)
	}

	ec, err := network.NewEndorsementClient([]network.PeerConf{cfg.Orchestrator.ToPeerConf()}, signer, cfg.ChannelID, namespace, "1.0")
	if err != nil {
		return orchestrator.InvocationResponse{}, fmt.Errorf("create orchestrator grpc client: %w", err)
	}
	defer ec.Close() //nolint:errcheck

	args, err := orchestratorProposalArgs(operation, txArgs)
	if err != nil {
		return orchestrator.InvocationResponse{}, err
	}

	end, err := ec.ExecuteTransaction(ctx, namespace, "1.0", args)
	if err != nil {
		return orchestrator.InvocationResponse{}, fmt.Errorf("orchestrator grpc call failed: %w", err)
	}
	if len(end.Responses) == 0 || end.Responses[0] == nil || end.Responses[0].Response == nil {
		return orchestrator.InvocationResponse{}, fmt.Errorf("orchestrator returned no response")
	}

	resp := end.Responses[0].Response
	var out orchestrator.InvocationResponse
	if err := json.Unmarshal(resp.Payload, &out); err != nil {
		return orchestrator.InvocationResponse{}, fmt.Errorf("decode orchestrator grpc response: %w", err)
	}
	return out, nil
}

func orchestratorProposalArgs(operation string, txArgs [][]byte) ([][]byte, error) {
	var marker string
	switch operation {
	case "invoke":
		marker = orchestrator.GRPCOperationInvoke
	case "query":
		marker = orchestrator.GRPCOperationQuery
	default:
		return nil, fmt.Errorf("unknown orchestrator operation %q", operation)
	}

	args := make([][]byte, 0, 1+len(txArgs))
	args = append(args, []byte(marker))
	for _, arg := range txArgs {
		args = append(args, append([]byte(nil), arg...))
	}
	return args, nil
}

func namespaceOrDefault(cmd *cobra.Command, cfgNamespace string) string {
	if ns, _ := cmd.Flags().GetString("namespace"); ns != "" {
		return ns
	}
	return cfgNamespace
}
