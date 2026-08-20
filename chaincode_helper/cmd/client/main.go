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
	"time"

	"chaincode_helper/pkg/config"
	"chaincode_helper/pkg/coordinator"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-common/common/viperutil"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"github.com/hyperledger/fabric-x-sdk/network"
	nfab "github.com/hyperledger/fabric-x-sdk/network/fabric"
	nfabx "github.com/hyperledger/fabric-x-sdk/network/fabricx"
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

	// Coordinator is the V1 client-facing ProcessProposal endpoint.
	Coordinator *config.ClientConfig `mapstructure:"coordinator"`

	// Endorsers is the list of ProcessProposal endpoints, one per organization.
	// Each entry has its own TLS configuration because helpers run at different orgs.
	Endorsers []config.ClientConfig `mapstructure:"endorsers"`

	// Orderer is the ordering service endpoint the signed transaction is submitted to.
	// Required for invoke; ignored by query.
	Orderer *config.ClientConfig `mapstructure:"orderer"`
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
		Long: `Client sends invocations to the coordinator when configured, or directly
to helper services for lower-level testing.

  query  — endorse only; prints the response payload (read-only)
  invoke — runs the write path and prints the coordinator/direct result`,
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
			if cfg.Coordinator != nil {
				res, err := callCoordinator(cmd.Context(), cfg, ns, "query", txArgs)
				if err != nil {
					return err
				}
				if res.Status < 200 || res.Status >= 400 {
					return fmt.Errorf("coordinator returned error status %d: %s", res.Status, res.Message)
				}
				cmd.Print(res.Payload)
				return nil
			}

			signer, err := identity.SignerFromMSP(cfg.Identity.MSPDir, cfg.Identity.MspID)
			if err != nil {
				return fmt.Errorf("load identity: %w", err)
			}

			ec, err := buildEndorsementClient(cfg, signer, ns)
			if err != nil {
				return err
			}
			defer ec.Close() //nolint:errcheck

			end, err := ec.ExecuteTransaction(cmd.Context(), ns, "1.0", txArgs)
			if err != nil {
				return fmt.Errorf("endorsement failed: %w", err)
			}
			if len(end.Responses) == 0 {
				return nil
			}
			resp := end.Responses[0].Response
			if resp.Status < 200 || resp.Status >= 400 {
				return fmt.Errorf("endorser returned error status %d: %s", resp.Status, resp.Message)
			}
			cmd.Print(string(resp.Payload))
			return nil
		},
	}
	return cmd
}

func newInvokeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   `invoke '{"function":"...","Args":[]}'`,
		Short: "Endorse a transaction and submit it to the orderer",
		Long: `Endorse a transaction and submit it to the orderer.
When configured with a coordinator endpoint, this waits for Notification
Service finality and prints the commit status. In direct-helper mode, it only
submits to the orderer and does not wait for finality.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, _, txArgs, err := prepare(cmd, args[0])
			if err != nil {
				return err
			}
			ns := namespaceOrDefault(cmd, cfg.Namespace)
			if cfg.Coordinator != nil {
				res, err := callCoordinator(cmd.Context(), cfg, ns, "invoke", txArgs)
				if err != nil {
					return err
				}
				out, err := json.MarshalIndent(res, "", "  ")
				if err != nil {
					return err
				}
				cmd.Print(string(out))
				return nil
			}
			if cfg.Orderer == nil {
				return fmt.Errorf("orderer is required for invoke")
			}

			signer, err := identity.SignerFromMSP(cfg.Identity.MSPDir, cfg.Identity.MspID)
			if err != nil {
				return fmt.Errorf("load identity: %w", err)
			}

			logger := flogging.MustGetLogger("client")

			ec, err := buildEndorsementClient(cfg, signer, ns)
			if err != nil {
				return err
			}
			defer ec.Close() //nolint:errcheck

			ordererConfs := []network.OrdererConf{cfg.Orderer.ToOrdererConf()}
			waitAfterSubmit, err := cmd.Flags().GetDuration("wait-after-submit")
			if err != nil {
				return err
			}
			var submitter *network.FabricSubmitter
			switch cfg.Protocol {
			case "fabric":
				submitter, err = nfab.NewSubmitter(cmd.Context(), ordererConfs, signer, waitAfterSubmit, logger)
			case "fabric-x", "":
				submitter, err = nfabx.NewSubmitter(cmd.Context(), ordererConfs, signer, waitAfterSubmit, logger)
			default:
				return fmt.Errorf("unknown protocol %q: must be \"fabric\" or \"fabric-x\"", cfg.Protocol)
			}
			if err != nil {
				return fmt.Errorf("create submitter: %w", err)
			}
			defer submitter.Close() //nolint:errcheck

			logger.Debugf("sending proposal to %d endorser(s)", len(cfg.Endorsers))
			end, err := ec.ExecuteTransaction(cmd.Context(), ns, "1.0", txArgs)
			if err != nil {
				return fmt.Errorf("endorsement failed: %w", err)
			}
			if len(end.Responses) == 0 {
				return fmt.Errorf("no responses")
			}
			resp := end.Responses[0].Response
			if resp.Status < 200 || resp.Status >= 400 {
				return fmt.Errorf("endorser returned error status %d: %s", resp.Status, resp.Message)
			}

			logger.Debugf("submitting transaction to orderer")
			if err := submitter.Submit(cmd.Context(), end); err != nil {
				return fmt.Errorf("submit failed: %w", err)
			}
			logger.Debugf("transaction submitted")
			cmd.Print(string(resp.Payload))
			return nil
		},
	}
	cmd.Flags().Duration("wait-after-submit", 2*time.Second, "delay before closing the orderer stream after submit")
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
	if cfg.Coordinator != nil {
		if cfg.Coordinator.Endpoint == nil {
			return fmt.Errorf("coordinator.endpoint is required")
		}
		if cfg.Identity == nil {
			return fmt.Errorf("identity is required for coordinator transport")
		}
		return nil
	}
	if cfg.Identity == nil {
		return fmt.Errorf("identity is required")
	}
	if len(cfg.Endorsers) == 0 {
		return fmt.Errorf("at least one endorser is required")
	}
	return nil
}

func callCoordinator(ctx context.Context, cfg Config, namespace, operation string, txArgs [][]byte) (coordinator.InvocationResponse, error) {
	signer, err := identity.SignerFromMSP(cfg.Identity.MSPDir, cfg.Identity.MspID)
	if err != nil {
		return coordinator.InvocationResponse{}, fmt.Errorf("load identity: %w", err)
	}

	ec, err := network.NewEndorsementClient([]network.PeerConf{cfg.Coordinator.ToPeerConf()}, signer, cfg.ChannelID, namespace, "1.0")
	if err != nil {
		return coordinator.InvocationResponse{}, fmt.Errorf("create coordinator grpc client: %w", err)
	}
	defer ec.Close() //nolint:errcheck

	args, err := coordinatorProposalArgs(operation, txArgs)
	if err != nil {
		return coordinator.InvocationResponse{}, err
	}

	end, err := ec.ExecuteTransaction(ctx, namespace, "1.0", args)
	if err != nil {
		return coordinator.InvocationResponse{}, fmt.Errorf("coordinator grpc call failed: %w", err)
	}
	if len(end.Responses) == 0 || end.Responses[0] == nil || end.Responses[0].Response == nil {
		return coordinator.InvocationResponse{}, fmt.Errorf("coordinator returned no response")
	}

	resp := end.Responses[0].Response
	var out coordinator.InvocationResponse
	if err := json.Unmarshal(resp.Payload, &out); err != nil {
		return coordinator.InvocationResponse{}, fmt.Errorf("decode coordinator grpc response: %w", err)
	}
	return out, nil
}

func coordinatorProposalArgs(operation string, txArgs [][]byte) ([][]byte, error) {
	var marker string
	switch operation {
	case "invoke":
		marker = coordinator.GRPCOperationInvoke
	case "query":
		marker = coordinator.GRPCOperationQuery
	default:
		return nil, fmt.Errorf("unknown coordinator operation %q", operation)
	}

	args := make([][]byte, 0, 1+len(txArgs))
	args = append(args, []byte(marker))
	for _, arg := range txArgs {
		args = append(args, append([]byte(nil), arg...))
	}
	return args, nil
}

func buildEndorsementClient(cfg Config, signer identity.Signer, namespace string) (*network.EndorsementClient, error) {
	peerConfs := make([]network.PeerConf, len(cfg.Endorsers))
	for i := range cfg.Endorsers {
		peerConfs[i] = cfg.Endorsers[i].ToPeerConf()
	}
	ec, err := network.NewEndorsementClient(peerConfs, signer, cfg.ChannelID, namespace, "1.0")
	if err != nil {
		return nil, fmt.Errorf("create endorsement client: %w", err)
	}
	return ec, nil
}

func namespaceOrDefault(cmd *cobra.Command, cfgNamespace string) string {
	if ns, _ := cmd.Flags().GetString("namespace"); ns != "" {
		return ns
	}
	return cfgNamespace
}
