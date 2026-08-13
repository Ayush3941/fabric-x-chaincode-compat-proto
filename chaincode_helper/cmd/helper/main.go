/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"chaincode_helper/pkg/api"
	"chaincode_helper/pkg/config"
	"chaincode_helper/pkg/shim"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-committer/utils/serve"
	"github.com/hyperledger/fabric-x-common/common/viperutil"
	"github.com/spf13/cobra"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd := &cobra.Command{
		Use:   "helper",
		Short: "Helper - Fabric-X chaincode compatibility helper",
		Long: `Chaincode helper exposes the Fabric ProcessProposal endpoint without
maintaining a local world-state database. It is a scaffold for connecting an external
Fabric chaincode-as-a-service process to Fabric-X Query Service reads and Fabric-X
transaction endorsement.`,
		RunE: run,
	}

	cmd.Flags().StringP("config", "c", "", "Path to configuration file")
	cmd.Flags().String("log-level", "INFO", "Log level (DEBUG, INFO, WARNING, ERROR)")
	cmd.MarkFlagRequired("config")

	if err := cmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	// Load configuration
	parser := viperutil.New()
	parser.SetConfigName("helper")
	configFile, _ := cmd.Flags().GetString("config")
	f, err := os.Open(configFile)
	if err != nil {
		return fmt.Errorf("failed to open config: %w", err)
	}
	if err := parser.ReadConfig(f); err != nil {
		f.Close()
		return fmt.Errorf("failed to read config: %w", err)
	}
	f.Close()

	// Parse and validate
	var cfg config.Config
	if err := parser.EnhancedExactUnmarshal(&cfg); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// Setup logging
	if cmd.Flags().Changed("log-level") {
		logLevel, _ := cmd.Flags().GetString("log-level")
		cfg.Logging.LogSpec = logLevel
	}
	flogging.Init(cfg.Logging)
	logger := flogging.MustGetLogger("main")
	logger.Infof("starting helper (config: %v)", parser.ConfigFileUsed())

	// Create service
	shimConnector, err := shim.NewConnector(shim.Config{Endpoint: cfg.ChaincodeService.Address()})
	if err != nil {
		return fmt.Errorf("failed to create shim connector: %w", err)
	}
	executors := map[string]api.Executor{
		cfg.Namespace: NewChaincodeServiceExecutor(shimConnector),
	}
	svcCfg := api.ServiceConfig{
		ChannelID:    cfg.ChannelID,
		Protocol:     cfg.Protocol,
		QueryService: cfg.QueryService.ToPeerConf(),
	}
	svc, err := api.New(svcCfg, cfg.Identity.MSPDir, cfg.Identity.MspID, executors, flogging.MustGetLogger("helper"))
	if err != nil {
		return fmt.Errorf("failed to create service: %w", err)
	}

	// Start service
	// This handles:
	// - gRPC server creation with TLS
	// - Network listener binding
	// - Service registration
	// - Background task execution
	// - Graceful shutdown on context cancellation
	if err := serve.StartAndServe(cmd.Context(), svc, &serve.Config{GRPC: *cfg.Server}); err != nil {
		return fmt.Errorf("service failed: %w", err)
	}

	logger.Info("service shutdown complete")
	return nil
}
