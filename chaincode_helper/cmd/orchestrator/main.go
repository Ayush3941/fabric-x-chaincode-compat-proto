/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"chaincode_helper/pkg/orchestrator"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-common/common/viperutil"
	"github.com/spf13/cobra"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cmd := &cobra.Command{
		Use:          "orchestrator",
		Short:        "Orchestrator - client-facing Fabric-X chaincode invocation service",
		RunE:         run,
		SilenceUsage: true,
	}
	cmd.Flags().StringP("config", "c", "", "Path to configuration file")
	cmd.Flags().String("log-level", "INFO", "Log level (DEBUG, INFO, WARNING, ERROR)")
	cmd.MarkFlagRequired("config")

	if err := cmd.ExecuteContext(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, _ []string) error {
	parser := viperutil.New()
	configFile, _ := cmd.Flags().GetString("config")
	f, err := os.Open(configFile)
	if err != nil {
		return fmt.Errorf("open config: %w", err)
	}
	if err := parser.ReadConfig(f); err != nil {
		f.Close()
		return fmt.Errorf("read config: %w", err)
	}
	f.Close()

	var cfg orchestrator.Config
	if err := parser.EnhancedExactUnmarshal(&cfg); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	logging := flogging.Config{LogSpec: "info:grpc=error"}
	if cmd.Flags().Changed("log-level") {
		logLevel, _ := cmd.Flags().GetString("log-level")
		logging.LogSpec = logLevel
	}
	flogging.Init(logging)

	logger := flogging.MustGetLogger("orchestrator")
	svc, err := orchestrator.New(cmd.Context(), cfg, logger)
	if err != nil {
		return fmt.Errorf("create orchestrator: %w", err)
	}
	defer svc.Close() //nolint:errcheck

	if err := svc.Run(cmd.Context()); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
