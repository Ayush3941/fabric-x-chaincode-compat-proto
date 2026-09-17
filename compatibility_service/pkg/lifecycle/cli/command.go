// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"fmt"
	"io"
	"os"

	"compatibility_service/pkg/config"
	"compatibility_service/pkg/lifecycle"
	"github.com/hyperledger/fabric-x-common/common/viperutil"
	"github.com/hyperledger/fabric-x-sdk/identity"
	"github.com/hyperledger/fabric-x-sdk/network"
	"github.com/spf13/cobra"
)

type clientConfig struct {
	Identity     *config.IdentityConfig `mapstructure:"identity"`
	Orchestrator *config.ClientConfig   `mapstructure:"orchestrator"`
}

// NewCommand returns the lifecycle command tree.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "lifecycle",
		Short: "Manage compatibility-layer chaincode lifecycle",
	}
	cmd.PersistentFlags().StringP("config", "c", "", "Path to lifecycle client configuration file")
	cmd.AddCommand(newPackageCommand())
	cmd.AddCommand(newInstallCommand())
	cmd.AddCommand(newQueryInstalledCommand())
	return cmd
}

func newPackageCommand() *cobra.Command {
	var opts lifecycle.PackageOptions
	cmd := &cobra.Command{
		Use:   "package",
		Short: "Package external chaincode connection metadata",
		RunE: func(_ *cobra.Command, _ []string) error {
			info, err := lifecycle.PackageExternalChaincode(opts)
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stdout, "label=%s\n", info.Label)
			fmt.Fprintf(os.Stdout, "output=%s\n", info.Output)
			fmt.Fprintf(os.Stdout, "package_id=%s\n", info.PackageID)
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.SourceDir, "path", "", "Directory containing connection.json and metadata.json")
	cmd.Flags().StringVar(&opts.OutputPath, "output", "", "Output chaincode package path")
	cmd.Flags().StringVar(&opts.Label, "label", "", "Package label; updates metadata.json before packaging")
	cmd.MarkFlagRequired("path")   //nolint:errcheck
	cmd.MarkFlagRequired("output") //nolint:errcheck
	return cmd
}

func newInstallCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "install PACKAGE",
		Short: "Install a packaged external chaincode into a running orchestrator",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			packageBytes, err := os.ReadFile(args[0])
			if err != nil {
				return fmt.Errorf("read package: %w", err)
			}
			client, closeClient, signer, err := lifecycleClient(cmd)
			if err != nil {
				return err
			}
			defer closeClient()

			req, err := lifecycle.SignRequest(signer, lifecycle.InstallRequest{Package: packageBytes})
			if err != nil {
				return err
			}
			res, err := client.Install(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("install package: %w", err)
			}
			printPackage(os.Stdout, res.Package)
			return nil
		},
	}
	return cmd
}

func newQueryInstalledCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queryinstalled",
		Short: "List packages installed in a running orchestrator",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, closeClient, signer, err := lifecycleClient(cmd)
			if err != nil {
				return err
			}
			defer closeClient()

			req, err := lifecycle.SignRequest(signer, lifecycle.QueryInstalledRequest{})
			if err != nil {
				return err
			}
			res, err := client.QueryInstalled(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("query installed packages: %w", err)
			}
			if len(res.Packages) == 0 {
				fmt.Fprintln(os.Stdout, "installed_packages=0")
				return nil
			}
			for i, pkg := range res.Packages {
				if i > 0 {
					fmt.Fprintln(os.Stdout)
				}
				printPackageSummary(os.Stdout, pkg)
			}
			return nil
		},
	}
	return cmd
}

func lifecycleClient(cmd *cobra.Command) (*lifecycle.Client, func(), identity.Signer, error) {
	cfg, err := loadClientConfig(cmd)
	if err != nil {
		return nil, nil, identity.Signer{}, err
	}
	signer, err := identity.SignerFromMSP(cfg.Identity.MSPDir, cfg.Identity.MspID)
	if err != nil {
		return nil, nil, identity.Signer{}, fmt.Errorf("load identity: %w", err)
	}
	peer, err := network.NewPeer(cfg.Orchestrator.ToPeerConf())
	if err != nil {
		return nil, nil, identity.Signer{}, fmt.Errorf("connect orchestrator: %w", err)
	}
	closeClient := func() {
		peer.Close() //nolint:errcheck
	}
	return lifecycle.NewClient(peer.Connection()), closeClient, signer, nil
}

func loadClientConfig(cmd *cobra.Command) (clientConfig, error) {
	configFile, _ := cmd.Flags().GetString("config")
	if configFile == "" {
		return clientConfig{}, fmt.Errorf("config is required")
	}
	f, err := os.Open(configFile)
	if err != nil {
		return clientConfig{}, fmt.Errorf("open config: %w", err)
	}
	defer f.Close() //nolint:errcheck

	parser := viperutil.New()
	if err := parser.ReadConfig(f); err != nil {
		return clientConfig{}, fmt.Errorf("read config: %w", err)
	}
	var cfg clientConfig
	if err := parser.EnhancedExactUnmarshal(&cfg); err != nil {
		return clientConfig{}, fmt.Errorf("invalid config: %w", err)
	}
	if cfg.Identity == nil {
		return clientConfig{}, fmt.Errorf("identity is required")
	}
	if cfg.Orchestrator == nil || cfg.Orchestrator.Endpoint == nil {
		return clientConfig{}, fmt.Errorf("orchestrator.endpoint is required")
	}
	return cfg, nil
}

func printPackage(w io.Writer, pkg lifecycle.InstalledPackage) {
	fmt.Fprintf(w, "package_id=%s\n", pkg.PackageID)
	fmt.Fprintf(w, "label=%s\n", pkg.Label)
	fmt.Fprintf(w, "type=%s\n", pkg.Type)
	fmt.Fprintf(w, "address=%s\n", pkg.Address)
	fmt.Fprintf(w, "installed_by_msp=%s\n", pkg.InstalledByMSP)
	fmt.Fprintf(w, "installed_at=%s\n", pkg.InstalledAt)
}

func printPackageSummary(w io.Writer, pkg lifecycle.InstalledPackageSummary) {
	fmt.Fprintf(w, "package_id=%s\n", pkg.PackageID)
	fmt.Fprintf(w, "label=%s\n", pkg.Label)
}
