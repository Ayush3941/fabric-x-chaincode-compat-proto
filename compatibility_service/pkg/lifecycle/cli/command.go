// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"fmt"
	"io"
	"os"
	"sort"

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
	cmd.AddCommand(newApproveForMyOrgCommand())
	cmd.AddCommand(newCheckCommitReadinessCommand())
	cmd.AddCommand(newCommitCommand())
	cmd.AddCommand(newQueryCommittedCommand())
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

func newApproveForMyOrgCommand() *cobra.Command {
	var def lifecycle.ChaincodeDefinition
	var packageID string
	cmd := &cobra.Command{
		Use:   "approveformyorg",
		Short: "Approve a chaincode definition for this organization",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, closeClient, signer, err := lifecycleClient(cmd)
			if err != nil {
				return err
			}
			defer closeClient()

			req, err := lifecycle.SignRequest(signer, lifecycle.ApproveForMyOrgRequest{
				Definition: def,
				PackageID:  packageID,
			})
			if err != nil {
				return err
			}
			res, err := client.ApproveForMyOrg(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("approve for my org: %w", err)
			}
			printApproval(os.Stdout, res.Approval)
			return nil
		},
	}
	addDefinitionFlags(cmd, &def)
	cmd.Flags().StringVar(&packageID, "package-id", "", "Installed package ID to approve")
	cmd.MarkFlagRequired("package-id") //nolint:errcheck
	return cmd
}

func newCheckCommitReadinessCommand() *cobra.Command {
	var def lifecycle.ChaincodeDefinition
	cmd := &cobra.Command{
		Use:   "checkcommitreadiness",
		Short: "Check whether configured organizations approved a definition",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, closeClient, signer, err := lifecycleClient(cmd)
			if err != nil {
				return err
			}
			defer closeClient()

			req, err := lifecycle.SignRequest(signer, lifecycle.CheckCommitReadinessRequest{Definition: def})
			if err != nil {
				return err
			}
			res, err := client.CheckCommitReadiness(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("check commit readiness: %w", err)
			}
			printApprovals(os.Stdout, res.Approvals)
			return nil
		},
	}
	addDefinitionFlags(cmd, &def)
	return cmd
}

func newCommitCommand() *cobra.Command {
	var def lifecycle.ChaincodeDefinition
	cmd := &cobra.Command{
		Use:   "commit",
		Short: "Commit a chaincode definition into orchestrator lifecycle state",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, closeClient, signer, err := lifecycleClient(cmd)
			if err != nil {
				return err
			}
			defer closeClient()

			req, err := lifecycle.SignRequest(signer, lifecycle.CommitRequest{Definition: def})
			if err != nil {
				return err
			}
			res, err := client.Commit(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("commit definition: %w", err)
			}
			printCommitted(os.Stdout, res.Definition)
			if res.Ledger.TxID != "" {
				fmt.Fprintln(os.Stdout)
				printLedgerCommit(os.Stdout, res.Ledger)
			}
			if len(res.Approvals) > 0 {
				fmt.Fprintln(os.Stdout)
				printApprovals(os.Stdout, res.Approvals)
			}
			return nil
		},
	}
	addDefinitionFlags(cmd, &def)
	return cmd
}

func newQueryCommittedCommand() *cobra.Command {
	var name, version string
	cmd := &cobra.Command{
		Use:   "querycommitted",
		Short: "Query committed chaincode definitions",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, closeClient, signer, err := lifecycleClient(cmd)
			if err != nil {
				return err
			}
			defer closeClient()

			req, err := lifecycle.SignRequest(signer, lifecycle.QueryCommittedRequest{
				Name:    name,
				Version: version,
			})
			if err != nil {
				return err
			}
			res, err := client.QueryCommitted(cmd.Context(), req)
			if err != nil {
				return fmt.Errorf("query committed definitions: %w", err)
			}
			if len(res.Definitions) == 0 {
				fmt.Fprintln(os.Stdout, "committed_definitions=0")
				return nil
			}
			for i, committed := range res.Definitions {
				if i > 0 {
					fmt.Fprintln(os.Stdout)
				}
				printCommitted(os.Stdout, committed)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&name, "name", "n", "", "Chaincode name")
	cmd.Flags().StringVarP(&version, "version", "v", "", "Chaincode version")
	return cmd
}

func addDefinitionFlags(cmd *cobra.Command, def *lifecycle.ChaincodeDefinition) {
	cmd.Flags().StringVarP(&def.Name, "name", "n", "", "Chaincode name")
	cmd.Flags().StringVarP(&def.Version, "version", "v", "", "Chaincode version")
	cmd.Flags().Int64Var(&def.Sequence, "sequence", 0, "Definition sequence")
	cmd.Flags().BoolVar(&def.InitRequired, "init-required", false, "Whether Init is required")
	cmd.MarkFlagRequired("name")     //nolint:errcheck
	cmd.MarkFlagRequired("version")  //nolint:errcheck
	cmd.MarkFlagRequired("sequence") //nolint:errcheck
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

func printApproval(w io.Writer, approval lifecycle.ApprovedDefinition) {
	printDefinition(w, approval.Definition)
	fmt.Fprintf(w, "package_id=%s\n", approval.PackageID)
	fmt.Fprintf(w, "package_address=%s\n", approval.PackageAddress)
	fmt.Fprintf(w, "approving_msp=%s\n", approval.ApprovingMSP)
	fmt.Fprintf(w, "approved_at=%s\n", approval.ApprovedAt)
}

func printCommitted(w io.Writer, committed lifecycle.CommittedDefinition) {
	printDefinition(w, committed.Definition)
	fmt.Fprintf(w, "initialized=%t\n", committed.Initialized)
	fmt.Fprintf(w, "committed_at=%s\n", committed.CommittedAt)
}

func printLedgerCommit(w io.Writer, ledger lifecycle.LedgerCommitResult) {
	fmt.Fprintf(w, "ledger_tx_id=%s\n", ledger.TxID)
	fmt.Fprintf(w, "ledger_status=%s\n", ledger.Status)
	fmt.Fprintf(w, "ledger_block_num=%d\n", ledger.BlockNum)
	fmt.Fprintf(w, "ledger_tx_num=%d\n", ledger.TxNum)
	fmt.Fprintf(w, "ledger_namespace=%s\n", ledger.Namespace)
	fmt.Fprintf(w, "ledger_key=%s\n", ledger.Key)
}

func printDefinition(w io.Writer, def lifecycle.ChaincodeDefinition) {
	fmt.Fprintf(w, "name=%s\n", def.Name)
	fmt.Fprintf(w, "version=%s\n", def.Version)
	fmt.Fprintf(w, "sequence=%d\n", def.Sequence)
	fmt.Fprintf(w, "init_required=%t\n", def.InitRequired)
}

func printApprovals(w io.Writer, approvals map[string]bool) {
	keys := make([]string, 0, len(approvals))
	for key := range approvals {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Fprintf(w, "%s=%t\n", key, approvals[key])
	}
}
