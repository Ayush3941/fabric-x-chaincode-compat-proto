// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"compatibility_service/internal/testutil"
	"compatibility_service/pkg/auth"
	"compatibility_service/pkg/lifecycle"
	sdk "github.com/hyperledger/fabric-x-sdk"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func TestLifecycleInstallCommandInterface(t *testing.T) {
	file := filepath.Join(t.TempDir(), "package.tgz")
	testutil.WriteFile(t, file, []byte("archive"))
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"removed package command", []string{"package"}, `unknown command "package"`},
		{"required path", []string{"install"}, `required flag(s) "path"`},
		{"no positional archive", []string{"install", "--path", t.TempDir(), file}, "unknown command"},
		{"path must be directory", []string{"install", "--path", file}, "is not a directory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{Use: "orchestrator", RunE: func(*cobra.Command, []string) error {
				t.Fatal("unexpected service startup")
				return nil
			}}
			cmd.AddCommand(NewCommand())
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs(append([]string{"lifecycle"}, tc.args...))
			if err := cmd.ExecuteContext(t.Context()); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestInstallDirectoryUsesSignedLifecycleRPC(t *testing.T) {
	org := testutil.NewMSP(t, "org-0")
	now := time.Now()
	admin := org.Issue(t, "admin", now.Add(-time.Hour), now.Add(time.Hour))
	client := org.Issue(t, "client", now.Add(-time.Hour), now.Add(time.Hour))
	node := org.Issue(t, "peer", now.Add(-time.Hour), now.Add(time.Hour))
	trust, err := auth.NewVerifier([]auth.MSPConfig{org.Config})
	if err != nil {
		t.Fatal(err)
	}
	policies := make(map[string]string)
	for _, operation := range []string{lifecycle.OperationInstall, lifecycle.OperationQueryInstalled, lifecycle.OperationApprove, lifecycle.OperationReadiness, lifecycle.OperationCommit, lifecycle.OperationQueryCommitted} {
		policies[operation] = "OR('org-0.admin')"
	}
	authorizer, err := lifecycle.NewAuthorizer(trust, "test-channel", "org-0", lifecycle.AuthConfig{
		Policies:      policies,
		Orchestrators: []lifecycle.OrchestratorIdentity{{MSPID: "org-0", CertPath: node.CertPath}},
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := lifecycle.NewMemoryStore("test-channel")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	lifecycle.RegisterLifecycleServer(server, lifecycle.NewServer(store, lifecycle.ServerConfig{
		ChannelID: "test-channel", MSPID: "org-0", Auth: authorizer, Signer: node.Signer,
	}, sdk.NoOpLogger{}, nil))
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(listener) }()

	source := t.TempDir()
	metadata := `{"path":"","type":"external","label":"sample_1"}`
	testutil.WriteFile(t, filepath.Join(source, "connection.json"), []byte(`{"address":"127.0.0.1:9999","tls_required":false}`))
	testutil.WriteFile(t, filepath.Join(source, "metadata.json"), []byte(metadata))
	var originalID string
	for _, tc := range []struct {
		name      string
		actor     testutil.Actor
		label     string
		wantError bool
		wantCount int
	}{
		{name: "client denied", actor: client, wantError: true},
		{name: "admin installs folder", actor: admin, wantCount: 1},
		{name: "repeated install retains package ID", actor: admin, wantCount: 1},
		{name: "label override", actor: admin, label: "sample_2", wantCount: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, err := json.Marshal(map[string]any{
				"channel-id":   "test-channel",
				"identity":     map[string]any{"msp-id": "org-0", "msp-dir": filepath.Dir(filepath.Dir(tc.actor.CertPath))},
				"orchestrator": map[string]any{"tls": map[string]any{"mode": "none"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(t.TempDir(), "client.yaml")
			testutil.WriteFile(t, configPath, config)
			cmd := NewCommand()
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			var output bytes.Buffer
			cmd.SetOut(&output)
			cmd.SetErr(io.Discard)
			args := []string{"install", "--path", source, "-c", configPath, "--host", listener.Addr().String()}
			if tc.label != "" {
				args = append(args, "--label", tc.label)
			}
			cmd.SetArgs(args)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err = cmd.ExecuteContext(ctx)
			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "PermissionDenied") {
					t.Fatalf("ordinary client install = %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			installed, err := store.ListInstalled(t.Context())
			if err != nil || len(installed) != tc.wantCount {
				t.Fatalf("installed packages = %v, error = %v", installed, err)
			}
			if !tc.wantError {
				pkg := installed[len(installed)-1]
				if !strings.Contains(output.String(), "package_id="+pkg.PackageID) || pkg.Address != "127.0.0.1:9999" || pkg.InstalledByMSP != "org-0" {
					t.Fatalf("unexpected install result: %s %+v", output.String(), pkg)
				}
				if tc.label == "" {
					if originalID != "" && originalID != pkg.PackageID {
						t.Fatal("repeated folder install changed package ID")
					}
					originalID = pkg.PackageID
				}
			}
		})
	}
	data, err := os.ReadFile(filepath.Join(source, "metadata.json"))
	if err != nil || string(data) != metadata {
		t.Fatalf("source metadata changed: %s %v", data, err)
	}
	entries, err := os.ReadDir(source)
	if err != nil || len(entries) != 2 {
		t.Fatalf("install wrote files into source directory: %v %v", entries, err)
	}
}
