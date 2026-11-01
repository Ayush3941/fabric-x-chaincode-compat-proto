// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestBuildExternalChaincodePackagePreservesSourceAndFabricShape(t *testing.T) {
	sourceDir := t.TempDir()
	writeTestFile(t, filepath.Join(sourceDir, "connection.json"), `{"address":"127.0.0.1:9999","tls_required":false}`)
	originalMetadata := `{"path":"","type":"external","label":"original"}`
	writeTestFile(t, filepath.Join(sourceDir, "metadata.json"), originalMetadata)
	writeTestFile(t, filepath.Join(sourceDir, "META-INF", "statedb", "couchdb", "indexes", "indexOwner.json"), `{"index":{"fields":["owner"]}}`)

	info, err := BuildExternalChaincodePackage(PackageOptions{
		SourceDir: sourceDir,
		Label:     "fabcar_1",
	})
	if err != nil {
		t.Fatalf("BuildExternalChaincodePackage failed: %v", err)
	}
	if info.Label != "fabcar_1" {
		t.Fatalf("label = %q, want fabcar_1", info.Label)
	}
	if want := fmt.Sprintf("fabcar_1:%x", sha256.Sum256(info.PackageTGZ)); info.PackageID != want {
		t.Fatalf("package id = %q, want %q", info.PackageID, want)
	}

	var metadata map[string]any
	data, err := os.ReadFile(filepath.Join(sourceDir, "metadata.json"))
	if err != nil {
		t.Fatalf("read source metadata: %v", err)
	}
	if string(data) != originalMetadata {
		t.Fatalf("source metadata changed: %s", data)
	}
	if err := json.Unmarshal(info.MetadataJSON, &metadata); err != nil {
		t.Fatalf("decode package metadata: %v", err)
	}
	if metadata["type"] != externalChaincodeType || metadata["label"] != "fabcar_1" || metadata["path"] != "" {
		t.Fatalf("unexpected metadata: %#v", metadata)
	}

	outerEntries, innerEntries := readPackageEntries(t, info.PackageTGZ)
	if !slices.Equal(outerEntries, []string{"metadata.json", "code.tar.gz"}) {
		t.Fatalf("outer entries = %#v", outerEntries)
	}
	if !slices.Contains(innerEntries, "connection.json") {
		t.Fatalf("inner entries missing connection.json: %#v", innerEntries)
	}
	if !slices.Contains(innerEntries, "META-INF/statedb/couchdb/indexes/indexOwner.json") {
		t.Fatalf("inner entries missing META-INF: %#v", innerEntries)
	}
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if !slices.Equal(names, []string{"META-INF", "connection.json", "metadata.json"}) {
		t.Fatalf("source directory changed: %v", names)
	}
}

func TestBuildExternalChaincodePackageUsesExistingLabelAndStableID(t *testing.T) {
	sourceDir := t.TempDir()
	writeTestFile(t, filepath.Join(sourceDir, "connection.json"), `{"address":"127.0.0.1:9999","tls_required":false}`)
	writeTestFile(t, filepath.Join(sourceDir, "metadata.json"), `{"path":"","type":"external","label":"sample_1"}`)

	info, err := BuildExternalChaincodePackage(PackageOptions{SourceDir: sourceDir})
	if err != nil {
		t.Fatalf("BuildExternalChaincodePackage failed: %v", err)
	}
	if info.Label != "sample_1" {
		t.Fatalf("label = %q, want sample_1", info.Label)
	}
	again, err := BuildExternalChaincodePackage(PackageOptions{SourceDir: sourceDir})
	if err != nil {
		t.Fatal(err)
	}
	if again.PackageID != info.PackageID || !bytes.Equal(again.PackageTGZ, info.PackageTGZ) {
		t.Fatal("identical folder contents produced different packages")
	}
	writeTestFile(t, filepath.Join(sourceDir, "connection.json"), `{"address":"127.0.0.1:10000","tls_required":false}`)
	changed, err := BuildExternalChaincodePackage(PackageOptions{SourceDir: sourceDir})
	if err != nil {
		t.Fatal(err)
	}
	if changed.PackageID == info.PackageID {
		t.Fatal("changed connection metadata reused the old package id")
	}
}

func TestBuildExternalChaincodePackageDoesNotCreateMetadata(t *testing.T) {
	sourceDir := t.TempDir()
	writeTestFile(t, filepath.Join(sourceDir, "connection.json"), `{"address":"127.0.0.1:9999","tls_required":false}`)
	_, err := BuildExternalChaincodePackage(PackageOptions{SourceDir: sourceDir, Label: "sample_1"})
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(sourceDir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "connection.json" {
		t.Fatalf("build modified source directory: %v %v", entries, err)
	}
}

func TestBuildExternalChaincodePackageRejectsInvalidInput(t *testing.T) {
	for _, tc := range []struct {
		name       string
		connection string
		metadata   string
		label      string
	}{
		{name: "missing connection", label: "sample"},
		{name: "missing label", connection: `{"address":"127.0.0.1:9999"}`},
		{name: "invalid connection JSON", connection: `{`, label: "sample"},
		{name: "missing address", connection: `{}`, label: "sample"},
		{name: "invalid metadata JSON", connection: `{"address":"127.0.0.1:9999"}`, metadata: `{`, label: "sample"},
		{name: "null metadata", connection: `{"address":"127.0.0.1:9999"}`, metadata: `null`, label: "sample"},
		{name: "unsupported type", connection: `{"address":"127.0.0.1:9999"}`, metadata: `{"type":"golang"}`, label: "sample"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sourceDir := t.TempDir()
			if tc.connection != "" {
				writeTestFile(t, filepath.Join(sourceDir, "connection.json"), tc.connection)
			}
			if tc.metadata != "" {
				writeTestFile(t, filepath.Join(sourceDir, "metadata.json"), tc.metadata)
			}
			if _, err := BuildExternalChaincodePackage(PackageOptions{SourceDir: sourceDir, Label: tc.label}); err == nil {
				t.Fatal("expected invalid package error")
			}
		})
	}
	_, err := BuildExternalChaincodePackage(PackageOptions{})
	if err == nil {
		t.Fatal("expected missing path error")
	}
}

func writeTestFile(t *testing.T, path, value string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(value), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func readPackageEntries(t *testing.T, data []byte) ([]string, []string) {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("open gzip: %v", err)
	}
	defer gz.Close() //nolint:errcheck
	tr := tar.NewReader(gz)

	var outer []string
	var codePackage []byte
	for {
		hdr, err := tr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("read outer tar: %v", err)
		}
		outer = append(outer, hdr.Name)
		if hdr.Name == "code.tar.gz" {
			codePackage, err = io.ReadAll(tr)
			if err != nil {
				t.Fatalf("read code package: %v", err)
			}
		}
	}
	if len(codePackage) == 0 {
		t.Fatal("code.tar.gz not found")
	}

	gz, err = gzip.NewReader(bytes.NewReader(codePackage))
	if err != nil {
		t.Fatalf("open inner gzip: %v", err)
	}
	defer gz.Close() //nolint:errcheck
	tr = tar.NewReader(gz)
	var inner []string
	for {
		hdr, err := tr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("read inner tar: %v", err)
		}
		inner = append(inner, hdr.Name)
	}
	return outer, inner
}
