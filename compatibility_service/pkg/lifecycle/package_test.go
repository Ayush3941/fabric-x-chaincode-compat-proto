// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestPackageExternalChaincodeMutatesMetadataAndBuildsFabricShape(t *testing.T) {
	sourceDir := t.TempDir()
	writeTestFile(t, filepath.Join(sourceDir, "connection.json"), `{"address":"127.0.0.1:9999","tls_required":false}`)
	writeTestFile(t, filepath.Join(sourceDir, "META-INF", "statedb", "couchdb", "indexes", "indexOwner.json"), `{"index":{"fields":["owner"]}}`)

	output := filepath.Join(t.TempDir(), "fabcar.tgz")
	info, err := PackageExternalChaincode(PackageOptions{
		SourceDir:  sourceDir,
		OutputPath: output,
		Label:      "fabcar_1",
	})
	if err != nil {
		t.Fatalf("PackageExternalChaincode failed: %v", err)
	}
	if info.Label != "fabcar_1" {
		t.Fatalf("label = %q, want fabcar_1", info.Label)
	}
	if info.PackageID == "" {
		t.Fatal("package id is empty")
	}

	var metadata map[string]any
	data, err := os.ReadFile(filepath.Join(sourceDir, "metadata.json"))
	if err != nil {
		t.Fatalf("read mutated metadata: %v", err)
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		t.Fatalf("decode mutated metadata: %v", err)
	}
	if metadata["type"] != externalChaincodeType || metadata["label"] != "fabcar_1" || metadata["path"] != "" {
		t.Fatalf("unexpected metadata: %#v", metadata)
	}

	outerEntries, innerEntries := readPackageEntries(t, output)
	if !slices.Equal(outerEntries, []string{"metadata.json", "code.tar.gz"}) {
		t.Fatalf("outer entries = %#v", outerEntries)
	}
	if !slices.Contains(innerEntries, "connection.json") {
		t.Fatalf("inner entries missing connection.json: %#v", innerEntries)
	}
	if !slices.Contains(innerEntries, "META-INF/statedb/couchdb/indexes/indexOwner.json") {
		t.Fatalf("inner entries missing META-INF: %#v", innerEntries)
	}
}

func TestPackageExternalChaincodeUsesExistingMetadataLabel(t *testing.T) {
	sourceDir := t.TempDir()
	writeTestFile(t, filepath.Join(sourceDir, "connection.json"), `{"address":"127.0.0.1:9999","tls_required":false}`)
	writeTestFile(t, filepath.Join(sourceDir, "metadata.json"), `{"path":"","type":"external","label":"sample_1"}`)

	info, err := PackageExternalChaincode(PackageOptions{
		SourceDir:  sourceDir,
		OutputPath: filepath.Join(t.TempDir(), "sample.tgz"),
	})
	if err != nil {
		t.Fatalf("PackageExternalChaincode failed: %v", err)
	}
	if info.Label != "sample_1" {
		t.Fatalf("label = %q, want sample_1", info.Label)
	}
}

func TestPackageExternalChaincodeRequiresLabel(t *testing.T) {
	sourceDir := t.TempDir()
	writeTestFile(t, filepath.Join(sourceDir, "connection.json"), `{"address":"127.0.0.1:9999","tls_required":false}`)

	_, err := PackageExternalChaincode(PackageOptions{
		SourceDir:  sourceDir,
		OutputPath: filepath.Join(t.TempDir(), "sample.tgz"),
	})
	if err == nil {
		t.Fatal("expected missing label error")
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

func readPackageEntries(t *testing.T, path string) ([]string, []string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open package: %v", err)
	}
	defer file.Close() //nolint:errcheck
	gz, err := gzip.NewReader(file)
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
