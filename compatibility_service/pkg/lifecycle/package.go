// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const externalChaincodeType = "external"

// PackageOptions controls external chaincode package creation.
type PackageOptions struct {
	SourceDir  string
	OutputPath string
	Label      string
}

// PackageInfo describes the generated lifecycle package.
type PackageInfo struct {
	Label     string
	Type      string
	Output    string
	PackageID string
}

// PackageDescriptor is the normalized content of a lifecycle package.
type PackageDescriptor struct {
	Label          string
	Type           string
	PackageID      string
	Address        string
	MetadataJSON   []byte
	ConnectionJSON []byte
	PackageTGZ     []byte
}

// PackageExternalChaincode creates a Fabric lifecycle package for CCAAS-style
// external chaincode. The source directory contains connection.json and
// metadata.json. The output package contains metadata.json and code.tar.gz.
func PackageExternalChaincode(opts PackageOptions) (PackageInfo, error) {
	sourceDir, err := cleanDir(opts.SourceDir)
	if err != nil {
		return PackageInfo{}, err
	}
	outputPath, err := cleanOutput(opts.OutputPath)
	if err != nil {
		return PackageInfo{}, err
	}

	connPath := filepath.Join(sourceDir, "connection.json")
	if err := requireRegularFile(connPath); err != nil {
		return PackageInfo{}, err
	}

	metadata, err := readAndNormalizeMetadata(sourceDir, opts.Label)
	if err != nil {
		return PackageInfo{}, err
	}
	metadataBytes, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return PackageInfo{}, fmt.Errorf("marshal metadata: %w", err)
	}
	metadataBytes = append(metadataBytes, '\n')
	if err := os.WriteFile(filepath.Join(sourceDir, "metadata.json"), metadataBytes, 0o644); err != nil {
		return PackageInfo{}, fmt.Errorf("write metadata.json: %w", err)
	}

	codePackage, err := buildCodePackage(sourceDir)
	if err != nil {
		return PackageInfo{}, err
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return PackageInfo{}, fmt.Errorf("create output directory: %w", err)
	}
	if err := writeOuterPackage(outputPath, metadataBytes, codePackage); err != nil {
		return PackageInfo{}, err
	}

	packageBytes, err := os.ReadFile(outputPath)
	if err != nil {
		return PackageInfo{}, fmt.Errorf("read generated package: %w", err)
	}
	desc, err := InspectPackageBytes(packageBytes)
	if err != nil {
		return PackageInfo{}, fmt.Errorf("inspect generated package: %w", err)
	}
	label := metadataString(metadata, "label")
	return PackageInfo{
		Label:     label,
		Type:      externalChaincodeType,
		Output:    outputPath,
		PackageID: desc.PackageID,
	}, nil
}

// InspectPackageBytes validates a lifecycle package and extracts the metadata
// needed by install/queryinstalled.
func InspectPackageBytes(packageBytes []byte) (PackageDescriptor, error) {
	if len(packageBytes) == 0 {
		return PackageDescriptor{}, errors.New("package is empty")
	}

	outer, err := readTarGz(packageBytes)
	if err != nil {
		return PackageDescriptor{}, fmt.Errorf("read package: %w", err)
	}
	metadataBytes, ok := outer["metadata.json"]
	if !ok {
		return PackageDescriptor{}, errors.New("package missing metadata.json")
	}
	codePackage, ok := outer["code.tar.gz"]
	if !ok {
		return PackageDescriptor{}, errors.New("package missing code.tar.gz")
	}

	var metadata map[string]any
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return PackageDescriptor{}, fmt.Errorf("decode metadata.json: %w", err)
	}
	label := metadataString(metadata, "label")
	if label == "" {
		return PackageDescriptor{}, errors.New("metadata.json label is required")
	}
	chaincodeType := metadataString(metadata, "type")
	if chaincodeType == "" {
		return PackageDescriptor{}, errors.New("metadata.json type is required")
	}
	if chaincodeType != externalChaincodeType {
		return PackageDescriptor{}, fmt.Errorf("metadata.json type must be %q", externalChaincodeType)
	}

	inner, err := readTarGz(codePackage)
	if err != nil {
		return PackageDescriptor{}, fmt.Errorf("read code.tar.gz: %w", err)
	}
	connectionBytes, ok := inner["connection.json"]
	if !ok {
		return PackageDescriptor{}, errors.New("code.tar.gz missing connection.json")
	}

	address, err := connectionAddress(connectionBytes)
	if err != nil {
		return PackageDescriptor{}, err
	}
	sum := sha256.Sum256(packageBytes)
	return PackageDescriptor{
		Label:          label,
		Type:           chaincodeType,
		PackageID:      label + ":" + hex.EncodeToString(sum[:]),
		Address:        address,
		MetadataJSON:   append([]byte(nil), metadataBytes...),
		ConnectionJSON: append([]byte(nil), connectionBytes...),
		PackageTGZ:     append([]byte(nil), packageBytes...),
	}, nil
}

func readTarGz(data []byte) (map[string][]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("open gzip: %w", err)
	}
	defer gz.Close() //nolint:errcheck

	entries := map[string][]byte{}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("read tar: %w", err)
		}
		name, err := tarName(hdr.Name)
		if err != nil {
			return nil, err
		}
		if hdr.Typeflag != tar.TypeReg && hdr.Typeflag != tar.TypeRegA {
			continue
		}
		value, err := io.ReadAll(tr)
		if err != nil {
			return nil, fmt.Errorf("read tar entry %s: %w", name, err)
		}
		entries[name] = value
	}
	return entries, nil
}

func connectionAddress(connectionBytes []byte) (string, error) {
	var connection map[string]any
	if err := json.Unmarshal(connectionBytes, &connection); err != nil {
		return "", fmt.Errorf("decode connection.json: %w", err)
	}
	address, _ := connection["address"].(string)
	if address == "" {
		return "", errors.New("connection.json address is required")
	}
	return address, nil
}

func cleanDir(path string) (string, error) {
	if path == "" {
		return "", errors.New("path is required")
	}
	clean := filepath.Clean(path)
	info, err := os.Stat(clean)
	if err != nil {
		return "", fmt.Errorf("stat path: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("path %q is not a directory", clean)
	}
	return clean, nil
}

func cleanOutput(path string) (string, error) {
	if path == "" {
		return "", errors.New("output is required")
	}
	return filepath.Clean(path), nil
}

func requireRegularFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("required file %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("required file %s is not a regular file", path)
	}
	return nil
}

func readAndNormalizeMetadata(sourceDir, labelOverride string) (map[string]any, error) {
	metadataPath := filepath.Join(sourceDir, "metadata.json")
	metadata := map[string]any{}
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("read metadata.json: %w", err)
		}
	} else if len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, &metadata); err != nil {
			return nil, fmt.Errorf("decode metadata.json: %w", err)
		}
	}

	if labelOverride != "" {
		metadata["label"] = labelOverride
	}
	if _, ok := metadata["path"]; !ok {
		metadata["path"] = ""
	}
	if metadataString(metadata, "type") == "" {
		metadata["type"] = externalChaincodeType
	}
	if metadataString(metadata, "type") != externalChaincodeType {
		return nil, fmt.Errorf("metadata.json type must be %q", externalChaincodeType)
	}
	if metadataString(metadata, "label") == "" {
		return nil, errors.New("metadata.json label is required; pass --label or set metadata.json label")
	}
	return metadata, nil
}

func metadataString(metadata map[string]any, key string) string {
	value, _ := metadata[key].(string)
	return value
}

func buildCodePackage(sourceDir string) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)

	if err := addFile(tw, filepath.Join(sourceDir, "connection.json"), "connection.json"); err != nil {
		return nil, err
	}
	metaInf := filepath.Join(sourceDir, "META-INF")
	if info, err := os.Stat(metaInf); err == nil && info.IsDir() {
		if err := addTree(tw, sourceDir, metaInf); err != nil {
			return nil, err
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("stat META-INF: %w", err)
	}

	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("close code tar: %w", err)
	}
	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("close code gzip: %w", err)
	}
	return buf.Bytes(), nil
}

func addTree(tw *tar.Writer, root, dir string) error {
	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == dir {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		name, err := tarName(rel)
		if err != nil {
			return err
		}
		if d.IsDir() {
			return addDir(tw, path, name)
		}
		if d.Type().IsRegular() {
			return addFile(tw, path, name)
		}
		return fmt.Errorf("unsupported package entry %s", path)
	})
}

func writeOuterPackage(outputPath string, metadataBytes, codePackage []byte) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create package: %w", err)
	}
	defer file.Close() //nolint:errcheck

	gz := gzip.NewWriter(file)
	tw := tar.NewWriter(gz)
	if err := writeBytes(tw, "metadata.json", metadataBytes, 0o644); err != nil {
		return err
	}
	if err := writeBytes(tw, "code.tar.gz", codePackage, 0o644); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("close package tar: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("close package gzip: %w", err)
	}
	return nil
}

func addDir(tw *tar.Writer, path, name string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	hdr := &tar.Header{
		Name:     name + "/",
		Mode:     int64(info.Mode().Perm()),
		ModTime:  time.Unix(0, 0),
		Typeflag: tar.TypeDir,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("write tar header %s: %w", name, err)
	}
	return nil
}

func addFile(tw *tar.Writer, path, name string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	return writeBytes(tw, name, data, 0o644)
}

func writeBytes(tw *tar.Writer, name string, data []byte, mode int64) error {
	name, err := tarName(name)
	if err != nil {
		return err
	}
	hdr := &tar.Header{
		Name:    name,
		Mode:    mode,
		Size:    int64(len(data)),
		ModTime: time.Unix(0, 0),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("write tar header %s: %w", name, err)
	}
	if _, err := io.Copy(tw, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("write tar body %s: %w", name, err)
	}
	return nil
}

func tarName(path string) (string, error) {
	name := filepath.ToSlash(filepath.Clean(path))
	if name == "." || name == "" || strings.HasPrefix(name, "../") || strings.HasPrefix(name, "/") || strings.Contains(name, "/../") {
		return "", fmt.Errorf("invalid package path %q", path)
	}
	return name, nil
}
