// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"compatibility_service/pkg/ccresolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type server struct {
	name        string
	version     string
	sequence    int64
	org0Address string
	org1Address string
}

func main() {
	var (
		listen      = flag.String("listen", "127.0.0.1:9300", "gRPC listen address")
		name        = flag.String("name", "sample", "chaincode name to resolve")
		version     = flag.String("version", "1.0", "chaincode version to resolve")
		sequence    = flag.Int64("sequence", 0, "chaincode sequence to resolve; 0 accepts any sequence")
		org0Address = flag.String("org0-address", "127.0.0.1:9999", "org-0 CCAAS address")
		org1Address = flag.String("org1-address", "127.0.0.1:10000", "org-1 CCAAS address")
		tlsMode     = flag.String("tls-mode", "mtls", "TLS mode: tls or mtls")
		tlsCert     = flag.String("tls-cert", "", "resolver server TLS certificate")
		tlsKey      = flag.String("tls-key", "", "resolver server TLS private key")
		clientCAs   = flag.String("client-ca", "", "comma-separated client CA certificates for mtls")
	)
	flag.Parse()

	tlsCfg, err := loadServerTLSConfig(*tlsMode, *tlsCert, *tlsKey, splitCSV(*clientCAs))
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolver TLS error: %v\n", err)
		os.Exit(1)
	}

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolver listen error: %v\n", err)
		os.Exit(1)
	}
	opts := []grpc.ServerOption{grpc.Creds(credentials.NewTLS(tlsCfg))}
	grpcServer := grpc.NewServer(opts...)
	ccresolver.RegisterResolverServer(grpcServer, server{
		name:        *name,
		version:     *version,
		sequence:    *sequence,
		org0Address: *org0Address,
		org1Address: *org1Address,
	})

	fmt.Fprintf(os.Stderr, "resolver gRPC listening on %s tls=%s\n", *listen, *tlsMode)
	if err := grpcServer.Serve(listener); err != nil {
		fmt.Fprintf(os.Stderr, "resolver serve error: %v\n", err)
		os.Exit(1)
	}
}

func (s server) Resolve(_ context.Context, req *ccresolver.ResolveRequest) (*ccresolver.ResolveResponse, error) {
	if req == nil || req.Name != s.name || req.Version != s.version {
		return &ccresolver.ResolveResponse{}, nil
	}
	if s.sequence > 0 && req.Sequence != s.sequence {
		return &ccresolver.ResolveResponse{}, nil
	}
	address := ""
	switch req.MSPID {
	case "org-0":
		address = s.org0Address
	case "org-1":
		address = s.org1Address
	default:
		return &ccresolver.ResolveResponse{}, nil
	}
	return &ccresolver.ResolveResponse{
		Found:   true,
		Address: address,
		TLSMode: "none",
	}, nil
}

func loadServerTLSConfig(mode, certPath, keyPath string, caPaths []string) (*tls.Config, error) {
	if mode != "tls" && mode != "mtls" {
		return nil, fmt.Errorf("tls-mode must be tls or mtls")
	}
	if certPath == "" {
		return nil, fmt.Errorf("tls-cert is required")
	}
	if keyPath == "" {
		return nil, fmt.Errorf("tls-key is required")
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
	}
	if mode == "mtls" {
		if len(caPaths) == 0 {
			return nil, fmt.Errorf("client-ca is required for mtls")
		}
		pool := x509.NewCertPool()
		for _, path := range caPaths {
			pemBytes, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("read client CA %s: %w", path, err)
			}
			if ok := pool.AppendCertsFromPEM(pemBytes); !ok {
				return nil, fmt.Errorf("parse client CA %s", path)
			}
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
