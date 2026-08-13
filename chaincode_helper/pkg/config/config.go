/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/hyperledger/fabric-lib-go/common/flogging"
	"github.com/hyperledger/fabric-x-committer/utils/serve"
	"github.com/hyperledger/fabric-x-sdk/network"
)

// Config holds all configuration for the helper.
type Config struct {
	// ChannelID is the channel to connect to.
	ChannelID string `mapstructure:"channel-id"`

	// Namespace is the Fabric-X namespace this helper serves.
	Namespace string `mapstructure:"namespace"`

	// Server is the main gRPC server configuration with TLS, rate limiting, etc.
	Server *serve.ServerConfig `mapstructure:"server"`

	// QueryService is the Fabric-X Query Service used for all committed state reads.
	QueryService ClientConfig `mapstructure:"query-service"`

	// ChaincodeService is the external Fabric chaincode-as-a-service endpoint
	// used by the shim bridge.
	ChaincodeService ChaincodeServiceConfig `mapstructure:"chaincode-service"`

	// Identity is optional Fabric MSP identity configuration
	Identity *IdentityConfig `mapstructure:"identity,omitempty"`

	// Logging configuration
	Logging flogging.Config `mapstructure:"logging"`

	// Protocol selects the network protocol. This scaffold supports only "fabric-x".
	Protocol string `mapstructure:"protocol"`
}

// IdentityConfig defines the component's MSP.
type IdentityConfig struct {
	// MspID indicates to which MSP this client belongs to.
	MspID  string `mapstructure:"msp-id" yaml:"msp-id"`
	MSPDir string `mapstructure:"msp-dir" yaml:"msp-dir"`
}

// ClientConfig contains a single endpoint, TLS config, and retry profile.
type ClientConfig struct {
	Endpoint *Endpoint `mapstructure:"endpoint"  yaml:"endpoint"`
	TLS      TLSConfig `mapstructure:"tls"       yaml:"tls"`
}

// ChaincodeServiceConfig identifies the external CCAAS process.
type ChaincodeServiceConfig struct {
	Endpoint *Endpoint `mapstructure:"endpoint"  yaml:"endpoint"`
	TLS      TLSConfig `mapstructure:"tls"       yaml:"tls"`
}

// Address returns the configured chaincode-as-a-service endpoint.
func (c ChaincodeServiceConfig) Address() string {
	if c.Endpoint == nil {
		return ""
	}
	return c.Endpoint.Address()
}

// ToPeerConf converts a ClientConfig to the SDK's PeerConf.
func (c ClientConfig) ToPeerConf() network.PeerConf {
	return network.PeerConf{
		Address: c.Endpoint.Address(),
		TLS: network.TLSConfig{
			Mode:        c.TLS.Mode,
			CertPath:    c.TLS.CertPath,
			KeyPath:     c.TLS.KeyPath,
			CACertPaths: c.TLS.CACertPaths,
			ServerName:  c.TLS.ServerName,
		},
	}
}

// ToOrdererConf converts a ClientConfig to the SDK's OrdererConf.
func (c ClientConfig) ToOrdererConf() network.OrdererConf {
	return network.OrdererConf{
		Address: c.Endpoint.Address(),
		TLS: network.TLSConfig{
			Mode:        c.TLS.Mode,
			CertPath:    c.TLS.CertPath,
			KeyPath:     c.TLS.KeyPath,
			CACertPaths: c.TLS.CACertPaths,
			ServerName:  c.TLS.ServerName,
		},
	}
}

// TLSConfig holds the TLS options and certificate paths
// used for secure communication between servers and clients.
// Credentials are built based on the configuration mode.
// For example, If only server-side TLS is required, the certificate pool (certPool) is not built (for a server),
// since the relevant certificates paths are defined in the YAML according to the selected mode.
type TLSConfig struct {
	Mode string `mapstructure:"mode"`
	// CertPath is the path to the certificate file (public key).
	CertPath string `mapstructure:"cert-path"`
	// KeyPath is the path to the key file (private key).
	KeyPath     string   `mapstructure:"key-path"`
	CACertPaths []string `mapstructure:"ca-cert-paths"`
	ServerName  string   `mapstructure:"server-name"`
}

// Endpoint describes a remote endpoint.
type Endpoint struct {
	Host string `mapstructure:"host" json:"host,omitempty" yaml:"host,omitempty"`
	Port int    `mapstructure:"port" json:"port,omitempty" yaml:"port,omitempty"`
}

// Address returns a string representation of the endpoint's address.
func (e *Endpoint) Address() string {
	// JoinHostPort defaults to ipv6 for localhost,
	// which is not always wanted.
	if e.Host == "localhost" {
		return fmt.Sprintf("%s:%d", e.Host, e.Port)
	}
	return net.JoinHostPort(e.Host, strconv.Itoa(e.Port))
}

func (cfg Config) Validate() error {
	var errs []error

	if cfg.ChannelID == "" {
		errs = append(errs, errors.New("channel-id is required"))
	}
	if cfg.Namespace == "" {
		errs = append(errs, errors.New("namespace is required"))
	}
	if p := cfg.Protocol; p != "" && p != "fabric-x" {
		errs = append(errs, errors.New("protocol must be fabric-x"))
	}
	if cfg.Identity == nil {
		errs = append(errs, errors.New("identity is required"))
	}
	if cfg.QueryService.Endpoint == nil {
		errs = append(errs, errors.New("query-service.endpoint is required"))
	}
	if cfg.ChaincodeService.Endpoint == nil {
		errs = append(errs, errors.New("chaincode-service.endpoint is required"))
	}
	if cfg.Server == nil {
		errs = append(errs, errors.New("server configuration is required"))
	}

	return errors.Join(errs...)
}
