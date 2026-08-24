// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"strings"
	"testing"
	"time"

	cfgpkg "compatibility_service/pkg/config"
	"github.com/hyperledger/fabric-x-committer/utils/connection"
	"github.com/hyperledger/fabric-x-committer/utils/serve"
)

func TestRequestTimeoutDefault(t *testing.T) {
	var cfg Config
	if got := cfg.requestTimeout(); got != 60*time.Second {
		t.Fatalf("expected default request timeout 60s, got %s", got)
	}
}

func TestRequestTimeoutConfigured(t *testing.T) {
	cfg := Config{RequestTimeout: 12 * time.Second}
	if got := cfg.requestTimeout(); got != 12*time.Second {
		t.Fatalf("expected configured request timeout 12s, got %s", got)
	}
}

func TestValidateRejectsInvalidTimeouts(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "negative request timeout",
			cfg:  validConfigWithTimeouts(-time.Second, 20*time.Second),
			want: "request-timeout must not be negative",
		},
		{
			name: "negative finality timeout",
			cfg:  validConfigWithTimeouts(20*time.Second, -time.Second),
			want: "finality-timeout must not be negative",
		},
		{
			name: "request timeout shorter than finality timeout",
			cfg:  validConfigWithTimeouts(5*time.Second, 20*time.Second),
			want: "request-timeout must be greater than or equal to finality-timeout",
		},
		{
			name: "configured request timeout shorter than default finality timeout",
			cfg:  validConfigWithTimeouts(20*time.Second, 0),
			want: "request-timeout must be greater than or equal to finality-timeout",
		},
		{
			name: "default request timeout shorter than configured finality timeout",
			cfg:  validConfigWithTimeouts(0, 90*time.Second),
			want: "request-timeout must be greater than or equal to finality-timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("expected error containing %q, got %v", tt.want, err)
			}
		})
	}
}

func validConfigWithTimeouts(requestTimeout, finalityTimeout time.Duration) Config {
	endpoint := &cfgpkg.Endpoint{Host: "127.0.0.1", Port: 1}
	return Config{
		ChannelID:       "channelqc4",
		Namespace:       "0",
		Server:          &serve.ServerConfig{Endpoint: connection.Endpoint{Host: "127.0.0.1", Port: 1}},
		Identity:        &cfgpkg.IdentityConfig{MSPDir: "msp", MspID: "org-0"},
		QueryService:    cfgpkg.ClientConfig{Endpoint: endpoint},
		ChaincodeSvc:    cfgpkg.ChaincodeServiceConfig{Endpoint: endpoint},
		Orderer:         &cfgpkg.ClientConfig{Endpoint: endpoint},
		NotificationSvc: &cfgpkg.ClientConfig{Endpoint: endpoint},
		RequestTimeout:  requestTimeout,
		FinalityTimeout: finalityTimeout,
	}
}
