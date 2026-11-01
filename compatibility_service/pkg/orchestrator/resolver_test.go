// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"testing"

	"compatibility_service/pkg/lifecycle"
)

func TestStaticChaincodeResolverMatchesDefinition(t *testing.T) {
	resolver, err := newStaticChaincodeResolver([]StaticChaincodeConnectionConfig{{
		MSPID:    "org-0",
		Name:     "sample",
		Version:  "1.0",
		Sequence: 1,
		Address:  "127.0.0.1:9999",
	}}, nil)
	if err != nil {
		t.Fatalf("newStaticChaincodeResolver failed: %v", err)
	}

	conn, ok, err := resolver.Resolve(context.Background(), chaincodeResolverRequest{
		MSPID: "org-0",
		Definition: lifecycle.ChaincodeDefinition{
			Name:     "sample",
			Version:  "1.0",
			Sequence: 1,
		},
	})
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if !ok {
		t.Fatal("expected static resolver match")
	}
	if conn.Address != "127.0.0.1:9999" || conn.TLSMode != "none" {
		t.Fatalf("connection = %#v", conn)
	}
}

func TestStaticChaincodeResolverSkipsWrongMSP(t *testing.T) {
	resolver, err := newStaticChaincodeResolver([]StaticChaincodeConnectionConfig{{
		MSPID:   "org-1",
		Name:    "sample",
		Version: "1.0",
		Address: "127.0.0.1:10000",
	}}, nil)
	if err != nil {
		t.Fatalf("newStaticChaincodeResolver failed: %v", err)
	}

	_, ok, err := resolver.Resolve(context.Background(), chaincodeResolverRequest{
		MSPID: "org-0",
		Definition: lifecycle.ChaincodeDefinition{
			Name:     "sample",
			Version:  "1.0",
			Sequence: 1,
		},
	})
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if ok {
		t.Fatal("unexpected static resolver match for wrong MSP")
	}
}
