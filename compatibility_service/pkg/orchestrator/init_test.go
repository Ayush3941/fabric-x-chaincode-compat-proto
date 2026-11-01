// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"testing"

	"compatibility_service/pkg/lifecycle"
)

func TestInitStateRequiresCommittedUninitializedDefinition(t *testing.T) {
	for _, scenario := range []string{"uninitialized", "initialized", "not-required", "missing"} {
		t.Run(scenario, func(t *testing.T) {
			store, err := lifecycle.NewMemoryStore("channelqc4")
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close() //nolint:errcheck
			def := lifecycle.ChaincodeDefinition{Name: "sample", Version: "1.0", Sequence: 1, InitRequired: scenario != "not-required"}
			if scenario != "missing" {
				if _, err := store.UpsertCommittedFromLedger(t.Context(), def, scenario == "initialized" || scenario == "not-required"); err != nil {
					t.Fatal(err)
				}
			}
			service := &Service{lifecycle: store}
			request := InvocationRequest{ChaincodeName: def.Name, ChaincodeVersion: def.Version, IsInit: true}
			for _, operation := range []string{GRPCOperationInvoke, GRPCOperationEndorse} {
				got, err := service.validateInitState(t.Context(), request, "1", operation)
				if scenario == "uninitialized" {
					if err != nil || got == nil || *got != def {
						t.Fatalf("valid initialization rejected: definition=%v error=%v", got, err)
					}
				} else if err == nil {
					t.Fatalf("initialization accepted for %s definition", scenario)
				}
			}
			request.IsInit = false
			for _, operation := range []string{GRPCOperationInvoke, GRPCOperationQuery, GRPCOperationEndorse} {
				_, err := service.validateInitState(t.Context(), request, "1", operation)
				if (err != nil) != (scenario == "uninitialized") {
					t.Fatalf("normal %s init gate for %s: %v", operation, scenario, err)
				}
			}
		})
	}
}
