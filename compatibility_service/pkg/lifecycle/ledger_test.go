// SPDX-License-Identifier: Apache-2.0

package lifecycle

import "testing"

func TestLifecycleIndexEntryRoundTrip(t *testing.T) {
	def := ChaincodeDefinition{Name: "sample", Version: "1.0", Sequence: 2, InitRequired: true}
	entry := NewLifecycleIndexEntry(7, LifecycleEventInitialized, def, true)
	data, err := MarshalLifecycleIndexEntry(entry)
	if err != nil {
		t.Fatalf("MarshalLifecycleIndexEntry failed: %v", err)
	}
	got, err := UnmarshalLifecycleIndexEntry(data)
	if err != nil {
		t.Fatalf("UnmarshalLifecycleIndexEntry failed: %v", err)
	}
	if got.Number != 7 || got.EventType != LifecycleEventInitialized || got.DefinitionKey != LedgerKey(def) {
		t.Fatalf("unexpected entry: %#v", got)
	}
	if got.CCID != "sample:1.0" || !got.Initialized || !got.InitRequired {
		t.Fatalf("unexpected entry fields: %#v", got)
	}
}

func TestLifecycleHighWatermarkRoundTrip(t *testing.T) {
	data := MarshalLifecycleHighWatermark(42)
	got, err := UnmarshalLifecycleHighWatermark(data)
	if err != nil {
		t.Fatalf("UnmarshalLifecycleHighWatermark failed: %v", err)
	}
	if got != 42 {
		t.Fatalf("high watermark = %d, want 42", got)
	}
}
