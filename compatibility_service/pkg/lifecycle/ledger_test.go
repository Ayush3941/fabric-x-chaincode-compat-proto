// SPDX-License-Identifier: Apache-2.0

package lifecycle

import "testing"

func TestLifecycleIndexEntryRoundTrip(t *testing.T) {
	def := ChaincodeDefinition{Name: "sample", Version: "1.0", Sequence: 2, InitRequired: true}
	entry := NewLifecycleIndexEntry("channelqc4", 7, LifecycleEventInitialized, def, true)
	data, err := MarshalLifecycleIndexEntry(entry)
	if err != nil {
		t.Fatalf("MarshalLifecycleIndexEntry failed: %v", err)
	}
	got, err := UnmarshalLifecycleIndexEntry(data)
	if err != nil {
		t.Fatalf("UnmarshalLifecycleIndexEntry failed: %v", err)
	}
	if got.Number != 7 || got.EventType != LifecycleEventInitialized || got.DefinitionKey != LedgerKey("channelqc4", def.Name) {
		t.Fatalf("unexpected entry: %#v", got)
	}
	if got.ChannelID != "channelqc4" || !got.Initialized || !got.InitRequired {
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

func TestLifecycleLedgerIdentityIsChannelAndName(t *testing.T) {
	first := LedgerKey("channel-a", "sample")
	if first == LedgerKey("channel-b", "sample") || first == LedgerKey("channel-a", "other") {
		t.Fatal("channel or name is missing from ledger identity")
	}
	if LedgerKey("channel/a", "b") == LedgerKey("channel", "a/b") {
		t.Fatal("ambiguous lifecycle key encoding")
	}
	if LifecycleHighWatermarkKey("channel-a") == LifecycleHighWatermarkKey("channel-b") || LifecycleIndexEntryKey("channel-a", 1) == LifecycleIndexEntryKey("channel-b", 1) {
		t.Fatal("index is shared across channels")
	}
	for _, version := range []string{"1.0", "2.0", "2.1"} {
		def := ChaincodeDefinition{Name: "sample", Version: version, Sequence: 1}
		data, err := MarshalLedgerDefinition("channel-a", def, true)
		if err != nil {
			t.Fatal(err)
		}
		got, err := UnmarshalLedgerDefinition(data)
		if err != nil || got.ChannelID != "channel-a" || ChaincodeDefinitionFromLedger(got) != def {
			t.Fatalf("definition round trip failed: %+v %v", got, err)
		}
	}
	if _, err := UnmarshalLedgerDefinition([]byte(`{"ccid":"sample:1.0","name":"sample","version":"1.0","sequence":1}`)); err == nil {
		t.Fatal("legacy record accepted without channel identity")
	}
}
