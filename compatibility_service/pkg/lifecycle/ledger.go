// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
)

const (
	// DefaultLedgerNamespace is the current prototype namespace used for shared
	// lifecycle records.
	DefaultLedgerNamespace = "0"

	lifecycleLedgerSchema     = 2
	LifecycleEventCommit      = "commit"
	LifecycleEventInitialized = "initialized"
)

// LedgerCommitResult describes the committed Fabric-X lifecycle transaction.
type LedgerCommitResult struct {
	TxID      string `json:"tx_id,omitempty"`
	Status    string `json:"status,omitempty"`
	BlockNum  uint64 `json:"block_num,omitempty"`
	TxNum     uint32 `json:"tx_num,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Key       string `json:"key,omitempty"`
}

// LedgerCommitter submits the shared lifecycle definition to Fabric-X.
type LedgerCommitter interface {
	CommitLifecycleDefinition(context.Context, ChaincodeDefinition) (LedgerCommitResult, error)
}

// LedgerBackend also verifies shared state before an RPC updates a local mirror.
type LedgerBackend interface {
	LedgerCommitter
	ReadLifecycleDefinition(context.Context, ChaincodeDefinition) (LedgerDefinition, bool, error)
}

// LedgerFinalizer allows a backend with pending recovery to own the small
// amount of bookkeeping left after a lifecycle transaction commits.
type LedgerFinalizer interface {
	FinalizeLifecycleDefinition(context.Context, ChaincodeDefinition, bool) (CommittedDefinition, error)
}

// LedgerDefinition is the deterministic shared value stored in Fabric-X state.
type LedgerDefinition struct {
	ChannelID    string `json:"channel_id"`
	Name         string `json:"name"`
	Version      string `json:"version"`
	Sequence     int64  `json:"sequence"`
	InitRequired bool   `json:"init_required"`
	Initialized  bool   `json:"initialized"`
}

// LifecycleIndexEntry is the append-only pointer written for every lifecycle
// ledger mutation. Startup sync point-reads these entries instead of scanning
// blocks or relying on range queries.
type LifecycleIndexEntry struct {
	SchemaVersion int    `json:"schema_version"`
	Number        uint64 `json:"number"`
	EventType     string `json:"event_type"`
	DefinitionKey string `json:"definition_key"`
	ChannelID     string `json:"channel_id"`
	Name          string `json:"name"`
	Version       string `json:"version"`
	Sequence      int64  `json:"sequence"`
	InitRequired  bool   `json:"init_required"`
	Initialized   bool   `json:"initialized"`
}

func channelKeyPrefix(channelID string) string {
	return "_lifecycle/channels/" + url.PathEscape(channelID) + "/"
}

// LedgerKey is stable across every revision of this chaincode on the channel.
func LedgerKey(channelID, name string) string {
	return channelKeyPrefix(channelID) + "chaincodes/" + url.PathEscape(name)
}

func LifecycleHighWatermarkKey(channelID string) string {
	return channelKeyPrefix(channelID) + "index/high_watermark"
}

func LifecycleIndexEntryKey(channelID string, number uint64) string {
	return fmt.Sprintf("%sindex/%020d", channelKeyPrefix(channelID), number)
}

func MarshalLedgerDefinition(channelID string, def ChaincodeDefinition, initialized bool) ([]byte, error) {
	if channelID == "" {
		return nil, fmt.Errorf("channel ID is required")
	}
	if err := validateDefinition(def); err != nil {
		return nil, err
	}
	return json.Marshal(LedgerDefinition{
		ChannelID:    channelID,
		Name:         def.Name,
		Version:      def.Version,
		Sequence:     def.Sequence,
		InitRequired: def.InitRequired,
		Initialized:  initialized,
	})
}

func UnmarshalLedgerDefinition(data []byte) (LedgerDefinition, error) {
	var value LedgerDefinition
	if err := json.Unmarshal(data, &value); err != nil {
		return LedgerDefinition{}, err
	}
	if value.ChannelID == "" {
		return LedgerDefinition{}, fmt.Errorf("lifecycle record has no channel ID; legacy name:version records require redeployment")
	}
	if err := validateDefinition(ChaincodeDefinitionFromLedger(value)); err != nil {
		return LedgerDefinition{}, err
	}
	return value, nil
}

func ChaincodeDefinitionFromLedger(value LedgerDefinition) ChaincodeDefinition {
	return ChaincodeDefinition{
		Name:         value.Name,
		Version:      value.Version,
		Sequence:     value.Sequence,
		InitRequired: value.InitRequired,
	}
}

func NewLifecycleIndexEntry(channelID string, number uint64, eventType string, def ChaincodeDefinition, initialized bool) LifecycleIndexEntry {
	return LifecycleIndexEntry{
		SchemaVersion: lifecycleLedgerSchema,
		Number:        number,
		EventType:     eventType,
		DefinitionKey: LedgerKey(channelID, def.Name),
		ChannelID:     channelID,
		Name:          def.Name,
		Version:       def.Version,
		Sequence:      def.Sequence,
		InitRequired:  def.InitRequired,
		Initialized:   initialized,
	}
}

func MarshalLifecycleIndexEntry(entry LifecycleIndexEntry) ([]byte, error) {
	return json.Marshal(entry)
}

func UnmarshalLifecycleIndexEntry(data []byte) (LifecycleIndexEntry, error) {
	var entry LifecycleIndexEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return LifecycleIndexEntry{}, err
	}
	if entry.SchemaVersion != lifecycleLedgerSchema || entry.ChannelID == "" || entry.Name == "" ||
		entry.DefinitionKey != LedgerKey(entry.ChannelID, entry.Name) {
		return LifecycleIndexEntry{}, fmt.Errorf("invalid channel-scoped lifecycle index entry")
	}
	return entry, nil
}

func MarshalLifecycleHighWatermark(value uint64) []byte {
	return []byte(strconv.FormatUint(value, 10))
}

func UnmarshalLifecycleHighWatermark(data []byte) (uint64, error) {
	if len(data) == 0 {
		return 0, nil
	}
	return strconv.ParseUint(string(data), 10, 64)
}
