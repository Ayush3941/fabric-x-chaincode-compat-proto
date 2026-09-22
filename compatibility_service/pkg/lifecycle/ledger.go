// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
)

const (
	// DefaultLedgerNamespace is the current prototype namespace used for shared
	// lifecycle records.
	DefaultLedgerNamespace = "0"

	ledgerKeyPrefix           = "_lifecycle/chaincodes/"
	lifecycleIndexPrefix      = "_lifecycle/index/"
	LifecycleHighWatermarkKey = lifecycleIndexPrefix + "high_watermark"
	lifecycleLedgerSchema     = 1
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

// LedgerDefinition is the deterministic shared value stored in Fabric-X state.
type LedgerDefinition struct {
	CCID         string `json:"ccid"`
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
	CCID          string `json:"ccid"`
	Name          string `json:"name"`
	Version       string `json:"version"`
	Sequence      int64  `json:"sequence"`
	InitRequired  bool   `json:"init_required"`
	Initialized   bool   `json:"initialized"`
}

func DefinitionCCID(def ChaincodeDefinition) string {
	return fmt.Sprintf("%s:%s", def.Name, def.Version)
}

func LedgerKey(def ChaincodeDefinition) string {
	return ledgerKeyPrefix + DefinitionCCID(def)
}

func LifecycleIndexEntryKey(number uint64) string {
	return fmt.Sprintf("%s%020d", lifecycleIndexPrefix, number)
}

func MarshalLedgerDefinition(def ChaincodeDefinition, initialized bool) ([]byte, error) {
	return json.Marshal(LedgerDefinition{
		CCID:         DefinitionCCID(def),
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
	if value.CCID == "" {
		value.CCID = fmt.Sprintf("%s:%s", value.Name, value.Version)
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

func NewLifecycleIndexEntry(number uint64, eventType string, def ChaincodeDefinition, initialized bool) LifecycleIndexEntry {
	ccid := DefinitionCCID(def)
	return LifecycleIndexEntry{
		SchemaVersion: lifecycleLedgerSchema,
		Number:        number,
		EventType:     eventType,
		DefinitionKey: LedgerKey(def),
		CCID:          ccid,
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
	if entry.SchemaVersion == 0 {
		entry.SchemaVersion = lifecycleLedgerSchema
	}
	if entry.DefinitionKey == "" && entry.Name != "" && entry.Version != "" {
		entry.DefinitionKey = LedgerKey(ChaincodeDefinitionFromLedger(LedgerDefinition{
			Name:         entry.Name,
			Version:      entry.Version,
			Sequence:     entry.Sequence,
			InitRequired: entry.InitRequired,
		}))
	}
	if entry.CCID == "" && entry.Name != "" && entry.Version != "" {
		entry.CCID = fmt.Sprintf("%s:%s", entry.Name, entry.Version)
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
