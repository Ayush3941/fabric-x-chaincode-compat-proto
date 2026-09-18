// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
)

const (
	// DefaultLedgerNamespace is the current prototype namespace used for shared
	// lifecycle records.
	DefaultLedgerNamespace = "0"

	ledgerKeyPrefix = "_lifecycle/chaincodes/"
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

func DefinitionCCID(def ChaincodeDefinition) string {
	return fmt.Sprintf("%s:%s", def.Name, def.Version)
}

func LedgerKey(def ChaincodeDefinition) string {
	return ledgerKeyPrefix + DefinitionCCID(def)
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
