// SPDX-License-Identifier: Apache-2.0

package orchestrator

import (
	"context"
	"fmt"

	"compatibility_service/pkg/lifecycle"
)

func (s *Service) syncLifecycleFromLedger(ctx context.Context) error {
	if s == nil || s.lifecycle == nil || s.queryService == nil {
		return nil
	}
	namespace := s.cfg.lifecycleNamespace()
	value, _, ok, err := s.lifecycleRow(ctx, namespace, lifecycle.LifecycleHighWatermarkKey)
	if err != nil {
		return err
	}
	if !ok {
		s.logger.Infof("lifecycle ledger sync namespace=%s high_watermark=0 definitions=0", namespace)
		return nil
	}
	highWatermark, err := lifecycle.UnmarshalLifecycleHighWatermark(value)
	if err != nil {
		return fmt.Errorf("decode lifecycle high watermark: %w", err)
	}

	hydrated := 0
	for i := uint64(1); i <= highWatermark; i++ {
		entryKey := lifecycle.LifecycleIndexEntryKey(i)
		entryValue, _, ok, err := s.lifecycleRow(ctx, namespace, entryKey)
		if err != nil {
			return fmt.Errorf("read lifecycle index entry %s: %w", entryKey, err)
		}
		if !ok {
			return fmt.Errorf("missing lifecycle index entry %s", entryKey)
		}
		entry, err := lifecycle.UnmarshalLifecycleIndexEntry(entryValue)
		if err != nil {
			return fmt.Errorf("decode lifecycle index entry %s: %w", entryKey, err)
		}
		defKey := entry.DefinitionKey
		if defKey == "" {
			return fmt.Errorf("lifecycle index entry %s missing definition key", entryKey)
		}
		defValue, _, ok, err := s.lifecycleRow(ctx, namespace, defKey)
		if err != nil {
			return fmt.Errorf("read lifecycle definition %s: %w", defKey, err)
		}
		if !ok {
			return fmt.Errorf("missing lifecycle definition %s from index %s", defKey, entryKey)
		}
		ledgerDef, err := lifecycle.UnmarshalLedgerDefinition(defValue)
		if err != nil {
			return fmt.Errorf("decode lifecycle definition %s: %w", defKey, err)
		}
		def := lifecycle.ChaincodeDefinitionFromLedger(ledgerDef)
		if _, err := s.lifecycle.UpsertCommittedFromLedger(ctx, def, ledgerDef.Initialized); err != nil {
			return fmt.Errorf("hydrate lifecycle definition %s: %w", defKey, err)
		}
		hydrated++
	}
	s.logger.Infof("lifecycle ledger sync namespace=%s high_watermark=%d index_entries=%d",
		namespace, highWatermark, hydrated)
	return nil
}
