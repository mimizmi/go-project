package unit_test

import (
	"testing"
	"time"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
)

func TestChangeEvent_IdempotencyKey_Deterministic(t *testing.T) {
	e := &core.ChangeEvent{
		EventID:     "uuid-1",
		SourceID:    "his_mysql_01",
		Table:       "patients",
		OpType:      core.OpInsert,
		PrimaryKeys: map[string]interface{}{"patient_id": 1001},
		BinlogFile:  "mysql-bin.000003",
		BinlogPos:   12345,
		TxnID:       "txn-abc",
	}

	key1 := e.IdempotencyKey()
	key2 := e.IdempotencyKey()

	if key1 != key2 {
		t.Errorf("idempotency key not deterministic: %s vs %s", key1, key2)
	}
	if len(key1) != 64 {
		t.Errorf("expected 64-char SHA256 hex, got %d", len(key1))
	}
}

func TestChangeEvent_IdempotencyKey_DifferentEvents(t *testing.T) {
	base := &core.ChangeEvent{
		SourceID:    "his_mysql_01",
		Table:       "patients",
		OpType:      core.OpInsert,
		PrimaryKeys: map[string]interface{}{"patient_id": 1001},
		BinlogFile:  "mysql-bin.000003",
		BinlogPos:   12345,
	}
	other := &core.ChangeEvent{
		SourceID:    "his_mysql_01",
		Table:       "patients",
		OpType:      core.OpInsert,
		PrimaryKeys: map[string]interface{}{"patient_id": 1002}, // 不同主键
		BinlogFile:  "mysql-bin.000003",
		BinlogPos:   12345,
	}
	if base.IdempotencyKey() == other.IdempotencyKey() {
		t.Error("different events should produce different idempotency keys")
	}
}

func TestChangeEvent_IdempotencyKey_RetryProducesSameKey(t *testing.T) {
	// 重试场景：EventID 不同（UUID 重新生成），但位点相同 → key 应相同
	e1 := &core.ChangeEvent{
		EventID:    "uuid-first-attempt",
		SourceID:   "his_mysql_01",
		Table:      "patients",
		OpType:     core.OpUpdate,
		PrimaryKeys: map[string]interface{}{"patient_id": 2001},
		BinlogFile: "mysql-bin.000005",
		BinlogPos:  99999,
		TxnID:      "txn-xyz",
	}
	e2 := &core.ChangeEvent{
		EventID:    "uuid-retry-attempt", // 不同 UUID
		SourceID:   "his_mysql_01",
		Table:      "patients",
		OpType:     core.OpUpdate,
		PrimaryKeys: map[string]interface{}{"patient_id": 2001},
		BinlogFile: "mysql-bin.000005",
		BinlogPos:  99999,
		TxnID:      "txn-xyz",
	}
	if e1.IdempotencyKey() != e2.IdempotencyKey() {
		t.Error("retry should produce same idempotency key regardless of EventID")
	}
}

func TestChangeEvent_PartitionKey(t *testing.T) {
	e := core.NewChangeEvent()
	e.Table = "patients"
	e.PrimaryKeys = map[string]interface{}{"patient_id": 42}

	pk := e.PartitionKey()
	if pk == "" {
		t.Error("partition key should not be empty")
	}
}

func TestChangeEvent_OpTypes(t *testing.T) {
	cases := []struct {
		op     core.OpType
		hasAfter bool
		hasBefore bool
	}{
		{core.OpInsert, true, false},
		{core.OpUpdate, true, true},
		{core.OpDelete, false, true},
	}
	for _, tc := range cases {
		e := core.NewChangeEvent()
		e.OpType = tc.op
		if tc.hasAfter {
			e.After = map[string]interface{}{"col": "val"}
		}
		if tc.hasBefore {
			e.Before = map[string]interface{}{"col": "old"}
		}
		if tc.hasAfter && e.After == nil {
			t.Errorf("op %s should have After", tc.op)
		}
		if tc.hasBefore && e.Before == nil {
			t.Errorf("op %s should have Before", tc.op)
		}
	}
}

func TestOffsetPosition_IsValid(t *testing.T) {
	// MySQL 有效位点
	mysqlPos := &core.OffsetPosition{
		SourceType: core.SourceMySQL,
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  1234,
	}
	if !mysqlPos.IsValid() {
		t.Error("MySQL position with file+pos should be valid")
	}

	// MySQL 无效位点（pos=0）
	invalidMySQL := &core.OffsetPosition{
		SourceType: core.SourceMySQL,
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  0,
	}
	if invalidMySQL.IsValid() {
		t.Error("MySQL position with pos=0 should be invalid")
	}

	// SQL Server 有效位点
	ssPos := &core.OffsetPosition{
		SourceType: core.SourceSQLServer,
		LSN:        "0x0000002A000001E80003",
	}
	if !ssPos.IsValid() {
		t.Error("SQL Server position with LSN should be valid")
	}

	// nil 位点
	if (*core.OffsetPosition)(nil).IsValid() {
		t.Error("nil position should be invalid")
	}
}

func TestOffsetPosition_JSON_RoundTrip(t *testing.T) {
	pos := &core.OffsetPosition{
		SourceID:   "his_mysql_01",
		SourceType: core.SourceMySQL,
		BinlogFile: "mysql-bin.000003",
		BinlogPos:  12345,
		KafkaTxnID: "txn-123",
		EventCount: 500,
		UpdatedAt:  time.Now().UTC().Truncate(time.Second),
	}

	jsonStr := pos.ToJSON()
	restored, err := core.OffsetPositionFromJSON(jsonStr)
	if err != nil {
		t.Fatalf("FromJSON failed: %v", err)
	}
	if restored.BinlogFile != pos.BinlogFile {
		t.Errorf("BinlogFile mismatch: %s vs %s", restored.BinlogFile, pos.BinlogFile)
	}
	if restored.BinlogPos != pos.BinlogPos {
		t.Errorf("BinlogPos mismatch: %d vs %d", restored.BinlogPos, pos.BinlogPos)
	}
	if restored.KafkaTxnID != pos.KafkaTxnID {
		t.Errorf("KafkaTxnID mismatch: %s vs %s", restored.KafkaTxnID, pos.KafkaTxnID)
	}
}
