package unit_test

import (
	"os"
	"testing"
	"time"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
	"github.com/mimizh/hospital-cdc-platform/internal/offset"
)

func newTestStore(t *testing.T) *offset.SQLiteOffsetStore {
	t.Helper()
	f, err := os.CreateTemp("", "offset_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	t.Cleanup(func() { os.Remove(f.Name()) })

	store, err := offset.NewSQLiteOffsetStore(f.Name())
	if err != nil {
		t.Fatalf("NewSQLiteOffsetStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestOffsetStore_SaveAndLoad(t *testing.T) {
	store := newTestStore(t)

	pos := &core.OffsetPosition{
		SourceID:   "his_mysql_01",
		SourceType: core.SourceMySQL,
		BinlogFile: "mysql-bin.000001",
		BinlogPos:  5678,
		KafkaTxnID: "txn-001",
		EventCount: 100,
		UpdatedAt:  time.Now().UTC(),
	}

	if err := store.Save("his_mysql_01", pos, "txn-001"); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := store.Load("his_mysql_01")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected non-nil position")
	}
	if loaded.BinlogFile != pos.BinlogFile {
		t.Errorf("BinlogFile: got %s, want %s", loaded.BinlogFile, pos.BinlogFile)
	}
	if loaded.BinlogPos != pos.BinlogPos {
		t.Errorf("BinlogPos: got %d, want %d", loaded.BinlogPos, pos.BinlogPos)
	}
}

func TestOffsetStore_LoadNonExistent(t *testing.T) {
	store := newTestStore(t)
	pos, err := store.Load("non_existent_source")
	if err != nil {
		t.Fatalf("Load non-existent should not error: %v", err)
	}
	if pos != nil {
		t.Error("expected nil for non-existent source")
	}
}

func TestOffsetStore_Upsert(t *testing.T) {
	store := newTestStore(t)

	pos1 := &core.OffsetPosition{
		SourceID: "src1", SourceType: core.SourceMySQL,
		BinlogFile: "mysql-bin.000001", BinlogPos: 100,
		UpdatedAt: time.Now().UTC(),
	}
	pos2 := &core.OffsetPosition{
		SourceID: "src1", SourceType: core.SourceMySQL,
		BinlogFile: "mysql-bin.000002", BinlogPos: 200,
		UpdatedAt: time.Now().UTC(),
	}

	if err := store.Save("src1", pos1, "txn-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Save("src1", pos2, "txn-2"); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load("src1")
	if err != nil {
		t.Fatal(err)
	}
	// 应返回最新位点
	if loaded.BinlogFile != "mysql-bin.000002" {
		t.Errorf("expected latest position, got %s", loaded.BinlogFile)
	}
}

func TestOffsetStore_LastTxnID(t *testing.T) {
	store := newTestStore(t)

	pos := &core.OffsetPosition{
		SourceID: "src2", SourceType: core.SourceMySQL,
		BinlogFile: "mysql-bin.000003", BinlogPos: 999,
		UpdatedAt: time.Now().UTC(),
	}
	if err := store.Save("src2", pos, "my-txn-id"); err != nil {
		t.Fatal(err)
	}

	txnID, err := store.LastTxnID("src2")
	if err != nil {
		t.Fatal(err)
	}
	if txnID != "my-txn-id" {
		t.Errorf("expected 'my-txn-id', got %s", txnID)
	}
}

func TestOffsetStore_ListSources(t *testing.T) {
	store := newTestStore(t)

	for _, id := range []string{"src_a", "src_b", "src_c"} {
		pos := &core.OffsetPosition{
			SourceID: id, SourceType: core.SourceMySQL,
			BinlogFile: "mysql-bin.000001", BinlogPos: 1,
			UpdatedAt: time.Now().UTC(),
		}
		if err := store.Save(id, pos, "txn-x"); err != nil {
			t.Fatal(err)
		}
	}

	sources, err := store.ListSources()
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 3 {
		t.Errorf("expected 3 sources, got %d", len(sources))
	}
}

func TestOffsetStore_SQLServerPosition(t *testing.T) {
	store := newTestStore(t)

	pos := &core.OffsetPosition{
		SourceID:   "lis_sqlserver_01",
		SourceType: core.SourceSQLServer,
		LSN:        "0x0000002A000001E80003",
		UpdatedAt:  time.Now().UTC(),
		EventCount: 250,
	}
	if err := store.Save("lis_sqlserver_01", pos, "txn-ss-001"); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.Load("lis_sqlserver_01")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.LSN != pos.LSN {
		t.Errorf("LSN: got %s, want %s", loaded.LSN, pos.LSN)
	}
}
