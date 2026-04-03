package unit_test

import (
	"testing"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
	"github.com/mimizh/hospital-cdc-platform/internal/transport"
)

func TestSerializer_RoundTrip(t *testing.T) {
	s := &transport.Serializer{}

	original := core.NewChangeEvent()
	original.SourceID = "his_mysql_01"
	original.Table = "patients"
	original.OpType = core.OpUpdate
	original.PrimaryKeys = map[string]interface{}{"patient_id": float64(1001)} // JSON 数字默认 float64
	original.Before = map[string]interface{}{"name": "张三", "age": float64(30)}
	original.After = map[string]interface{}{"name": "张三", "age": float64(31)}
	original.BinlogFile = "mysql-bin.000003"
	original.BinlogPos = 12345

	bytes, err := s.Serialize(original)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if len(bytes) == 0 {
		t.Fatal("serialized bytes should not be empty")
	}

	restored, err := s.Deserialize(bytes)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}

	if restored.EventID != original.EventID {
		t.Errorf("EventID: got %s, want %s", restored.EventID, original.EventID)
	}
	if restored.SourceID != original.SourceID {
		t.Errorf("SourceID: got %s, want %s", restored.SourceID, original.SourceID)
	}
	if restored.Table != original.Table {
		t.Errorf("Table: got %s, want %s", restored.Table, original.Table)
	}
	if restored.OpType != original.OpType {
		t.Errorf("OpType: got %s, want %s", restored.OpType, original.OpType)
	}
	if restored.BinlogFile != original.BinlogFile {
		t.Errorf("BinlogFile: got %s, want %s", restored.BinlogFile, original.BinlogFile)
	}
	if restored.BinlogPos != original.BinlogPos {
		t.Errorf("BinlogPos: got %d, want %d", restored.BinlogPos, original.BinlogPos)
	}
}

func TestSerializer_InvalidJSON(t *testing.T) {
	s := &transport.Serializer{}
	_, err := s.Deserialize([]byte("not-json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestSerializer_AllOpTypes(t *testing.T) {
	s := &transport.Serializer{}
	for _, op := range []core.OpType{core.OpInsert, core.OpUpdate, core.OpDelete} {
		e := core.NewChangeEvent()
		e.OpType = op
		e.SourceID = "test"
		e.Table = "t"
		b, err := s.Serialize(e)
		if err != nil {
			t.Errorf("Serialize %s: %v", op, err)
			continue
		}
		r, err := s.Deserialize(b)
		if err != nil {
			t.Errorf("Deserialize %s: %v", op, err)
			continue
		}
		if r.OpType != op {
			t.Errorf("op type mismatch: got %s, want %s", r.OpType, op)
		}
	}
}
