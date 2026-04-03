// Package core 定义系统核心数据模型和抽象接口。
package core

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// OpType 变更操作类型
type OpType string

const (
	OpInsert OpType = "INSERT"
	OpUpdate OpType = "UPDATE"
	OpDelete OpType = "DELETE"
)

// SourceType 数据源类型
type SourceType string

const (
	SourceMySQL      SourceType = "MYSQL"
	SourceSQLServer  SourceType = "SQLSERVER"
)

// ChangeEvent 统一变更事件模型，所有 CDC 源产出此结构。
//
// 设计原则：
//   - before/after 前后镜像: INSERT 只有 After，DELETE 只有 Before，UPDATE 两者都有
//   - PrimaryKeys 独立字段: 便于路由和幂等键计算
//   - IdempotencyKey 基于位点信息而非 EventID，确保重试幂等
type ChangeEvent struct {
	// 事件标识
	EventID   string    `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`

	// 来源信息
	SourceType SourceType `json:"source_type"`
	SourceID   string     `json:"source_id"`
	Database   string     `json:"database"`
	SchemaName string     `json:"schema_name,omitempty"` // SQL Server 有 schema 概念
	Table      string     `json:"table"`

	// 变更内容
	OpType      OpType                 `json:"op_type"`
	PrimaryKeys map[string]interface{} `json:"primary_keys"`
	Before      map[string]interface{} `json:"before,omitempty"` // UPDATE/DELETE 前镜像
	After       map[string]interface{} `json:"after,omitempty"`  // INSERT/UPDATE 后镜像

	// 事务与位点
	TxnID      string `json:"txn_id,omitempty"`       // 源库事务 ID (MySQL GTID / SS txn)
	BinlogFile string `json:"binlog_file,omitempty"`  // MySQL binlog 文件名
	BinlogPos  uint32 `json:"binlog_pos,omitempty"`   // MySQL binlog 偏移
	LSN        string `json:"lsn,omitempty"`          // SQL Server LSN

	// 元数据
	SchemaVersion   int               `json:"schema_version"`
	SourceTimestamp time.Time         `json:"source_timestamp,omitempty"`
	Headers         map[string]string `json:"headers,omitempty"`
}

// NewChangeEvent 创建带唯一 EventID 的变更事件。
func NewChangeEvent() *ChangeEvent {
	return &ChangeEvent{
		EventID:       uuid.New().String(),
		Timestamp:     time.Now().UTC(),
		PrimaryKeys:   make(map[string]interface{}),
		Headers:       make(map[string]string),
		SchemaVersion: 1,
	}
}

// IdempotencyKey 幂等键：基于 (SourceID, Table, PrimaryKeys, OpType, 位点信息) 的确定性哈希。
//
// 不使用 EventID（UUID），因为重试时 EventID 可能不同；
// 位点信息保证同一条源变更重试时产生相同的幂等键。
func (e *ChangeEvent) IdempotencyKey() string {
	type keyPayload struct {
		SourceID    string                 `json:"source_id"`
		Table       string                 `json:"table"`
		PrimaryKeys map[string]interface{} `json:"primary_keys"`
		OpType      OpType                 `json:"op_type"`
		TxnID       string                 `json:"txn_id"`
		BinlogFile  string                 `json:"binlog_file"`
		BinlogPos   uint32                 `json:"binlog_pos"`
		LSN         string                 `json:"lsn"`
	}
	payload := keyPayload{
		SourceID:    e.SourceID,
		Table:       e.Table,
		PrimaryKeys: e.PrimaryKeys,
		OpType:      e.OpType,
		TxnID:       e.TxnID,
		BinlogFile:  e.BinlogFile,
		BinlogPos:   e.BinlogPos,
		LSN:         e.LSN,
	}
	b, _ := json.Marshal(payload)
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h)
}

// PartitionKey 分区键：取 PrimaryKeys 中第一个值，用于 Kafka 分区路由。
func (e *ChangeEvent) PartitionKey() string {
	for _, v := range e.PrimaryKeys {
		return fmt.Sprintf("%v", v)
	}
	return e.Table
}
