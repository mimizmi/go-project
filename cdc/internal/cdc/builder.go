// Package cdc 实现多数据库 CDC 采集层。
package cdc

import (
	"fmt"
	"time"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
)

// EventBuilder 将原始日志行数据封装为标准 ChangeEvent。
//
// 职责：类型规范化、主键提取、敏感字段脱敏。
type EventBuilder struct {
	sourceID   string
	sourceType core.SourceType
	database   string
	schemaName string
	maskFields map[string][]string // table -> []field
}

// NewEventBuilder 创建 EventBuilder。
func NewEventBuilder(
	sourceID string,
	sourceType core.SourceType,
	database, schemaName string,
	maskFields map[string][]string,
) *EventBuilder {
	if maskFields == nil {
		maskFields = make(map[string][]string)
	}
	return &EventBuilder{
		sourceID:   sourceID,
		sourceType: sourceType,
		database:   database,
		schemaName: schemaName,
		maskFields: maskFields,
	}
}

// BuildMySQL 从 MySQL binlog 行事件构建 ChangeEvent。
func (b *EventBuilder) BuildMySQL(
	table string,
	opType core.OpType,
	primaryKeys map[string]interface{},
	before, after map[string]interface{},
	binlogFile string,
	binlogPos uint32,
	txnID string,
	sourceTS time.Time,
) *core.ChangeEvent {
	e := core.NewChangeEvent()
	e.SourceType = b.sourceType
	e.SourceID = b.sourceID
	e.Database = b.database
	e.Table = table
	e.OpType = opType
	e.PrimaryKeys = primaryKeys
	e.Before = b.normalize(b.mask(table, before))
	e.After = b.normalize(b.mask(table, after))
	e.BinlogFile = binlogFile
	e.BinlogPos = binlogPos
	e.TxnID = txnID
	e.SourceTimestamp = sourceTS
	return e
}

// BuildSQLServer 从 SQL Server CDC 行数据构建 ChangeEvent。
// operation: 1=DELETE, 2=INSERT, 3=UPDATE_BEFORE, 4=UPDATE_AFTER
func (b *EventBuilder) BuildSQLServer(
	table string,
	operation int,
	primaryKeys map[string]interface{},
	before, after map[string]interface{},
	lsn string,
	txnID string,
	sourceTS time.Time,
) *core.ChangeEvent {
	opMap := map[int]core.OpType{
		1: core.OpDelete,
		2: core.OpInsert,
		3: core.OpUpdate,
		4: core.OpUpdate,
	}
	opType, ok := opMap[operation]
	if !ok {
		opType = core.OpInsert
	}

	e := core.NewChangeEvent()
	e.SourceType = b.sourceType
	e.SourceID = b.sourceID
	e.Database = b.database
	e.SchemaName = b.schemaName
	e.Table = table
	e.OpType = opType
	e.PrimaryKeys = primaryKeys
	e.Before = b.normalize(b.mask(table, before))
	e.After = b.normalize(b.mask(table, after))
	e.LSN = lsn
	e.TxnID = txnID
	e.SourceTimestamp = sourceTS
	return e
}

// normalize 将数据库原生类型规范化为 JSON 可序列化的基础类型。
func (b *EventBuilder) normalize(row map[string]interface{}) map[string]interface{} {
	if row == nil {
		return nil
	}
	result := make(map[string]interface{}, len(row))
	for k, v := range row {
		switch val := v.(type) {
		case []byte:
			result[k] = string(val)
		case time.Time:
			result[k] = val.UTC().Format(time.RFC3339Nano)
		default:
			result[k] = v
		}
	}
	return result
}

// mask 对指定表的敏感字段替换为 "***"。
func (b *EventBuilder) mask(table string, row map[string]interface{}) map[string]interface{} {
	if row == nil {
		return nil
	}
	fields, ok := b.maskFields[table]
	if !ok || len(fields) == 0 {
		return row
	}
	masked := make(map[string]interface{}, len(row))
	fieldSet := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		fieldSet[f] = struct{}{}
	}
	for k, v := range row {
		if _, sensitive := fieldSet[k]; sensitive {
			masked[k] = "***"
		} else {
			masked[k] = v
		}
	}
	return masked
}

// ExtractPrimaryKeys 从行数据按主键列名提取主键 map。
// pkColumns 为空时，降级使用列名以 "_id" 结尾或名为 "id" 的列。
func ExtractPrimaryKeys(row map[string]interface{}, pkColumns []string) map[string]interface{} {
	if len(pkColumns) > 0 {
		pks := make(map[string]interface{}, len(pkColumns))
		for _, col := range pkColumns {
			if v, ok := row[col]; ok {
				pks[col] = v
			}
		}
		if len(pks) > 0 {
			return pks
		}
	}
	// 降级：猜测主键列
	for _, suffix := range []string{"id"} {
		for k, v := range row {
			if k == suffix || len(k) > 3 && k[len(k)-3:] == "_id" {
				return map[string]interface{}{k: v}
			}
		}
	}
	return map[string]interface{}{"_rowkey": fmt.Sprintf("%v", row)}
}
