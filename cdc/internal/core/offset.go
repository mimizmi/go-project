package core

import (
	"encoding/json"
	"time"
)

// OffsetPosition 统一位点模型，适配不同数据库类型。
//
// MySQL 位点:  BinlogFile + BinlogPos（或 GTID）
// SQL Server: LSN
// 公共字段:   KafkaTxnID 关联最后一次 Kafka 事务
type OffsetPosition struct {
	SourceID   string     `json:"source_id"`
	SourceType SourceType `json:"source_type"`

	// MySQL 位点
	BinlogFile string `json:"binlog_file,omitempty"`
	BinlogPos  uint32 `json:"binlog_pos,omitempty"`
	GTID       string `json:"gtid,omitempty"`

	// SQL Server 位点
	LSN           string    `json:"lsn,omitempty"`
	PollTimestamp time.Time `json:"poll_timestamp,omitempty"`

	// Kafka 事务关联
	KafkaTxnID    string `json:"kafka_txn_id,omitempty"`
	KafkaTxnEpoch int32  `json:"kafka_txn_epoch,omitempty"`

	// 元数据
	UpdatedAt  time.Time `json:"updated_at"`
	EventCount int64     `json:"event_count"`
}

// NewOffsetPosition 创建一个初始位点。
func NewOffsetPosition(sourceID string, sourceType SourceType) *OffsetPosition {
	return &OffsetPosition{
		SourceID:   sourceID,
		SourceType: sourceType,
		UpdatedAt:  time.Now().UTC(),
	}
}

// IsValid 判断位点是否有效（可用于断点续传）。
func (p *OffsetPosition) IsValid() bool {
	if p == nil {
		return false
	}
	switch p.SourceType {
	case SourceMySQL:
		return p.BinlogFile != "" && p.BinlogPos > 0
	case SourceSQLServer:
		return p.LSN != ""
	}
	return false
}

// ToJSON 序列化为 JSON 字符串（用于 SQLite 历史记录）。
func (p *OffsetPosition) ToJSON() string {
	b, _ := json.Marshal(p)
	return string(b)
}

// OffsetPositionFromJSON 从 JSON 字符串反序列化。
func OffsetPositionFromJSON(s string) (*OffsetPosition, error) {
	var p OffsetPosition
	if err := json.Unmarshal([]byte(s), &p); err != nil {
		return nil, err
	}
	return &p, nil
}
