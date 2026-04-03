package core

import "context"

// ICdcSource CDC 数据源抽象接口。
//
// 实现类：MySQLCdcSource（binlog）、SQLServerCdcSource（CDC 轮询）
type ICdcSource interface {
	// SourceID 返回逻辑源标识，全局唯一
	SourceID() string

	// Start 从指定位点启动采集；pos 为 nil 表示首次启动（从当前最新位点开始）
	Start(ctx context.Context, pos *OffsetPosition) error

	// Events 返回变更事件 channel，采集协程向其写入，调用方只读
	Events() <-chan *ChangeEvent

	// Errors 返回采集错误 channel
	Errors() <-chan error

	// CurrentPosition 返回当前已读取到的位点
	CurrentPosition() *OffsetPosition

	// Close 优雅停止，释放连接
	Close() error
}

// IOffsetStore 位点持久化存储抽象接口。
//
// 实现类：SQLiteOffsetStore
type IOffsetStore interface {
	// Load 加载指定源的最近一次成功提交位点；若无记录则返回 nil, nil
	Load(sourceID string) (*OffsetPosition, error)

	// Save 原子保存位点；txnID 关联 Kafka 事务 ID，用于恢复判定
	Save(sourceID string, pos *OffsetPosition, txnID string) error

	// LastTxnID 返回最后成功提交的 Kafka 事务 ID
	LastTxnID(sourceID string) (string, error)

	// ListSources 返回所有已记录的 sourceID
	ListSources() ([]string, error)

	// Close 关闭存储连接
	Close() error
}

// ISinkWriter 目标库写入器抽象接口。
//
// 实现类：PostgresSinkWriter
type ISinkWriter interface {
	// WriteBatch 幂等批量写入，内部处理 UPSERT + 去重，返回实际写入数（去重后）
	WriteBatch(ctx context.Context, events []*ChangeEvent) (int, error)

	// Flush 确保所有缓冲数据落盘
	Flush(ctx context.Context) error

	// Close 关闭连接
	Close() error
}
