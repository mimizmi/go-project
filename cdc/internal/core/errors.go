package core

import "errors"

// 定义各层错误哨兵值，供 errors.Is 判断使用。

var (
	// CDC 采集层
	ErrCdcConnection = errors.New("cdc: connection failed")
	ErrCdcRead       = errors.New("cdc: read error")
	ErrSchemaChange  = errors.New("cdc: schema changed")
	ErrMaxRetries    = errors.New("cdc: max retries exceeded")

	// Kafka 传输层
	ErrKafkaTxn      = errors.New("kafka: transaction error")
	ErrKafkaProduce  = errors.New("kafka: produce error")
	ErrSerialization = errors.New("kafka: serialization error")

	// 位点管理
	ErrOffsetLoad = errors.New("offset: load failed")
	ErrOffsetSave = errors.New("offset: save failed")

	// Sink 写入
	ErrSinkWrite      = errors.New("sink: write error")
	ErrSinkConnection = errors.New("sink: connection failed")

	// 配置
	ErrConfiguration = errors.New("config: invalid configuration")
)

// CdcError 包装 CDC 层错误，携带来源信息。
type CdcError struct {
	SourceID string
	Cause    error
}

func (e *CdcError) Error() string {
	return "cdc[" + e.SourceID + "]: " + e.Cause.Error()
}

func (e *CdcError) Unwrap() error { return e.Cause }

// WrapCdcError 包装 CDC 错误。
func WrapCdcError(sourceID string, err error) error {
	if err == nil {
		return nil
	}
	return &CdcError{SourceID: sourceID, Cause: err}
}
