// Package transport 实现 Kafka 事务型生产/消费与序列化路由。
package transport

import (
	"encoding/json"
	"fmt"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
)

// Serializer ChangeEvent <-> []byte 序列化器（JSON 格式）。
type Serializer struct{}

// Serialize 将 ChangeEvent 序列化为 JSON bytes。
func (s *Serializer) Serialize(e *core.ChangeEvent) ([]byte, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", core.ErrSerialization, err)
	}
	return b, nil
}

// Deserialize 将 JSON bytes 反序列化为 ChangeEvent。
func (s *Serializer) Deserialize(b []byte) (*core.ChangeEvent, error) {
	var e core.ChangeEvent
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, fmt.Errorf("%w: %v", core.ErrSerialization, err)
	}
	return &e, nil
}
