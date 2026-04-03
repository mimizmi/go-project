package transport

import (
	"fmt"

	"github.com/mimizh/hospital-cdc-platform/internal/config"
	"github.com/mimizh/hospital-cdc-platform/internal/core"
)

// TopicRouter 根据配置将 ChangeEvent 路由到目标 Kafka topic 和 partition key。
type TopicRouter struct {
	mappings map[string]config.TopicMapping // key: sourceID+"."+table
	cfg      *config.Config
}

// NewTopicRouter 从配置创建路由器。
func NewTopicRouter(cfg *config.Config) *TopicRouter {
	m := make(map[string]config.TopicMapping, len(cfg.Topics))
	for _, t := range cfg.Topics {
		m[t.SourceID+"."+t.Table] = t
	}
	return &TopicRouter{mappings: m, cfg: cfg}
}

// Route 返回事件对应的 Kafka topic 名称和 partition key。
func (r *TopicRouter) Route(e *core.ChangeEvent) (topic string, partitionKey string) {
	key := e.SourceID + "." + e.Table
	if m, ok := r.mappings[key]; ok {
		// 从事件数据中提取配置的分区键字段
		pk := ""
		if m.PartitionKeyField != "" {
			if v, found := e.PrimaryKeys[m.PartitionKeyField]; found {
				pk = fmt.Sprintf("%v", v)
			}
		}
		if pk == "" {
			pk = e.PartitionKey()
		}
		return m.Topic, pk
	}
	// 默认规则
	return fmt.Sprintf("cdc.%s.%s", e.Database, e.Table), e.PartitionKey()
}
