package transport

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
)

// ExactlyOnceConsumer 封装 confluent-kafka Consumer，
// 使用 isolation.level=read_committed + 手动提交 offset，
// 配合 Sink 侧幂等写入实现消费端 Exactly-Once。
type ExactlyOnceConsumer struct {
	consumer   *kafka.Consumer
	serializer *Serializer
	topics     []string
	groupID    string
	logger     *zap.Logger
}

// NewExactlyOnceConsumer 创建 read_committed Consumer 并订阅指定 topics。
func NewExactlyOnceConsumer(
	bootstrapServers string,
	groupID string,
	topics []string,
	logger *zap.Logger,
) (*ExactlyOnceConsumer, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        bootstrapServers,
		"group.id":                 groupID,
		"isolation.level":          "read_committed", // 只消费已提交事务的消息
		"enable.auto.commit":       false,            // 手动提交 offset
		"auto.offset.reset":        "earliest",
		"fetch.min.bytes":          1,
		"fetch.wait.max.ms":        500,
		"max.poll.interval.ms":     300000,
		"session.timeout.ms":       30000,
		"heartbeat.interval.ms":    3000,
	})
	if err != nil {
		return nil, fmt.Errorf("new kafka consumer: %w", err)
	}

	if err := c.SubscribeTopics(topics, nil); err != nil {
		c.Close()
		return nil, fmt.Errorf("subscribe topics %v: %w", topics, err)
	}

	return &ExactlyOnceConsumer{
		consumer:   c,
		serializer: &Serializer{},
		topics:     topics,
		groupID:    groupID,
		logger:     logger,
	}, nil
}

// PollBatch 拉取一批消息，最多 maxCount 条或等待 timeout 后返回。
// 返回 ChangeEvent 列表（已过滤控制消息）和对应原始 kafka.Message（用于提交 offset）。
func (c *ExactlyOnceConsumer) PollBatch(maxCount int, timeout time.Duration) ([]*core.ChangeEvent, []*kafka.Message, error) {
	deadline := time.Now().Add(timeout)
	var events []*core.ChangeEvent
	var msgs []*kafka.Message

	for len(events) < maxCount && time.Now().Before(deadline) {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		msg, err := c.consumer.ReadMessage(remaining)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				break // 超时，返回已收集的消息
			}
			return nil, nil, fmt.Errorf("read message: %w", err)
		}
		if msg == nil {
			break
		}

		e, err := c.serializer.Deserialize(msg.Value)
		if err != nil {
			c.logger.Warn("deserialize failed, skipping",
				zap.String("topic", *msg.TopicPartition.Topic),
				zap.Error(err))
			continue
		}
		events = append(events, e)
		msgs = append(msgs, msg)
	}
	return events, msgs, nil
}

// CommitOffsets 手动提交指定消息集合的 offsets（+1）。
// 必须在 Sink 写入成功后调用，确保 offset 提交与数据写入顺序绑定。
func (c *ExactlyOnceConsumer) CommitOffsets(msgs []*kafka.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	offsets := make([]kafka.TopicPartition, len(msgs))
	for i, msg := range msgs {
		offsets[i] = kafka.TopicPartition{
			Topic:     msg.TopicPartition.Topic,
			Partition: msg.TopicPartition.Partition,
			Offset:    msg.TopicPartition.Offset + 1,
		}
	}
	_, err := c.consumer.CommitOffsets(offsets)
	if err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}
	return nil
}

// Close 关闭 Consumer，提交当前已处理 offset。
func (c *ExactlyOnceConsumer) Close() error {
	return c.consumer.Close()
}
