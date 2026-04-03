package engine

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
	"github.com/mimizh/hospital-cdc-platform/internal/monitoring"
	"github.com/mimizh/hospital-cdc-platform/internal/transport"
)

// ConsumerPipeline 消费端主循环：
//
//	poll Kafka → 幂等写入 PG → commit offset
//
// Exactly-Once 保证：
//  1. isolation.level=read_committed：只消费 Kafka 事务已提交的消息
//  2. PG 事务内幂等 UPSERT + 去重表（原子）
//  3. PG commit 成功后才 commit Kafka offset
//  4. 若 PG 失败 → Rollback → offset 不提交 → 下次重新消费
type ConsumerPipeline struct {
	consumer     *transport.ExactlyOnceConsumer
	sink         core.ISinkWriter
	metrics      *monitoring.Metrics
	batchSize    int
	batchTimeout time.Duration
	logger       *zap.Logger
}

// NewConsumerPipeline 创建消费端管道。
func NewConsumerPipeline(
	consumer *transport.ExactlyOnceConsumer,
	sink core.ISinkWriter,
	metrics *monitoring.Metrics,
	batchSize int,
	batchTimeout time.Duration,
	logger *zap.Logger,
) *ConsumerPipeline {
	return &ConsumerPipeline{
		consumer:     consumer,
		sink:         sink,
		metrics:      metrics,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		logger:       logger,
	}
}

// Run 启动消费主循环，阻塞直到 ctx 取消。
//
// 主循环逻辑（对应计划中第四节 4.2）：
//  1. poll_batch(max=batchSize, timeout=batchTimeout)
//  2. BEGIN PG TRANSACTION
//     for each event: check dedup → UPSERT → mark dedup
//     COMMIT PG TRANSACTION           ──┐
//  3. consumer.CommitOffsets()          │ 顺序绑定
//     若步骤2失败: ROLLBACK, 不执行步骤3 ─┘
func (p *ConsumerPipeline) Run(ctx context.Context) error {
	p.logger.Info("consumer pipeline started")

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("consumer pipeline stopping")
			return nil
		default:
		}

		// 1. 拉取一批消息
		events, msgs, err := p.consumer.PollBatch(p.batchSize, p.batchTimeout)
		if err != nil {
			p.logger.Error("poll batch failed", zap.Error(err))
			// 短暂等待后继续（Kafka 重连由 client 自动处理）
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
			continue
		}
		if len(events) == 0 {
			continue
		}

		// 2. 幂等写入 Sink（内部 PG 事务）
		start := time.Now()
		written, err := p.sink.WriteBatch(ctx, events)
		if err != nil {
			p.logger.Error("sink write failed",
				zap.Int("batch_size", len(events)),
				zap.Error(err))
			// 不提交 offset，下次重新消费
			continue
		}

		// 3. PG 成功后提交 Kafka offset
		if err := p.consumer.CommitOffsets(msgs); err != nil {
			// offset 提交失败：下次会重复消费，但幂等键确保不重复写入
			p.logger.Warn("commit offsets failed, will reprocess on restart",
				zap.Error(err))
		}

		// 指标更新
		elapsed := time.Since(start).Seconds()
		skipped := len(events) - written
		for _, e := range events {
			p.metrics.SinkEventsTotal.WithLabelValues(e.Table, string(e.OpType)).Inc()
			p.metrics.SinkWriteLatency.WithLabelValues(e.Table).Observe(elapsed / float64(len(events)))
			if e.SourceTimestamp.IsZero() {
				continue
			}
			lag := time.Since(e.SourceTimestamp).Seconds()
			p.metrics.E2ELatency.WithLabelValues(e.SourceID, e.Table).Observe(lag)
		}
		if skipped > 0 {
			p.metrics.SinkDuplicatesTotal.WithLabelValues(events[0].Table).Add(float64(skipped))
		}

		p.logger.Debug("batch processed",
			zap.Int("total", len(events)),
			zap.Int("written", written),
			zap.Int("skipped", skipped),
			zap.Duration("elapsed", time.Since(start)),
		)

		// 检查 ctx
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
}

// formatTopics 辅助函数：打印订阅的 topic 列表（供日志使用）。
func formatTopics(events []*core.ChangeEvent) string {
	seen := map[string]bool{}
	for _, e := range events {
		seen[e.Table] = true
	}
	topics := make([]string, 0, len(seen))
	for t := range seen {
		topics = append(topics, t)
	}
	return fmt.Sprintf("%v", topics)
}
