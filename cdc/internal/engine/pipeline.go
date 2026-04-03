// Package engine 实现管道编排、协调与容错恢复。
package engine

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"go.uber.org/zap"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
	"github.com/mimizh/hospital-cdc-platform/internal/monitoring"
	"github.com/mimizh/hospital-cdc-platform/internal/transport"
)

// SourcePipeline 采集端主循环：
//
//	CDC 读事件 → Kafka 事务发送 → 原子提交位点
//
// Exactly-Once 保证：
//  1. Kafka 事务（begin/send/commit）保证消息原子可见
//  2. 先 commit Kafka 事务，后 save 位点到 SQLite
//  3. 若位点保存前崩溃 → 重启从旧位点重放 → 消费端幂等键去重
type SourcePipeline struct {
	source       core.ICdcSource
	producer     *transport.TransactionalProducer
	offsetStore  core.IOffsetStore
	metrics      *monitoring.Metrics
	batchSize    int
	batchTimeout time.Duration
	retryPolicy  RetryPolicy
	logger       *zap.Logger
}

// RetryPolicy 指数退避 + 随机抖动重连策略。
type RetryPolicy struct {
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	Multiplier float64
	Jitter     float64 // 比例，如 0.3 表示 ±30%
	MaxRetries int      // -1 = 无限
}

// Delay 计算第 attempt 次重试的等待时长。
func (r RetryPolicy) Delay(attempt int) time.Duration {
	d := float64(r.BaseDelay) * math.Pow(r.Multiplier, float64(attempt))
	if d > float64(r.MaxDelay) {
		d = float64(r.MaxDelay)
	}
	jitter := d * r.Jitter * (2*rand.Float64() - 1)
	return time.Duration(d + jitter)
}

// ShouldRetry 判断是否继续重试。
func (r RetryPolicy) ShouldRetry(attempt int) bool {
	return r.MaxRetries < 0 || attempt < r.MaxRetries
}

// DefaultRetryPolicy 默认重试策略（无限重试，最大 60s）。
var DefaultRetryPolicy = RetryPolicy{
	BaseDelay:  1 * time.Second,
	MaxDelay:   60 * time.Second,
	Multiplier: 2.0,
	Jitter:     0.3,
	MaxRetries: -1,
}

// NewSourcePipeline 创建采集端管道。
func NewSourcePipeline(
	source core.ICdcSource,
	producer *transport.TransactionalProducer,
	offsetStore core.IOffsetStore,
	metrics *monitoring.Metrics,
	batchSize int,
	batchTimeout time.Duration,
	logger *zap.Logger,
) *SourcePipeline {
	return &SourcePipeline{
		source:       source,
		producer:     producer,
		offsetStore:  offsetStore,
		metrics:      metrics,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		retryPolicy:  DefaultRetryPolicy,
		logger:       logger,
	}
}

// Run 启动采集主循环，阻塞直到 ctx 取消。
//
// 主循环逻辑：
//  1. 从 offsetStore 加载上次成功位点
//  2. 启动 CDC source 从该位点开始
//  3. 收集一批事件（batchSize 条 或 batchTimeout 超时）
//  4. begin Kafka 事务 → send 所有事件 → commit 事务
//  5. commit 成功后原子保存位点到 SQLite
//  6. 若 commit 失败 → abort → 从上次位点重试
func (p *SourcePipeline) Run(ctx context.Context) error {
	sourceID := p.source.SourceID()

	// 1. 加载上次位点
	pos, err := p.offsetStore.Load(sourceID)
	if err != nil {
		return fmt.Errorf("load offset for %s: %w", sourceID, err)
	}

	// 2. 启动 CDC 采集
	if err := p.source.Start(ctx, pos); err != nil {
		return fmt.Errorf("start source %s: %w", sourceID, err)
	}
	defer p.source.Close() //nolint:errcheck

	p.logger.Info("source pipeline started",
		zap.String("source_id", sourceID),
		zap.Bool("resuming", pos != nil && pos.IsValid()))

	// 3. 主事件循环
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("source pipeline stopping", zap.String("source_id", sourceID))
			return nil
		default:
		}

		batch, err := p.collectBatch(ctx)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			continue
		}

		if err := p.sendBatchWithRetry(ctx, batch); err != nil {
			return err
		}
	}
}

// collectBatch 收集最多 batchSize 条事件，或等待 batchTimeout 后返回。
func (p *SourcePipeline) collectBatch(ctx context.Context) ([]*core.ChangeEvent, error) {
	var batch []*core.ChangeEvent
	deadline := time.NewTimer(p.batchTimeout)
	defer deadline.Stop()

	for len(batch) < p.batchSize {
		select {
		case <-ctx.Done():
			return batch, nil

		case e, ok := <-p.source.Events():
			if !ok {
				// channel 已关闭，source 已停止
				return batch, nil
			}
			batch = append(batch, e)
			p.metrics.CdcEventsTotal.WithLabelValues(
				p.source.SourceID(), e.Table, string(e.OpType),
			).Inc()

		case err := <-p.source.Errors():
			p.logger.Error("source error", zap.String("source_id", p.source.SourceID()), zap.Error(err))
			p.metrics.CdcErrorsTotal.WithLabelValues(p.source.SourceID(), "source_error").Inc()
			return batch, err

		case <-deadline.C:
			return batch, nil
		}
	}
	return batch, nil
}

// sendBatchWithRetry 带重试地将一批事件通过 Kafka 事务发送，并原子提交位点。
//
// 原子性保证：
//   - Kafka commit 成功 → 消息对 read_committed 可见
//   - commit 成功后立即保存位点到 SQLite
//   - 若位点保存失败（崩溃）→ 重启后重放，消费端幂等键负责去重
func (p *SourcePipeline) sendBatchWithRetry(ctx context.Context, batch []*core.ChangeEvent) error {
	sourceID := p.source.SourceID()
	for attempt := 0; ; attempt++ {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		txnID, err := p.sendBatch(batch)
		if err == nil {
			// 成功：保存位点
			curPos := p.source.CurrentPosition()
			curPos.EventCount += int64(len(batch))
			if saveErr := p.offsetStore.Save(sourceID, curPos, txnID); saveErr != nil {
				// 位点保存失败不影响数据正确性（消费端幂等），记录告警
				p.logger.Error("offset save failed after commit",
					zap.String("source_id", sourceID),
					zap.String("txn_id", txnID),
					zap.Error(saveErr))
			}
			p.metrics.KafkaTxnTotal.WithLabelValues(sourceID, "committed").Inc()
			return nil
		}

		// 失败处理
		p.logger.Warn("kafka txn failed, will retry",
			zap.String("source_id", sourceID),
			zap.Int("attempt", attempt),
			zap.Error(err))
		p.metrics.KafkaTxnTotal.WithLabelValues(sourceID, "aborted").Inc()

		if !p.retryPolicy.ShouldRetry(attempt) {
			return fmt.Errorf("max retries exceeded for source %s: %w", sourceID, err)
		}

		delay := p.retryPolicy.Delay(attempt)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
}

// sendBatch 执行单次 Kafka 事务：begin → send all → commit/abort。
func (p *SourcePipeline) sendBatch(batch []*core.ChangeEvent) (txnID string, err error) {
	start := time.Now()

	if err := p.producer.BeginTxn(); err != nil {
		return "", err
	}

	for _, e := range batch {
		if sendErr := p.producer.Send(e); sendErr != nil {
			_ = p.producer.AbortTxn()
			return "", fmt.Errorf("%w: send event: %v", core.ErrKafkaProduce, sendErr)
		}
	}

	txnID, err = p.producer.CommitTxn()
	if err != nil {
		_ = p.producer.AbortTxn()
		return "", err
	}

	p.metrics.KafkaSendLatency.WithLabelValues(p.source.SourceID()).Observe(
		time.Since(start).Seconds(),
	)
	return txnID, nil
}
