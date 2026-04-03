package engine

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/mimizh/hospital-cdc-platform/internal/config"
	"github.com/mimizh/hospital-cdc-platform/internal/core"
	"github.com/mimizh/hospital-cdc-platform/internal/monitoring"
	"github.com/mimizh/hospital-cdc-platform/internal/sink"
	"github.com/mimizh/hospital-cdc-platform/internal/transport"
)

// Coordinator 管理所有 SourcePipeline 和 ConsumerPipeline 的生命周期。
//
// 职责：
//   - 根据配置创建并启动各 pipeline
//   - 监听 SIGTERM/SIGINT 触发优雅关停
//   - 超时强制退出（30s）
//   - 统一错误处理和重启
type Coordinator struct {
	cfg         *config.Config
	offsetStore core.IOffsetStore
	metrics     *monitoring.Metrics
	logger      *zap.Logger
	recovery    *RecoveryManager
	cancelFuncs []context.CancelFunc
	wg          sync.WaitGroup
}

// NewCoordinator 创建协调器。
func NewCoordinator(
	cfg *config.Config,
	offsetStore core.IOffsetStore,
	metrics *monitoring.Metrics,
	logger *zap.Logger,
) *Coordinator {
	return &Coordinator{
		cfg:         cfg,
		offsetStore: offsetStore,
		metrics:     metrics,
		logger:      logger,
		recovery:    NewRecoveryManager(offsetStore, logger),
	}
}

// Run 启动完整数据中台，阻塞直到收到停止信号。
func (c *Coordinator) Run(ctx context.Context) error {
	// 信号处理
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 启动监控端点
	go c.metrics.Serve(c.cfg.Metrics.Port, c.logger)

	// 执行恢复规划
	sourceIDs := make([]string, len(c.cfg.Sources))
	for i, s := range c.cfg.Sources {
		sourceIDs[i] = s.ID
	}
	plans, err := c.recovery.Plan(runCtx, sourceIDs)
	if err != nil {
		return fmt.Errorf("recovery planning: %w", err)
	}

	// 启动 Sink（共享一个 PG 连接池）
	sinkWriter, err := sink.NewPostgresSinkWriter(
		runCtx,
		c.cfg.Sink.PGDSN,
		c.cfg.Sink.DedupRetentionHours,
		c.logger,
	)
	if err != nil {
		return fmt.Errorf("init sink: %w", err)
	}
	defer sinkWriter.Close() //nolint:errcheck

	// 启动消费端管道
	topics := collectTopics(c.cfg)
	consumer, err := transport.NewExactlyOnceConsumer(
		c.cfg.Kafka.BootstrapServers,
		"hospital-cdc-consumer",
		topics,
		c.logger,
	)
	if err != nil {
		return fmt.Errorf("init consumer: %w", err)
	}
	defer consumer.Close() //nolint:errcheck

	consumerPipeline := NewConsumerPipeline(
		consumer, sinkWriter, c.metrics,
		c.cfg.Pipeline.BatchSize,
		c.cfg.Pipeline.BatchTimeout,
		c.logger,
	)
	c.startPipeline(runCtx, "consumer", func(ctx context.Context) error {
		return consumerPipeline.Run(ctx)
	})

	// 启动各数据源采集管道
	router := transport.NewTopicRouter(c.cfg)
	for _, plan := range plans {
		src := c.buildSource(plan.SourceID)
		if src == nil {
			c.logger.Warn("source config not found", zap.String("source_id", plan.SourceID))
			continue
		}

		producer, err := transport.NewTransactionalProducer(
			c.cfg.Kafka.BootstrapServers,
			plan.SourceID,
			router,
			c.logger,
		)
		if err != nil {
			return fmt.Errorf("init producer for %s: %w", plan.SourceID, err)
		}

		pipeline := NewSourcePipeline(
			src, producer, c.offsetStore, c.metrics,
			c.cfg.Pipeline.BatchSize,
			c.cfg.Pipeline.BatchTimeout,
			c.logger,
		)

		sourceID := plan.SourceID
		c.startPipeline(runCtx, "source-"+sourceID, func(ctx context.Context) error {
			return pipeline.Run(ctx)
		})
	}

	c.logger.Info("coordinator running",
		zap.Int("sources", len(c.cfg.Sources)),
		zap.Int("topics", len(topics)))

	// 等待停止信号
	select {
	case sig := <-sigCh:
		c.logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
	case <-ctx.Done():
		c.logger.Info("context cancelled, shutting down")
	}

	// 优雅关停（最多 30s）
	c.logger.Info("graceful shutdown initiated")
	cancel()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.logger.Info("all pipelines stopped gracefully")
	case <-time.After(30 * time.Second):
		c.logger.Warn("shutdown timeout, forcing exit")
		os.Exit(1)
	}
	return nil
}

// startPipeline 在独立 goroutine 中运行管道，自动重启（直到 ctx 取消）。
func (c *Coordinator) startPipeline(ctx context.Context, name string, fn func(context.Context) error) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			c.logger.Info("starting pipeline", zap.String("name", name))
			if err := fn(ctx); err != nil {
				c.logger.Error("pipeline error, restarting",
					zap.String("name", name),
					zap.Error(err))
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
				}
			} else {
				return // 正常退出（ctx 已取消）
			}
		}
	}()
}

// buildSource 根据配置 ID 创建对应的 ICdcSource。
func (c *Coordinator) buildSource(sourceID string) core.ICdcSource {
	for _, s := range c.cfg.Sources {
		if s.ID != sourceID {
			continue
		}
		switch s.Type {
		case "mysql":
			return buildMySQLSource(s, c.logger)
		case "sqlserver":
			return buildSQLServerSource(s, c.logger)
		}
	}
	return nil
}

// collectTopics 从配置中收集所有唯一 topic 名称。
func collectTopics(cfg *config.Config) []string {
	seen := map[string]bool{}
	var topics []string
	for _, m := range cfg.Topics {
		if !seen[m.Topic] {
			seen[m.Topic] = true
			topics = append(topics, m.Topic)
		}
	}
	return topics
}
