package transport

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
)

// TransactionalProducer 封装 confluent-kafka 事务型 Producer。
//
// 关键配置：
//   - enable.idempotence = true
//   - transactional.id   = "cdc-producer-{sourceID}"（每个源唯一，epoch fencing 防旧事务）
//   - acks = all
//
// 使用流程：
//
//	BeginTxn() -> Send()... -> CommitTxn() 或 AbortTxn()
type TransactionalProducer struct {
	producer   *kafka.Producer
	serializer *Serializer
	router     *TopicRouter
	sourceID   string
	txnID      string // 最后一次事务的唯一标识（取 Kafka 内部 PID:epoch）
	logger     *zap.Logger
}

// NewTransactionalProducer 创建并初始化事务型 Producer。
func NewTransactionalProducer(
	bootstrapServers string,
	sourceID string,
	router *TopicRouter,
	logger *zap.Logger,
) (*TransactionalProducer, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":                     bootstrapServers,
		"transactional.id":                      "cdc-producer-" + sourceID,
		"enable.idempotence":                    true,
		"acks":                                  "all",
		"max.in.flight.requests.per.connection": 5,
		"retries":                               2147483647,
		"delivery.timeout.ms":                   120000,
		"transaction.timeout.ms":                120000,
	})
	if err != nil {
		return nil, fmt.Errorf("new kafka producer: %w", err)
	}

	// 初始化事务（注册 transactional.id，获取 PID + epoch）
	initCtx, initCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer initCancel()
	if err := p.InitTransactions(initCtx); err != nil {
		p.Close()
		return nil, fmt.Errorf("init transactions: %w", err)
	}

	tp := &TransactionalProducer{
		producer:   p,
		serializer: &Serializer{},
		router:     router,
		sourceID:   sourceID,
		logger:     logger,
	}

	// 使用 nil delivery channel 时必须手动排空 Events() 通道，
	// 否则投递报告积压会阻塞 librdkafka 后台线程，导致 Flush 永久挂起。
	go tp.drainEvents()

	return tp, nil
}

// drainEvents 持续读取 producer 事件通道，记录投递失败日志。
// 当 producer 关闭时（Events() 通道关闭）自动退出。
func (p *TransactionalProducer) drainEvents() {
	for e := range p.producer.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				p.logger.Warn("message delivery failed",
					zap.String("source_id", p.sourceID),
					zap.String("topic", *ev.TopicPartition.Topic),
					zap.Error(ev.TopicPartition.Error))
			}
		case kafka.Error:
			p.logger.Warn("producer error",
				zap.String("source_id", p.sourceID),
				zap.Error(ev))
		}
	}
}

// BeginTxn 开始一个新 Kafka 事务。
func (p *TransactionalProducer) BeginTxn() error {
	if err := p.producer.BeginTransaction(); err != nil {
		return fmt.Errorf("%w: begin: %v", core.ErrKafkaTxn, err)
	}
	return nil
}

// Send 在当前事务中发送一个变更事件。
func (p *TransactionalProducer) Send(e *core.ChangeEvent) error {
	payload, err := p.serializer.Serialize(e)
	if err != nil {
		return err
	}
	topic, partKey := p.router.Route(e)

	return p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(partKey),
		Value: payload,
		Headers: []kafka.Header{
			{Key: "source_id", Value: []byte(e.SourceID)},
			{Key: "op_type", Value: []byte(string(e.OpType))},
			{Key: "event_id", Value: []byte(e.EventID)},
		},
	}, nil) // nil = 不使用 delivery channel，依赖事务保证
}

// CommitTxn 提交当前事务，等待所有已发送消息交付确认。
// 返回事务标识字符串，用于与位点关联。
func (p *TransactionalProducer) CommitTxn() (txnID string, err error) {
	// 先刷新确保所有消息已进入 broker（超时与 delivery.timeout.ms 对齐）
	remaining := p.producer.Flush(120 * 1000) // 120s
	if remaining > 0 {
		abortCtx, abortCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer abortCancel()
		_ = p.producer.AbortTransaction(abortCtx)
		// Purge 清空 producer 内部队列，防止下次 retry 消息重复累积
		_ = p.producer.Purge(kafka.PurgeInFlight | kafka.PurgeQueue)
		return "", fmt.Errorf("%w: flush timeout, %d messages undelivered", core.ErrKafkaTxn, remaining)
	}

	commitCtx, commitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer commitCancel()
	if err := p.producer.CommitTransaction(commitCtx); err != nil {
		return "", fmt.Errorf("%w: commit: %v", core.ErrKafkaTxn, err)
	}

	// 使用时间戳作为轻量事务标识（生产环境可换为 PID:epoch）
	txnID = fmt.Sprintf("%s-%d", p.sourceID, time.Now().UnixNano())
	p.txnID = txnID
	p.logger.Debug("kafka txn committed",
		zap.String("source_id", p.sourceID),
		zap.String("txn_id", txnID))
	return txnID, nil
}

// AbortTxn 中止当前事务，所有已发送消息对 read_committed 消费者不可见。
func (p *TransactionalProducer) AbortTxn() error {
	abortCtx, abortCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer abortCancel()
	if err := p.producer.AbortTransaction(abortCtx); err != nil {
		return fmt.Errorf("%w: abort: %v", core.ErrKafkaTxn, err)
	}
	_ = p.producer.Purge(kafka.PurgeInFlight | kafka.PurgeQueue)
	p.logger.Warn("kafka txn aborted", zap.String("source_id", p.sourceID))
	return nil
}

// LastTxnID 返回最后一次成功 commit 的事务标识。
func (p *TransactionalProducer) LastTxnID() string { return p.txnID }

// Close 关闭 Producer，释放资源。
func (p *TransactionalProducer) Close() {
	p.producer.Close()
}
