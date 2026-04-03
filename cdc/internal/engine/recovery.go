package engine

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
)

// RecoveryManager 启动时执行容错恢复流程。
//
// 恢复状态机（对应计划第六节）：
//  1. 加载 SQLite 中保存的位点
//  2. 若无位点 → 全新启动，从当前最新位点开始采集
//  3. 若有位点 → 验证最后一次 Kafka 事务状态
//     - 事务已 commit → 从已保存位点继续采集
//     - 事务未知/已 abort → 从已保存位点重试（可能重复，消费端幂等键处理）
type RecoveryManager struct {
	offsetStore core.IOffsetStore
	logger      *zap.Logger
}

// NewRecoveryManager 创建恢复管理器。
func NewRecoveryManager(offsetStore core.IOffsetStore, logger *zap.Logger) *RecoveryManager {
	return &RecoveryManager{offsetStore: offsetStore, logger: logger}
}

// RecoveryPlan 描述单个数据源的恢复方案。
type RecoveryPlan struct {
	SourceID   string
	Position   *core.OffsetPosition // nil = 全新启动
	IsResume   bool                 // true = 断点续传, false = 全新启动
	MayHaveDup bool                 // true = 可能有重复，需消费端幂等处理
}

// Plan 为所有已知数据源生成恢复方案。
func (r *RecoveryManager) Plan(ctx context.Context, sourceIDs []string) ([]*RecoveryPlan, error) {
	plans := make([]*RecoveryPlan, 0, len(sourceIDs))
	for _, id := range sourceIDs {
		plan, err := r.planForSource(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("recovery plan for %s: %w", id, err)
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

func (r *RecoveryManager) planForSource(_ context.Context, sourceID string) (*RecoveryPlan, error) {
	pos, err := r.offsetStore.Load(sourceID)
	if err != nil {
		return nil, err
	}

	// 无历史位点 → 全新启动
	if pos == nil || !pos.IsValid() {
		r.logger.Info("recovery: fresh start", zap.String("source_id", sourceID))
		return &RecoveryPlan{
			SourceID: sourceID,
			Position: nil,
			IsResume: false,
		}, nil
	}

	// 有位点 → 断点续传
	// 注：Kafka 的 epoch fencing 机制保证旧 Producer 的残留事务被 abort；
	//     即使最后一次事务 commit 状态不确定，从旧位点重放也是安全的（消费端幂等）。
	lastTxn, err := r.offsetStore.LastTxnID(sourceID)
	if err != nil {
		r.logger.Warn("cannot verify last txn, will replay from last position",
			zap.String("source_id", sourceID), zap.Error(err))
		return &RecoveryPlan{
			SourceID:   sourceID,
			Position:   pos,
			IsResume:   true,
			MayHaveDup: true,
		}, nil
	}

	r.logger.Info("recovery: resuming from saved position",
		zap.String("source_id", sourceID),
		zap.String("last_txn", lastTxn),
		zap.String("position", pos.ToJSON()))

	return &RecoveryPlan{
		SourceID:   sourceID,
		Position:   pos,
		IsResume:   true,
		MayHaveDup: true, // 保守策略：总假设可能有重复，由消费端幂等处理
	}, nil
}
