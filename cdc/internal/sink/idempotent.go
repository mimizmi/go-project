// Package sink 实现目标库 ODS 幂等写入。
package sink

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// IdempotencyGuard 通过 _cdc_applied_events 去重表实现消费端幂等。
//
// 写入流程（同一 PG 事务内执行）：
//  1. 检查 idempotency_key 是否已存在于去重表
//  2. 若已存在 → 跳过（返回 false）
//  3. 若不存在 → 执行业务 UPSERT + 插入去重记录（返回 true）
//
// 整个流程在调用方的 PG 事务中运行，保证业务写入与去重记录的原子性。
type IdempotencyGuard struct {
	retentionHours int
}

// NewIdempotencyGuard 创建幂等守卫。
func NewIdempotencyGuard(retentionHours int) *IdempotencyGuard {
	if retentionHours <= 0 {
		retentionHours = 168 // 默认 7 天
	}
	return &IdempotencyGuard{retentionHours: retentionHours}
}

// InitSchema 在目标 PG 中创建去重表（幂等，可重复调用）。
func (g *IdempotencyGuard) InitSchema(ctx context.Context, conn *pgx.Conn) error {
	_, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS _cdc_applied_events (
			idempotency_key  CHAR(64)     PRIMARY KEY,
			source_id        VARCHAR(64)  NOT NULL,
			table_name       VARCHAR(128) NOT NULL,
			applied_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_cdc_applied_at
			ON _cdc_applied_events(applied_at);
	`)
	return err
}

// IsDuplicate 在给定事务中检查幂等键是否已处理。
func (g *IdempotencyGuard) IsDuplicate(ctx context.Context, tx pgx.Tx, idempotencyKey string) (bool, error) {
	var exists bool
	err := tx.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM _cdc_applied_events WHERE idempotency_key = $1)`,
		idempotencyKey,
	).Scan(&exists)
	return exists, err
}

// MarkApplied 在给定事务中写入去重记录。
func (g *IdempotencyGuard) MarkApplied(ctx context.Context, tx pgx.Tx, idempotencyKey, sourceID, table string) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO _cdc_applied_events (idempotency_key, source_id, table_name, applied_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT DO NOTHING`,
		idempotencyKey, sourceID, table, time.Now().UTC(),
	)
	return err
}

// Cleanup 清理过期去重记录（应由定时任务调用）。
func (g *IdempotencyGuard) Cleanup(ctx context.Context, conn *pgx.Conn) (int64, error) {
	threshold := time.Now().UTC().Add(-time.Duration(g.retentionHours) * time.Hour)
	tag, err := conn.Exec(ctx,
		`DELETE FROM _cdc_applied_events WHERE applied_at < $1`, threshold,
	)
	if err != nil {
		return 0, fmt.Errorf("cleanup dedup table: %w", err)
	}
	return tag.RowsAffected(), nil
}
