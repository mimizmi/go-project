package sink

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
)

// PostgresSinkWriter 向 PostgreSQL ODS 幂等写入变更事件，实现 ISinkWriter。
//
// 每批事件在单个 PG 事务内处理：
//  1. 去重表检查（同事务）
//  2. 业务表 UPSERT / DELETE
//  3. 写入去重记录
//  4. COMMIT → 成功后由调用方 commit Kafka offset
//
// 若 PG 事务失败 → ROLLBACK → Kafka offset 不提交 → 下次重新消费 → 幂等键再次拦截
type PostgresSinkWriter struct {
	pool    *pgxpool.Pool
	dedup   *IdempotencyGuard
	logger  *zap.Logger
}

// NewPostgresSinkWriter 创建并初始化 PostgreSQL Sink。
func NewPostgresSinkWriter(ctx context.Context, dsn string, dedupRetentionHours int, logger *zap.Logger) (*PostgresSinkWriter, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: connect: %v", core.ErrSinkConnection, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("%w: ping: %v", core.ErrSinkConnection, err)
	}

	dedup := NewIdempotencyGuard(dedupRetentionHours)

	// 初始化去重表（用一个临时连接）
	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, err
	}
	defer conn.Release()
	if err := dedup.InitSchema(ctx, conn.Conn()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("init dedup schema: %w", err)
	}

	return &PostgresSinkWriter{pool: pool, dedup: dedup, logger: logger}, nil
}

// WriteBatch 在单个 PG 事务内幂等写入一批事件，返回实际写入数。
func (w *PostgresSinkWriter) WriteBatch(ctx context.Context, events []*core.ChangeEvent) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}

	conn, err := w.pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("%w: acquire conn: %v", core.ErrSinkWrite, err)
	}
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, fmt.Errorf("%w: begin tx: %v", core.ErrSinkWrite, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	written := 0
	for _, e := range events {
		key := e.IdempotencyKey()

		// 幂等检查
		dup, err := w.dedup.IsDuplicate(ctx, tx, key)
		if err != nil {
			return 0, fmt.Errorf("%w: dedup check: %v", core.ErrSinkWrite, err)
		}
		if dup {
			w.logger.Debug("duplicate event skipped",
				zap.String("source_id", e.SourceID),
				zap.String("table", e.Table),
				zap.String("key", key[:8]+"..."))
			continue
		}

		// 业务写入
		if err := w.applyEvent(ctx, tx, e); err != nil {
			return 0, fmt.Errorf("%w: apply event to %s: %v", core.ErrSinkWrite, e.Table, err)
		}

		// 记录去重
		if err := w.dedup.MarkApplied(ctx, tx, key, e.SourceID, e.Table); err != nil {
			return 0, fmt.Errorf("%w: mark applied: %v", core.ErrSinkWrite, err)
		}
		written++
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("%w: commit: %v", core.ErrSinkWrite, err)
	}
	return written, nil
}

// applyEvent 在事务内对单条事件执行 UPSERT 或 DELETE。
//
// 目标表命名规则：ods_{source_database}_{table}（统一前缀，避免跨库命名冲突）
// 若目标表不存在，会自动创建（仅学术原型，生产环境应提前建好表）。
func (w *PostgresSinkWriter) applyEvent(ctx context.Context, tx pgx.Tx, e *core.ChangeEvent) error {
	targetTable := fmt.Sprintf("ods_%s_%s", strings.ToLower(e.Database), strings.ToLower(e.Table))

	switch e.OpType {
	case core.OpDelete:
		return w.execDelete(ctx, tx, targetTable, e)
	case core.OpInsert, core.OpUpdate:
		return w.execUpsert(ctx, tx, targetTable, e)
	}
	return nil
}

// execUpsert 对 INSERT/UPDATE 执行幂等 UPSERT。
// 自动建表（仅原型），主键列取自 PrimaryKeys。
func (w *PostgresSinkWriter) execUpsert(ctx context.Context, tx pgx.Tx, table string, e *core.ChangeEvent) error {
	row := e.After
	if row == nil {
		row = e.Before // 降级
	}
	if len(row) == 0 {
		return nil
	}

	// 确保表存在
	if err := w.ensureTable(ctx, tx, table, row, e.PrimaryKeys); err != nil {
		return err
	}

	cols, vals, pkCols := buildUpsertArgs(row, e.PrimaryKeys)
	setClauses := make([]string, 0, len(cols))
	for _, c := range cols {
		if _, isPK := e.PrimaryKeys[c]; !isPK {
			setClauses = append(setClauses, fmt.Sprintf(`"%s" = EXCLUDED."%s"`, c, c))
		}
	}

	placeholders := make([]string, len(vals))
	for i := range vals {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	// 追加 CDC 元数据列
	cols = append(cols, "_cdc_source_id", "_cdc_op_type", "_cdc_updated_at")
	vals = append(vals, e.SourceID, string(e.OpType), time.Now().UTC())
	placeholders = append(placeholders, fmt.Sprintf("$%d", len(vals)-2), fmt.Sprintf("$%d", len(vals)-1), fmt.Sprintf("$%d", len(vals)))
	setClauses = append(setClauses,
		`"_cdc_source_id" = EXCLUDED."_cdc_source_id"`,
		`"_cdc_op_type" = EXCLUDED."_cdc_op_type"`,
		`"_cdc_updated_at" = EXCLUDED."_cdc_updated_at"`,
	)

	conflictCols := make([]string, 0, len(pkCols))
	for _, pk := range pkCols {
		conflictCols = append(conflictCols, fmt.Sprintf(`"%s"`, pk))
	}

	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = fmt.Sprintf(`"%s"`, c)
	}

	sql := fmt.Sprintf(
		`INSERT INTO "%s" (%s) VALUES (%s)
		 ON CONFLICT (%s) DO UPDATE SET %s`,
		table,
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(conflictCols, ", "),
		strings.Join(setClauses, ", "),
	)

	_, err := tx.Exec(ctx, sql, vals...)
	return err
}

// execDelete 对 DELETE 事件执行物理删除。
func (w *PostgresSinkWriter) execDelete(ctx context.Context, tx pgx.Tx, table string, e *core.ChangeEvent) error {
	if len(e.PrimaryKeys) == 0 {
		return nil
	}
	conds := make([]string, 0, len(e.PrimaryKeys))
	vals := make([]interface{}, 0, len(e.PrimaryKeys))
	i := 1
	for k, v := range e.PrimaryKeys {
		conds = append(conds, fmt.Sprintf(`"%s" = $%d`, k, i))
		vals = append(vals, v)
		i++
	}
	sql := fmt.Sprintf(`DELETE FROM "%s" WHERE %s`, table, strings.Join(conds, " AND "))
	_, err := tx.Exec(ctx, sql, vals...)
	return err
}

// ensureTable 若目标表不存在则动态创建（原型用途）。
func (w *PostgresSinkWriter) ensureTable(
	ctx context.Context,
	tx pgx.Tx,
	table string,
	row map[string]interface{},
	pks map[string]interface{},
) error {
	colDefs := make([]string, 0, len(row)+3)
	pkNames := make([]string, 0)
	for k := range pks {
		pkNames = append(pkNames, fmt.Sprintf(`"%s"`, k))
	}
	sort.Strings(pkNames)

	// 所有业务列用 TEXT（原型简化；生产应根据 schema 建表）
	seen := map[string]bool{}
	for k := range row {
		if seen[k] {
			continue
		}
		seen[k] = true
		colDefs = append(colDefs, fmt.Sprintf(`"%s" TEXT`, k))
	}
	colDefs = append(colDefs,
		`"_cdc_source_id" TEXT`,
		`"_cdc_op_type" TEXT`,
		`"_cdc_updated_at" TIMESTAMPTZ`,
	)

	pkClause := ""
	if len(pkNames) > 0 {
		pkClause = fmt.Sprintf(", PRIMARY KEY (%s)", strings.Join(pkNames, ", "))
	}

	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (%s%s)`,
		table, strings.Join(colDefs, ", "), pkClause)
	_, err := tx.Exec(ctx, sql)
	return err
}

// Flush 当前实现无写缓冲，此方法为接口占位。
func (w *PostgresSinkWriter) Flush(_ context.Context) error { return nil }

// Close 关闭连接池。
func (w *PostgresSinkWriter) Close() error {
	w.pool.Close()
	return nil
}

// -------------------------------------------------------------------
// 工具函数
// -------------------------------------------------------------------

// normalizeVal 将 Go 类型转换为 pgx 可写入 TEXT 列的格式。
// 所有目标列均为 TEXT，需将数值/布尔类型显式转为字符串。
func normalizeVal(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", val)
	}
}

// buildUpsertArgs 从行 map 提取有序列名、值和主键列名。
func buildUpsertArgs(
	row map[string]interface{},
	pks map[string]interface{},
) (cols []string, vals []interface{}, pkCols []string) {
	for k := range row {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	vals = make([]interface{}, len(cols))
	for i, c := range cols {
		vals[i] = normalizeVal(row[c])
	}
	for k := range pks {
		pkCols = append(pkCols, k)
	}
	sort.Strings(pkCols)
	return
}
