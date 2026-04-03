// Package syncer 实现 PostgreSQL ODS → ClickHouse 增量同步。
//
// 设计要点：
//   - 使用 _cdc_updated_at 水位线追踪已同步位点，增量拉取新/变更行。
//   - 每次同步后持久化水位线，重启后从上次成功点续传。
//   - ClickHouse 目标表使用 ReplacingMergeTree，INSERT 幂等，重复执行安全。
package syncer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	chtypes "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/mimizh/med-insight/internal/config"
	"github.com/mimizh/med-insight/internal/metrics"
)

// SyncWorker 负责从 PostgreSQL ODS 增量拉取并写入 ClickHouse。
type SyncWorker struct {
	pg     *pgx.Conn
	ch     chtypes.Conn
	state  *SyncState
	cfg    config.SyncerConfig
	logger *zap.Logger
	m      *metrics.Metrics
}

// New 创建 SyncWorker。
func New(pg *pgx.Conn, ch chtypes.Conn, statePath string, cfg config.SyncerConfig, logger *zap.Logger, m *metrics.Metrics) *SyncWorker {
	return &SyncWorker{
		pg:     pg,
		ch:     ch,
		state:  newSyncState(statePath),
		cfg:    cfg,
		logger: logger,
		m:      m,
	}
}

// Run 启动定时同步循环，直到 ctx 取消。
func (w *SyncWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(w.cfg.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	w.logger.Info("syncer started", zap.Int("interval_s", w.cfg.IntervalSeconds))

	// 立即执行一次
	w.syncAll(ctx)

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("syncer stopped")
			return
		case <-ticker.C:
			w.syncAll(ctx)
		}
	}
}

// syncAll 同步所有 ODS 表。
// 水位线更新策略：使用批次中最后一行的 _cdc_updated_at，
// 而非 time.Now()，避免跳过批量大小内的剩余行。
func (w *SyncWorker) syncAll(ctx context.Context) {
	tables := []struct {
		odsTable string
		syncFn   func(context.Context, time.Time) (int, time.Time, error)
	}{
		{"ods_hospital_his_patients", w.syncPatients},
		{"ods_hospital_his_visits", w.syncVisits},
		{"ods_hospital_his_orders", w.syncOrders},
		{"ods_hospital_lis_lab_results", w.syncLabResults},
	}

	for _, t := range tables {
		watermark := w.state.Get(t.odsTable)
		start := time.Now()

		n, newWatermark, err := t.syncFn(ctx, watermark)
		if err != nil {
			w.logger.Error("sync failed", zap.String("table", t.odsTable), zap.Error(err))
			continue
		}

		if n > 0 {
			w.state.Set(t.odsTable, newWatermark)
			w.logger.Info("synced", zap.String("table", t.odsTable),
				zap.Int("rows", n), zap.Time("new_watermark", newWatermark))
		}

		elapsed := time.Since(start).Seconds()
		w.m.SyncRows.WithLabelValues(t.odsTable).Add(float64(n))
		w.m.SyncLag.WithLabelValues(t.odsTable).Set(elapsed)
	}
}

// syncPatients 增量同步患者表。
func (w *SyncWorker) syncPatients(ctx context.Context, since time.Time) (int, time.Time, error) {
	rows, err := w.pg.Query(ctx, `
		SELECT patient_id, name, gender, birth_date, address, created_at,
		       _cdc_source_id, _cdc_op_type, _cdc_updated_at
		FROM ods_hospital_his_patients
		WHERE _cdc_updated_at > $1
		ORDER BY _cdc_updated_at
		LIMIT $2`,
		since, w.cfg.BatchSize)
	if err != nil {
		return 0, since, fmt.Errorf("query patients: %w", err)
	}
	defer rows.Close()

	batch, err := w.ch.PrepareBatch(ctx, `INSERT INTO dim_patients`)
	if err != nil {
		return 0, since, fmt.Errorf("prepare batch: %w", err)
	}

	count := 0
	var lastTs time.Time
	for rows.Next() {
		var (
			patientID, name, gender, birthDate, address, createdAt string
			cdcSourceID, cdcOpType                                  string
			cdcUpdatedAt                                            time.Time
		)
		if err := rows.Scan(&patientID, &name, &gender, &birthDate, &address, &createdAt,
			&cdcSourceID, &cdcOpType, &cdcUpdatedAt); err != nil {
			return count, lastTs, err
		}
		if err := batch.Append(patientID, name, gender, birthDate, address, createdAt,
			cdcSourceID, cdcOpType, cdcUpdatedAt); err != nil {
			return count, lastTs, err
		}
		lastTs = cdcUpdatedAt
		count++
	}
	if count == 0 {
		_ = batch.Abort()
		return 0, since, nil
	}
	return count, lastTs, batch.Send()
}

// syncVisits 增量同步就诊表。
func (w *SyncWorker) syncVisits(ctx context.Context, since time.Time) (int, time.Time, error) {
	rows, err := w.pg.Query(ctx, `
		SELECT visit_id, patient_id, dept, doctor, visit_type,
		       admit_time::timestamptz, discharge_time::timestamptz,
		       diagnosis, created_at,
		       _cdc_source_id, _cdc_op_type, _cdc_updated_at
		FROM ods_hospital_his_visits
		WHERE _cdc_updated_at > $1
		ORDER BY _cdc_updated_at
		LIMIT $2`,
		since, w.cfg.BatchSize)
	if err != nil {
		return 0, since, fmt.Errorf("query visits: %w", err)
	}
	defer rows.Close()

	batch, err := w.ch.PrepareBatch(ctx, `INSERT INTO fact_visits`)
	if err != nil {
		return 0, since, fmt.Errorf("prepare batch: %w", err)
	}

	count := 0
	var lastTs time.Time
	for rows.Next() {
		var (
			visitID, patientID, dept, doctor, visitType, diagnosis, createdAt string
			admitTime, cdcUpdatedAt                                            time.Time
			dischargeTime                                                       *time.Time
			cdcSourceID, cdcOpType                                              string
		)
		if err := rows.Scan(&visitID, &patientID, &dept, &doctor, &visitType,
			&admitTime, &dischargeTime, &diagnosis, &createdAt,
			&cdcSourceID, &cdcOpType, &cdcUpdatedAt); err != nil {
			return count, lastTs, err
		}
		if err := batch.Append(visitID, patientID, dept, doctor, visitType,
			admitTime, dischargeTime, diagnosis, createdAt,
			cdcSourceID, cdcOpType, cdcUpdatedAt); err != nil {
			return count, lastTs, err
		}
		lastTs = cdcUpdatedAt
		count++
	}
	if count == 0 {
		_ = batch.Abort()
		return 0, since, nil
	}
	return count, lastTs, batch.Send()
}

// syncOrders 增量同步医嘱表。
func (w *SyncWorker) syncOrders(ctx context.Context, since time.Time) (int, time.Time, error) {
	rows, err := w.pg.Query(ctx, `
		SELECT order_id, visit_id, drug_name, dosage, frequency, route,
		       order_time::timestamptz, doctor, status, created_at,
		       _cdc_source_id, _cdc_op_type, _cdc_updated_at
		FROM ods_hospital_his_orders
		WHERE _cdc_updated_at > $1
		ORDER BY _cdc_updated_at
		LIMIT $2`,
		since, w.cfg.BatchSize)
	if err != nil {
		return 0, since, fmt.Errorf("query orders: %w", err)
	}
	defer rows.Close()

	batch, err := w.ch.PrepareBatch(ctx, `INSERT INTO fact_orders`)
	if err != nil {
		return 0, since, fmt.Errorf("prepare batch: %w", err)
	}

	count := 0
	var lastTs time.Time
	for rows.Next() {
		var (
			orderID, visitID, drugName, dosage, frequency, route string
			orderTime, cdcUpdatedAt                               time.Time
			doctor, status, createdAt                             string
			cdcSourceID, cdcOpType                                string
		)
		if err := rows.Scan(&orderID, &visitID, &drugName, &dosage, &frequency, &route,
			&orderTime, &doctor, &status, &createdAt,
			&cdcSourceID, &cdcOpType, &cdcUpdatedAt); err != nil {
			return count, lastTs, err
		}
		if err := batch.Append(orderID, visitID, drugName, dosage, frequency, route,
			orderTime, doctor, status, createdAt,
			cdcSourceID, cdcOpType, cdcUpdatedAt); err != nil {
			return count, lastTs, err
		}
		lastTs = cdcUpdatedAt
		count++
	}
	if count == 0 {
		_ = batch.Abort()
		return 0, since, nil
	}
	return count, lastTs, batch.Send()
}

// syncLabResults 增量同步检验结果表。
func (w *SyncWorker) syncLabResults(ctx context.Context, since time.Time) (int, time.Time, error) {
	rows, err := w.pg.Query(ctx, `
		SELECT result_id, visit_id, patient_id, item_code, item_name,
		       value::float8, unit, ref_range,
		       is_abnormal::bool,
		       result_time::timestamptz, report_time::timestamptz,
		       lab_section, created_at,
		       _cdc_source_id, _cdc_op_type, _cdc_updated_at
		FROM ods_hospital_lis_lab_results
		WHERE _cdc_updated_at > $1
		ORDER BY _cdc_updated_at
		LIMIT $2`,
		since, w.cfg.BatchSize)
	if err != nil {
		return 0, since, fmt.Errorf("query lab_results: %w", err)
	}
	defer rows.Close()

	batch, err := w.ch.PrepareBatch(ctx, `INSERT INTO fact_lab_results`)
	if err != nil {
		return 0, since, fmt.Errorf("prepare batch: %w", err)
	}

	count := 0
	var lastTs time.Time
	for rows.Next() {
		var (
			resultID, visitID, patientID, itemCode, itemName string
			value                                             *float64
			unit, refRange, labSection, createdAt            string
			isAbnormal                                        bool
			resultTime, cdcUpdatedAt                          time.Time
			reportTime                                        *time.Time
			cdcSourceID, cdcOpType                            string
		)
		if err := rows.Scan(&resultID, &visitID, &patientID, &itemCode, &itemName,
			&value, &unit, &refRange, &isAbnormal,
			&resultTime, &reportTime, &labSection, &createdAt,
			&cdcSourceID, &cdcOpType, &cdcUpdatedAt); err != nil {
			return count, lastTs, err
		}
		abnormalUint := uint8(0)
		if isAbnormal {
			abnormalUint = 1
		}
		if err := batch.Append(resultID, visitID, patientID, itemCode, itemName,
			value, unit, refRange, abnormalUint,
			resultTime, reportTime, labSection, createdAt,
			cdcSourceID, cdcOpType, cdcUpdatedAt); err != nil {
			return count, lastTs, err
		}
		lastTs = cdcUpdatedAt
		count++
	}
	if count == 0 {
		_ = batch.Abort()
		return 0, since, nil
	}
	return count, lastTs, batch.Send()
}

// ========== SyncState：水位线持久化 ==========

// SyncState 记录每张 ODS 表最后成功同步的时间水位线，持久化到 JSON 文件。
type SyncState struct {
	path string
	mu   sync.Mutex
	data map[string]time.Time
}

func newSyncState(path string) *SyncState {
	s := &SyncState{
		path: path,
		data: make(map[string]time.Time),
	}
	s.load()
	return s
}

// Get 返回指定表的上次同步水位线；若无记录则返回零值（同步全量）。
func (s *SyncState) Get(table string) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.data[table]
	if !ok {
		return time.Time{} // 零值触发全量同步
	}
	return t
}

// Set 更新水位线并持久化。
func (s *SyncState) Set(table string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[table] = t
	s.save()
}

func (s *SyncState) load() {
	b, err := os.ReadFile(s.path)
	if err != nil {
		return // 首次启动，文件不存在
	}
	_ = json.Unmarshal(b, &s.data)
}

func (s *SyncState) save() {
	b, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(s.path, b, 0644)
}
