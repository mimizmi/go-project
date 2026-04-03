// Package query 实现缓存感知的 ClickHouse 查询引擎。
//
// 查询路径（由快到慢）：
//
//	L1 本地缓存 → L2 Redis → ClickHouse
//
// 所有查询通过 cache.Manager 走统一缓存路径，保证防击穿/雪崩/穿透策略一致生效。
package query

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/mimizh/med-insight/internal/cache"
	"github.com/mimizh/med-insight/internal/metrics"
)

// Engine 缓存感知查询引擎。
type Engine struct {
	ch      driver.Conn
	cache   *cache.Manager
	metrics *metrics.Metrics
}

// New 创建 Engine。
func New(ch driver.Conn, cache *cache.Manager, m *metrics.Metrics) *Engine {
	return &Engine{ch: ch, cache: cache, metrics: m}
}

// ─────────────────────────────────────────────────────────────────────────────
// 数据传输对象（DTO）
// ─────────────────────────────────────────────────────────────────────────────

// PatientRow 患者明细。
type PatientRow struct {
	PatientID string `json:"patient_id"`
	Name      string `json:"name"`
	Gender    string `json:"gender"`
	BirthDate string `json:"birth_date"`
	Address   string `json:"address"`
}

// VisitRow 就诊明细。
type VisitRow struct {
	VisitID    string    `json:"visit_id"`
	PatientID  string    `json:"patient_id"`
	Dept       string    `json:"dept"`
	Doctor     string    `json:"doctor"`
	VisitType  string    `json:"visit_type"`
	AdmitTime  time.Time `json:"admit_time"`
	Diagnosis  string    `json:"diagnosis"`
}

// ─────────────────────────────────────────────────────────────────────────────
// 查询方法（查明细，供 FHIR Gateway 消费）
// ─────────────────────────────────────────────────────────────────────────────

// GetPatient 按 ID 查询患者详情。
func (e *Engine) GetPatient(ctx context.Context, patientID string) (*PatientRow, error) {
	key := fmt.Sprintf("fhir:patient:%s", patientID)
	e.cache.RegisterKey(key)

	raw, err := e.cache.Get(ctx, key, func() (interface{}, error) {
		return e.fetchPatient(ctx, patientID)
	})
	if err == cache.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var p PatientRow
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func (e *Engine) fetchPatient(ctx context.Context, patientID string) (interface{}, error) {
	row := e.ch.QueryRow(ctx, `
		SELECT patient_id, name, gender, birth_date, address
		FROM dim_patients FINAL
		WHERE patient_id = ?
		LIMIT 1`, patientID)

	var p PatientRow
	if err := row.Scan(&p.PatientID, &p.Name, &p.Gender, &p.BirthDate, &p.Address); err != nil {
		return nil, fmt.Errorf("fetchPatient: %w", err)
	}
	return &p, nil
}

// GetVisit 按 ID 查询就诊记录（供 FHIR Encounter 资源映射）。
func (e *Engine) GetVisit(ctx context.Context, visitID string) (*VisitRow, error) {
	key := fmt.Sprintf("fhir:encounter:%s", visitID)
	e.cache.RegisterKey(key)

	raw, err := e.cache.Get(ctx, key, func() (interface{}, error) {
		return e.fetchVisit(ctx, visitID)
	})
	if err == cache.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var v VisitRow
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

func (e *Engine) fetchVisit(ctx context.Context, visitID string) (interface{}, error) {
	row := e.ch.QueryRow(ctx, `
		SELECT visit_id, patient_id, dept, doctor, visit_type, admit_time, diagnosis
		FROM fact_visits FINAL
		WHERE visit_id = ?
		LIMIT 1`, visitID)

	var v VisitRow
	if err := row.Scan(&v.VisitID, &v.PatientID, &v.Dept, &v.Doctor, &v.VisitType, &v.AdmitTime, &v.Diagnosis); err != nil {
		return nil, fmt.Errorf("fetchVisit: %w", err)
	}
	return &v, nil
}

