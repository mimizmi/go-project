// Package clickhouse 封装 ClickHouse 连接与建表 DDL。
package clickhouse

// DDL 语句列表，按依赖顺序执行。
// 设计说明：
//   - dim_patients / fact_visits / fact_orders / fact_lab_results：ReplacingMergeTree，
//     使用 _cdc_updated_at 作为版本列，保证 ODS 同步时幂等更新。
//   - v_visit_wide：ClickHouse VIEW，将患者、就诊、医嘱关联为"主题宽表"，
//     简化查询复杂度，所有通过此视图的查询共享统一指标口径。
//   - agg_dept_daily_visits + mv_dept_daily_visits：科室日门诊量预聚合（SummingMergeTree + MATERIALIZED VIEW），
//     将实时聚合转为"查询即取"，目标降低 Top-K 报表延迟至亚秒级。
//   - agg_drug_daily_orders + mv_drug_daily_orders：药品日消耗预聚合。
//   - agg_patient_monthly + mv_patient_monthly：月度患者统计预聚合。
var schemaDDL = []string{
	// ===== 维度表：患者 =====
	`CREATE TABLE IF NOT EXISTS dim_patients (
		patient_id    String,
		name          String,
		gender        String,
		birth_date    String,
		address       String,
		created_at    String,
		_cdc_source_id  String,
		_cdc_op_type    String,
		_cdc_updated_at DateTime64(3, 'UTC')
	) ENGINE = ReplacingMergeTree(_cdc_updated_at)
	PARTITION BY tuple()
	ORDER BY patient_id
	SETTINGS index_granularity = 8192`,

	// ===== 事实表：就诊 =====
	`CREATE TABLE IF NOT EXISTS fact_visits (
		visit_id       String,
		patient_id     String,
		dept           String,
		doctor         String,
		visit_type     String,
		admit_time     DateTime64(3, 'UTC'),
		discharge_time Nullable(DateTime64(3, 'UTC')),
		diagnosis      String,
		created_at     String,
		_cdc_source_id   String,
		_cdc_op_type     String,
		_cdc_updated_at  DateTime64(3, 'UTC')
	) ENGINE = ReplacingMergeTree(_cdc_updated_at)
	PARTITION BY toYYYYMM(admit_time)
	ORDER BY (dept, admit_time, visit_id)
	SETTINGS index_granularity = 8192`,

	// ===== 事实表：医嘱 =====
	`CREATE TABLE IF NOT EXISTS fact_orders (
		order_id    String,
		visit_id    String,
		drug_name   String,
		dosage      String,
		frequency   String,
		route       String,
		order_time  DateTime64(3, 'UTC'),
		doctor      String,
		status      String,
		created_at  String,
		_cdc_source_id   String,
		_cdc_op_type     String,
		_cdc_updated_at  DateTime64(3, 'UTC')
	) ENGINE = ReplacingMergeTree(_cdc_updated_at)
	PARTITION BY toYYYYMM(order_time)
	ORDER BY (drug_name, order_time, order_id)
	SETTINGS index_granularity = 8192`,

	// ===== 事实表：检验结果 =====
	`CREATE TABLE IF NOT EXISTS fact_lab_results (
		result_id    String,
		visit_id     String,
		patient_id   String,
		item_code    String,
		item_name    String,
		value        Nullable(Float64),
		unit         String,
		ref_range    String,
		is_abnormal  UInt8,
		result_time  DateTime64(3, 'UTC'),
		report_time  Nullable(DateTime64(3, 'UTC')),
		lab_section  String,
		created_at   String,
		_cdc_source_id   String,
		_cdc_op_type     String,
		_cdc_updated_at  DateTime64(3, 'UTC')
	) ENGINE = ReplacingMergeTree(_cdc_updated_at)
	PARTITION BY toYYYYMM(result_time)
	ORDER BY (lab_section, result_time, result_id)
	SETTINGS index_granularity = 8192`,

	// ===== 主题宽表视图（统一查询口径）=====
	// 将三张表的常用维度提前整合，简化报表查询复杂度。
	// 注：此为 ClickHouse VIEW，查询时实时 JOIN；
	//     高频查询通过下方预聚合表或缓存进一步加速。
	`CREATE OR REPLACE VIEW v_visit_wide AS
	SELECT
		v.visit_id,
		v.patient_id,
		p.name          AS patient_name,
		p.gender,
		p.birth_date,
		v.dept,
		v.doctor        AS visit_doctor,
		v.visit_type,
		v.admit_time,
		v.discharge_time,
		v.diagnosis,
		dateDiff('day', toDate(p.birth_date), toDate(v.admit_time)) / 365 AS age_at_visit,
		dateDiff('hour', v.admit_time, coalesce(v.discharge_time, now64())) AS los_hours
	FROM fact_visits AS v
	LEFT JOIN dim_patients AS p ON v.patient_id = p.patient_id`,

	// ===== 预聚合目标表：科室日就诊统计 =====
	// SummingMergeTree 自动将相同排序键的行合并求和，
	// 查询时 SELECT sum() 即可，无需扫描明细。
	`CREATE TABLE IF NOT EXISTS agg_dept_daily_visits (
		visit_date   Date,
		dept         String,
		visit_type   String,
		visit_count  UInt64,
		patient_count UInt64
	) ENGINE = SummingMergeTree((visit_count, patient_count))
	PARTITION BY toYYYYMM(visit_date)
	ORDER BY (dept, visit_date, visit_type)`,

	// ===== 物化视图：科室日就诊量 =====
	// 每次 INSERT 到 fact_visits 时自动触发，增量写入 agg_dept_daily_visits。
	`CREATE MATERIALIZED VIEW IF NOT EXISTS mv_dept_daily_visits
	TO agg_dept_daily_visits AS
	SELECT
		toDate(admit_time)     AS visit_date,
		dept,
		visit_type,
		count()                AS visit_count,
		uniq(patient_id)       AS patient_count
	FROM fact_visits
	GROUP BY visit_date, dept, visit_type`,

	// ===== 预聚合目标表：药品日消耗统计 =====
	`CREATE TABLE IF NOT EXISTS agg_drug_daily_orders (
		order_date      Date,
		drug_name       String,
		total_orders    UInt64,
		executed_orders UInt64,
		cancelled_orders UInt64
	) ENGINE = SummingMergeTree((total_orders, executed_orders, cancelled_orders))
	PARTITION BY toYYYYMM(order_date)
	ORDER BY (drug_name, order_date)`,

	// ===== 物化视图：药品日消耗 =====
	`CREATE MATERIALIZED VIEW IF NOT EXISTS mv_drug_daily_orders
	TO agg_drug_daily_orders AS
	SELECT
		toDate(order_time)                         AS order_date,
		drug_name,
		count()                                    AS total_orders,
		countIf(status = 'executed')               AS executed_orders,
		countIf(status = 'cancelled')              AS cancelled_orders
	FROM fact_orders
	GROUP BY order_date, drug_name`,

	// ===== 预聚合目标表：月度患者量统计 =====
	`CREATE TABLE IF NOT EXISTS agg_patient_monthly (
		year_month   String,
		dept         String,
		visit_type   String,
		patient_count UInt64,
		visit_count   UInt64
	) ENGINE = SummingMergeTree((patient_count, visit_count))
	PARTITION BY year_month
	ORDER BY (year_month, dept, visit_type)`,

	// ===== 物化视图：月度患者量 =====
	`CREATE MATERIALIZED VIEW IF NOT EXISTS mv_patient_monthly
	TO agg_patient_monthly AS
	SELECT
		formatDateTime(admit_time, '%Y-%m') AS year_month,
		dept,
		visit_type,
		uniq(patient_id)                    AS patient_count,
		count()                             AS visit_count
	FROM fact_visits
	GROUP BY year_month, dept, visit_type`,

	// ===== 同步水位线状态表 =====
	`CREATE TABLE IF NOT EXISTS _sync_state (
		table_name       String,
		last_synced_at   DateTime64(3, 'UTC'),
		rows_synced      UInt64,
		updated_at       DateTime64(3, 'UTC')
	) ENGINE = ReplacingMergeTree(updated_at)
	ORDER BY table_name`,
}
