-- PostgreSQL ODS 初始化脚本

-- 创建 ODS 用户和数据库
-- 去重表（由 Go 代码自动创建，此处备用）
CREATE TABLE IF NOT EXISTS _cdc_applied_events (
    idempotency_key  CHAR(64)     PRIMARY KEY,
    source_id        VARCHAR(64)  NOT NULL,
    table_name       VARCHAR(128) NOT NULL,
    applied_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cdc_applied_at ON _cdc_applied_events(applied_at);

-- ODS 目标表（由 Go 代码动态创建，此处预建供测试）
CREATE TABLE IF NOT EXISTS ods_hospital_his_patients (
    patient_id    TEXT PRIMARY KEY,
    name          TEXT,
    gender        TEXT,
    birth_date    TEXT,
    id_card       TEXT,
    phone         TEXT,
    address       TEXT,
    created_at    TEXT,
    updated_at    TEXT,
    _cdc_source_id   TEXT,
    _cdc_op_type     TEXT,
    _cdc_updated_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ods_hospital_his_visits (
    visit_id       TEXT PRIMARY KEY,
    patient_id     TEXT,
    dept           TEXT,
    doctor         TEXT,
    visit_type     TEXT,
    admit_time     TEXT,
    discharge_time TEXT,
    diagnosis      TEXT,
    created_at     TEXT,
    updated_at     TEXT,
    _cdc_source_id   TEXT,
    _cdc_op_type     TEXT,
    _cdc_updated_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ods_hospital_his_orders (
    order_id    TEXT PRIMARY KEY,
    visit_id    TEXT,
    drug_name   TEXT,
    dosage      TEXT,
    frequency   TEXT,
    route       TEXT,
    order_time  TEXT,
    doctor      TEXT,
    status      TEXT,
    created_at  TEXT,
    updated_at  TEXT,
    _cdc_source_id   TEXT,
    _cdc_op_type     TEXT,
    _cdc_updated_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ods_hospital_lis_lab_results (
    result_id    TEXT PRIMARY KEY,
    visit_id     TEXT,
    patient_id   TEXT,
    item_code    TEXT,
    item_name    TEXT,
    value        TEXT,
    unit         TEXT,
    ref_range    TEXT,
    is_abnormal  TEXT,
    result_time  TEXT,
    report_time  TEXT,
    lab_section  TEXT,
    created_at   TEXT,
    updated_at   TEXT,
    _cdc_source_id   TEXT,
    _cdc_op_type     TEXT,
    _cdc_updated_at  TIMESTAMPTZ
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ods_user;
GRANT USAGE ON SCHEMA public TO ods_user;

\echo 'PostgreSQL ODS initialized successfully'
