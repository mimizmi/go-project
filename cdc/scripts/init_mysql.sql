-- MySQL 源库初始化脚本
-- 运行前确认：binlog_format=ROW, binlog_row_image=FULL

CREATE DATABASE IF NOT EXISTS hospital_his CHARACTER SET utf8mb4;
USE hospital_his;

-- CDC 专用用户
CREATE USER IF NOT EXISTS 'cdc_user'@'%' IDENTIFIED BY 'cdc_pass';
GRANT REPLICATION CLIENT, REPLICATION SLAVE, SELECT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;

-- ===== 患者表 =====
CREATE TABLE IF NOT EXISTS patients (
    patient_id   BIGINT AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(64)  NOT NULL,
    gender       CHAR(1)      NOT NULL COMMENT 'M/F',
    birth_date   DATE,
    id_card      VARCHAR(18)  COMMENT '身份证（脱敏字段）',
    phone        VARCHAR(20)  COMMENT '联系电话（脱敏字段）',
    address      VARCHAR(255),
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='患者基础信息';

-- ===== 就诊记录表 =====
CREATE TABLE IF NOT EXISTS visits (
    visit_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    patient_id   BIGINT       NOT NULL,
    dept         VARCHAR(64)  NOT NULL COMMENT '科室',
    doctor       VARCHAR(64),
    visit_type   VARCHAR(16)  NOT NULL DEFAULT 'outpatient' COMMENT 'outpatient/inpatient/emergency',
    admit_time   DATETIME     NOT NULL,
    discharge_time DATETIME,
    diagnosis    VARCHAR(512),
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_patient (patient_id),
    INDEX idx_admit_time (admit_time)
) ENGINE=InnoDB COMMENT='就诊记录';

-- ===== 医嘱表 =====
CREATE TABLE IF NOT EXISTS orders (
    order_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    visit_id     BIGINT       NOT NULL,
    drug_name    VARCHAR(128),
    dosage       VARCHAR(64),
    frequency    VARCHAR(32),
    route        VARCHAR(32)  COMMENT '给药途径',
    order_time   DATETIME     NOT NULL,
    doctor       VARCHAR(64),
    status       VARCHAR(16)  NOT NULL DEFAULT 'pending' COMMENT 'pending/executed/cancelled',
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_visit (visit_id)
) ENGINE=InnoDB COMMENT='医嘱';

-- 验证 binlog 配置
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';
SHOW MASTER STATUS;
