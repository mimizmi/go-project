-- SQL Server 源库初始化脚本
-- 需要 SA 或 sysadmin 权限执行

USE master;
GO

-- 创建数据库
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'hospital_lis')
    CREATE DATABASE hospital_lis;
GO

USE hospital_lis;
GO

-- 启用数据库级 CDC
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'hospital_lis' AND is_cdc_enabled = 1)
BEGIN
    EXEC sys.sp_cdc_enable_db;
    PRINT 'CDC enabled on hospital_lis';
END
GO

-- ===== 检验结果表 =====
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'lab_results')
BEGIN
    CREATE TABLE dbo.lab_results (
        result_id    BIGINT IDENTITY(1,1) PRIMARY KEY,
        visit_id     BIGINT        NOT NULL,
        patient_id   BIGINT        NOT NULL,
        item_code    VARCHAR(32)   NOT NULL,
        item_name    VARCHAR(128)  NOT NULL,
        value        DECIMAL(18,4),
        unit         VARCHAR(32),
        ref_range    VARCHAR(64),
        is_abnormal  BIT           NOT NULL DEFAULT 0,
        result_time  DATETIME2     NOT NULL,
        report_time  DATETIME2,
        lab_section  VARCHAR(64),
        created_at   DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        updated_at   DATETIME2     NOT NULL DEFAULT GETUTCDATE()
    );
    PRINT 'Table lab_results created';
END
GO

-- 为 lab_results 启用 CDC
IF NOT EXISTS (
    SELECT 1 FROM cdc.change_tables
    WHERE source_object_id = OBJECT_ID('dbo.lab_results')
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N'lab_results',
        @role_name     = NULL,
        @supports_net_changes = 1;
    PRINT 'CDC enabled on lab_results';
END
GO

-- CDC 用户（只读权限）
IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = 'cdc_reader')
BEGIN
    CREATE LOGIN cdc_reader WITH PASSWORD = 'CdcReader!Pass123';
    CREATE USER cdc_reader FOR LOGIN cdc_reader;
    GRANT SELECT ON SCHEMA::cdc TO cdc_reader;
    GRANT SELECT ON SCHEMA::dbo TO cdc_reader;
    GRANT EXECUTE ON sys.fn_cdc_get_all_changes_dbo_lab_results TO cdc_reader;
    PRINT 'CDC reader user created';
END
GO

-- 验证 CDC 状态
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'hospital_lis';
SELECT source_schema, source_table, capture_instance FROM cdc.change_tables;
GO
