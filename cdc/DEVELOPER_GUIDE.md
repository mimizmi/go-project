# 医院异构系统实时数据中台 — 开发者文档

> 本文档面向后续对接开发，涵盖系统架构、模块说明、配置参考、开发规范和常见问题。

---

## 目录

1. [整体架构](#1-整体架构)
2. [技术栈](#2-技术栈)
3. [快速启动](#3-快速启动)
4. [目录结构](#4-目录结构)
5. [配置参考](#5-配置参考)
6. [核心模块详解](#6-核心模块详解)
   - 6.1 [数据模型 (core)](#61-数据模型-core)
   - 6.2 [CDC 采集层 (cdc)](#62-cdc-采集层-cdc)
   - 6.3 [Kafka 传输层 (transport)](#63-kafka-传输层-transport)
   - 6.4 [位点管理 (offset)](#64-位点管理-offset)
   - 6.5 [ODS 写入层 (sink)](#65-ods-写入层-sink)
   - 6.6 [管道编排 (engine)](#66-管道编排-engine)
   - 6.7 [监控指标 (monitoring)](#67-监控指标-monitoring)
7. [数据流全链路](#7-数据流全链路)
8. [Exactly-Once 语义实现](#8-exactly-once-语义实现)
9. [容错与恢复机制](#9-容错与恢复机制)
10. [扩展开发指南](#10-扩展开发指南)
    - 10.1 [新增数据源](#101-新增数据源)
    - 10.2 [新增 Sink 目标](#102-新增-sink-目标)
    - 10.3 [新增监控指标](#103-新增监控指标)
11. [CLI 工具参考](#11-cli-工具参考)
12. [脚本工具参考](#12-脚本工具参考)
13. [测试指南](#13-测试指南)
14. [监控与运维](#14-监控与运维)
15. [常见问题排查](#15-常见问题排查)
16. [已知限制与后续规划](#16-已知限制与后续规划)

---

## 1. 整体架构

```
                         ┌─────────────────────────────────────────────────────┐
                         │                   CDC Platform                       │
                         │                                                       │
  MySQL HIS              │  ┌──────────────┐      ┌───────────────────────────┐ │
  (hospital_his) ──────► │  │ MySQLCdcSource│      │  TransactionalProducer    │ │
  binlog row-mode         │  │  (canal)      │─────►│  (Kafka 事务)            │ │
                         │  └──────────────┘      └───────────┬───────────────┘ │
                         │                                     │                 │
  SQL Server LIS          │  ┌──────────────┐                  │ topics:         │
  (hospital_lis)  ──────► │  │SQLServerCdc   │                  │ cdc.his.*       │
  CT 表轮询                │  │Source (poll)  │─────►────────────┤ cdc.lis.*       │
                         │  └──────────────┘                  │                 │
                         │                                     ▼                 │
                         │  ┌─────────────────────────────────────────────────┐ │
                         │  │              Kafka (KRaft, port 9092)            │ │
                         │  │   cdc.his.patients / cdc.his.visits / ...        │ │
                         │  └─────────────────────┬───────────────────────────┘ │
                         │                        │ read_committed               │
                         │  ┌─────────────────────▼───────────────────────────┐ │
                         │  │           ExactlyOnceConsumer                    │ │
                         │  │     (ConsumerPipeline → PostgresSinkWriter)      │ │
                         │  └─────────────────────┬───────────────────────────┘ │
                         └───────────────────────────────────────────────────────┘
                                                  │ idempotent UPSERT
                                                  ▼
                                     PostgreSQL ODS (port 5433)
                                     hospital_ods
                                     ├── patients
                                     ├── visits
                                     ├── orders
                                     ├── lab_results
                                     └── _cdc_applied_events  ← 幂等去重表

  ┌────────────────────────────────────────────────────────────┐
  │  横切关注点                                                  │
  │  SQLite (data/offsets/offsets.db) ── 位点持久化             │
  │  Prometheus (port 9090)           ── 指标采集               │
  │  Grafana (port 3000)              ── 可视化大盘              │
  └────────────────────────────────────────────────────────────┘
```

**数据流方向（单向）：** 源库 → CDC Adapter → Kafka → Consumer → PostgreSQL ODS

---

## 2. 技术栈

| 组件 | 版本 | 用途 |
|------|------|------|
| Go | 1.22 | 主程序语言（CGO required for sqlite3） |
| confluent-kafka-go | v2.3.0 | Kafka 客户端（基于 librdkafka，支持事务） |
| go-mysql | v1.9.1 | MySQL binlog canal 解析 |
| go-mssqldb | v1.7.2 | SQL Server CDC 轮询驱动 |
| pgx/v5 | v5.5.5 | PostgreSQL 驱动（支持批量 COPY 和事务） |
| go-sqlite3 | v1.14.22 | 位点持久化（CGO，需要 gcc） |
| viper | v1.18.2 | YAML + 环境变量配置加载 |
| zap | v1.27.0 | 结构化日志 |
| prometheus/client_golang | v1.19.0 | 指标暴露 |
| cobra | v1.8.0 | CLI 框架 |
| MySQL | 8.0 | HIS 源库（binlog GTID 模式） |
| SQL Server | 2022 | LIS 源库（CDC 功能） |
| Kafka | 7.6.0 (KRaft) | 消息总线（无 ZooKeeper） |
| PostgreSQL | 16 | ODS 数据仓库 |
| Prometheus | latest | 时序指标存储 |
| Grafana | latest | 监控大盘 |

---

## 3. 快速启动

### 前置条件

```bash
# Linux / WSL Ubuntu 20.04+
go version       # 需要 1.22+
docker --version # Docker 24+
docker compose version # Compose v2+

# CGO 依赖（sqlite3 编译需要）
sudo apt-get install -y gcc libsqlite3-dev
```

### 一键启动

```bash
# 1. 克隆/进入项目目录
cd ~/project

# 2. 复制环境变量文件
cp .env.example .env
# 按需修改 .env 中的密码等配置

# 3. 启动全栈（约 60 秒完成初始化）
make up

# 4. 验证所有服务健康
docker compose ps
# 所有服务应显示 healthy 或 running

# 5. 访问控制台
# Grafana:  http://localhost:3000  (admin / admin)
# Kafka UI: http://localhost:8080
# 指标端点: http://localhost:8000/metrics
```

### 验证数据流

```bash
# 写入测试数据
make seed
# 默认: MySQL 10000 条 + SQL Server 5000 条

# 等待 30 秒后校验源目标一致性
make check
# 应输出: ALL CHECKS PASSED
```

### 停止服务

```bash
make down        # 停止并删除所有容器及数据卷
# 或
docker compose stop   # 仅停止，保留数据卷
```

---

## 4. 目录结构

```
project/
├── cmd/
│   ├── platform/
│   │   └── main.go          # 主程序入口：加载配置 → 初始化组件 → 启动 Coordinator
│   └── cli/
│       └── main.go          # 运维 CLI：offset list / offset reset
│
├── internal/                # 核心业务逻辑（不对外暴露）
│   ├── core/
│   │   ├── event.go         # ChangeEvent 数据模型
│   │   ├── offset.go        # OffsetPosition 模型
│   │   ├── interfaces.go    # ICdcSource / IOffsetStore / ISinkWriter 接口定义
│   │   └── errors.go        # 错误哨兵值
│   ├── cdc/
│   │   ├── mysql.go         # MySQL binlog canal 实现
│   │   ├── sqlserver.go     # SQL Server CT 表轮询实现
│   │   └── builder.go       # 原始行数据 → ChangeEvent 转换（含脱敏）
│   ├── transport/
│   │   ├── producer.go      # 事务型 Kafka 生产者
│   │   ├── consumer.go      # read_committed Kafka 消费者
│   │   ├── serializer.go    # ChangeEvent ↔ JSON 序列化
│   │   └── router.go        # 库表名 → Kafka topic + partition key 路由
│   ├── offset/
│   │   └── sqlite.go        # SQLite WAL 位点存储
│   ├── sink/
│   │   ├── postgres.go      # PostgreSQL 幂等 UPSERT 写入
│   │   └── idempotent.go    # _cdc_applied_events 去重守卫
│   ├── engine/
│   │   ├── pipeline.go      # SourcePipeline：采集→批次→Kafka 事务主循环
│   │   ├── consumer.go      # ConsumerPipeline：Kafka 消费→PG 写入主循环
│   │   ├── coordinator.go   # Coordinator：多源生命周期管理 + 优雅关停
│   │   ├── recovery.go      # RecoveryManager：重启后恢复规划
│   │   └── builder.go       # config.SourceConfig → ICdcSource 工厂
│   ├── monitoring/
│   │   └── metrics.go       # Prometheus Counter/Gauge/Histogram 定义
│   └── config/
│       └── config.go        # Viper 配置加载（YAML + 环境变量）
│
├── scripts/
│   ├── seed/main.go         # 测试数据生成器
│   ├── checker/main.go      # 源-目标一致性校验工具
│   ├── experiment/main.go   # 参数化实验执行器
│   ├── init_mysql.sql       # MySQL 建库建表（含 binlog 配置）
│   ├── init_sqlserver.sql   # SQL Server 建库建表（含 CDC 开启）
│   ├── init_postgres.sql    # PostgreSQL ODS 建表（含幂等去重表）
│   └── results/             # 实验结果输出目录
│
├── tests/
│   ├── unit/                # 纯单元测试（无外部依赖）
│   ├── integration/         # 集成测试（需 docker compose up，-tags=integration）
│   └── fault/               # 故障注入测试（-tags=fault）
│
├── configs/
│   ├── sources.yaml         # 数据源声明（ID、连接信息、表列表、脱敏字段）
│   └── topics.yaml          # Kafka topic 映射 + Sink 配置
│
├── docker/                  # 其他 Dockerfile（如 init-scripts 镜像）
├── grafana/                 # Grafana dashboard JSON + provisioning YAML
├── prometheus/
│   └── prometheus.yml       # Prometheus 抓取配置
├── data/
│   └── offsets/             # SQLite 数据库运行时文件（持久化卷）
│
├── docker-compose.yml       # 完整服务编排
├── Dockerfile               # 多阶段构建（builder + runtime）
├── Makefile                 # 构建/测试/实验快捷命令
├── go.mod / go.sum          # Go 依赖管理
└── .env / .env.example      # 环境变量配置模板
```

---

## 5. 配置参考

### 5.1 环境变量（.env）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MYSQL_HOST` | localhost | MySQL 主机地址 |
| `MYSQL_PORT` | 3306 | MySQL 端口 |
| `MYSQL_USER` | cdc_user | CDC 专用账号（需要 REPLICATION SLAVE 权限） |
| `MYSQL_PASSWORD` | cdc_pass | MySQL 密码 |
| `MYSQL_DATABASE` | hospital_his | 源数据库名 |
| `MYSQL_SERVER_ID` | 1001 | binlog replica server_id（全局唯一） |
| `SQLSERVER_HOST` | localhost | SQL Server 主机 |
| `SQLSERVER_PORT` | 1433 | SQL Server 端口 |
| `SQLSERVER_USER` | sa | SQL Server 用户 |
| `SQLSERVER_PASSWORD` | YourStrong!Pass123 | SQL Server 密码 |
| `SQLSERVER_DATABASE` | hospital_lis | 源数据库名 |
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka Broker 地址 |
| `KAFKA_TRANSACTION_TIMEOUT_MS` | 60000 | Kafka 事务超时（毫秒） |
| `PG_HOST` | localhost | PostgreSQL 主机 |
| `PG_PORT` | 5432 | PostgreSQL 端口 |
| `PG_USER` | ods_user | ODS 账号 |
| `PG_PASSWORD` | ods_pass | ODS 密码 |
| `PG_DATABASE` | hospital_ods | ODS 数据库名 |
| `OFFSET_STORE_PATH` | data/offsets/offsets.db | SQLite 位点文件路径 |
| `METRICS_PORT` | 8000 | Prometheus 指标暴露端口 |
| `LOG_LEVEL` | info | 日志级别（debug/info/warn/error） |
| `BATCH_SIZE` | 500 | 每批最大事件数 |
| `BATCH_TIMEOUT_MS` | 1000 | 批次超时（毫秒） |

### 5.2 sources.yaml（数据源声明）

```yaml
sources:
  - id: his_mysql_01           # 数据源唯一标识（全局唯一，用于位点键）
    type: mysql                # 类型: mysql | sqlserver
    host: ${MYSQL_HOST}        # 支持 ${ENV_VAR} 变量替换
    port: 3306
    user: ${MYSQL_USER}
    password: ${MYSQL_PASSWORD}
    database: hospital_his
    server_id: 1001            # [MySQL only] binlog replica ID，必须全局唯一
    tables:                    # 需要采集的表列表
      - patients
      - visits
      - orders
    mask_fields:               # 脱敏字段（采集侧替换为 ***，不进 Kafka）
      patients:
        - id_card
        - phone

  - id: lis_sqlserver_01
    type: sqlserver
    host: ${SQLSERVER_HOST}
    port: 1433
    user: ${SQLSERVER_USER}
    password: ${SQLSERVER_PASSWORD}
    database: hospital_lis
    tables:
      - lab_results

pipeline:
  batch_size: 500              # 批次大小（影响吞吐/延迟权衡）
  batch_timeout_ms: 1000       # 批次超时，不满 batch_size 时强制提交
  max_retries: -1              # -1 表示无限重试
  retry_base_delay_ms: 1000    # 重试初始等待（指数退避）
  retry_max_delay_ms: 60000    # 重试最大等待
```

### 5.3 topics.yaml（Kafka topic 与 Sink 配置）

```yaml
kafka:
  bootstrap_servers: ${KAFKA_BOOTSTRAP_SERVERS}
  transaction_timeout_ms: 60000

topics:
  - source_id: his_mysql_01        # 对应 sources.yaml 中的 id
    table: patients                # 表名
    topic: cdc.his.patients        # Kafka topic 名称（建议: cdc.{db}.{table}）
    partition_count: 3             # 分区数（影响并发消费）
    partition_key_field: patient_id # 用于 partition key 的字段（保证同一记录有序）

  - source_id: his_mysql_01
    table: visits
    topic: cdc.his.visits
    partition_count: 3
    partition_key_field: visit_id

  - source_id: his_mysql_01
    table: orders
    topic: cdc.his.orders
    partition_count: 3
    partition_key_field: order_id

  - source_id: lis_sqlserver_01
    table: lab_results
    topic: cdc.lis.lab_results
    partition_count: 3
    partition_key_field: result_id

default_topic_pattern: "cdc.{database}.{table}"  # 未显式配置时的 topic 命名规则

sink:
  pg_dsn: "postgres://${PG_USER}:${PG_PASSWORD}@${PG_HOST}:5432/${PG_DATABASE}?sslmode=disable"
  dedup_retention_hours: 168   # 幂等去重记录保留时长（7天）
```

---

## 6. 核心模块详解

### 6.1 数据模型 (core)

**ChangeEvent** — 平台内部统一数据载体：

```go
// internal/core/event.go
type ChangeEvent struct {
    SourceID  string                 // 数据源 ID（对应 sources.yaml[].id）
    Database  string                 // 源库名
    Table     string                 // 源表名
    Operation string                 // "insert" | "update" | "delete"
    Before    map[string]interface{} // 变更前的行数据（update/delete 有值）
    After     map[string]interface{} // 变更后的行数据（insert/update 有值）
    Timestamp time.Time              // 事件发生时间
    LSN       string                 // 逻辑序列号（MySQL: binlog pos; SS: LSN）
}

// 幂等键：SHA-256(sourceID + table + operation + LSN + primaryKey)
func (e *ChangeEvent) IdempotencyKey() string

// 分区键：用于 Kafka 消息路由（保证同 ID 的消息落到同一分区）
func (e *ChangeEvent) PartitionKey(field string) string
```

**OffsetPosition** — 位点：

```go
// internal/core/offset.go
type OffsetPosition struct {
    SourceID   string    // 数据源 ID
    // MySQL: 使用 BinlogFile + BinlogPos + GTIDSet
    BinlogFile string
    BinlogPos  uint32
    GTIDSet    string
    // SQL Server: 使用 LSN（十六进制字符串）
    LSN        string
    UpdatedAt  time.Time
}
```

**核心接口：**

```go
// internal/core/interfaces.go

// ICdcSource: CDC 采集适配器接口
type ICdcSource interface {
    Start(ctx context.Context, startPos *OffsetPosition) (<-chan *ChangeEvent, error)
    Close() error
}

// IOffsetStore: 位点持久化接口
type IOffsetStore interface {
    Save(ctx context.Context, pos *OffsetPosition) error
    Load(ctx context.Context, sourceID string) (*OffsetPosition, error)
}

// ISinkWriter: ODS 写入接口
type ISinkWriter interface {
    Write(ctx context.Context, events []*ChangeEvent) error
    Close() error
}
```

### 6.2 CDC 采集层 (cdc)

#### MySQL CDC（`internal/cdc/mysql.go`）

- 使用 `go-mysql` canal，以 **binlog ROW 模式**监听变更
- 启动时从 SQLite 读取 `BinlogFile:BinlogPos`，断点续传
- 支持 GTID 模式（MySQL 8.0 默认启用）
- 将 canal 的 `RowsEvent` 转换为 `ChangeEvent`

**MySQL 数据库必须配置：**
```ini
# my.cnf
[mysqld]
server-id       = 1
log_bin         = /var/lib/mysql/mysql-bin
binlog_format   = ROW
binlog_row_image = FULL
gtid_mode       = ON
enforce_gtid_consistency = ON
```

**CDC 账号权限：**
```sql
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'cdc_pass';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
```

#### SQL Server CDC（`internal/cdc/sqlserver.go`）

- **轮询模式**：每秒查询 `cdc.{capture_instance}_CT` 变更跟踪表
- 查询语句：`SELECT TOP 1000 ... FROM cdc.dbo_lab_results_CT WHERE __$start_lsn > @lastLSN ORDER BY __$start_lsn`
- 使用 `__$operation`（1=delete, 2=insert, 4=update after image）映射操作类型
- LSN 以十六进制字符串持久化

**SQL Server 开启 CDC：**
```sql
-- 在数据库级别开启
EXEC sys.sp_cdc_enable_db;

-- 在表级别开启
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name   = N'lab_results',
    @role_name     = NULL;
```

#### EventBuilder（`internal/cdc/builder.go`）

负责将各 CDC 源的原始行数据（`map[string]interface{}`）标准化为 `ChangeEvent`：
- 字段脱敏：对 `mask_fields` 中配置的字段，值替换为 `"***"`
- 类型归一化说明：SQL Server `BIT` 列经 go-mssqldb 驱动扫描为 `int64`（非 `bool`），在 `postgres.go` 的 `normalizeVal()` 中统一处理

### 6.3 Kafka 传输层 (transport)

#### TransactionalProducer（`internal/transport/producer.go`）

```
使用流程:
BeginTransaction()
  → for each event: Send(event)
CommitTransaction()  or  AbortTransaction()
```

- 每个数据源独立一个 `TransactionalProducer`，`transactional.id` 为 `cdc-producer-{sourceID}`
- **epoch fencing**：相同 `transactional.id` 的新实例启动会自动 fence 旧僵尸实例
- **drainEvents() goroutine**：必须持续消费 `producer.Events()` channel，否则 librdkafka 内部队列满后 `Flush()` 阻塞死锁

#### ExactlyOnceConsumer（`internal/transport/consumer.go`）

- `isolation.level = read_committed`：只读取已提交事务的消息，自动过滤 abort 事务消息
- 手动提交 offset（在 PG 写入成功后）

#### TopicRouter（`internal/transport/router.go`）

- 根据 `ChangeEvent.SourceID` + `Table` 查找 `topics.yaml` 中的映射
- 计算 `partition = hash(partitionKey) % partitionCount`，保证同一主键的变更有序

### 6.4 位点管理 (offset)

`internal/offset/sqlite.go` — SQLite WAL 模式实现：

```sql
-- 存储结构
CREATE TABLE offsets (
    source_id    TEXT PRIMARY KEY,
    binlog_file  TEXT,
    binlog_pos   INTEGER,
    gtid_set     TEXT,
    lsn          TEXT,
    updated_at   DATETIME
);
```

**提交顺序（原子性保证）：**
1. Kafka `CommitTransaction()` 成功
2. 再调用 `offsetStore.Save()`

最坏情况（Kafka commit 成功但 SQLite save 失败）：重启后会从上一个位点重放，消费端幂等键去重。

### 6.5 ODS 写入层 (sink)

#### PostgresSinkWriter（`internal/sink/postgres.go`）

核心写入逻辑（每批事件在单个 PG 事务内完成）：

```go
// 伪代码
tx.Begin()
for each event in batch:
    if idempotencyGuard.IsDuplicate(tx, event.IdempotencyKey()):
        skip
    upsert(tx, event)       // INSERT ... ON CONFLICT DO UPDATE
    idempotencyGuard.Record(tx, event.IdempotencyKey())
tx.Commit()
```

**UPSERT 语句（以 patients 表为例）：**
```sql
INSERT INTO patients (patient_id, name, gender, ...)
VALUES ($1, $2, $3, ...)
ON CONFLICT (patient_id) DO UPDATE SET
    name = EXCLUDED.name,
    gender = EXCLUDED.gender,
    ...
    _cdc_source = EXCLUDED._cdc_source,
    _cdc_ts = EXCLUDED._cdc_ts
```

**类型归一化（`normalizeVal`）：** PostgreSQL TEXT 列要求所有值为字符串类型，Go 中非 string/[]byte/nil 类型（如 SQL Server 扫描出的 `int64`）统一用 `fmt.Sprintf("%v", val)` 转换。

#### IdempotencyGuard（`internal/sink/idempotent.go`）

```sql
-- 去重表结构
CREATE TABLE _cdc_applied_events (
    idempotency_key TEXT PRIMARY KEY,
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 定期清理过期记录（默认保留 168 小时）
DELETE FROM _cdc_applied_events WHERE applied_at < NOW() - INTERVAL '168 hours';
```

### 6.6 管道编排 (engine)

#### SourcePipeline（`internal/engine/pipeline.go`）

采集端主循环（每个数据源一个 goroutine）：

```
loop:
  1. 从 CDC source channel 批量读取事件（最多 batch_size 条 或 等待 batch_timeout）
  2. producer.BeginTransaction()
  3. for each event: producer.Send(event)
  4. producer.CommitTransaction()  // 失败则 AbortTransaction() + 重试
  5. offsetStore.Save(lastEventLSN)
  6. 更新 Prometheus 指标
```

#### ConsumerPipeline（`internal/engine/consumer.go`）

消费端主循环（全局共享一个，订阅所有 topics）：

```
loop:
  1. consumer.Poll(batch) → 读取一批消息
  2. sinkWriter.Write(batch) → PG 幂等写入（含去重）
  3. consumer.CommitOffsets()
  4. 更新指标
  5. 出错则等待 2s 重试（topic 还未创建时也触发此路径）
```

#### Coordinator（`internal/engine/coordinator.go`）

- 启动时调用 `RecoveryManager.Plan()` 确定各源是否需要从保守位点重放
- 为每个数据源创建独立的 `SourcePipeline` goroutine
- 所有 pipeline 出错后自动重启（5s 冷却）
- 监听 `SIGTERM`/`SIGINT`，触发优雅关停（最多等待 30s）

#### RecoveryManager（`internal/engine/recovery.go`）

重启后的恢复规划：
- `MayHaveDup=false`：从 SQLite 记录的精确位点继续（正常路径）
- `MayHaveDup=true`：保守策略，从上一个位点稍前重放（消费端幂等处理重复）

### 6.7 监控指标 (monitoring)

所有指标通过 `http://localhost:8000/metrics` 暴露（Prometheus 格式）：

| 指标名 | 类型 | Labels | 说明 |
|--------|------|--------|------|
| `cdc_events_total` | Counter | source_id, table, operation | CDC 采集事件总数 |
| `cdc_lag_seconds` | Gauge | source_id | CDC 采集延迟（估算） |
| `kafka_txn_total` | Counter | source_id, status(committed/aborted) | Kafka 事务统计 |
| `kafka_send_total` | Counter | source_id, topic | Kafka 消息发送总数 |
| `sink_events_total` | Counter | table, operation | ODS 写入事件总数 |
| `sink_duplicates_total` | Counter | table | 被幂等键拦截的重复数 |
| `sink_write_duration_seconds` | Histogram | — | ODS 写入延迟分布 |
| `e2e_latency_seconds` | Histogram | source_id | 端到端延迟（源写入→ODS完成） |
| `offset_save_total` | Counter | source_id, status | 位点保存统计 |

---

## 7. 数据流全链路

以 MySQL 插入一条 `patients` 记录为例，完整的端到端流程：

```
1. 应用写入 MySQL
   INSERT INTO patients (name, gender) VALUES ('张三', 'M');
   ↓
2. MySQL 写入 binlog (ROW 格式)
   binlog event: Table_map + Write_rows
   ↓
3. MySQLCdcSource.canal 监听 binlog
   canal.RowsEvent → EventBuilder.Build()
   脱敏处理（id_card → "***"）
   → ChangeEvent{op:"insert", after:{name:"张三", gender:"M"}}
   ↓
4. SourcePipeline 批次积累
   收集至 batch_size=500 或 超时 1000ms
   ↓
5. TransactionalProducer
   BeginTransaction()
   Send(event) → 序列化为 JSON → 路由到 cdc.his.patients partition-0
   CommitTransaction()
   ↓
6. SQLite 位点保存
   offsetStore.Save({binlog_file, binlog_pos})
   ↓
7. ExactlyOnceConsumer (read_committed)
   Poll() → 仅读取已提交事务消息
   ↓
8. ConsumerPipeline → PostgresSinkWriter
   IdempotencyGuard 查重（_cdc_applied_events）
   INSERT INTO patients ... ON CONFLICT DO UPDATE
   记录幂等键
   PG 事务 COMMIT
   ↓
9. Kafka offset 提交
   consumer.CommitOffsets()
   ↓
10. Prometheus 指标更新
    e2e_latency_seconds.Observe(time.Since(event.Timestamp))
```

---

## 8. Exactly-Once 语义实现

### 生产端 EOS

```
关键配置（librdkafka）:
  transactional.id        = "cdc-producer-{sourceID}"   ← 跨重启保持一致
  acks                    = all                          ← 等所有副本确认
  enable.idempotence      = true                         ← 自动去重（Kafka 侧）
  max.in.flight.requests  = 1                           ← 防止乱序

事务流程:
  BeginTransaction()
    ├── Send() × N    ← 消息写入 Kafka，但标记为"未提交"
    └── CommitTransaction()  ← 原子提交，消费端 read_committed 才可见
                   或
    └── AbortTransaction()   ← 回滚，消费端永远不可见

Epoch Fencing（僵尸防护）:
  同一 transactional.id 的新实例启动后，旧实例的 CommitTransaction() 会报错
  → 旧实例自动 abort，避免双写
```

### 消费端 EOS

```
consumer 配置:
  isolation.level = read_committed   ← 只读取已提交的消息

写入 PG 时：
  BEGIN TRANSACTION
    ├── SELECT FROM _cdc_applied_events WHERE key = $1  ← 查重
    ├── INSERT INTO {table} ... ON CONFLICT DO UPDATE    ← 幂等写
    └── INSERT INTO _cdc_applied_events (key) VALUES ($1) ← 记录
  COMMIT

  ← 以上原子完成后，再 CommitOffsets() 到 Kafka
```

### 位点与 EOS 的关系

```
位点保存顺序（故意的）:
  Kafka CommitTransaction() 成功
    └── SQLite Save() 成功
           └── 正常继续

  若 SQLite Save() 失败（罕见）:
    重启后从旧位点重放 → 消息重新发到 Kafka → 消费端幂等键去重
    → 最终一致，无数据丢失，无数据多写（对应用层）
```

---

## 9. 容错与恢复机制

### 自动重启

所有 pipeline goroutine 由 `Coordinator.startPipeline()` 管理：
- 出错后等待 5 秒自动重启
- 接收到 `ctx.Done()` 时正常退出

### 优雅关停

```bash
# 发送 SIGTERM（docker compose stop 会发送此信号）
kill -TERM <pid>

# 关停流程：
# 1. 等待当前批次的 Kafka 事务完成
# 2. 等待当前 PG 写入事务完成
# 3. 关闭所有 pipeline（最多 30 秒）
# 4. 超时则 os.Exit(1)
```

### 故障场景分析

| 故障点 | 重启后行为 | 数据安全 |
|--------|-----------|---------|
| CDC 采集崩溃 | 从 SQLite 位点续传 | 最多重发一批，消费端去重 |
| Kafka 事务未提交 | librdkafka 自动 abort | 无影响，从当前位点继续 |
| PG 写入失败 | ConsumerPipeline 重试（2s后） | Kafka offset 未提交，消息重新处理 |
| SQLite 写入失败 | 重放上一批次 | 消费端幂等键去重，无重复 |
| Kafka Broker 宕机 | 生产者重试，指数退避（1s→60s） | 事务未提交，消息不丢 |
| PG 宕机 | 消费端重试，背压到 Kafka | Kafka 消息积压，恢复后继续 |

---

## 10. 扩展开发指南

### 10.1 新增数据源

**步骤 1：实现 `ICdcSource` 接口**

```go
// internal/cdc/oracle.go（示例）
package cdc

import (
    "context"
    "github.com/mimizh/hospital-cdc-platform/internal/core"
    "go.uber.org/zap"
)

type OracleCdcSource struct {
    cfg    config.SourceConfig
    logger *zap.Logger
}

func NewOracleCdcSource(cfg config.SourceConfig, logger *zap.Logger) *OracleCdcSource {
    return &OracleCdcSource{cfg: cfg, logger: logger}
}

// Start 开始监听变更，返回只读 channel
func (s *OracleCdcSource) Start(ctx context.Context, startPos *core.OffsetPosition) (<-chan *core.ChangeEvent, error) {
    ch := make(chan *core.ChangeEvent, 1000)
    go func() {
        defer close(ch)
        // 实现 Oracle LogMiner 或 GoldenGate 逻辑
        // 将变更转换为 ChangeEvent 发送到 ch
    }()
    return ch, nil
}

func (s *OracleCdcSource) Close() error {
    return nil
}
```

**步骤 2：在 builder.go 中注册**

```go
// internal/engine/builder.go
func buildOracleSource(cfg config.SourceConfig, logger *zap.Logger) core.ICdcSource {
    return cdc.NewOracleCdcSource(cfg, logger)
}
```

在 `Coordinator.buildSource()` 中添加 case：

```go
case "oracle":
    return buildOracleSource(s, c.logger)
```

**步骤 3：在 sources.yaml 中声明**

```yaml
sources:
  - id: oracle_his_01
    type: oracle
    host: ${ORACLE_HOST}
    port: 1521
    # ...
    tables:
      - prescriptions
```

**步骤 4：在 topics.yaml 中添加 topic 映射**

```yaml
topics:
  - source_id: oracle_his_01
    table: prescriptions
    topic: cdc.oracle.prescriptions
    partition_count: 3
    partition_key_field: prescription_id
```

**步骤 5：在 PostgreSQL 中建对应 ODS 表**

```sql
-- scripts/init_postgres.sql
CREATE TABLE IF NOT EXISTS prescriptions (
    prescription_id BIGINT PRIMARY KEY,
    patient_id      BIGINT,
    -- ...
    _cdc_source     TEXT,
    _cdc_ts         TIMESTAMPTZ
);
```

### 10.2 新增 Sink 目标

实现 `ISinkWriter` 接口即可替换 PostgreSQL：

```go
// internal/sink/elasticsearch.go（示例）
type ElasticsearchSinkWriter struct { /* ... */ }

func (w *ElasticsearchSinkWriter) Write(ctx context.Context, events []*core.ChangeEvent) error {
    // 批量写入 ES，事件级别幂等（以 IdempotencyKey 为文档 ID）
    for _, e := range events {
        doc := map[string]interface{}{
            "_id":       e.IdempotencyKey(),
            "operation": e.Operation,
            "data":      e.After,
            "timestamp": e.Timestamp,
        }
        // ES bulk index...
    }
    return nil
}

func (w *ElasticsearchSinkWriter) Close() error { return nil }
```

在 `Coordinator` 中替换 `sink.NewPostgresSinkWriter(...)` 调用即可。

### 10.3 新增监控指标

```go
// internal/monitoring/metrics.go
// 在 Metrics 结构体中添加：
MyCustomCounter *prometheus.CounterVec

// 在 NewMetrics() 中注册：
m.MyCustomCounter = promauto.NewCounterVec(
    prometheus.CounterOpts{
        Name: "my_custom_total",
        Help: "My custom metric description",
    },
    []string{"label1", "label2"},
)

// 在业务代码中使用：
metrics.MyCustomCounter.WithLabelValues("val1", "val2").Inc()
```

---

## 11. CLI 工具参考

```bash
# 构建 CLI
CGO_ENABLED=1 go build -o bin/cdc-cli ./cmd/cli

# 查看所有数据源的当前位点
./bin/cdc-cli offset list
# 输出示例:
# SOURCE ID           BINLOG FILE              POS       LSN              UPDATED AT
# his_mysql_01        mysql-bin.000003         4521      —                2026-03-18 04:48
# lis_sqlserver_01    —                        —         0x00000025...    2026-03-18 04:48

# 重置指定数据源位点（触发全量重新同步）
./bin/cdc-cli offset reset his_mysql_01
# 注意：重置后需要先清空 ODS 对应表，再重启 platform，否则产生重复数据

# 指定 SQLite 文件路径
./bin/cdc-cli --db /custom/path/offsets.db offset list
```

**重置位点的完整操作流程：**

```bash
# 1. 停止 platform
docker compose stop cdc-platform

# 2. 清空 ODS 目标表（如需全量重同步）
docker compose exec postgres psql -U ods_user -d hospital_ods \
  -c "TRUNCATE TABLE patients, visits, orders RESTART IDENTITY;"

# 3. 清空幂等去重表
docker compose exec postgres psql -U ods_user -d hospital_ods \
  -c "TRUNCATE TABLE _cdc_applied_events;"

# 4. 重置位点
./bin/cdc-cli offset reset his_mysql_01

# 5. 重启 platform
docker compose start cdc-platform
```

---

## 12. 脚本工具参考

### 数据生成器（seed）

```bash
# MySQL 写入 10000 条（insert 模式）
go run ./scripts/seed/main.go --source mysql --records 10000

# SQL Server 写入 5000 条
go run ./scripts/seed/main.go --source sqlserver --records 5000

# 混合操作（60% insert, 30% update, 10% delete）
go run ./scripts/seed/main.go --source mysql --records 5000 \
  --mix insert:60,update:30,delete:10

# 持续写入模式（每秒 100 条，用于压测/演示）
go run ./scripts/seed/main.go --source mysql --records 100000 \
  --continuous --rate 100

# 指定目标表
go run ./scripts/seed/main.go --source mysql --records 1000 \
  --tables patients,visits
```

**生成的表和数据类型：**

| 表 | 生成内容 |
|----|---------|
| patients | 姓名（患者N）、性别（M/F）、出生日期（随机1950-2010）、地址 |
| visits | 关联 patient_id、科室（内/外/儿等）、医生（医生N%50）、就诊类型、诊断 |
| orders | 关联 visit_id、药品名（6种常用药）、剂量（50-500mg）、用法、状态 |
| lab_results | visit_id、patient_id、检验项目（CBC001-CBC019）、数值、是否异常 |

### 一致性校验工具（checker）

```bash
# 校验所有默认表
go run ./scripts/checker/main.go --tables patients,visits,lab_results,orders

# 输出示例（正常情况）:
# [patients]  src=10000  dst=10000  diffs=0  dups=0  ✓ PASS
# [visits]    src=2341   dst=2341   diffs=0  dups=0  ✓ PASS
# [orders]    src=1823   dst=1823   diffs=0  dups=0  ✓ PASS
# [lab_results] src=5000 dst=5000  diffs=0  dups=0  ✓ PASS
# ALL CHECKS PASSED
```

### 实验执行器（experiment）

```bash
# 正确性实验（写入10000条，等待同步，校验一致性）
go run ./scripts/experiment/main.go --type correctness --records 10000
# 结果: scripts/results/correctness_*.json

# EOS 对比实验（分别测 eos 和 at_least_once 模式的重复率）
go run ./scripts/experiment/main.go --type eos --mode eos --records 5000
go run ./scripts/experiment/main.go --type eos --mode at_least_once --records 5000
# 结果: scripts/results/eos_*.json

# 性能基准（batch × partitions 矩阵测试）
go run ./scripts/experiment/main.go --type performance \
  --batch-sizes 100,500,1000 --partitions 1,3,6
# 结果: scripts/results/perf_*.csv（含吞吐/P50/P95/P99）
```

**已记录的性能实验结果（2026-03-18）：**

| batch_size | partitions | 吞吐 (events/s) | P50 延迟 | P99 延迟 |
|-----------|-----------|----------------|---------|---------|
| 100 | 1 | ~18,000 | 210ms | 890ms |
| 500 | 3 | ~89,000 | 78ms | 412ms |
| 1000 | 6 | 161,472 | 44ms | 294ms |

---

## 13. 测试指南

### 单元测试（无外部依赖）

```bash
make test-unit
# 或
go test ./tests/unit/... -v -count=1
```

测试覆盖：
- `EventBuilder` 字段脱敏逻辑
- `normalizeVal()` 类型转换（含 SQL Server int64 → TEXT）
- `TopicRouter` 路由逻辑
- `Serializer` JSON 序列化/反序列化
- `OffsetPosition` 序列化

### 集成测试（需运行 docker compose up）

```bash
INTEGRATION_FULL=1 make test-integration
# 或
go test ./tests/integration/... -v -count=1 -tags=integration
```

测试覆盖：
- MySQL → Kafka → PostgreSQL 全链路写入
- SQL Server CDC → Kafka → PostgreSQL 全链路
- 幂等写入（同一事件写入两次，ODS 只有一条记录）
- offset 持久化与恢复

### 故障注入测试（需运行 docker compose up）

```bash
FAULT_TESTS=1 make test-fault
# 或
go test ./tests/fault/... -v -count=1 -tags=fault -timeout=300s
```

测试场景：
- `source_crash`：模拟 MySQL 连接中断，验证重连后数据不丢
- Kafka broker 短暂宕机后的 EOS 保证
- PG 连接中断后的重试与幂等

---

## 14. 监控与运维

### Grafana 大盘

访问 `http://localhost:3000`（admin/admin），自动导入以下大盘：

- **CDC Overview**：各源的采集速率、延迟、Kafka 事务成功率
- **ODS Sink**：写入速率、重复率、写入延迟分布
- **E2E Latency**：端到端 P50/P95/P99 延迟

### 关键告警指标

| 场景 | 监控指标 | 告警阈值建议 |
|------|---------|------------|
| CDC 采集停滞 | `rate(cdc_events_total[5m]) == 0` | 5分钟无采集 |
| Kafka 事务失败率高 | `rate(kafka_txn_total{status="aborted"}[5m]) > 0.01` | 1% 失败率 |
| ODS 写入延迟高 | `sink_write_duration_seconds{quantile="0.99"} > 5` | P99 > 5秒 |
| 端到端延迟高 | `e2e_latency_seconds{quantile="0.95"} > 10` | P95 > 10秒 |
| 幂等重复率异常 | `rate(sink_duplicates_total[5m]) > 0` | 有重复出现 |

### 日志查看

```bash
# 实时日志
make logs
# 或
docker compose logs -f cdc-platform

# 过滤错误日志
docker compose logs cdc-platform 2>&1 | grep '"level":"error"'

# 查看所有服务日志
docker compose logs --tail=50
```

### 数据库直连

```bash
# MySQL
docker compose exec mysql mysql -uroot -proot hospital_his

# SQL Server
docker compose exec sqlserver /opt/mssql-tools/bin/sqlcmd \
  -S localhost -U sa -P 'YourStrong!Pass123' -d hospital_lis

# PostgreSQL ODS
docker compose exec postgres psql -U ods_user -d hospital_ods

# 查看 ODS 各表记录数
docker compose exec postgres psql -U ods_user -d hospital_ods \
  -c "SELECT 'patients' AS t, COUNT(*) FROM patients
      UNION ALL SELECT 'visits', COUNT(*) FROM visits
      UNION ALL SELECT 'orders', COUNT(*) FROM orders
      UNION ALL SELECT 'lab_results', COUNT(*) FROM lab_results;"
```

### Kafka 管理

```bash
# 通过 Kafka UI: http://localhost:8080

# 命令行查看 topics
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# 查看 topic 详情
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic cdc.his.patients

# 查看 consumer group 消费进度（lag）
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group hospital-cdc-consumer

# 手动删除 topic（重置实验环境时使用）
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic cdc.his.patients
```

---

## 15. 常见问题排查

### Q1: `make seed` 报 `go: command not found`

```bash
# Go 未在 PATH 中，使用完整路径
export PATH=/usr/local/go/bin:$PATH && make seed
```

### Q2: `unable to encode X into text format for text (OID 25)`

**原因：** SQL Server `BIT` 类型列经 go-mssqldb 扫描为 `int64`，pgx 无法直接写入 TEXT 列。

**已修复位置：** `internal/sink/postgres.go` 的 `normalizeVal()` 函数 — 所有非 string/nil 类型统一用 `fmt.Sprintf("%v", val)` 转换。

**如果出现类似错误（新的类型）：** 在 `normalizeVal()` 中添加对应 case。

### Q3: `Unknown topic or partition`

**原因：** Consumer 在数据源产生数据之前启动，topic 尚未创建（topic 由第一条消息触发 auto-create）。

**解决：** 无需处理，ConsumerPipeline 出错后 2s 自动重试，待 topic 创建后自动恢复。

### Q4: 源目标计数不一致（如 src=52521, dst=22521）

**原因：** 平台启动前已有历史数据在 MySQL，而 CDC 从当前 binlog 位点开始，历史数据未被捕获。

**解决：全量重置流程：**

```bash
# 1. 停止 platform
docker compose stop cdc-platform

# 2. 清空 MySQL 数据
docker compose exec mysql mysql -uroot -proot hospital_his \
  -e "TRUNCATE TABLE orders; TRUNCATE TABLE visits; TRUNCATE TABLE patients;"

# 3. 清空 ODS 数据
docker compose exec postgres psql -U ods_user -d hospital_ods \
  -c "TRUNCATE TABLE orders, visits, patients, lab_results, _cdc_applied_events;"

# 4. 删除 Kafka topics
for t in cdc.his.patients cdc.his.visits cdc.his.orders cdc.lis.lab_results; do
  docker compose exec kafka kafka-topics \
    --bootstrap-server localhost:9092 --delete --topic $t
done

# 5. 清空 SQLite 位点
rm -f data/offsets/offsets.db

# 6. 重启 platform，重新写入数据
docker compose start cdc-platform
make seed
```

### Q5: `docker compose build --no-cache` 失败（SSL/网络错误）

**原因：** `--no-cache` 强制重新 `go mod download`，需要外网访问。

**解决：** 使用 `docker compose up -d --build`（复用缓存的 Go module 层）。

### Q6: Kafka 事务超时 (`transaction.max.timeout.ms`)

**现象：** 日志中出现 `transaction.timeout.ms ... exceeds the broker-configured value`。

**解决：**

```bash
# 方式 1：调低 topics.yaml 中的 transaction_timeout_ms（小于 Broker 的 15 分钟默认值）
# 当前配置 60000ms 应正常，无需修改

# 方式 2：如果调高 Broker 侧配置
docker compose exec kafka kafka-configs --bootstrap-server localhost:9092 \
  --entity-type brokers --entity-default \
  --alter --add-config transaction.max.timeout.ms=900000
```

### Q7: PostgreSQL `ON CONFLICT` 不生效（每次都插入新行）

**原因：** ODS 表没有正确设置 PRIMARY KEY。

**检查：**

```sql
\d patients  -- 查看表结构，确认有 PRIMARY KEY
```

如缺失，手动添加：

```sql
ALTER TABLE patients ADD PRIMARY KEY (patient_id);
```

### Q8: 幂等去重表（`_cdc_applied_events`）过大

**症状：** PostgreSQL 磁盘占用异常增大。

**处理：** 去重表默认保留 168 小时，由 `IdempotencyGuard` 定期清理。如需手动清理：

```sql
DELETE FROM _cdc_applied_events WHERE applied_at < NOW() - INTERVAL '24 hours';
VACUUM ANALYZE _cdc_applied_events;
```

---

## 16. 已知限制与后续规划

### 当前限制

| 限制 | 说明 |
|------|------|
| 单 Consumer 实例 | 所有 topic 由一个 ConsumerPipeline 串行处理，高吞吐场景建议改为多实例并行 |
| SQL Server 轮询延迟 | CT 表轮询间隔 1s，最坏情况下延迟略高于 binlog 实时推送 |
| 无 DDL 变更同步 | 仅支持 DML（INSERT/UPDATE/DELETE），不支持 ALTER TABLE 等 DDL |
| ODS 表须预先建好 | `postgres.go` 的 UPSERT 语句是动态生成的，依赖 ODS 表已存在 |
| 无背压控制 | 当 PG 写入慢时，Kafka consumer 积压依靠 Kafka 分区缓冲，无显式背压 |

### 后续可扩展方向

1. **多 Consumer 实例**：按 topic 维度分拆 ConsumerPipeline，提升消费并行度
2. **支持 Oracle LogMiner**：实现 `OracleCdcSource`，接入 Oracle 数据库
3. **DDL 变更传播**：解析 DDL binlog 事件，自动 ALTER ODS 表结构
4. **Schema Registry 集成**：将 Avro/Protobuf 替换当前 JSON 序列化，减少消息体积
5. **多 ODS 目标**：支持同时写入 ES、ClickHouse、数据湖等多目标
6. **Web 管理界面**：可视化管理数据源配置、位点、告警规则
7. **水平扩展**：当前单节点部署，可通过 Kafka 分区数 + Consumer Group 实现水平扩展

---

*最后更新：2026-03-18*
