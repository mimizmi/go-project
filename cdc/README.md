# 医院异构系统实时数据中台

**多数据库 CDC 采集 · Exactly-Once 投递 · 增量位点管理**

---

## 架构概览

```
MySQL (binlog)  ──┐
                  ├──► CDC Adapter ──► Kafka (事务) ──► Consumer ──► PostgreSQL ODS
SQL Server (CDC)──┘      │                                │
                         ▼                                ▼
                   SQLite 位点存储              幂等去重表(_cdc_applied_events)
                         │                                │
                    Prometheus ◄──────────────────────────┘
                         │
                    Grafana Dashboard
```

### 关键设计决策

| 问题 | 方案 |
|------|------|
| 异构 CDC 采集 | MySQL: go-mysql canal (binlog ROW); SQL Server: 轮询 `cdc.fn_cdc_get_all_changes` |
| 端到端 Exactly-Once | 生产端: Kafka 事务 (begin/commit/abort) + epoch fencing; 消费端: `read_committed` + PG 事务内幂等 UPSERT + 去重表 |
| 位点原子提交 | 先 Kafka commit → 后 SQLite WAL save（最坏情况重放，消费端幂等键去重） |
| 容错恢复 | 重启时从 SQLite 最后成功位点继续；`MayHaveDup=true` 保守策略，消费端幂等处理 |
| 敏感字段 | EventBuilder.mask() 在采集侧替换为 `***`，不进入 Kafka |

---

## 快速开始

### 前提
- Go 1.22+（在 Linux/WSL 内）
- Docker & Docker Compose

### 一键启动全栈

```bash
# 1. 复制环境变量配置
cp .env.example .env

# 2. 启动所有服务（MySQL, SQL Server, Kafka, PostgreSQL, Prometheus, Grafana）
make up

# 3. 访问
#   Grafana:  http://localhost:3000  (admin/admin)
#   Kafka UI: http://localhost:8080
#   Metrics:  http://localhost:8000/metrics
```

### 本地构建

```bash
# 安装依赖（需 CGO，用于 go-sqlite3）
CGO_ENABLED=1 go build -o bin/cdc-platform ./cmd/platform
CGO_ENABLED=1 go build -o bin/cdc-cli     ./cmd/cli
```

---

## 项目结构

```
hospital-cdc-platform/
├── cmd/
│   ├── platform/main.go        # 主程序入口
│   └── cli/main.go             # 运维 CLI（位点查询/重置）
├── internal/
│   ├── core/                   # 统一数据模型与抽象接口
│   │   ├── event.go            # ChangeEvent + IdempotencyKey
│   │   ├── offset.go           # OffsetPosition（MySQL binlog / SS LSN）
│   │   ├── interfaces.go       # ICdcSource, IOffsetStore, ISinkWriter
│   │   └── errors.go           # 自定义错误哨兵值
│   ├── cdc/                    # CDC 采集层
│   │   ├── mysql.go            # MySQL binlog canal
│   │   ├── sqlserver.go        # SQL Server CDC 轮询
│   │   └── builder.go          # 原始行数据 → ChangeEvent
│   ├── transport/              # Kafka 传输层
│   │   ├── producer.go         # 事务型 Producer
│   │   ├── consumer.go         # read_committed Consumer
│   │   ├── serializer.go       # JSON 序列化
│   │   └── router.go           # 库/表 → topic 路由
│   ├── offset/                 # 位点持久化
│   │   └── sqlite.go           # SQLite WAL 实现
│   ├── sink/                   # ODS 写入层
│   │   ├── postgres.go         # 幂等 UPSERT
│   │   └── idempotent.go       # 去重表守卫
│   ├── engine/                 # 管道编排
│   │   ├── pipeline.go         # 采集端主循环（EOS 核心）
│   │   ├── consumer.go         # 消费端主循环
│   │   ├── coordinator.go      # 多源生命周期管理
│   │   ├── recovery.go         # 容错恢复状态机
│   │   └── builder.go          # config → ICdcSource 工厂
│   ├── monitoring/
│   │   └── metrics.go          # Prometheus 指标 + /metrics 端点
│   └── config/
│       └── config.go           # Viper 配置加载
├── tests/
│   ├── unit/                   # 无外部依赖单元测试
│   ├── integration/            # testcontainers 集成测试 (-tags=integration)
│   └── fault/                  # 故障注入测试 (-tags=fault)
├── scripts/
│   ├── seed/main.go            # 模拟医院数据生成
│   ├── checker/main.go         # 源-目标一致性校验
│   ├── experiment/main.go      # 参数化实验执行器
│   ├── init_mysql.sql
│   ├── init_sqlserver.sql
│   └── init_postgres.sql
├── configs/
│   ├── sources.yaml            # 数据源声明
│   └── topics.yaml             # Kafka topic 映射
├── docker-compose.yml
├── Dockerfile
└── Makefile
```

---

## 实验运行

### 正确性实验

```bash
make experiment-correctness
# 生成 10000 条数据 → 等待同步 → 校验源目标一致性
```

### EOS 对比实验

```bash
make experiment-eos
# 分别在 EOS 和 At-Least-Once 模式下统计重复行数
```

### 容错实验

```bash
FAULT_TESTS=1 make experiment-fault
# 注入 source_crash，验证恢复后不重不漏
```

### 性能基准

```bash
make experiment-perf
# batch_size: 100/500/1000 × partitions: 1/3/6
# 输出: scripts/results/perf_*.csv（吞吐/P50/P95/P99 延迟）
```

---

## 运行测试

```bash
# 单元测试（无外部依赖）
make test-unit

# 集成测试（需要 docker compose up）
INTEGRATION_FULL=1 make test-integration

# 容错测试（需要 docker compose up）
FAULT_TESTS=1 make test-fault
```

---

## CLI 工具

```bash
# 查看所有数据源位点
./bin/cdc-cli offset list

# 重置指定数据源位点
./bin/cdc-cli offset reset his_mysql_01
```

---

## 监控指标

| 指标 | 说明 |
|------|------|
| `cdc_events_total` | 各源/表/操作类型的采集事件总数 |
| `cdc_lag_seconds` | CDC 采集延迟（估算）|
| `kafka_txn_total{status="committed/aborted"}` | Kafka 事务提交/中止数 |
| `sink_events_total` | 实际写入 ODS 的事件数 |
| `sink_duplicates_total` | 被幂等键拦截的重复事件数 |
| `e2e_latency_seconds` | 端到端延迟（源写入 → ODS 写入）|

Grafana 大盘地址：`http://localhost:3000`（自动导入）

---

## 参考文献

见 [project.md](project.md) 参考文献部分。
