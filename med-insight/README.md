# Med-Insight 医疗数据洞察引擎

`github.com/mimizh/med-insight` 是一个高性能、多级缓存的医院数据查询引擎，面向实时报表和 FHIR 标准接口的医疗数据中台。

**关键词**：ClickHouse OLAP、多级缓存(L1/L2)、防击穿/防雪崩/防穿透、预聚合物化视图、CDC 增量同步、FHIR R4 标准

---

## 核心特性

- **多级缓存架构**：L1(本地sync.Map) + L2(Redis) + Bloom过滤器，兼具低延迟与高并发
- **缓存防护三件套**：
  - 防击穿（singleflight 合并穿透请求）
  - 防雪崩（TTL 随机抖动 ±20%）
  - 防穿透（布隆过滤器拦截非法 key）
- **预聚合加速**：SummingMergeTree + 物化视图，Top-K 报表 P50 延迟 < 1ms
- **水位线同步**：PostgreSQL ODS → ClickHouse，每 5s 增量批处理 500 条记录
- **FHIR R4 兼容**：标准医疗数据交换接口（Patient / Encounter 资源）
- **Prometheus 可观测性**：详细的查询延迟、缓存命中率、QPS 指标

---

## 系统架构

```
┌─────────────────────────────────────────────────────────────────┐
│                    Hospital Data Platform                       │
└─────────────────────────────────────────────────────────────────┘
         │                                  │
         │ CDC Module (~cdc)                │
         v                                  v
    ┌─────────────┐        ┌──────────────────┐
    │   MySQL     │        │  PostgreSQL ODS  │
    │  SQLServer  │ ──────▶│  (watermark)     │
    │   Kafka     │        │ :5433            │
    └─────────────┘        └──────────────────┘
                                    │
                  ┌─────────────────┼─────────────────┐
                  │                 │                 │
                  v                 v                 v
             ┌─────────┐      ┌──────────┐      ┌─────────────┐
             │  Syncer │      │L1 Cache  │      │ Bloom Filter│
             │ Worker  │      │(sync.Map)│      │ (防穿透)    │
             └────┬────┘      └────▲─────┘      └─────────────┘
                  │                │
                  │         ┌──────┴──────┐
                  │         │             │
                  v         v             v
           ┌────────────────────────────────────┐
           │      ClickHouse OLAP Engine        │
           │  - dim_patients                     │
           │  - fact_visits                      │
           │  - fact_orders                      │
           │  - fact_lab_results                 │
           │  - agg_dept_daily_visits (MV)       │
           │  - agg_drug_daily_orders (MV)       │
           │  - agg_patient_monthly (MV)         │
           │  - v_visit_wide (VIEW)              │
           │  :9000 (native) / :8123 (HTTP)      │
           └────────┬──────────────────────────┘
                    │
         ┌──────────┴──────────┐
         │                     │
         v                     v
    ┌─────────────┐      ┌──────────────────┐
    │L2 Cache     │      │  Query Engine    │
    │(Redis)      │      │  API Server      │
    │:16379       │      │  :8082           │
    └─────────────┘      └────┬─────────────┘
                               │
                ┌──────────────┼──────────────┐
                │              │              │
                v              v              v
           ┌─────────┐  ┌──────────┐  ┌──────────┐
           │ Reports │  │FHIR APIs │  │ Metrics  │
           │ Queries │  │(Patient) │  │(Prom)    │
           └─────────┘  │(Encounter)  └──────────┘
                        └──────────┘
```

**数据流向**：
1. **上游依赖**：CDC 模块提供 PostgreSQL ODS 数据源（多源异构数据已在 CDC 整合）
2. **增量同步**：Syncer Worker 每 5s 轮询 ODS，通过水位线机制提取变更
3. **批处理**：单批 500 条记录插入 ClickHouse，触发物化视图自动预聚合
4. **多级缓存**：查询命中 L1 → L2 → 预聚合表 → 明细宽表，利用缓存加速热点访问
5. **API 暴露**：HTTP 服务提供报表接口（预聚合）、FHIR 接口（明细）、健康检查

---

## 项目结构

```
project2/
├── cmd/
│   ├── server/main.go          # 查询 API 服务入口
│   └── syncer/main.go          # 增量同步服务入口
├── configs/
│   └── config.yaml             # 配置文件（DSN、缓存、同步参数）
├── internal/
│   ├── api/                    # HTTP 路由与 FHIR 处理器
│   │   ├── server.go           # 服务注册与路由
│   │   └── fhir.go             # FHIR R4 资源映射
│   ├── cache/                  # 多级缓存（防击穿/雪崩/穿透）
│   │   ├── local.go            # L1 本地缓存 (sync.Map)
│   │   ├── redis.go            # L2 Redis 缓存
│   │   ├── manager.go          # 统一缓存管理器
│   │   └── cache_test.go       # 缓存单元测试
│   ├── clickhouse/             # ClickHouse 客户端与 schema DDL
│   │   ├── client.go           # 连接与执行
│   │   └── schema.go           # 建表与索引
│   ├── config/                 # 配置结构体与加载
│   │   └── config.go
│   ├── metrics/                # Prometheus 指标
│   │   └── metrics.go          # 查询延迟、缓存命中率、QPS
│   ├── query/                  # 查询引擎（报表 + FHIR）
│   │   └── engine.go           # DTO、查询方法、缓存路径
│   └── syncer/                 # 增量同步 Worker
│       └── worker.go           # 水位线管理、批处理
├── scripts/
│   ├── experiment/main.go      # 实验基准测试（论文数据收集）
│   │   └── modes: query_latency / cache_perf / concurrency
│   └── results/                # 实验结果 JSON 文件
├── docker-compose.yml          # ClickHouse + Redis 编排
├── Makefile                    # 构建、运行、测试脚本
├── go.mod / go.sum             # Go 依赖管理
└── README.md                   # 本文件
```

---

## 快速开始

### 前置条件

- **Go 1.22+**
- **Docker + Docker Compose**
- **CDC 项目**（约 15-20GB 磁盘空间用于 MySQL/PostgreSQL）

### 1. 启动 CDC 上游数据源

```bash
cd ~/cdc
make up
# 等待 PostgreSQL ODS(:5433)、Kafka、MySQL、SQLServer 启动
# 若需生成测试数据：
make seed
```

### 2. 启动本项目基础设施

```bash
cd ~/project2
make up
# 启动 ClickHouse(:9000) 和 Redis(:16379)
# docker compose up -d
```

确认服务就绪：
```bash
# 测试 ClickHouse
docker exec mdi-clickhouse clickhouse-client --query "SELECT 1"

# 测试 Redis
docker exec mdi-redis redis-cli ping
```

### 3. 启动增量同步服务

```bash
make run-syncer &
# 或：go run ./cmd/syncer -config configs/config.yaml

# 观察日志（每 5s 一次轮询）：
# syncer starting postgres=postgres://ods_user:ods_pass@localhost:5433/hospital_ods interval_s=5
# sync batch completed: 125 patients, 340 visits
```

### 4. 启动查询 API 服务

```bash
make run-server
# 或：go run ./cmd/server -config configs/config.yaml

# 观察日志：
# HTTP server started addr=:8082
```

### 5. 验证服务

```bash
# 健康检查
curl http://localhost:8082/healthz
# {"status":"ok"}

# Prometheus 指标
curl http://localhost:8082/metrics | grep query_latency

# 科室日就诊量报表
curl "http://localhost:8082/api/v1/reports/dept-daily?dept=内科&date=2026-03-19"

# FHIR Patient 接口
curl "http://localhost:8082/api/v1/fhir/Patient/1"

# FHIR Encounter 接口
curl "http://localhost:8082/api/v1/fhir/Encounter/992"
```

---

## API 参考

### 健康检查

```
GET /healthz
```

**响应示例：**
```json
{
  "status": "ok"
}
```

---

### 科室日就诊量报表

```
GET /api/v1/reports/dept-daily?dept=<dept>&date=<YYYY-MM-DD>
```

**查询参数：**
- `dept`：科室名称（可选，不指定则返回全科室）
- `date`：日期（可选，YYYY-MM-DD 格式）

**响应示例：**
```json
{
  "data": [
    {
      "visit_date": "2026-03-19",
      "dept": "内科",
      "visit_type": "outpatient",
      "visit_count": 156,
      "patient_count": 142
    },
    {
      "visit_date": "2026-03-19",
      "dept": "内科",
      "visit_type": "inpatient",
      "visit_count": 89,
      "patient_count": 78
    }
  ],
  "count": 2
}
```

**性能特征：** P50 < 1ms（预聚合表命中）/ 0.25ms（缓存命中）

---

### 药品消耗报表

```
GET /api/v1/reports/drug-consumption?drug=<drug>&start_date=<date>&end_date=<date>
```

**查询参数：**
- `drug`：药品名称（可选）
- `start_date`：起始日期（可选，YYYY-MM-DD）
- `end_date`：终止日期（可选，YYYY-MM-DD）

**响应示例：**
```json
{
  "data": [
    {
      "order_date": "2026-03-19",
      "drug_name": "阿司匹林",
      "total_orders": 1024,
      "executed_orders": 1012,
      "cancelled_orders": 12
    }
  ],
  "count": 1
}
```

---

### 月度患者统计报表

```
GET /api/v1/reports/patient-monthly?year_month=<YYYY-MM>&dept=<dept>
```

**查询参数：**
- `year_month`：年月（YYYY-MM 格式，可选）
- `dept`：科室（可选）

**响应示例：**
```json
{
  "data": [
    {
      "year_month": "2026-03",
      "dept": "外科",
      "visit_type": "outpatient",
      "patient_count": 2541,
      "visit_count": 3208
    }
  ],
  "count": 1
}
```

---

### 就诊宽表查询

```
GET /api/v1/reports/visit-wide?dept=<dept>&type=<visit_type>&limit=<limit>
```

**查询参数：**
- `dept`：科室（可选）
- `type`：就诊类型（可选，outpatient/inpatient）
- `limit`：记录数限制（可选，默认 100，最大 1000）

**响应示例：**
```json
{
  "data": [
    {
      "visit_id": "V202603190001",
      "patient_name": "张三",
      "gender": "M",
      "dept": "骨科",
      "visit_type": "outpatient",
      "admit_time": "2026-03-19T09:00:00Z",
      "diagnosis": "腰椎间盘突出",
      "los_hours": 2
    }
  ],
  "count": 1
}
```

---

### FHIR Patient 资源

```
GET /api/v1/fhir/Patient/{id}
```

**响应示例（HL7 FHIR R4 映射）：**
```json
{
  "resourceType": "Patient",
  "id": "1",
  "identifier": [{
    "system": "http://hospital.example.com/patient",
    "value": "1"
  }],
  "name": [{
    "text": "张三",
    "given": ["三"],
    "family": "张"
  }],
  "gender": "male",
  "birthDate": "1980-05-15",
  "address": [{
    "text": "北京市朝阳区"
  }]
}
```

---

### FHIR Encounter 资源

```
GET /api/v1/fhir/Encounter/{id}
```

**响应示例（HL7 FHIR R4 映射）：**
```json
{
  "resourceType": "Encounter",
  "id": "992",
  "status": "finished",
  "class": {
    "code": "AMB",
    "display": "ambulatory"
  },
  "type": [{
    "text": "outpatient"
  }],
  "subject": {
    "reference": "Patient/1"
  },
  "period": {
    "start": "2026-03-19T09:00:00Z",
    "end": "2026-03-19T10:30:00Z"
  },
  "location": [{
    "location": {
      "display": "内科诊室"
    }
  }],
  "diagnosis": [{
    "condition": {
      "display": "高血压"
    }
  }]
}
```

---

### Prometheus 指标

```
GET /metrics
```

暴露以下指标：
- `query_latency_seconds`：查询延迟分布（Histogram，端点标签）
- `query_requests_total`：查询请求计数（Counter，端点/状态标签）
- `cache_hits_total`：缓存命中计数（Counter，缓存层标签：L1/L2）
- `cache_misses_total`：缓存失效计数（Counter）
- `cache_size_bytes`：当前缓存大小（Gauge，缓存层标签）

**示例：**
```
# HELP query_latency_seconds Query latency in seconds
# TYPE query_latency_seconds histogram
query_latency_seconds_bucket{endpoint="dept-daily",le="0.001"} 523
query_latency_seconds_bucket{endpoint="dept-daily",le="0.01"} 531
query_latency_seconds_sum{endpoint="dept-daily"} 0.789
query_latency_seconds_count{endpoint="dept-daily"} 534

# HELP cache_hits_total Total cache hits
# TYPE cache_hits_total counter
cache_hits_total{level="L1"} 2841
cache_hits_total{level="L2"} 456
```

---

## 配置文件详解

**文件**：`configs/config.yaml`

```yaml
server:
  port: 8082              # HTTP 服务端口
  mode: release           # 运行模式（release/debug）

clickhouse:
  dsn: "clickhouse://default:@localhost:9000/hospital_dw"
  database: "hospital_dw"

postgres:
  dsn: "postgres://ods_user:ods_pass@localhost:5433/hospital_ods"

redis:
  addr: "localhost:16379"
  password: ""            # Redis 密码（留空则不认证）
  db: 0

cache:
  l1_ttl_seconds: 30      # L1 本地缓存 TTL（秒）
  l2_ttl_seconds: 300     # L2 Redis 缓存 TTL（秒）
  l1_max_items: 1000      # L1 最大缓存条目数
  ttl_jitter_pct: 20      # TTL 随机抖动百分比（防雪崩）
  bloom_expected: 100000  # 布隆过滤器预期元素数
  bloom_fp_rate: 0.01     # 假阳性率

syncer:
  interval_seconds: 5     # 同步轮询间隔（秒）
  batch_size: 500         # 单批处理行数

log:
  level: info             # 日志级别（debug/info/warn/error）
```

**环境变量覆盖**：
```bash
export CLICKHOUSE_DSN="clickhouse://user:pass@host:9000/db"
export POSTGRES_DSN="postgres://user:pass@host:5432/db"
export REDIS_ADDR="host:6379"
```

---

## 缓存设计：三大防护策略

### 1. 防击穿（Singleflight）

**问题**：热点 key 缓存失效时，大量并发请求同时穿透到数据库，引发雪崩。

**方案**：使用 `golang.org/x/sync/singleflight.Group` 合并并发请求。

**效果**（实验 2）：
- 100 并发请求同一 key
- **99 个请求被合并，仅 1 次数据库查询**
- P99 延迟从 50ms 降至 0.68ms

**代码示例**（`internal/cache/manager.go`）：
```go
// 防击穿：singleflight 合并并发穿透请求
raw, err, _ := mgr.group.Do(key, func() (interface{}, error) {
    v, err := fetcher()
    // ... 缓存回填
    return v, nil
})
```

---

### 2. 防雪崩（TTL 随机抖动）

**问题**：大量 key 在同一时刻过期，再次全量穿透数据库引发雪崩。

**方案**：L2 TTL 添加随机抖动，base ± jitterPct%，错开过期时间。

**公式**：
```
actual_ttl = base + random(-1, 1) * base * jitter_pct / 100
```

**配置**：
```yaml
l2_ttl_seconds: 300
ttl_jitter_pct: 20  # 实际 TTL 范围：[240s, 360s]
```

**效果**：平滑缓存失效波形，避免尖峰。

**代码**（`internal/cache/manager.go`）：
```go
func (mgr *Manager) l2TTLWithJitter() time.Duration {
    base := float64(mgr.cfg.L2TTLSeconds)
    jitter := base * float64(mgr.cfg.TTLJitterPct) / 100.0
    actual := base + (rand.Float64()*2-1)*jitter
    return time.Duration(actual) * time.Second
}
```

---

### 3. 防穿透（Bloom 过滤器）

**问题**：黑客利用不存在的 key 进行扫描攻击，每次都穿透到数据库。

**方案**：
1. 启动时初始化布隆过滤器，预期容量 100K，假阳性率 1%
2. 所有合法 key（查询后）注册到过滤器
3. 查询前先检查 key 是否在过滤器中，若不在则直接返回空

**特性**：
- 占用内存 < 20KB（100K 元素，1% 假阳性率）
- 单次查询 O(k) ≈ O(1)
- 零 false negative（不存在的 key 必定拦截）
- 可控 false positive（配置假阳性率）

**代码**（`internal/cache/manager.go`）：
```go
// 防穿透：检查布隆过滤器
if !mgr.bloom.Test([]byte(key)) {
    return nil, ErrNotFound  // 直接返回，无需查询
}
```

---

## 性能数据表

基于实验模块（`scripts/experiment/main.go`）的实测结果。

### 实验 1：查询延迟对比

| 场景 | P50 延迟 | P99 延迟 | 备注 |
|------|----------|----------|------|
| PostgreSQL ODS 直查 | 0.46ms | 0.66ms | 基准（无缓存） |
| ClickHouse 宽表视图 | 0.91ms | 2.58ms | 明细扫描，无索引 |
| ClickHouse 预聚合表（冷） | 0.29ms | 7.53ms | SummingMergeTree，首次查询 |
| L1/L2 缓存命中 | 0.25ms | 0.59ms | **目标：亚毫秒级** |

**结论**：预聚合 + 多级缓存可将 P50 延迟降至 **0.25ms**（比 ODS 快 1.8 倍）。

---

### 实验 2：缓存命中率与防击穿

| 访问模式 | 命中率 | P99 延迟 | Singleflight 效果 |
|----------|--------|----------|-------------------|
| 均匀访问（5000 个热 key） | 35% | 10.3ms | - |
| 80/20 分布（1000 个热 key） | 75% | 0.68ms | 99/100 请求合并 |
| 单一热 key | 99% | 0.49ms | 100/100 请求合并 |

**结论**：
- L1 容量 1000 条，命中率受访问分布影响
- Singleflight 可合并 99% 以上穿透请求，大幅降低数据库压力

---

### 实验 3：并发性能

| 并发度 | QPS | P99 延迟 | 备注 |
|--------|-----|----------|------|
| 1 goroutine | 3,107 | 0.65ms | 单线程基准 |
| 20 goroutines | 21,941 | 4.1ms | **峰值吞吐** |
| 100 goroutines | 21,102 | 19.5ms | 开始出现竞争 |

**结论**：
- 最优并发度约 **20-30 goroutines**，QPS 可达 **21K+**
- 超过 100 并发后竞争增加，延迟升高但 QPS 保持稳定

---

## 数据模型

### 维度表（Dimension Tables）

**dim_patients**（患者维度表，ReplacingMergeTree）
```sql
patient_id, name, gender, birth_date, address,
_cdc_updated_at (版本列)
```

---

### 事实表（Fact Tables）

**fact_visits**（就诊事实表）
```sql
visit_id, patient_id, dept, doctor, visit_type,
admit_time, discharge_time, diagnosis,
_cdc_updated_at
```

**fact_orders**（医嘱事实表）
```sql
order_id, visit_id, drug_name, dosage, frequency, route,
order_time, doctor, status,
_cdc_updated_at
```

**fact_lab_results**（检验结果事实表）
```sql
result_id, visit_id, patient_id, item_code, item_name,
value, unit, ref_range, is_abnormal, result_time,
report_time, lab_section,
_cdc_updated_at
```

---

### 预聚合表与物化视图（Pre-aggregation + MV）

**agg_dept_daily_visits**（SummingMergeTree）
```sql
visit_date, dept, visit_type,
sum(visit_count), sum(patient_count)
```
由 `mv_dept_daily_visits` 物化视图自动维护，每插入新数据自动增量聚合。

**agg_drug_daily_orders**
```sql
order_date, drug_name,
sum(total_orders), sum(executed_orders), sum(cancelled_orders)
```

**agg_patient_monthly**
```sql
year_month, dept, visit_type,
sum(patient_count), sum(visit_count)
```

---

### 宽表视图（Star Schema View）

**v_visit_wide**（VIEW，关联患者 + 就诊 + 医嘱）
```sql
SELECT
  v.visit_id, p.patient_id, p.name AS patient_name, p.gender,
  v.dept, v.doctor, v.visit_type, v.admit_time, v.diagnosis,
  CAST(dateDiff('hour', v.admit_time, v.discharge_time) AS Int64) AS los_hours
FROM fact_visits v
FINAL
JOIN dim_patients p ON v.patient_id = p.patient_id
WHERE ...
```

设计优势：
- 统一查询口径，简化应用层逻辑
- 常用维度预关联，避免重复 JOIN
- 高频查询通过下方预聚合表或缓存进一步加速

---

## 增量同步机制

### 水位线（Watermark）原理

Syncer Worker 基于水位线实现幂等增量同步：

1. **初始状态**：水位线记录 `sync_state.json`
   ```json
   {
     "last_sync_time": "2026-03-19T10:30:00Z",
     "table_states": {
       "dim_patients": "2026-03-19T10:30:00Z",
       "fact_visits": "2026-03-19T10:30:00Z"
     }
   }
   ```

2. **每个周期**（5 秒）：
   ```sql
   SELECT * FROM dim_patients
   WHERE updated_at > last_watermark
   ORDER BY updated_at
   LIMIT 500
   ```

3. **批插入**：
   ```sql
   INSERT INTO clickhouse.dim_patients
   SELECT ... FROM staged_data
   ```
   ClickHouse ReplacingMergeTree 自动去重（按 PK + 版本列 `_cdc_updated_at`）

4. **前进水位线**：更新 `sync_state.json` 的时间戳

### 故障恢复

- 若 Syncer 奔溃，下次启动读取 `sync_state.json` 水位线，从断点继续
- 若数据库有重复（例如网络抖动导致重试），ClickHouse ReplacingMergeTree 自动去重

---

## 运行测试

### 单元测试

```bash
# 运行所有测试
make test

# 缓存单元测试
make test-cache

# 查询引擎测试
make test-query
```

### 实验基准测试

实验模块位于 `scripts/experiment/main.go`，可独立运行以收集论文数据。

**实验 1：查询延迟对比**
```bash
make experiment-query
# 输出：PostgreSQL vs ClickHouse vs 缓存的 P50/P99 延迟
# 结果保存至 scripts/results/query_latency_*.json
```

**实验 2：缓存命中率与防击穿**
```bash
make experiment-cache
# 输出：命中率、Singleflight 合并率、P99 延迟
# 模式：均匀访问、80/20 分布、单热 key
```

**实验 3：并发压测**
```bash
make experiment-concurrency
# 输出：1~100 goroutines 下的 QPS、P99 延迟曲线
```

**运行全部实验**
```bash
make experiment-all
# 完整数据集，用于论文发表
```

---

## 依赖与环境

### Go 依赖

```
- github.com/ClickHouse/clickhouse-go/v2 v2.23.2
  ClickHouse 官方 Go 驱动（批处理、事务支持）

- github.com/jackc/pgx/v5 v5.5.5
  PostgreSQL 驱动（连接池、上下文支持）

- github.com/redis/go-redis/v9 v9.5.1
  Redis 客户端库

- github.com/bits-and-blooms/bloom/v3 v3.7.0
  布隆过滤器实现

- github.com/prometheus/client_golang v1.19.0
  Prometheus 指标导出

- golang.org/x/sync v0.7.0
  singleflight（防击穿）

- go.uber.org/zap v1.27.0
  高性能结构化日志
```

### 容器镜像

```
- clickhouse/clickhouse-server:24.3
- redis:7-alpine
```

### 网络要求

| 组件 | 端口 | 用途 |
|------|------|------|
| ClickHouse | 9000 | Native 协议（本项目使用） |
| ClickHouse | 8123 | HTTP 接口（调试） |
| Redis | 16379 | 缓存存储（L2） |
| API Server | 8082 | HTTP 查询服务 |
| PostgreSQL ODS | 5433 | 数据同步源（CDC 提供） |

---

## 上游依赖：CDC 模块

本项目依赖 `~/cdc` 项目提供的 PostgreSQL ODS 数据源。

### CDC 启动

```bash
cd ~/cdc
make up

# 若需要测试数据，执行 seeder
make seed

# 验证 ODS 就绪
psql -h localhost -p 5433 -U ods_user -d hospital_ods -c "SELECT COUNT(*) FROM dim_patients"
```

### 说明

- **MySQL/SQLServer** → Kafka CDC → **PostgreSQL ODS**
- ODS 包含所有源表副本（通过 CDC 增量更新）
- 本项目无需关心上游 CDC 实现，只需连接 ODS DSN

### ODS 表结构

本项目假设 ODS 已提供以下表（及对应的 `updated_at` 审计字段）：
- `dim_patients`
- `fact_visits`
- `fact_orders`
- `fact_lab_results`

---

## 常见问题

### Q1：为什么 L1 容量只有 1000 条？

**A**：目标是低延迟而非大容量。L1 是进程内本地缓存（sync.Map），受内存限制。主要用于热点数据（Top-K 报表）。大容量缓存由 L2 Redis 承载（512MB）。

### Q2：Redis 不可用会怎样？

**A**：系统降级运行。启动日志会提示：
```
WARN  redis unavailable, L2 cache disabled
```
此时仅使用 L1 本地缓存，命中率下降但服务可用（单机部署足以支撑本地缓存）。

### Q3：Bloom 过滤器会误判吗？

**A**：不会误判"不存在"（false negative = 0）。但有 1% 的概率误认为"存在"（false positive = 1%）。对于防穿透而言可接受，因为即使误判通过一次查询，也不会造成安全问题。

### Q4：数据延迟多少？

**A**：TTL 取决于配置：
- L1 TTL：30 秒
- L2 TTL：300 秒（± 20% 随机抖动）
- 缓存失效后实时查询 ClickHouse（P50 < 1ms）

因此数据最大延迟 ≈ L2 TTL + 查询延迟 ≈ 300s（5 分钟）。若需更低延迟，调小 TTL 或禁用 L2。

### Q5：支持分布式部署吗？

**A**：暂不支持分布式查询引擎本身。但可扩展：
- 多个实例共享后端 ClickHouse + Redis
- 在 Nginx 前面进行负载均衡
- L1 缓存无法跨实例共享（各自独立），但 L2 Redis 是共享的

---

## 性能优化建议

### 短期（配置调优）

1. **增加 L1 容量**（如果内存充足）
   ```yaml
   l1_max_items: 5000  # 从 1000 增加到 5000
   ```
   效果：命中率 +10~20%

2. **调整 L2 TTL**（根据数据实时性要求）
   ```yaml
   l2_ttl_seconds: 600  # 从 300 增加到 600（允许 10 分钟延迟）
   ```
   效果：L2 命中率提升，数据库压力 -30%

3. **增大同步批量**
   ```yaml
   syncer.batch_size: 1000  # 从 500 增加到 1000
   ```
   效果：同步吞吐 +100%，但增加内存占用

### 中期（架构优化）

1. **预热热点数据**
   - Syncer 启动后执行一次全表扫描，将结果预加载到 L1/L2
   - 避免冷启动的缓存失效波形

2. **分离读写**
   - 创建 ClickHouse 副本，查询打到只读副本
   - 同步独占写副本

3. **超时熔断**
   - 若查询超过 100ms，降级返回缓存（即使过期）
   - 防止级联故障

### 长期（基础设施升级）

1. **更换存储**：使用 SSD 替代 HDD
2. **网络优化**：确保 ClickHouse/Redis 在同一数据中心
3. **垂直扩容**：增加 ClickHouse/Redis 内存规格

---

## 故障排查

### 问题：Syncer 卡住不动

```bash
# 检查 PostgreSQL 连接
psql -h localhost -p 5433 -U ods_user -d hospital_ods -c "\dt"

# 检查 ClickHouse 连接
docker exec mdi-clickhouse clickhouse-client --query "SELECT 1"

# 查看 Syncer 日志
tail -f logs/syncer.log
```

### 问题：API 查询返回 500 错误

```bash
# 检查查询引擎日志
tail -f logs/server.log | grep ERROR

# 验证 ClickHouse 表存在
docker exec mdi-clickhouse clickhouse-client --query "SHOW TABLES IN hospital_dw"

# 检查 Redis 连接（若启用）
redis-cli -p 16379 ping
```

### 问题：缓存命中率低于预期

```bash
# 查看缓存指标
curl http://localhost:8082/metrics | grep cache

# 分析访问模式
curl http://localhost:8082/metrics | grep query_requests | head -20

# 若 L1 满了，增加容量或调整 TTL
```

---

## 参考资源

- **ClickHouse 官方文档**：https://clickhouse.com/docs/
- **FHIR R4 标准**：https://www.hl7.org/fhir/
- **Go singleflight**：https://pkg.go.dev/golang.org/x/sync/singleflight
- **布隆过滤器**：https://en.wikipedia.org/wiki/Bloom_filter

---

## 许可证

MIT License

---

## 作者

Medical Data Platform Team

**创建日期**：2026-03-19

**最后更新**：2026-03-19
