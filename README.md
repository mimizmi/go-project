# 医院异构系统实时数据中台

基于 CDC + FHIR 标准的医院异构系统实时数据集成平台。通过实时数据捕获、OLAP 分析引擎和统一 FHIR 接口服务，打破医院 HIS、LIS 等系统间的"信息孤岛"，实现患者诊疗数据的统一视图。

## 系统架构

```
┌─────────────┐  ┌───────────────┐
│ MySQL (HIS) │  │ SQL Server(LIS)│    数据源层
└──────┬──────┘  └───────┬───────┘
       │ binlog          │ CDC polling
       ▼                 ▼
┌─────────────────────────────────┐
│        CDC 采集平台 (Go)        │    数据采集层
│   Exactly-Once → Kafka → ODS   │
└───────────────┬─────────────────┘
                │ PostgreSQL ODS
                ▼
┌─────────────────────────────────┐
│     Med-Insight 分析引擎 (Go)   │    数据分析层
│  ClickHouse OLAP + L1/L2 缓存  │
└───────────────┬─────────────────┘
                │ REST API
                ▼
┌─────────────────────────────────┐
│    FHIR Gateway (Node.js)       │    服务层
│  EMPI + 权限控制 + FHIR 映射   │
└───────────────┬─────────────────┘
                │ FHIR R4
                ▼
┌─────────────────────────────────┐
│     管理控制台 (Vue 3)          │    应用层
│  接口测试 / 映射配置 / 权限管理 │
└─────────────────────────────────┘
```

## 项目结构

```
go-project/
├── cdc/                    # 模块一：实时数据采集平台
│   ├── cmd/                #   平台入口 + CLI 工具
│   ├── internal/           #   核心实现（CDC源/Kafka传输/ODS写入/流水线编排）
│   ├── configs/            #   数据源配置 + Kafka Topic 映射
│   └── scripts/            #   种子数据生成 / 一致性校验 / 实验脚本
│
├── med-insight/            # 模块二：多级缓存查询引擎
│   ├── cmd/                #   查询服务 + 增量同步进程
│   ├── internal/           #   ClickHouse客户端/多级缓存/查询引擎
│   └── scripts/            #   性能实验（延迟/缓存/并发）
│
├── fhir-gateway/           # 模块三：统一FHIR接口服务
│   ├── server/             #   Express 后端（EMPI/权限/映射引擎/API网关）
│   └── web/                #   Vue 3 + Element Plus 管理控制台
│
├── docker-compose.yml      # 统一基础设施编排（全部14个服务）
├── start-all.sh            # 一键启动全平台
├── stop-all.sh             # 一键停止
└── verify-all.sh           # 全链路验证（20项检查）
```

## 技术栈

| 层级 | 技术 |
|------|------|
| 数据采集 | Go 1.22, go-mysql (binlog), go-mssqldb (CDC), confluent-kafka-go (Exactly-Once) |
| 消息队列 | Apache Kafka 7.6 (KRaft 模式, 事务) |
| ODS 存储 | PostgreSQL 16 (幂等 UPSERT + 去重表) |
| OLAP 分析 | ClickHouse 24.3 (ReplacingMergeTree + 物化视图预聚合) |
| 分布式缓存 | Redis 7 (L2) + sync.Map (L1) + Bloom Filter + Singleflight |
| API 网关 | Node.js 20 + Express |
| 前端 | Vue 3 + Element Plus + Vite |
| 监控 | Prometheus + Grafana |

## 快速启动

### 前置要求

- Docker + Docker Compose
- Go 1.22+
- Node.js 20+

### 一键启动

```bash
# 启动全部服务（约2分钟）
bash start-all.sh

# 全链路验证
bash verify-all.sh

# 停止全部
bash stop-all.sh
```

### 分步启动

```bash
# 1. 全部基础设施（MySQL/SQLServer/Kafka/PostgreSQL/ClickHouse/Redis/Grafana）
docker compose up -d

# 2. Med-Insight 服务
cd med-insight && make build
./bin/syncer -config configs/config.yaml -state data/sync_state.json &
./bin/query-server -config configs/config.yaml &

# 3. 注入种子数据
cd cdc && go run ./scripts/seed/main.go --source mysql --records 100

# 4. FHIR Gateway
cd fhir-gateway/server && npm install && node src/app.js &

# 5. 前端
cd fhir-gateway/web && npm install && npx vite --host &
```

## 服务端口

| 服务 | 端口 | 说明 |
|------|------|------|
| MySQL (HIS) | 3306 | 源数据库 |
| SQL Server (LIS) | 14330 | 源数据库 |
| Kafka | 9092 | 消息队列 |
| Kafka UI | 8080 | Kafka 可视化 |
| PostgreSQL ODS | 5433 | 中间存储 |
| CDC Metrics | 8000 | Prometheus 指标 |
| Prometheus | 9090 | 指标采集 |
| Grafana | 3000 | 监控面板 (admin/admin) |
| ClickHouse | 9000/8123 | OLAP 引擎 |
| Redis | 16379 | 分布式缓存 |
| Med-Insight API | 8082 | 查询服务 |
| **FHIR Gateway** | **3001** | **统一接口服务** |
| **管理控制台** | **5173** | **前端界面** |

## 演示账号

| 角色 | 用户名 | 密码 | 权限说明 |
|------|--------|------|----------|
| 管理员 | admin | admin123 | 全部资源、全部科室 |
| 医生 | doctor_zhang | 123456 | 本科室(心内科)全部资源 |
| 护士 | nurse_li | 123456 | 本科室患者基本信息+医嘱 |
| 检验技师 | lab_wang | 123456 | 检验报告相关 |

## 核心业务流程

```
请求 → JWT认证 → 权限检查(RBAC+ABAC) → EMPI患者识别 → 数据聚合(Med-Insight)
    → FHIR映射转换 → 字段级过滤 → FHIR R4响应
```

## FHIR 映射规则

支持 4 种 FHIR R4 资源，映射规则可通过管理控制台在线编辑并热更新：

| FHIR 资源 | 源数据表 | 源系统 |
|-----------|---------|--------|
| Patient | ods_hospital_his_patients | HIS (MySQL) |
| Encounter | ods_hospital_his_visits | HIS (MySQL) |
| MedicationRequest | ods_hospital_his_orders | HIS (MySQL) |
| DiagnosticReport | ods_hospital_lis_lab_results | LIS (SQL Server) |

## 实验与论文数据

```bash
# CDC 实验
cd cdc
make experiment-correctness   # 数据一致性验证
make experiment-eos           # Exactly-Once 语义验证
make experiment-perf          # 吞吐量与延迟

# Med-Insight 实验
cd med-insight
make experiment-query         # 查询延迟对比（ODS vs ClickHouse vs 缓存）
make experiment-cache         # 缓存命中率与防击穿
make experiment-all           # 运行全部实验
```
