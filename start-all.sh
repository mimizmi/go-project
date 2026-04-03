#!/bin/bash
#
# 医院异构系统实时数据中台 - 全平台一键启动脚本
#
# 使用根目录统一 docker-compose.yml 编排所有基础设施服务。
#
# 端口分配：
#   CDC 层:       MySQL=3306  SQLServer=14330  Kafka=9092  KafkaUI=8080
#                 PostgreSQL=5433  Prometheus=9090  Grafana=3000  CDC-Platform=8000
#   分析层:       ClickHouse=9000/8123  Redis=16379  QueryServer=8082
#   服务层:       FHIR-Gateway=3001  Frontend=5173
#

set -e
BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

log()  { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $1"; }
ok()   { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
fail() { echo -e "${RED}[✗]${NC} $1"; }

# ===========================================================================
# Step 1: 启动全部基础设施 (统一 docker-compose)
# ===========================================================================
log "========== Step 1: 启动基础设施容器 =========="
cd "$BASE_DIR"
# 只启动基础设施容器，fhir-server/fhir-web 由后续步骤原生启动
# （原生进程可直接通过 localhost 连接 med-insight，无需 host.docker.internal）
docker compose up -d mysql sqlserver kafka kafka-ui postgres cdc-platform \
  clickhouse redis prometheus grafana
log "等待数据库和Kafka就绪 (约30s)..."
sleep 30

# 检查关键服务
if docker exec cdc-mysql mysqladmin ping -h localhost -proot &>/dev/null; then
  ok "MySQL ready (3306)"
else
  warn "MySQL 可能还未就绪，继续..."
fi

if docker exec cdc-postgres pg_isready -U ods_user -d hospital_ods &>/dev/null; then
  ok "PostgreSQL ODS ready (5433)"
else
  warn "PostgreSQL 可能还未就绪，继续..."
fi

if docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list &>/dev/null; then
  ok "Kafka ready (9092)"
else
  warn "Kafka 可能还未就绪，继续..."
fi

if docker exec mdi-clickhouse clickhouse-client --query "SELECT 1" &>/dev/null; then
  ok "ClickHouse ready (9000)"
else
  warn "ClickHouse 可能还未就绪，继续..."
fi

if docker exec mdi-redis redis-cli ping &>/dev/null; then
  ok "Redis ready (16379)"
else
  warn "Redis 可能还未就绪，继续..."
fi

# ===========================================================================
# Step 2: 构建并启动 Med-Insight 服务 (Syncer + QueryServer)
# ===========================================================================
log "========== Step 2: 构建并启动 Med-Insight 服务 =========="
cd "$BASE_DIR/med-insight"
make build 2>&1 | tail -3

# 先杀掉可能存在的旧进程
pkill -f "bin/syncer" 2>/dev/null || true
pkill -f "bin/query-server" 2>/dev/null || true
sleep 1

# 启动 syncer (后台)
./bin/syncer -config configs/config.yaml -state data/sync_state.json &>/dev/null &
SYNCER_PID=$!
ok "Syncer 已启动 (PID=$SYNCER_PID)"

# 启动 query-server (后台)
./bin/query-server -config configs/config.yaml &>/dev/null &
SERVER_PID=$!
sleep 2
if curl -s http://localhost:8082/healthz | grep -q "ok"; then
  ok "Query Server ready (8082, PID=$SERVER_PID)"
else
  warn "Query Server 可能还未就绪"
fi

# ===========================================================================
# Step 3: 检查种子数据
# ===========================================================================
log "========== Step 3: 检查种子数据 =========="
PATIENT_COUNT=$(docker exec cdc-mysql \
  mysql -uroot -proot hospital_his -N -e "SELECT COUNT(*) FROM patients" 2>/dev/null || echo "0")

if [ "$PATIENT_COUNT" -lt "10" ] 2>/dev/null; then
  log "数据量较少($PATIENT_COUNT)，注入种子数据..."
  cd "$BASE_DIR/cdc"
  go run ./scripts/seed/main.go --source mysql --records 100 2>&1 | tail -3 || warn "种子数据注入失败(可忽略)"
  sleep 5
  ok "种子数据已注入"
else
  ok "数据已存在 ($PATIENT_COUNT 条患者记录)"
fi

# ===========================================================================
# Step 4: 启动 FHIR Gateway (后端)
# ===========================================================================
log "========== Step 4: 启动 FHIR Gateway 后端 =========="
cd "$BASE_DIR/fhir-gateway/server"

pkill -f "node src/app.js" 2>/dev/null || true
sleep 1

node src/app.js &>/dev/null &
GATEWAY_PID=$!
sleep 2
if curl -s http://localhost:3001/api/health | grep -q "ok"; then
  ok "FHIR Gateway ready (3001, PID=$GATEWAY_PID)"
else
  warn "FHIR Gateway 可能还未就绪"
fi

# ===========================================================================
# Step 5: 启动前端 (Vite Dev Server)
# ===========================================================================
log "========== Step 5: 启动前端 =========="
cd "$BASE_DIR/fhir-gateway/web"
npx vite --host &>/dev/null &
WEB_PID=$!
sleep 3
ok "前端开发服务器已启动 (5173, PID=$WEB_PID)"

# ===========================================================================
# 启动完成 - 输出汇总
# ===========================================================================
echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║          医院异构系统实时数据中台 - 全平台启动完成           ║${NC}"
echo -e "${GREEN}╠══════════════════════════════════════════════════════════════╣${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  ${CYAN}数据采集层 (CDC):${NC}                                          ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    CDC Platform Metrics   http://localhost:8000/metrics       ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    Kafka UI               http://localhost:8080               ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    Grafana Dashboard      http://localhost:3000 (admin/admin) ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    Prometheus              http://localhost:9090               ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  ${CYAN}数据分析层 (Med-Insight):${NC}                                   ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    Query API              http://localhost:8082               ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    ClickHouse HTTP        http://localhost:8123               ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  ${CYAN}服务层 (FHIR Gateway):${NC}                                     ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    FHIR API               http://localhost:3001               ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    ${YELLOW}管理控制台 (前端)    http://localhost:5173${NC}               ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  ${CYAN}演示账号:${NC}                                                  ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    管理员   admin / admin123                                  ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    医生     doctor_zhang / 123456                             ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    护士     nurse_li / 123456                                 ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}    检验师   lab_wang / 123456                                 ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "停止所有服务: ${YELLOW}bash stop-all.sh${NC}"
echo -e "进程PID: syncer=$SYNCER_PID  query-server=$SERVER_PID  gateway=$GATEWAY_PID  web=$WEB_PID"
echo ""

# 将 PID 写入文件供 stop 使用
echo "$SYNCER_PID $SERVER_PID $GATEWAY_PID $WEB_PID" > "$BASE_DIR/.platform-pids"
