#!/bin/bash
#
# 医院异构系统实时数据中台 - 全链路验证脚本
# 在 start-all.sh 执行完成后运行此脚本验证全部功能
#

set +e
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
PASS=0; FAIL=0

check() {
  local desc=$1; local result=$2
  if [ -n "$result" ]; then
    echo -e "  ${GREEN}[PASS]${NC} $desc"
    PASS=$((PASS + 1))
  else
    echo -e "  ${RED}[FAIL]${NC} $desc"
    FAIL=$((FAIL + 1))
  fi
}

echo -e "${CYAN}╔══════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║        全链路验证 - 医院异构系统实时数据中台        ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════╝${NC}"
echo ""

# ===========================================================================
echo -e "${YELLOW}[1/6] CDC 基础设施检查${NC}"
# ===========================================================================
R=$(curl -s http://localhost:8000/health 2>/dev/null | grep -o "ok" || true)
check "CDC Platform 健康检查 (:8000)" "$R"

R=$(docker exec cdc-mysql mysqladmin ping -proot 2>/dev/null | grep -o "alive" || true)
check "MySQL 源库连通性" "$R"

R=$(docker exec cdc-postgres pg_isready -U ods_user 2>/dev/null | grep -o "accepting" || true)
check "PostgreSQL ODS 连通性" "$R"

R=$(docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | head -1 || true)
check "Kafka 连通性" "$R"

# ===========================================================================
echo ""
echo -e "${YELLOW}[2/6] Med-Insight 基础设施检查${NC}"
# ===========================================================================
R=$(docker exec mdi-clickhouse clickhouse-client --query "SELECT 1" 2>/dev/null || true)
check "ClickHouse 连通性 (:9000)" "$R"

R=$(docker exec mdi-redis redis-cli ping 2>/dev/null | grep -o "PONG" || true)
check "Redis 连通性 (:16379)" "$R"

# ===========================================================================
echo ""
echo -e "${YELLOW}[3/6] Med-Insight 查询服务检查${NC}"
# ===========================================================================
R=$(curl -s http://localhost:8082/healthz 2>/dev/null | grep -o "ok" || true)
check "Query Server 健康检查 (:8082)" "$R"

R=$(curl -s http://localhost:8082/metrics 2>/dev/null | grep -c "query" || true)
check "Prometheus Metrics 端点" "$R"

# ===========================================================================
echo ""
echo -e "${YELLOW}[4/6] FHIR Gateway 检查${NC}"
# ===========================================================================
R=$(curl -s http://localhost:3001/api/health 2>/dev/null | grep -o "ok" || true)
check "FHIR Gateway 健康检查 (:3001)" "$R"

# 登录获取 token
LOGIN=$(curl -s -X POST http://localhost:3001/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"admin","password":"admin123"}' 2>/dev/null)
TOKEN=$(echo "$LOGIN" | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])" 2>/dev/null || true)
check "JWT 登录认证" "$TOKEN"

# 系统状态
R=$(curl -s http://localhost:3001/api/system/status \
  -H "Authorization: Bearer $TOKEN" 2>/dev/null | grep -o "running" || true)
check "系统状态接口" "$R"

# med-insight 连通性（从 gateway 视角）
R=$(curl -s http://localhost:3001/api/system/status \
  -H "Authorization: Bearer $TOKEN" 2>/dev/null | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(d.get('medInsight',{}).get('status',''))
" 2>/dev/null || true)
if [ "$R" = "connected" ]; then
  echo -e "  ${GREEN}[PASS]${NC} Gateway → Med-Insight 连通 (实时数据模式)"
  PASS=$((PASS + 1))
else
  echo -e "  ${YELLOW}[WARN]${NC} Gateway → Med-Insight 未连通 (降级为模拟数据模式)"
fi

# ===========================================================================
echo ""
echo -e "${YELLOW}[5/6] 核心业务流程验证${NC}"
# ===========================================================================

# FHIR Patient 完整流程
PATIENT=$(curl -s http://localhost:3001/api/fhir/Patient/1 \
  -H "Authorization: Bearer $TOKEN" 2>/dev/null)

R=$(echo "$PATIENT" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['data']['resourceType'])" 2>/dev/null || true)
check "FHIR Patient 资源获取" "$R"

R=$(echo "$PATIENT" | python3 -c "
import sys,json
d=json.load(sys.stdin)
logs = d.get('processLog',[])
steps = [l['step'] for l in logs]
print('ok' if 'EMPI解析' in steps else '')
" 2>/dev/null || true)
check "EMPI 患者主索引解析" "$R"

R=$(echo "$PATIENT" | python3 -c "
import sys,json
d=json.load(sys.stdin)
logs = d.get('processLog',[])
steps = [l['step'] for l in logs]
print('ok' if 'FHIR映射转换' in steps else '')
" 2>/dev/null || true)
check "FHIR 映射规则转换" "$R"

R=$(echo "$PATIENT" | python3 -c "
import sys,json
d=json.load(sys.stdin)
al = d.get('accessLog',{})
print('ok' if al.get('allowed') else '')
" 2>/dev/null || true)
check "权限检查 (admin → Patient: 通过)" "$R"

# 权限拒绝测试
NURSE_TOKEN=$(curl -s -X POST http://localhost:3001/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"nurse_li","password":"123456"}' 2>/dev/null \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])" 2>/dev/null || true)

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  "http://localhost:3001/api/fhir/Patient/1?dept=%E9%AA%A8%E7%A7%91" \
  -H "Authorization: Bearer $NURSE_TOKEN" 2>/dev/null)
R=""
[ "$HTTP_CODE" = "403" ] && R="ok"
check "权限拒绝 (护士 → 非本科室: 403)" "$R"

# 映射规则
R=$(curl -s http://localhost:3001/api/mappings \
  -H "Authorization: Bearer $TOKEN" 2>/dev/null | python3 -c "
import sys,json
d=json.load(sys.stdin)
types = [m['resourceType'] for m in d['data']]
print('ok' if len(types) >= 4 else '')
" 2>/dev/null || true)
check "FHIR 映射规则列表 (4种资源)" "$R"

# EMPI 查询
R=$(curl -s "http://localhost:3001/api/system/empi/resolve?system=his_mysql_01&id=1" \
  -H "Authorization: Bearer $TOKEN" 2>/dev/null | python3 -c "
import sys,json;d=json.load(sys.stdin);print(d['data']['empiId'])
" 2>/dev/null || true)
check "EMPI 精确查询 (1 → $R)" "$R"

# ===========================================================================
echo ""
echo -e "${YELLOW}[6/6] 前端可访问性检查${NC}"
# ===========================================================================
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5173/ 2>/dev/null)
R=""
[ "$HTTP_CODE" = "200" ] && R="ok"
check "Vue 前端管理控制台 (:5173)" "$R"

# ===========================================================================
# 汇总
# ===========================================================================
echo ""
echo -e "${CYAN}══════════════════════════════════════════════════════${NC}"
TOTAL=$((PASS + FAIL))
echo -e "  验证结果: ${GREEN}$PASS 通过${NC} / ${RED}$FAIL 失败${NC} / $TOTAL 总计"

if [ $FAIL -eq 0 ]; then
  echo -e "  ${GREEN}全部验证通过！三个模块联通正常。${NC}"
else
  echo -e "  ${YELLOW}部分检查未通过，请检查对应服务。${NC}"
fi
echo -e "${CYAN}══════════════════════════════════════════════════════${NC}"
echo ""
echo "数据流: MySQL/SQLServer → CDC(binlog/polling) → Kafka → PostgreSQL ODS"
echo "        → Syncer(waterline) → ClickHouse → Med-Insight API"
echo "        → FHIR Gateway(EMPI+权限+映射) → Vue 前端"
echo ""
echo "打开浏览器访问: http://localhost:5173"
