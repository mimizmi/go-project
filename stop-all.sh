#!/bin/bash
#
# 医院异构系统实时数据中台 - 全平台停止脚本
#

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'
ok() { echo -e "${GREEN}[✓]${NC} $1"; }
log() { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $1"; }

log "停止所有应用进程..."

# 停止 Node.js / Vite
pkill -f "node src/app.js" 2>/dev/null && ok "FHIR Gateway 已停止" || true
pkill -f "vite" 2>/dev/null && ok "前端开发服务器已停止" || true

# 停止 Go 服务
pkill -f "bin/syncer" 2>/dev/null && ok "Syncer 已停止" || true
pkill -f "bin/query-server" 2>/dev/null && ok "Query Server 已停止" || true

sleep 1

log "停止全部基础设施..."
cd "$BASE_DIR" && docker compose down 2>/dev/null
ok "全部基础设施已停止"

rm -f "$BASE_DIR/.platform-pids"
echo ""
ok "所有服务已停止"
