.PHONY: all build run-server run-syncer test experiment-query experiment-cache clean help

# ── 变量 ──────────────────────────────────────────────────────────────────────
BIN_DIR     := bin
SERVER_BIN  := $(BIN_DIR)/query-server
SYNCER_BIN  := $(BIN_DIR)/syncer
CONFIG      := configs/config.yaml
RESULTS_DIR := scripts/results

# ── 构建 ──────────────────────────────────────────────────────────────────────
all: build

build: $(SERVER_BIN) $(SYNCER_BIN)

$(SERVER_BIN):
	@mkdir -p $(BIN_DIR)
	go build -o $@ ./cmd/server

$(SYNCER_BIN):
	@mkdir -p $(BIN_DIR)
	go build -o $@ ./cmd/syncer

# ── 运行服务（基础设施由根目录 docker-compose.yml 统一管理）────────────────
run-server: $(SERVER_BIN)
	$(SERVER_BIN) -config $(CONFIG)

run-syncer: $(SYNCER_BIN)
	$(SYNCER_BIN) -config $(CONFIG) -state data/sync_state.json

# ── 测试 ──────────────────────────────────────────────────────────────────────
test:
	go test ./... -v -count=1

test-cache:
	go test ./internal/cache/... -v -count=1 -run TestCache

test-query:
	go test ./internal/query/... -v -count=1

# ── 实验基准 ──────────────────────────────────────────────────────────────────
# 实验1：查询延迟对比（预聚合 vs 明细扫描 vs 缓存命中）
experiment-query:
	@mkdir -p $(RESULTS_DIR)
	go run ./scripts/experiment \
		-mode query_latency \
		-config $(CONFIG) \
		-output $(RESULTS_DIR)/query_latency_$$(date +%Y%m%d_%H%M%S).json
	@echo "Results saved to $(RESULTS_DIR)"

# 实验2：缓存命中率与防击穿效果
experiment-cache:
	@mkdir -p $(RESULTS_DIR)
	go run ./scripts/experiment \
		-mode cache_perf \
		-config $(CONFIG) \
		-output $(RESULTS_DIR)/cache_perf_$$(date +%Y%m%d_%H%M%S).json
	@echo "Results saved to $(RESULTS_DIR)"

# 实验3：并发压测
experiment-concurrency:
	@mkdir -p $(RESULTS_DIR)
	go run ./scripts/experiment \
		-mode concurrency \
		-config $(CONFIG) \
		-output $(RESULTS_DIR)/concurrency_$$(date +%Y%m%d_%H%M%S).json

# 运行所有实验（完整论文数据收集）
experiment-all: experiment-query experiment-cache experiment-concurrency
	@echo "All experiments completed. Results in $(RESULTS_DIR)/"

# ── 清理 ──────────────────────────────────────────────────────────────────────
clean:
	rm -rf $(BIN_DIR) data/

# ── 帮助 ──────────────────────────────────────────────────────────────────────
help:
	@echo "医院数据中台查询引擎 - 可用命令："
	@echo ""
	@echo "  make build               编译所有二进制"
	@echo "  make run-server          启动查询 API 服务（:8082）"
	@echo "  make run-syncer          启动 ODS→ClickHouse 同步进程"
	@echo "  make test                运行所有单元测试"
	@echo "  make experiment-query    实验1：查询延迟对比"
	@echo "  make experiment-cache    实验2：缓存命中率"
	@echo "  make experiment-all      运行全部实验（论文数据）"
	@echo ""
	@echo "基础设施由根目录 docker-compose.yml 统一管理："
	@echo "  cd ~/go-project && docker compose up -d"
