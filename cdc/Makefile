.PHONY: build test test-unit test-integration test-fault lint \
        seed check experiment-correctness experiment-eos experiment-fault experiment-perf clean

# ===== 构建 =====
build:
	CGO_ENABLED=1 go build -o bin/cdc-platform ./cmd/platform
	CGO_ENABLED=1 go build -o bin/cdc-cli ./cmd/cli

# 基础设施由根目录 docker-compose.yml 统一管理：
#   cd ~/go-project && docker compose up -d

# ===== 测试 =====
test:
	go test ./... -v -count=1

test-unit:
	go test ./tests/unit/... -v -count=1

test-integration:
	go test ./tests/integration/... -v -count=1 -tags=integration

test-fault:
	go test ./tests/fault/... -v -count=1 -tags=fault -timeout=300s

# ===== 实验 =====
seed:
	go run ./scripts/seed/main.go --source mysql --records 10000
	go run ./scripts/seed/main.go --source sqlserver --records 5000

check:
	go run ./scripts/checker/main.go --tables patients,visits,lab_results,orders

experiment-correctness:
	go run ./scripts/experiment/main.go --type correctness --records 10000

experiment-eos:
	go run ./scripts/experiment/main.go --type eos --mode eos --records 5000
	go run ./scripts/experiment/main.go --type eos --mode at_least_once --records 5000

experiment-fault:
	go run ./scripts/experiment/main.go --type fault --scenario source_crash --records 10000

experiment-perf:
	go run ./scripts/experiment/main.go --type performance \
		--batch-sizes 100,500,1000 --partitions 1,3,6

# ===== 代码检查 =====
lint:
	go vet ./...
	@echo "vet passed"

# ===== 清理 =====
clean:
	rm -rf bin/ data/offsets/*.db scripts/results/*.json scripts/results/*.csv
	go clean -cache
