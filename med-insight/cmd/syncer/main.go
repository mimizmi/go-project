// Command syncer 启动 ODS→ClickHouse 增量同步服务。
//
// 功能：轮询 PostgreSQL ODS，将变更行增量写入 ClickHouse，触发物化视图自动预聚合。
// 启动：go run ./cmd/syncer  或  ./bin/syncer
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	chclient "github.com/mimizh/med-insight/internal/clickhouse"
	"github.com/mimizh/med-insight/internal/config"
	"github.com/mimizh/med-insight/internal/metrics"
	"github.com/mimizh/med-insight/internal/syncer"
)

func main() {
	cfgPath := flag.String("config", "configs/config.yaml", "配置文件路径")
	statePath := flag.String("state", "data/sync_state.json", "水位线状态文件路径")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		panic("load config: " + err.Error())
	}

	logger := buildLogger(cfg.Log.Level)
	defer logger.Sync() //nolint:errcheck

	m := metrics.New()

	// ── PostgreSQL ODS ─────────────────────────────────────────────────────
	pgConn, err := pgx.Connect(context.Background(), cfg.Postgres.DSN)
	if err != nil {
		logger.Fatal("connect postgres", zap.Error(err))
	}
	defer pgConn.Close(context.Background())

	// ── ClickHouse ─────────────────────────────────────────────────────────
	chClient, err := chclient.New(cfg.ClickHouse.DSN, cfg.ClickHouse.Database, logger)
	if err != nil {
		logger.Fatal("connect clickhouse", zap.Error(err))
	}
	defer chClient.Close()

	// ── 确保状态目录存在 ───────────────────────────────────────────────────
	if err := os.MkdirAll(filepath.Dir(*statePath), 0755); err != nil {
		logger.Fatal("create state dir", zap.Error(err))
	}

	// ── 启动同步 Worker ────────────────────────────────────────────────────
	worker := syncer.New(pgConn, chClient.Conn(), *statePath, cfg.Syncer, logger, m)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger.Info("syncer starting",
		zap.String("postgres", cfg.Postgres.DSN),
		zap.String("clickhouse", cfg.ClickHouse.DSN),
		zap.Int("interval_s", cfg.Syncer.IntervalSeconds),
	)

	worker.Run(ctx)
	logger.Info("syncer stopped")
}

func buildLogger(level string) *zap.Logger {
	lvl := zapcore.InfoLevel
	_ = lvl.UnmarshalText([]byte(level))
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(lvl)
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, _ := cfg.Build()
	return logger
}
