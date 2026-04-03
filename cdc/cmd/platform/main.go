// 医院异构系统实时数据中台 — 主程序入口
package main

import (
	"context"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/mimizh/hospital-cdc-platform/internal/config"
	"github.com/mimizh/hospital-cdc-platform/internal/engine"
	"github.com/mimizh/hospital-cdc-platform/internal/monitoring"
	"github.com/mimizh/hospital-cdc-platform/internal/offset"
)

func main() {
	// 日志初始化
	logger := newLogger(os.Getenv("LOG_LEVEL"))
	defer logger.Sync() //nolint:errcheck

	// 配置加载
	sourcesPath := envOr("SOURCES_CONFIG", "configs/sources.yaml")
	topicsPath := envOr("TOPICS_CONFIG", "configs/topics.yaml")
	cfg, err := config.Load(sourcesPath, topicsPath)
	if err != nil {
		logger.Fatal("load config failed", zap.Error(err))
	}
	logger.Info("config loaded",
		zap.Int("sources", len(cfg.Sources)),
		zap.Int("topics", len(cfg.Topics)))

	// 位点存储
	dbPath := envOr("OFFSET_STORE_PATH", "data/offsets/offsets.db")
	offsetStore, err := offset.NewSQLiteOffsetStore(dbPath)
	if err != nil {
		logger.Fatal("init offset store failed", zap.Error(err))
	}
	defer offsetStore.Close() //nolint:errcheck

	// 指标
	metrics := monitoring.NewMetrics()

	// 启动协调器
	coordinator := engine.NewCoordinator(cfg, offsetStore, metrics, logger)
	if err := coordinator.Run(context.Background()); err != nil {
		logger.Fatal("coordinator exited with error", zap.Error(err))
	}
}

func newLogger(level string) *zap.Logger {
	lvl := zapcore.InfoLevel
	_ = lvl.UnmarshalText([]byte(level))

	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(lvl)
	if os.Getenv("LOG_FORMAT") == "console" {
		cfg.Encoding = "console"
	}
	l, _ := cfg.Build()
	return l
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
