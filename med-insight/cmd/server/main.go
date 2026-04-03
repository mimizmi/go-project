// Command server 启动查询 API 服务。
//
// 功能：ClickHouse 多级缓存查询引擎 + 报表接口 + FHIR R4 接口
// 启动：go run ./cmd/server  或  ./bin/query-server
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/mimizh/med-insight/internal/api"
	"github.com/mimizh/med-insight/internal/cache"
	chclient "github.com/mimizh/med-insight/internal/clickhouse"
	"github.com/mimizh/med-insight/internal/config"
	"github.com/mimizh/med-insight/internal/metrics"
	"github.com/mimizh/med-insight/internal/query"
)

func main() {
	cfgPath := flag.String("config", "configs/config.yaml", "配置文件路径")
	flag.Parse()

	// ── 加载配置 ───────────────────────────────────────────────────────────
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		panic("load config: " + err.Error())
	}

	// ── 初始化日志 ─────────────────────────────────────────────────────────
	logger := buildLogger(cfg.Log.Level)
	defer logger.Sync() //nolint:errcheck

	// ── Prometheus 指标 ────────────────────────────────────────────────────
	m := metrics.New()

	// ── ClickHouse 连接 ────────────────────────────────────────────────────
	chClient, err := chclient.New(cfg.ClickHouse.DSN, cfg.ClickHouse.Database, logger)
	if err != nil {
		logger.Fatal("connect clickhouse", zap.Error(err))
	}
	defer chClient.Close()

	// ── Redis L2 缓存 ──────────────────────────────────────────────────────
	redisCache, err := cache.NewRedisCache(cfg.Redis)
	if err != nil {
		logger.Warn("redis unavailable, L2 cache disabled", zap.Error(err))
		// 降级：L2 不可用时仍可运行（仅 L1 缓存）
		redisCache = nil
	}

	// ── L1 本地缓存 + 缓存管理器 ──────────────────────────────────────────
	l1 := cache.NewLocalCache(cfg.Cache.L1MaxItems)
	var l2 *cache.RedisCache
	if redisCache != nil {
		l2 = redisCache
		defer l2.Close()
	}
	cacheMgr := cache.New(l1, l2, cfg.Cache, m)

	// ── 查询引擎 ───────────────────────────────────────────────────────────
	engine := query.New(chClient.Conn(), cacheMgr, m)

	// ── HTTP 服务 ──────────────────────────────────────────────────────────
	srv := api.New(engine, m, logger, cfg.Server.Port)

	// ── 优雅退出 ───────────────────────────────────────────────────────────
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	logger.Info("query engine server starting",
		zap.Int("port", cfg.Server.Port),
		zap.String("clickhouse", cfg.ClickHouse.DSN),
	)

	if err := srv.Start(ctx); err != nil {
		logger.Error("server error", zap.Error(err))
		os.Exit(1)
	}
	logger.Info("server stopped gracefully")
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
