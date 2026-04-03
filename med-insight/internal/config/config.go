// Package config 加载并持有全局配置。
package config

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

// Config 全局配置结构体。
type Config struct {
	Server     ServerConfig     `mapstructure:"server"`
	ClickHouse ClickHouseConfig `mapstructure:"clickhouse"`
	Postgres   PostgresConfig   `mapstructure:"postgres"`
	Redis      RedisConfig      `mapstructure:"redis"`
	Cache      CacheConfig      `mapstructure:"cache"`
	Syncer     SyncerConfig     `mapstructure:"syncer"`
	Log        LogConfig        `mapstructure:"log"`
}

// ServerConfig HTTP 服务配置。
type ServerConfig struct {
	Port int    `mapstructure:"port"`
	Mode string `mapstructure:"mode"`
}

// ClickHouseConfig ClickHouse 连接配置。
type ClickHouseConfig struct {
	DSN      string `mapstructure:"dsn"`
	Database string `mapstructure:"database"`
}

// PostgresConfig PostgreSQL ODS 连接配置。
type PostgresConfig struct {
	DSN string `mapstructure:"dsn"`
}

// RedisConfig Redis 连接配置。
type RedisConfig struct {
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

// CacheConfig 多级缓存配置。
type CacheConfig struct {
	L1TTLSeconds  int     `mapstructure:"l1_ttl_seconds"`
	L2TTLSeconds  int     `mapstructure:"l2_ttl_seconds"`
	L1MaxItems    int     `mapstructure:"l1_max_items"`
	TTLJitterPct  int     `mapstructure:"ttl_jitter_pct"`
	BloomExpected uint    `mapstructure:"bloom_expected"`
	BloomFPRate   float64 `mapstructure:"bloom_fp_rate"`
}

// SyncerConfig ODS→ClickHouse 同步配置。
type SyncerConfig struct {
	IntervalSeconds int `mapstructure:"interval_seconds"`
	BatchSize       int `mapstructure:"batch_size"`
}

// LogConfig 日志配置。
type LogConfig struct {
	Level string `mapstructure:"level"`
}

// Load 从文件加载配置；支持环境变量覆盖。
func Load(path string) (*Config, error) {
	if path == "" {
		path = "configs/config.yaml"
	}
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("config file not found: %w", err)
	}

	viper.SetConfigFile(path)
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	// 环境变量覆盖
	if dsn := os.Getenv("CLICKHOUSE_DSN"); dsn != "" {
		cfg.ClickHouse.DSN = dsn
	}
	if dsn := os.Getenv("POSTGRES_DSN"); dsn != "" {
		cfg.Postgres.DSN = dsn
	}
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		cfg.Redis.Addr = addr
	}

	return &cfg, nil
}
