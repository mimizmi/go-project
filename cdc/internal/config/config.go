// Package config 通过 Viper 加载 YAML + 环境变量配置。
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config 全局配置根结构。
type Config struct {
	Sources  []SourceConfig  `mapstructure:"sources"`
	Pipeline PipelineConfig  `mapstructure:"pipeline"`
	Kafka    KafkaConfig     `mapstructure:"kafka"`
	Topics   []TopicMapping  `mapstructure:"topics"`
	Sink     SinkConfig      `mapstructure:"sink"`
	Metrics  MetricsConfig   `mapstructure:"metrics"`
	Log      LogConfig       `mapstructure:"log"`
}

// SourceConfig 单个数据源配置。
type SourceConfig struct {
	ID         string              `mapstructure:"id"`
	Type       string              `mapstructure:"type"` // "mysql" | "sqlserver"
	Host       string              `mapstructure:"host"`
	Port       int                 `mapstructure:"port"`
	User       string              `mapstructure:"user"`
	Password   string              `mapstructure:"password"`
	Database   string              `mapstructure:"database"`
	ServerID   uint32              `mapstructure:"server_id"`   // MySQL only
	Tables     []string            `mapstructure:"tables"`
	MaskFields map[string][]string `mapstructure:"mask_fields"`
}

// PipelineConfig 管道运行参数。
type PipelineConfig struct {
	BatchSize         int           `mapstructure:"batch_size"`
	BatchTimeout      time.Duration `mapstructure:"batch_timeout_ms"`
	MaxRetries        int           `mapstructure:"max_retries"`
	RetryBaseDelay    time.Duration `mapstructure:"retry_base_delay_ms"`
	RetryMaxDelay     time.Duration `mapstructure:"retry_max_delay_ms"`
}

// KafkaConfig Kafka 集群配置。
type KafkaConfig struct {
	BootstrapServers   string `mapstructure:"bootstrap_servers"`
	TransactionTimeout int    `mapstructure:"transaction_timeout_ms"`
}

// TopicMapping 库/表到 Kafka 主题的映射规则。
type TopicMapping struct {
	SourceID        string `mapstructure:"source_id"`
	Table           string `mapstructure:"table"`
	Topic           string `mapstructure:"topic"`
	PartitionCount  int    `mapstructure:"partition_count"`
	PartitionKeyField string `mapstructure:"partition_key_field"`
}

// SinkConfig 目标库配置。
type SinkConfig struct {
	PGDSN              string `mapstructure:"pg_dsn"`
	DedupRetentionHours int   `mapstructure:"dedup_retention_hours"`
}

// MetricsConfig 监控配置。
type MetricsConfig struct {
	Port int `mapstructure:"port"`
}

// LogConfig 日志配置。
type LogConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"` // "json" | "console"
}

// Load 从 YAML 文件和环境变量加载配置。
// 环境变量优先级高于文件配置，KEY 形如 CDC_KAFKA_BOOTSTRAP_SERVERS。
func Load(sourcesPath, topicsPath string) (*Config, error) {
	v := viper.New()

	// 环境变量
	v.SetEnvPrefix("CDC")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	v.AutomaticEnv()

	// 默认值
	v.SetDefault("pipeline.batch_size", 500)
	v.SetDefault("pipeline.batch_timeout_ms", 1000)
	v.SetDefault("pipeline.max_retries", -1)
	v.SetDefault("pipeline.retry_base_delay_ms", 1000)
	v.SetDefault("pipeline.retry_max_delay_ms", 60000)
	v.SetDefault("metrics.port", 8000)
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
	v.SetDefault("sink.dedup_retention_hours", 168)

	// 加载 sources.yaml
	v.SetConfigFile(sourcesPath)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("load sources config: %w", err)
	}

	// 合并 topics.yaml
	v2 := viper.New()
	v2.SetConfigFile(topicsPath)
	if err := v2.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("load topics config: %w", err)
	}
	for _, k := range v2.AllKeys() {
		v.Set(k, v2.Get(k))
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	// 展开 YAML 中的 ${ENV_VAR} 引用
	for i, s := range cfg.Sources {
		cfg.Sources[i].Host = os.ExpandEnv(s.Host)
		cfg.Sources[i].User = os.ExpandEnv(s.User)
		cfg.Sources[i].Password = os.ExpandEnv(s.Password)
		cfg.Sources[i].Database = os.ExpandEnv(s.Database)
	}
	cfg.Kafka.BootstrapServers = os.ExpandEnv(cfg.Kafka.BootstrapServers)
	cfg.Sink.PGDSN = os.ExpandEnv(cfg.Sink.PGDSN)

	// ms 转 duration
	cfg.Pipeline.BatchTimeout = time.Duration(v.GetInt("pipeline.batch_timeout_ms")) * time.Millisecond
	cfg.Pipeline.RetryBaseDelay = time.Duration(v.GetInt("pipeline.retry_base_delay_ms")) * time.Millisecond
	cfg.Pipeline.RetryMaxDelay = time.Duration(v.GetInt("pipeline.retry_max_delay_ms")) * time.Millisecond

	return &cfg, nil
}

// TopicFor 按 sourceID + table 查找映射规则，未找到时用默认规则。
func (c *Config) TopicFor(sourceID, database, table string) string {
	for _, m := range c.Topics {
		if m.SourceID == sourceID && m.Table == table {
			return m.Topic
		}
	}
	return fmt.Sprintf("cdc.%s.%s", database, table)
}
