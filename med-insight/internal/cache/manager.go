package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"golang.org/x/sync/singleflight"

	"github.com/mimizh/med-insight/internal/config"
	"github.com/mimizh/med-insight/internal/metrics"
)

// ErrNotFound key 不在布隆过滤器中（防穿透）。
var ErrNotFound = fmt.Errorf("cache: key not found in bloom filter")

// Manager 多级缓存管理器：防击穿(singleflight) + 防雪崩(TTL抖动) + 防穿透(bloom filter)。
type Manager struct {
	l1    *LocalCache
	l2    *RedisCache // nil = Redis 不可用，降级为纯 L1
	group singleflight.Group
	bloom *bloom.BloomFilter
	cfg   config.CacheConfig
	m     *metrics.Metrics
}

// New 创建 Manager。l2 可为 nil。
func New(l1 *LocalCache, l2 *RedisCache, cfg config.CacheConfig, m *metrics.Metrics) *Manager {
	bf := bloom.NewWithEstimates(cfg.BloomExpected, cfg.BloomFPRate)
	return &Manager{l1: l1, l2: l2, bloom: bf, cfg: cfg, m: m}
}

// RegisterKey 将合法 key 注册到布隆过滤器。
func (mgr *Manager) RegisterKey(key string) {
	mgr.bloom.Add([]byte(key))
}

// Get 按 L1 → L2 → fetcher 顺序查询，自动回填缓存。
func (mgr *Manager) Get(ctx context.Context, key string, fetcher func() (interface{}, error)) ([]byte, error) {
	// 防穿透
	if !mgr.bloom.Test([]byte(key)) {
		return nil, ErrNotFound
	}

	// L1
	if v, ok := mgr.l1.Get(key); ok {
		mgr.m.CacheHits.WithLabelValues("L1").Inc()
		return v, nil
	}

	// L2（可选）
	if mgr.l2 != nil {
		if v, ok := mgr.l2.Get(ctx, key); ok {
			mgr.m.CacheHits.WithLabelValues("L2").Inc()
			mgr.l1.Set(key, v, mgr.l1TTL())
			return v, nil
		}
	}

	// 防击穿：singleflight 合并并发穿透请求
	raw, err, _ := mgr.group.Do(key, func() (interface{}, error) {
		v, err := fetcher()
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal result: %w", err)
		}
		if mgr.l2 != nil {
			mgr.l2.Set(ctx, key, b, mgr.l2TTLWithJitter()) // 防雪崩：随机 TTL
		}
		mgr.l1.Set(key, b, mgr.l1TTL())
		mgr.bloom.Add([]byte(key))
		return b, nil
	})
	if err != nil {
		return nil, err
	}

	mgr.m.CacheMisses.Inc()
	return raw.([]byte), nil
}

// Set 直接写入缓存（预热/主动刷新）。
func (mgr *Manager) Set(ctx context.Context, key string, value interface{}) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	mgr.l1.Set(key, b, mgr.l1TTL())
	if mgr.l2 != nil {
		mgr.l2.Set(ctx, key, b, mgr.l2TTLWithJitter())
	}
	mgr.bloom.Add([]byte(key))
	return nil
}

// Invalidate 主动失效 key。
func (mgr *Manager) Invalidate(ctx context.Context, key string) {
	mgr.l1.Delete(key)
	if mgr.l2 != nil {
		mgr.l2.Delete(ctx, key)
	}
}

// UpdateMetrics 更新缓存容量 Prometheus 指标。
func (mgr *Manager) UpdateMetrics(ctx context.Context) {
	mgr.m.CacheSize.WithLabelValues("L1").Set(float64(mgr.l1.Len()))
	if mgr.l2 != nil {
		mgr.m.CacheSize.WithLabelValues("L2").Set(float64(mgr.l2.DBSize(ctx)))
	}
}

func (mgr *Manager) l1TTL() time.Duration {
	return time.Duration(mgr.cfg.L1TTLSeconds) * time.Second
}

// l2TTLWithJitter 防雪崩：base ± jitterPct%。
func (mgr *Manager) l2TTLWithJitter() time.Duration {
	base := float64(mgr.cfg.L2TTLSeconds)
	jitter := base * float64(mgr.cfg.TTLJitterPct) / 100.0
	actual := base + (rand.Float64()*2-1)*jitter
	if actual < 1 {
		actual = 1
	}
	return time.Duration(actual) * time.Second
}
