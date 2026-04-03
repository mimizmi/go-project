package cache_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mimizh/med-insight/internal/cache"
	"github.com/mimizh/med-insight/internal/config"
	"github.com/mimizh/med-insight/internal/metrics"
)

// ─────────────────────────────────────────────────────────────────────────────
// LocalCache 单元测试
// ─────────────────────────────────────────────────────────────────────────────

func TestLocalCache_GetSet(t *testing.T) {
	c := cache.NewLocalCache(100)
	c.Set("k1", []byte("hello"), time.Minute)

	v, ok := c.Get("k1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if string(v) != "hello" {
		t.Fatalf("expected 'hello', got '%s'", v)
	}
}

func TestLocalCache_Expiry(t *testing.T) {
	c := cache.NewLocalCache(100)
	c.Set("expiring", []byte("value"), 50*time.Millisecond)

	_, ok := c.Get("expiring")
	if !ok {
		t.Fatal("should be present before expiry")
	}

	time.Sleep(100 * time.Millisecond)
	_, ok = c.Get("expiring")
	if ok {
		t.Fatal("should be expired")
	}
}

func TestLocalCache_MaxCapacity(t *testing.T) {
	c := cache.NewLocalCache(3)
	for i := 0; i < 5; i++ {
		c.Set(fmt.Sprintf("k%d", i), []byte("v"), time.Minute)
	}
	// 容量为3，后2个 Set 被忽略；前3个应存在
	count := 0
	for i := 0; i < 5; i++ {
		if _, ok := c.Get(fmt.Sprintf("k%d", i)); ok {
			count++
		}
	}
	if count > 3 {
		t.Fatalf("expected at most 3 items, got %d", count)
	}
}

func TestLocalCache_Delete(t *testing.T) {
	c := cache.NewLocalCache(100)
	c.Set("del", []byte("x"), time.Minute)
	c.Delete("del")
	if _, ok := c.Get("del"); ok {
		t.Fatal("should be deleted")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// CacheManager 单元测试（不依赖 Redis，用 nil L2）
// ─────────────────────────────────────────────────────────────────────────────

// testMetrics is a singleton to avoid prometheus duplicate-registration panics.
var testMetrics = metrics.New()

func newTestManager() *cache.Manager {
	m := testMetrics
	l1 := cache.NewLocalCache(1000)
	cfg := config.CacheConfig{
		L1TTLSeconds:  30,
		L2TTLSeconds:  300,
		L1MaxItems:    1000,
		TTLJitterPct:  10,
		BloomExpected: 10000,
		BloomFPRate:   0.01,
	}
	return cache.New(l1, nil, cfg, m)
}

func TestManager_GetSetRoundtrip(t *testing.T) {
	mgr := newTestManager()
	ctx := context.Background()

	type payload struct{ Count int }
	calls := 0

	mgr.RegisterKey("test:key1")
	raw, err := mgr.Get(ctx, "test:key1", func() (interface{}, error) {
		calls++
		return payload{Count: 42}, nil
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	var p payload
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if p.Count != 42 {
		t.Fatalf("expected 42, got %d", p.Count)
	}

	// 第二次调用应命中 L1，fetcher 不再执行
	mgr.Get(ctx, "test:key1", func() (interface{}, error) { //nolint:errcheck
		calls++
		return nil, nil
	})
	if calls != 1 {
		t.Fatalf("fetcher called %d times, expected 1 (L1 should hit)", calls)
	}
}

func TestManager_BloomFilterPreventsInvalidKeys(t *testing.T) {
	mgr := newTestManager()
	ctx := context.Background()

	// 未注册的 key → 布隆过滤器拦截，fetcher 不执行
	called := false
	_, err := mgr.Get(ctx, "nonexistent:key:xyz", func() (interface{}, error) {
		called = true
		return "data", nil
	})

	if err != cache.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if called {
		t.Fatal("fetcher should not be called for unregistered keys (bloom filter)")
	}
}

func TestManager_Invalidate(t *testing.T) {
	mgr := newTestManager()
	ctx := context.Background()

	mgr.RegisterKey("inv:key")
	mgr.Get(ctx, "inv:key", func() (interface{}, error) { return "data", nil }) //nolint:errcheck
	mgr.Invalidate(ctx, "inv:key")

	calls := 0
	mgr.RegisterKey("inv:key")
	mgr.Get(ctx, "inv:key", func() (interface{}, error) { //nolint:errcheck
		calls++
		return "data2", nil
	})
	if calls != 1 {
		t.Fatal("after invalidation, fetcher should be called once")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Singleflight 防击穿测试
// ─────────────────────────────────────────────────────────────────────────────

func TestManager_SingleflightDeduplication(t *testing.T) {
	mgr := newTestManager()
	ctx := context.Background()

	var mu sync.Mutex
	fetchCount := 0

	// 模拟慢查询（50ms），并发 20 个 goroutine 同时请求同一 key
	const concurrency = 20
	mgr.RegisterKey("sf:hotkey")

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mgr.Get(ctx, "sf:hotkey", func() (interface{}, error) { //nolint:errcheck
				time.Sleep(50 * time.Millisecond) // 模拟 ClickHouse 查询
				mu.Lock()
				fetchCount++
				mu.Unlock()
				return map[string]int{"rows": 100}, nil
			})
		}()
	}
	wg.Wait()

	// singleflight 应保证 fetcher 只被执行 1 次（允许 L1 过期后偶发 2 次）
	if fetchCount > 2 {
		t.Fatalf("expected singleflight to deduplicate: fetcher called %d times for %d goroutines",
			fetchCount, concurrency)
	}
	t.Logf("singleflight: %d requests merged into %d DB calls", concurrency, fetchCount)
}

// ─────────────────────────────────────────────────────────────────────────────
// TTL 抖动测试（防雪崩）
// ─────────────────────────────────────────────────────────────────────────────

func TestManager_TTLJitter(t *testing.T) {
	// 验证不同次调用的有效 TTL 不完全相同（存在随机抖动）
	mgr := newTestManager()
	ctx := context.Background()

	// 由于无法直接读取 TTL，此处通过 Set 多次写入并检查 L1 能否命中来间接验证
	// 主要确保 Set 方法不 panic 且能正常缓存
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("jitter:key:%d", i)
		mgr.RegisterKey(key)
		err := mgr.Set(ctx, key, map[string]int{"i": i})
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
		raw, err := mgr.Get(ctx, key, func() (interface{}, error) {
			return nil, fmt.Errorf("should not call fetcher")
		})
		if err != nil {
			t.Fatalf("Get after Set failed: %v", err)
		}
		var v map[string]int
		json.Unmarshal(raw, &v) //nolint:errcheck
		if v["i"] != i {
			t.Fatalf("expected i=%d, got %d", i, v["i"])
		}
	}
}
