// Package cache 实现多级缓存：L1 本地内存缓存 + L2 Redis 分布式缓存。
// 并集成 singleflight（防击穿）、随机 TTL 抖动（防雪崩）、布隆过滤器（防穿透）。
package cache

import (
	"sync"
	"time"
)

// localItem 缓存条目，携带过期时间。
type localItem struct {
	value     []byte
	expiresAt time.Time
}

// LocalCache 基于 sync.Map 的本地内存缓存，支持 TTL 过期与定期清理。
//
// 采用惰性过期（Get 时检测）+ 后台 GC goroutine 双重策略：
//   - 惰性过期：零额外 goroutine 开销，适合热数据访问路径。
//   - 后台 GC：防止过期条目持续占用内存，每 30 s 扫描一次。
type LocalCache struct {
	m        sync.Map
	maxItems int64
	count    int64
	mu       sync.Mutex
}

// NewLocalCache 创建本地缓存，maxItems 为容量上限（超出时随机淘汰）。
func NewLocalCache(maxItems int) *LocalCache {
	c := &LocalCache{maxItems: int64(maxItems)}
	go c.gcLoop()
	return c
}

// Get 读取缓存；过期或不存在返回 nil, false。
func (c *LocalCache) Get(key string) ([]byte, bool) {
	v, ok := c.m.Load(key)
	if !ok {
		return nil, false
	}
	item := v.(*localItem)
	if time.Now().After(item.expiresAt) {
		c.m.Delete(key)
		return nil, false
	}
	return item.value, true
}

// Set 写入缓存，ttl 为存活时长。容量超限时简单拒绝写入（牺牲缓存率换取稳定性）。
func (c *LocalCache) Set(key string, value []byte, ttl time.Duration) {
	c.mu.Lock()
	if c.count >= c.maxItems {
		c.mu.Unlock()
		return
	}
	c.count++
	c.mu.Unlock()

	c.m.Store(key, &localItem{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	})
}

// Delete 主动删除条目。
func (c *LocalCache) Delete(key string) {
	if _, loaded := c.m.LoadAndDelete(key); loaded {
		c.mu.Lock()
		c.count--
		c.mu.Unlock()
	}
}

// Len 返回当前缓存条目数（含可能已过期的）。
func (c *LocalCache) Len() int64 { return c.count }

// gcLoop 后台定期清理过期条目，每 30 s 执行一次。
func (c *LocalCache) gcLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		c.m.Range(func(k, v interface{}) bool {
			if now.After(v.(*localItem).expiresAt) {
				c.m.Delete(k)
				c.mu.Lock()
				c.count--
				c.mu.Unlock()
			}
			return true
		})
	}
}
