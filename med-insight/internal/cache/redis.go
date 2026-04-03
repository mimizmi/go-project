package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mimizh/med-insight/internal/config"
)

// RedisCache L2 分布式 Redis 缓存，实现跨实例共享。
type RedisCache struct {
	client *redis.Client
	prefix string
}

// NewRedisCache 创建 Redis 缓存客户端。
func NewRedisCache(cfg config.RedisConfig) (*RedisCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     20,
		MinIdleConns: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &RedisCache{client: client, prefix: "hqe:"}, nil
}

// Get 读取 Redis 缓存。键不存在返回 nil, false（非错误）。
func (r *RedisCache) Get(ctx context.Context, key string) ([]byte, bool) {
	val, err := r.client.Get(ctx, r.prefix+key).Bytes()
	if err != nil {
		return nil, false // ErrNil 或网络错误均视为 miss
	}
	return val, true
}

// Set 写入 Redis 缓存，ttl 为存活时长。写入失败记录日志但不返回错误（缓存降级）。
func (r *RedisCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) {
	_ = r.client.Set(ctx, r.prefix+key, value, ttl).Err()
}

// Delete 删除缓存键（用于主动失效）。
func (r *RedisCache) Delete(ctx context.Context, key string) {
	_ = r.client.Del(ctx, r.prefix+key).Err()
}

// DBSize 返回 Redis 中以当前 prefix 开头的键数量（近似值，用于监控）。
func (r *RedisCache) DBSize(ctx context.Context) int64 {
	n, _ := r.client.DBSize(ctx).Result()
	return n
}

// Close 关闭 Redis 连接。
func (r *RedisCache) Close() error { return r.client.Close() }
