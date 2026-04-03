package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.uber.org/zap"
)

// Client 封装 ClickHouse 连接与建表迁移。
type Client struct {
	conn   driver.Conn
	db     string
	logger *zap.Logger
}

// New 创建 ClickHouse 连接，并自动执行 schema 迁移。
func New(dsn, database string, logger *zap.Logger) (*Client, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse dsn: %w", err)
	}
	opts.ConnMaxLifetime = time.Hour
	opts.MaxIdleConns = 5
	opts.MaxOpenConns = 10
	opts.DialTimeout = 10 * time.Second

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}

	c := &Client{conn: conn, db: database, logger: logger}
	if err := c.migrate(ctx); err != nil {
		return nil, fmt.Errorf("migrate schema: %w", err)
	}
	return c, nil
}

// Conn 返回底层驱动连接，供查询引擎直接使用。
func (c *Client) Conn() driver.Conn { return c.conn }

// migrate 按顺序执行 DDL 语句创建所有必要表和视图。
func (c *Client) migrate(ctx context.Context) error {
	for _, ddl := range schemaDDL {
		if err := c.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("exec DDL: %w\nSQL: %.200s", err, ddl)
		}
	}
	c.logger.Info("ClickHouse schema migrated", zap.String("database", c.db))
	return nil
}

// Close 关闭连接。
func (c *Client) Close() error { return c.conn.Close() }

// QueryRow 执行单行查询并扫描结果。
func (c *Client) QueryRow(ctx context.Context, query string, args ...interface{}) (driver.Row, error) {
	row := c.conn.QueryRow(ctx, query, args...)
	return row, nil
}

// Query 执行多行查询。
func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

// Exec 执行无返回值语句（INSERT/CREATE 等）。
func (c *Client) Exec(ctx context.Context, query string, args ...interface{}) error {
	return c.conn.Exec(ctx, query, args...)
}

// PrepareBatch 创建批量写入器，用于高效 INSERT。
func (c *Client) PrepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	return c.conn.PrepareBatch(ctx, query)
}
