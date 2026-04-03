package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"go.uber.org/zap"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
)

// SQLServerCdcSource 通过轮询 SQL Server 内置 CDC 函数实现增量采集。
//
// 前提：目标表已通过 sys.sp_cdc_enable_table 启用 CDC。
// 位点：__$start_lsn（十六进制字符串）
// 轮询间隔：PollInterval（默认 500ms）
type SQLServerCdcSource struct {
	cfg      SQLServerConfig
	db       *sql.DB
	builder  *EventBuilder
	eventCh  chan *core.ChangeEvent
	errCh    chan error
	logger   *zap.Logger
	curPos   *core.OffsetPosition
	pkCache  map[string][]string // table -> pk columns
}

// SQLServerConfig SQL Server 数据源配置。
type SQLServerConfig struct {
	SourceID    string
	Host        string
	Port        int
	User        string
	Password    string
	Database    string
	SchemaName  string // 通常为 "dbo"
	Tables      []string
	MaskFields  map[string][]string
	PollInterval time.Duration
}

// NewSQLServerCdcSource 创建 SQL Server CDC 数据源。
func NewSQLServerCdcSource(cfg SQLServerConfig, logger *zap.Logger) *SQLServerCdcSource {
	if cfg.SchemaName == "" {
		cfg.SchemaName = "dbo"
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	return &SQLServerCdcSource{
		cfg:     cfg,
		builder: NewEventBuilder(cfg.SourceID, core.SourceSQLServer, cfg.Database, cfg.SchemaName, cfg.MaskFields),
		eventCh: make(chan *core.ChangeEvent, 1024),
		errCh:   make(chan error, 8),
		logger:  logger,
		pkCache: make(map[string][]string),
	}
}

func (s *SQLServerCdcSource) SourceID() string                   { return s.cfg.SourceID }
func (s *SQLServerCdcSource) Events() <-chan *core.ChangeEvent    { return s.eventCh }
func (s *SQLServerCdcSource) Errors() <-chan error                { return s.errCh }
func (s *SQLServerCdcSource) CurrentPosition() *core.OffsetPosition {
	if s.curPos == nil {
		return core.NewOffsetPosition(s.cfg.SourceID, core.SourceSQLServer)
	}
	return s.curPos
}

// Start 启动 SQL Server CDC 轮询协程。
func (s *SQLServerCdcSource) Start(ctx context.Context, pos *core.OffsetPosition) error {
	// 关闭旧连接，重建 channels 防止旧 goroutine 残留错误污染新实例
	if s.db != nil {
		s.db.Close() //nolint:errcheck
		s.db = nil
	}
	s.eventCh = make(chan *core.ChangeEvent, 1024)
	s.errCh = make(chan error, 8)

	connStr := fmt.Sprintf(
		"server=%s;port=%d;user id=%s;password=%s;database=%s;encrypt=disable",
		s.cfg.Host, s.cfg.Port, s.cfg.User, s.cfg.Password, s.cfg.Database,
	)
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		return fmt.Errorf("%w: open: %v", core.ErrCdcConnection, err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close() //nolint:errcheck
		return fmt.Errorf("%w: ping: %v", core.ErrCdcConnection, err)
	}
	s.db = db

	// 加载各表主键
	for _, table := range s.cfg.Tables {
		pks, err := s.loadPrimaryKeys(ctx, table)
		if err != nil {
			s.logger.Warn("load pk failed, using heuristic",
				zap.String("table", table), zap.Error(err))
		}
		s.pkCache[table] = pks
	}

	// 起始 LSN
	startLSN := ""
	if pos != nil && pos.IsValid() {
		startLSN = pos.LSN
		s.logger.Info("sqlserver cdc resuming",
			zap.String("source_id", s.cfg.SourceID),
			zap.String("lsn", startLSN))
	} else {
		curLSN, err := s.currentMinLSN(ctx)
		if err != nil {
			return fmt.Errorf("get min lsn: %w", err)
		}
		startLSN = curLSN
		s.logger.Info("sqlserver cdc starting fresh",
			zap.String("source_id", s.cfg.SourceID),
			zap.String("lsn", startLSN))
	}

	go s.pollLoop(ctx, startLSN)
	return nil
}

// Close 关闭数据库连接。
func (s *SQLServerCdcSource) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// -------------------------------------------------------------------
// 内部轮询逻辑
// -------------------------------------------------------------------

func (s *SQLServerCdcSource) pollLoop(ctx context.Context, fromLSN string) {
	defer close(s.eventCh)
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	curLSN := fromLSN
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			maxLSN, err := s.currentMaxLSN(ctx)
			if err != nil {
				s.sendErr(fmt.Errorf("%w: get max lsn: %v", core.ErrCdcRead, err))
				return // DB 不可用，退出 goroutine，由 pipeline 重启
			}
			if maxLSN <= curLSN {
				continue
			}

			for _, table := range s.cfg.Tables {
				events, nextLSN, err := s.pollTable(ctx, table, curLSN, maxLSN)
				if err != nil {
					s.sendErr(fmt.Errorf("%w: poll %s: %v", core.ErrCdcRead, table, err))
					return
				}
				for _, e := range events {
					select {
					case s.eventCh <- e:
					case <-ctx.Done():
						return
					}
				}
				if nextLSN > curLSN {
					curLSN = nextLSN
				}
			}

			// 更新当前位点
			s.curPos = &core.OffsetPosition{
				SourceID:      s.cfg.SourceID,
				SourceType:    core.SourceSQLServer,
				LSN:           curLSN,
				PollTimestamp: time.Now().UTC(),
				UpdatedAt:     time.Now().UTC(),
			}
		}
	}
}

func (s *SQLServerCdcSource) pollTable(
	ctx context.Context,
	table, fromLSN, toLSN string,
) ([]*core.ChangeEvent, string, error) {
	// SQL Server CDC 函数命名规则: cdc.{schema}_{table}_CT
	captureInstance := fmt.Sprintf("%s_%s", s.cfg.SchemaName, table)

	// 直接查询 CDC 变更表 (_CT) 而非 TVF，避免 from_lsn < min_lsn 限制（error 313）。
	// 使用 TOP 1000 分批处理，防止大批量时 Kafka 事务 flush 超时。
	// 严格 > 避免重复消费上批最后一条。
	query := fmt.Sprintf(`
		SELECT TOP 1000 *
		FROM cdc.%s_CT
		WHERE __$start_lsn > %s
		  AND __$start_lsn <= %s
		ORDER BY __$start_lsn`, captureInstance, fromLSN, toLSN)

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		// 若 CDC 未启用，静默跳过
		if strings.Contains(err.Error(), "Invalid object name") {
			return nil, fromLSN, nil
		}
		return nil, fromLSN, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fromLSN, err
	}

	pkCols := s.pkCache[table]
	var events []*core.ChangeEvent
	lastLSN := fromLSN

	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fromLSN, err
		}

		rowMap := make(map[string]interface{}, len(cols))
		var lsnBytes []byte
		var operation int

		for i, col := range cols {
			switch col {
			case "__$start_lsn":
				if b, ok := vals[i].([]byte); ok {
					lsnBytes = b
				}
			case "__$operation":
				switch v := vals[i].(type) {
				case int64:
					operation = int(v)
				case int32:
					operation = int(v)
				}
			case "__$update_mask", "__$command_id", "__$seqval", "__$end_lsn":
				// 跳过 CDC 元数据列
			default:
				rowMap[col] = vals[i]
			}
		}

		lsn := fmt.Sprintf("0x%X", lsnBytes)
		pks := ExtractPrimaryKeys(rowMap, pkCols)

		var before, after map[string]interface{}
		switch operation {
		case 1: // DELETE
			before = rowMap
		case 2: // INSERT
			after = rowMap
		case 4: // UPDATE (after image)
			after = rowMap
		}

		e := s.builder.BuildSQLServer(
			table, operation, pks, before, after,
			lsn, "", time.Now().UTC(),
		)
		events = append(events, e)
		lastLSN = lsn
	}

	return events, lastLSN, rows.Err()
}

func (s *SQLServerCdcSource) currentMinLSN(ctx context.Context) (string, error) {
	var lsn []byte
	err := s.db.QueryRowContext(ctx, `SELECT sys.fn_cdc_get_min_lsn('') `).Scan(&lsn)
	if err != nil {
		return "0x00000000000000000000", nil
	}
	return fmt.Sprintf("0x%X", lsn), nil
}

func (s *SQLServerCdcSource) captureMinLSN(ctx context.Context, captureInstance string) (string, error) {
	var lsn []byte
	err := s.db.QueryRowContext(ctx, `SELECT sys.fn_cdc_get_min_lsn(@p1)`, captureInstance).Scan(&lsn)
	if err != nil {
		return "0x00000000000000000000", nil
	}
	return fmt.Sprintf("0x%X", lsn), nil
}

func (s *SQLServerCdcSource) currentMaxLSN(ctx context.Context) (string, error) {
	var lsn []byte
	err := s.db.QueryRowContext(ctx, `SELECT sys.fn_cdc_get_max_lsn()`).Scan(&lsn)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("0x%X", lsn), nil
}

func (s *SQLServerCdcSource) loadPrimaryKeys(ctx context.Context, table string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT c.COLUMN_NAME
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c
		  ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
		WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		  AND tc.TABLE_NAME = @p1
		  AND tc.TABLE_SCHEMA = @p2
		ORDER BY c.COLUMN_NAME`, table, s.cfg.SchemaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pks []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		pks = append(pks, col)
	}
	return pks, rows.Err()
}

func (s *SQLServerCdcSource) sendErr(err error) {
	select {
	case s.errCh <- err:
	default:
	}
}
