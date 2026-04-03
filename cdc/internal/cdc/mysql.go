package cdc

import (
	"context"
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"go.uber.org/zap"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
)

// MySQLCdcSource 基于 go-mysql canal 解析 binlog 的 MySQL CDC 实现。
//
// 要求：
//   - binlog_format = ROW
//   - binlog_row_image = FULL
//   - CDC 用户需要 REPLICATION CLIENT, REPLICATION SLAVE, SELECT 权限
//
// 位点：BinlogFile + BinlogPos
type MySQLCdcSource struct {
	cfg      MySQLConfig
	canal    *canal.Canal
	builder  *EventBuilder
	eventCh  chan *core.ChangeEvent
	errCh    chan error
	logger   *zap.Logger
	curPos   *core.OffsetPosition
}

// MySQLConfig MySQL 数据源连接配置。
type MySQLConfig struct {
	SourceID   string
	Host       string
	Port       uint16
	User       string
	Password   string
	Database   string
	Tables     []string // 空 = 监听所有表
	ServerID   uint32
	MaskFields map[string][]string
}

// NewMySQLCdcSource 创建 MySQL CDC 数据源。
func NewMySQLCdcSource(cfg MySQLConfig, logger *zap.Logger) *MySQLCdcSource {
	return &MySQLCdcSource{
		cfg:     cfg,
		builder: NewEventBuilder(cfg.SourceID, core.SourceMySQL, cfg.Database, "", cfg.MaskFields),
		eventCh: make(chan *core.ChangeEvent, 1024),
		errCh:   make(chan error, 8),
		logger:  logger,
	}
}

// SourceID 实现 ICdcSource。
func (s *MySQLCdcSource) SourceID() string { return s.cfg.SourceID }

// Events 返回事件 channel。
func (s *MySQLCdcSource) Events() <-chan *core.ChangeEvent { return s.eventCh }

// Errors 返回错误 channel。
func (s *MySQLCdcSource) Errors() <-chan error { return s.errCh }

// CurrentPosition 返回当前位点。
func (s *MySQLCdcSource) CurrentPosition() *core.OffsetPosition {
	if s.curPos == nil {
		return core.NewOffsetPosition(s.cfg.SourceID, core.SourceMySQL)
	}
	return s.curPos
}

// Start 启动 binlog 采集协程。
func (s *MySQLCdcSource) Start(ctx context.Context, pos *core.OffsetPosition) error {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	cfg.User = s.cfg.User
	cfg.Password = s.cfg.Password
	cfg.ServerID = s.cfg.ServerID
	cfg.Flavor = "mysql"
	cfg.Dump.ExecutionPath = "" // 不做全量 dump，仅增量

	if len(s.cfg.Tables) > 0 {
		includes := make([]string, len(s.cfg.Tables))
		for i, t := range s.cfg.Tables {
			includes[i] = s.cfg.Database + "\\." + t
		}
		cfg.IncludeTableRegex = includes
	}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return fmt.Errorf("create canal: %w", err)
	}
	s.canal = c
	c.SetEventHandler(&mysqlEventHandler{source: s})

	// 确定起始位点
	var startPos mysql.Position
	if pos != nil && pos.IsValid() {
		startPos = mysql.Position{Name: pos.BinlogFile, Pos: pos.BinlogPos}
		s.logger.Info("mysql cdc resuming",
			zap.String("source_id", s.cfg.SourceID),
			zap.String("file", pos.BinlogFile),
			zap.Uint32("pos", pos.BinlogPos))
	} else {
		// 从当前 master 位点开始
		masterPos, err := c.GetMasterPos()
		if err != nil {
			return fmt.Errorf("get master pos: %w", err)
		}
		startPos = masterPos
		s.logger.Info("mysql cdc starting fresh",
			zap.String("source_id", s.cfg.SourceID),
			zap.String("file", startPos.Name),
			zap.Uint32("pos", startPos.Pos))
	}

	go func() {
		defer close(s.eventCh)
		if err := c.RunFrom(startPos); err != nil {
			select {
			case s.errCh <- fmt.Errorf("%w: canal run: %v", core.ErrCdcRead, err):
			default:
			}
		}
	}()

	// 监听 ctx 取消
	go func() {
		<-ctx.Done()
		s.canal.Close()
	}()

	return nil
}

// Close 停止采集。
func (s *MySQLCdcSource) Close() error {
	if s.canal != nil {
		s.canal.Close()
	}
	return nil
}

// -------------------------------------------------------------------
// canal.EventHandler 实现
// -------------------------------------------------------------------

type mysqlEventHandler struct {
	canal.DummyEventHandler
	source *MySQLCdcSource
}

func (h *mysqlEventHandler) OnRow(e *canal.RowsEvent) error {
	table := e.Table.Name
	pkCols := make([]string, len(e.Table.PKColumns))
	for i, idx := range e.Table.PKColumns {
		pkCols[i] = e.Table.Columns[idx].Name
	}

	srcTS := time.Unix(int64(e.Header.Timestamp), 0).UTC()

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			rowMap := rowToMap(e.Table.Columns, row)
			pks := ExtractPrimaryKeys(rowMap, pkCols)
			event := h.source.builder.BuildMySQL(
				table, core.OpInsert, pks,
				nil, rowMap,
				h.source.canal.SyncedPosition().Name,
				h.source.canal.SyncedPosition().Pos,
				"", srcTS,
			)
			h.emit(event)
		}

	case canal.UpdateAction:
		for i := 0; i < len(e.Rows)-1; i += 2 {
			beforeMap := rowToMap(e.Table.Columns, e.Rows[i])
			afterMap := rowToMap(e.Table.Columns, e.Rows[i+1])
			pks := ExtractPrimaryKeys(afterMap, pkCols)
			event := h.source.builder.BuildMySQL(
				table, core.OpUpdate, pks,
				beforeMap, afterMap,
				h.source.canal.SyncedPosition().Name,
				h.source.canal.SyncedPosition().Pos,
				"", srcTS,
			)
			h.emit(event)
		}

	case canal.DeleteAction:
		for _, row := range e.Rows {
			rowMap := rowToMap(e.Table.Columns, row)
			pks := ExtractPrimaryKeys(rowMap, pkCols)
			event := h.source.builder.BuildMySQL(
				table, core.OpDelete, pks,
				rowMap, nil,
				h.source.canal.SyncedPosition().Name,
				h.source.canal.SyncedPosition().Pos,
				"", srcTS,
			)
			h.emit(event)
		}
	}
	return nil
}

func (h *mysqlEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, gtid mysql.GTIDSet, force bool) error {
	h.source.curPos = &core.OffsetPosition{
		SourceID:   h.source.cfg.SourceID,
		SourceType: core.SourceMySQL,
		BinlogFile: pos.Name,
		BinlogPos:  pos.Pos,
		UpdatedAt:  time.Now().UTC(),
	}
	return nil
}

func (h *mysqlEventHandler) String() string { return "hospital-cdc-mysql" }

func (h *mysqlEventHandler) emit(e *core.ChangeEvent) {
	h.source.eventCh <- e
}

// rowToMap 将 go-mysql schema 列切片转换为 map。
func rowToMap(cols []schema.TableColumn, row []interface{}) map[string]interface{} {
	m := make(map[string]interface{}, len(cols))
	for i, col := range cols {
		if i < len(row) {
			m[col.Name] = row[i]
		}
	}
	return m
}
