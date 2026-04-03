// Package offset 实现基于 SQLite WAL 的位点持久化存储。
package offset

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
)

const ddl = `
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS offsets (
	source_id        TEXT PRIMARY KEY,
	source_type      TEXT NOT NULL,
	binlog_file      TEXT,
	binlog_pos       INTEGER,
	gtid             TEXT,
	lsn              TEXT,
	poll_timestamp   TEXT,
	kafka_txn_id     TEXT,
	kafka_txn_epoch  INTEGER DEFAULT 0,
	updated_at       TEXT NOT NULL,
	event_count      INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS offset_history (
	id               INTEGER PRIMARY KEY AUTOINCREMENT,
	source_id        TEXT NOT NULL,
	position_json    TEXT NOT NULL,
	committed_at     TEXT NOT NULL,
	kafka_txn_id     TEXT
);

CREATE INDEX IF NOT EXISTS idx_history_source_time
	ON offset_history(source_id, committed_at DESC);
`

// SQLiteOffsetStore 基于 SQLite WAL 的位点存储，实现 core.IOffsetStore。
type SQLiteOffsetStore struct {
	db *sql.DB
}

// NewSQLiteOffsetStore 打开或创建 SQLite 数据库并初始化 schema。
func NewSQLiteOffsetStore(dbPath string) (*SQLiteOffsetStore, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir offset dir: %w", err)
	}
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite 单写者
	if _, err := db.Exec(ddl); err != nil {
		return nil, fmt.Errorf("init schema: %w", err)
	}
	return &SQLiteOffsetStore{db: db}, nil
}

// Load 加载指定源最近一次成功提交的位点。
func (s *SQLiteOffsetStore) Load(sourceID string) (*core.OffsetPosition, error) {
	row := s.db.QueryRow(`
		SELECT source_type, binlog_file, binlog_pos, gtid,
		       lsn, poll_timestamp, kafka_txn_id, kafka_txn_epoch,
		       updated_at, event_count
		FROM offsets WHERE source_id = ?`, sourceID)

	var (
		sourceType    string
		binlogFile    sql.NullString
		binlogPos     sql.NullInt64
		gtid          sql.NullString
		lsn           sql.NullString
		pollTs        sql.NullString
		kafkaTxnID    sql.NullString
		kafkaTxnEpoch sql.NullInt64
		updatedAt     string
		eventCount    int64
	)
	err := row.Scan(&sourceType, &binlogFile, &binlogPos, &gtid,
		&lsn, &pollTs, &kafkaTxnID, &kafkaTxnEpoch,
		&updatedAt, &eventCount)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", core.ErrOffsetLoad, err)
	}

	pos := &core.OffsetPosition{
		SourceID:      sourceID,
		SourceType:    core.SourceType(sourceType),
		BinlogFile:    binlogFile.String,
		BinlogPos:     uint32(binlogPos.Int64),
		GTID:          gtid.String,
		LSN:           lsn.String,
		KafkaTxnID:    kafkaTxnID.String,
		KafkaTxnEpoch: int32(kafkaTxnEpoch.Int64),
		EventCount:    eventCount,
	}
	if pollTs.Valid && pollTs.String != "" {
		t, _ := time.Parse(time.RFC3339, pollTs.String)
		pos.PollTimestamp = t
	}
	if updatedAt != "" {
		t, _ := time.Parse(time.RFC3339, updatedAt)
		pos.UpdatedAt = t
	}
	return pos, nil
}

// Save 在单个 SQLite 事务中原子更新当前位点 + 追加历史记录。
//
// 时序保证：调用方须先完成 Kafka 事务 commit，再调用 Save。
// 即使 Save 失败（进程崩溃），恢复时重新采集，消费端幂等键负责去重。
func (s *SQLiteOffsetStore) Save(sourceID string, pos *core.OffsetPosition, txnID string) error {
	pos.KafkaTxnID = txnID
	pos.UpdatedAt = time.Now().UTC()

	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("%w: begin tx: %v", core.ErrOffsetSave, err)
	}
	defer tx.Rollback() //nolint:errcheck

	_, err = tx.Exec(`
		INSERT OR REPLACE INTO offsets
		(source_id, source_type, binlog_file, binlog_pos, gtid,
		 lsn, poll_timestamp, kafka_txn_id, kafka_txn_epoch,
		 updated_at, event_count)
		VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		sourceID,
		string(pos.SourceType),
		nullStr(pos.BinlogFile),
		nullInt(int64(pos.BinlogPos)),
		nullStr(pos.GTID),
		nullStr(pos.LSN),
		nullStr(pos.PollTimestamp.Format(time.RFC3339)),
		nullStr(txnID),
		pos.KafkaTxnEpoch,
		pos.UpdatedAt.Format(time.RFC3339),
		pos.EventCount,
	)
	if err != nil {
		return fmt.Errorf("%w: upsert offsets: %v", core.ErrOffsetSave, err)
	}

	_, err = tx.Exec(`
		INSERT INTO offset_history (source_id, position_json, committed_at, kafka_txn_id)
		VALUES (?,?,?,?)`,
		sourceID,
		pos.ToJSON(),
		time.Now().UTC().Format(time.RFC3339),
		txnID,
	)
	if err != nil {
		return fmt.Errorf("%w: insert history: %v", core.ErrOffsetSave, err)
	}

	return tx.Commit()
}

// LastTxnID 返回最后一次成功提交的 Kafka 事务 ID。
func (s *SQLiteOffsetStore) LastTxnID(sourceID string) (string, error) {
	var txnID sql.NullString
	err := s.db.QueryRow(
		`SELECT kafka_txn_id FROM offsets WHERE source_id = ?`, sourceID,
	).Scan(&txnID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("%w: %v", core.ErrOffsetLoad, err)
	}
	return txnID.String, nil
}

// ListSources 返回所有已记录的 sourceID。
func (s *SQLiteOffsetStore) ListSources() ([]string, error) {
	rows, err := s.db.Query(`SELECT source_id FROM offsets`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// Close 关闭数据库连接。
func (s *SQLiteOffsetStore) Close() error { return s.db.Close() }

// -------------------------------------------------------------------
// 工具函数
// -------------------------------------------------------------------

func nullStr(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}

func nullInt(n int64) sql.NullInt64 {
	return sql.NullInt64{Int64: n, Valid: n != 0}
}
