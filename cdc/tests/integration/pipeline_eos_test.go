//go:build integration

// 全链路 Exactly-Once 集成测试
// 运行方式: go test ./tests/integration/... -tags=integration -v -timeout=300s
// 前提: docker compose up -d mysql kafka postgres 已运行
package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/mimizh/hospital-cdc-platform/internal/config"
	"github.com/mimizh/hospital-cdc-platform/internal/core"
	"github.com/mimizh/hospital-cdc-platform/internal/transport"
)

func kafkaBootstrap() string { return envOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") }
func mysqlDSN() string {
	return fmt.Sprintf("cdc_user:cdc_pass@tcp(%s:3306)/hospital_his", envOrDefault("MYSQL_HOST", "localhost"))
}
func pgDSN() string {
	return fmt.Sprintf("postgres://ods_user:ods_pass@%s:5432/hospital_ods?sslmode=disable",
		envOrDefault("PG_HOST", "localhost"))
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func testLogger(t *testing.T) *zap.Logger {
	t.Helper()
	l, _ := zap.NewDevelopment()
	return l
}

// minimalCfg 构建最小化 config.Config，将所有路由指向单个测试 topic。
func minimalCfg(topic string) *config.Config {
	return &config.Config{
		Kafka: config.KafkaConfig{BootstrapServers: kafkaBootstrap()},
		Topics: []config.TopicMapping{
			{SourceID: "his_mysql_01", Table: "patients", Topic: topic, PartitionKeyField: "patient_id"},
		},
	}
}

// TestKafkaTxn_CommitVisible_AbortInvisible 验证 Kafka 事务语义：
// - commit 后消息对 read_committed 可见
// - abort 后消息不可见
func TestKafkaTxn_CommitVisible_AbortInvisible(t *testing.T) {
	topic := fmt.Sprintf("test-eos-%d", time.Now().UnixNano())
	router := transport.NewTopicRouter(minimalCfg(topic))
	logger := testLogger(t)

	producer, err := transport.NewTransactionalProducer(kafkaBootstrap(), "test-src", router, logger)
	if err != nil {
		t.Skipf("kafka not available: %v", err)
	}
	defer producer.Close()

	// 1. commit 的消息
	event1 := makeTestEvent("patients", core.OpInsert, map[string]interface{}{"patient_id": 1})
	if err := producer.BeginTxn(); err != nil {
		t.Fatal(err)
	}
	if err := producer.Send(event1); err != nil {
		t.Fatal(err)
	}
	txnID, err := producer.CommitTxn()
	if err != nil {
		t.Fatalf("commit txn: %v", err)
	}
	t.Logf("committed txn: %s", txnID)

	// 2. abort 的消息
	event2 := makeTestEvent("patients", core.OpInsert, map[string]interface{}{"patient_id": 2})
	if err := producer.BeginTxn(); err != nil {
		t.Fatal(err)
	}
	if err := producer.Send(event2); err != nil {
		t.Fatal(err)
	}
	if err := producer.AbortTxn(); err != nil {
		t.Fatal(err)
	}

	// 3. read_committed 消费者只能看到 event1
	consumer, err := transport.NewExactlyOnceConsumer(
		kafkaBootstrap(),
		fmt.Sprintf("test-group-%d", time.Now().UnixNano()),
		[]string{topic},
		logger,
	)
	if err != nil {
		t.Skipf("kafka consumer unavailable: %v", err)
	}
	defer consumer.Close()

	events, _, err := consumer.PollBatch(10, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Errorf("read_committed: expected 1 event, got %d", len(events))
	}
	if len(events) > 0 {
		pk := fmt.Sprint(events[0].PrimaryKeys["patient_id"])
		if pk != "1" {
			t.Errorf("expected patient_id=1, got %s", pk)
		}
	}
}

// TestPipeline_MySQL_To_PG_Consistency 端到端一致性: MySQL INSERT -> Kafka -> PG
func TestPipeline_MySQL_To_PG_Consistency(t *testing.T) {
	if os.Getenv("INTEGRATION_FULL") == "" {
		t.Skip("set INTEGRATION_FULL=1 to run full pipeline test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("mysql", mysqlDSN())
	if err != nil {
		t.Skipf("mysql unavailable: %v", err)
	}
	defer db.Close()

	testID := time.Now().UnixNano()
	_, err = db.ExecContext(ctx,
		`INSERT INTO patients (name, gender, birth_date) VALUES (?, 'M', '1990-01-01')`,
		fmt.Sprintf("test-patient-%d", testID),
	)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	pgConn, err := pgx.Connect(ctx, pgDSN())
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	defer pgConn.Close(ctx)

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		pgConn.QueryRow(ctx,
			`SELECT COUNT(*) FROM ods_hospital_his_patients WHERE name = $1`,
			fmt.Sprintf("test-patient-%d", testID),
		).Scan(&count)
		if count > 0 {
			t.Logf("record propagated to ODS in %.1fs", time.Since(time.Unix(0, testID)).Seconds())
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Error("record not found in ODS within 30s")
}

func makeTestEvent(table string, op core.OpType, pks map[string]interface{}) *core.ChangeEvent {
	e := core.NewChangeEvent()
	e.SourceID = "his_mysql_01"
	e.Database = "hospital_his"
	e.Table = table
	e.OpType = op
	e.PrimaryKeys = pks
	e.After = pks
	e.BinlogFile = "mysql-bin.000001"
	e.BinlogPos = 100
	return e
}
