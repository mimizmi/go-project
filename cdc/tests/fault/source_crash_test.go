//go:build fault

// 容错测试：进程崩溃、Broker 宕机恢复验证
// 运行方式: go test ./tests/fault/... -tags=fault -v -timeout=300s
// 前提: FAULT_TESTS=1  docker compose up -d 全栈已运行
package fault_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
)

// TestSourceCrash_RecoveryNoDuplicate 场景：
//  1. 向 MySQL 持续写入数据
//  2. 强制重启 cdc-platform 容器（模拟进程崩溃）
//  3. 验证恢复后目标库数据不重不漏
func TestSourceCrash_RecoveryNoDuplicate(t *testing.T) {
	if os.Getenv("FAULT_TESTS") == "" {
		t.Skip("set FAULT_TESTS=1 to run fault injection tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	db := openMySQL(t)
	defer db.Close()

	batchID := time.Now().UnixNano()
	const recordCount = 100

	// 写入前半段数据
	for i := 0; i < recordCount/2; i++ {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO patients (name, gender) VALUES (?, 'F')`,
			fmt.Sprintf("crash-test-%d-%d", batchID, i),
		); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	t.Logf("inserted %d records (pre-crash)", recordCount/2)

	// 注入崩溃
	time.Sleep(2 * time.Second)
	dockerCmd(t, "restart", "cdc-platform")
	t.Log("cdc-platform restarted (crash simulated)")

	// 写入后半段数据
	for i := recordCount / 2; i < recordCount; i++ {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO patients (name, gender) VALUES (?, 'F')`,
			fmt.Sprintf("crash-test-%d-%d", batchID, i),
		); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	t.Logf("inserted %d records (post-crash)", recordCount/2)

	// 等待管道追平（最多 60s）
	time.Sleep(20 * time.Second)

	pgConn := openPostgres(t, ctx)
	defer pgConn.Close(ctx)

	// 行数一致性
	srcCount := countMySQL(t, db, fmt.Sprintf("name LIKE 'crash-test-%d-%%'", batchID))
	dstCount := countPG(t, pgConn, ctx, fmt.Sprintf("name LIKE 'crash-test-%d-%%'", batchID))

	t.Logf("src=%d  dst=%d", srcCount, dstCount)
	if srcCount != dstCount {
		t.Errorf("data mismatch after recovery: src=%d dst=%d", srcCount, dstCount)
	}

	// 无重复行
	dups := duplicateCountPG(t, pgConn, ctx, "ods_hospital_his_patients", "patient_id",
		fmt.Sprintf("name LIKE 'crash-test-%d-%%'", batchID))
	if dups > 0 {
		t.Errorf("found %d duplicate rows after crash recovery", dups)
	} else {
		t.Logf("✓ no duplicates after crash recovery")
	}
}

// TestKafkaBrokerDown_EventuallyDelivered 场景：停止 Kafka broker 30s 后恢复，验证最终一致。
func TestKafkaBrokerDown_EventuallyDelivered(t *testing.T) {
	if os.Getenv("FAULT_TESTS") == "" {
		t.Skip("set FAULT_TESTS=1 to run fault injection tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	db := openMySQL(t)
	defer db.Close()

	batchID := time.Now().UnixNano()
	const n = 50

	for i := 0; i < n; i++ {
		db.ExecContext(ctx, `INSERT INTO patients (name, gender) VALUES (?, 'M')`,
			fmt.Sprintf("broker-down-%d-%d", batchID, i))
	}

	t.Log("stopping kafka broker for 30s...")
	dockerCmd(t, "stop", "cdc-kafka")
	time.Sleep(30 * time.Second)
	dockerCmd(t, "start", "cdc-kafka")
	t.Log("kafka broker restarted")

	pgConn := openPostgres(t, ctx)
	defer pgConn.Close(ctx)

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		count := countPG(t, pgConn, ctx, fmt.Sprintf("name LIKE 'broker-down-%d-%%'", batchID))
		if count >= n {
			t.Logf("✓ all %d records delivered after broker recovery", n)
			return
		}
		time.Sleep(2 * time.Second)
	}
	got := countPG(t, pgConn, ctx, fmt.Sprintf("name LIKE 'broker-down-%d-%%'", batchID))
	if got < n {
		t.Errorf("only %d/%d records delivered after broker recovery", got, n)
	}
}

// -------------------------------------------------------------------
// 辅助函数
// -------------------------------------------------------------------

func openMySQL(t *testing.T) *sql.DB {
	t.Helper()
	host := envOr("MYSQL_HOST", "localhost")
	db, err := sql.Open("mysql",
		fmt.Sprintf("cdc_user:cdc_pass@tcp(%s:3306)/hospital_his", host))
	if err != nil {
		t.Skipf("mysql open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("mysql ping: %v", err)
	}
	return db
}

func openPostgres(t *testing.T, ctx context.Context) *pgx.Conn {
	t.Helper()
	host := envOr("PG_HOST", "localhost")
	dsn := fmt.Sprintf("postgres://ods_user:ods_pass@%s:5432/hospital_ods?sslmode=disable", host)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Skipf("postgres connect: %v", err)
	}
	return conn
}

func countMySQL(t *testing.T, db *sql.DB, where string) int {
	t.Helper()
	var n int
	db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM patients WHERE %s", where)).Scan(&n)
	return n
}

func countPG(t *testing.T, conn *pgx.Conn, ctx context.Context, where string) int {
	t.Helper()
	var n int
	conn.QueryRow(ctx,
		fmt.Sprintf(`SELECT COUNT(*) FROM ods_hospital_his_patients WHERE %s`, where),
	).Scan(&n)
	return n
}

func duplicateCountPG(t *testing.T, conn *pgx.Conn, ctx context.Context, table, pkCol, where string) int {
	t.Helper()
	var n int
	conn.QueryRow(ctx, fmt.Sprintf(`
		SELECT COUNT(*) FROM (
			SELECT "%s" FROM "%s" WHERE %s GROUP BY "%s" HAVING COUNT(*) > 1
		) dup`, pkCol, table, where, pkCol),
	).Scan(&n)
	return n
}

func dockerCmd(t *testing.T, subcmd, container string) {
	t.Helper()
	out, err := exec.Command("docker", subcmd, container).CombinedOutput()
	if err != nil {
		t.Logf("docker %s %s: %v — %s", subcmd, container, err, out)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
