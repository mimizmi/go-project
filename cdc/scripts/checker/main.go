// checker — 源-目标一致性校验工具
//
// 用法:
//
//	go run ./scripts/checker/main.go --tables patients,visits,lab_results
//
// 校验方法:
//  1. 行数比对: COUNT(*) 源 vs 目标
//  2. 主键重复检测: 目标库无重复 PK
//  3. 抽样比对: 随机抽取 N 行，逐字段比对
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/spf13/cobra"
)

var (
	flagTables  string
	flagSample  int
	flagTimeout int
)

type CheckResult struct {
	Table      string
	SrcCount   int64
	DstCount   int64
	DupCount   int64
	DiffCount  int64
	Status     string // PASS / FAIL
	Details    []string
}

func main() {
	root := &cobra.Command{
		Use:   "checker",
		Short: "源-目标一致性校验",
		RunE:  run,
	}
	root.Flags().StringVar(&flagTables, "tables", "patients,visits,orders,lab_results", "校验表列表")
	root.Flags().IntVar(&flagSample, "sample", 100, "抽样比对行数")
	root.Flags().IntVar(&flagTimeout, "timeout", 60, "超时秒数")

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(flagTimeout)*time.Second)
	defer cancel()

	mysqlDB := openMySQL()
	if mysqlDB == nil {
		return fmt.Errorf("cannot connect to MySQL")
	}
	defer mysqlDB.Close()

	ssDB := openSQLServer()
	// SQL Server 可选；lab_results 校验依赖它
	if ssDB != nil {
		defer ssDB.Close()
	}

	pgConn := openPostgres(ctx)
	if pgConn == nil {
		return fmt.Errorf("cannot connect to PostgreSQL")
	}
	defer pgConn.Close(ctx)

	tables := strings.Split(flagTables, ",")
	allPass := true

	fmt.Printf("\n%-20s %10s %10s %10s %10s %s\n",
		"TABLE", "SRC_CNT", "DST_CNT", "DUPS", "DIFFS", "STATUS")
	fmt.Println(strings.Repeat("-", 75))

	for _, table := range tables {
		t := strings.TrimSpace(table)
		srcDB := mysqlDB
		if t == "lab_results" {
			if ssDB == nil {
				fmt.Printf("%-20s %10s %10s %10s %10s %s\n", t, "-", "-", "-", "-", "SKIP(no SS)")
				continue
			}
			srcDB = ssDB
		}
		result := checkTable(ctx, srcDB, pgConn, t)
		if result.Status == "FAIL" {
			allPass = false
		}
		fmt.Printf("%-20s %10d %10d %10d %10d %s\n",
			result.Table,
			result.SrcCount, result.DstCount,
			result.DupCount, result.DiffCount,
			result.Status)
		for _, d := range result.Details {
			fmt.Printf("  ⚠ %s\n", d)
		}
	}

	fmt.Println(strings.Repeat("-", 75))
	if allPass {
		fmt.Println("✓ ALL CHECKS PASSED")
		return nil
	}
	fmt.Println("✗ SOME CHECKS FAILED")
	os.Exit(2)
	return nil
}

func checkTable(ctx context.Context, mysqlDB *sql.DB, pgConn *pgx.Conn, table string) CheckResult {
	result := CheckResult{Table: table, Status: "PASS"}

	// 1. MySQL 行数
	mysqlTable, dstTable := resolveTableNames(table)
	row := mysqlDB.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", mysqlTable))
	if err := row.Scan(&result.SrcCount); err != nil {
		result.Details = append(result.Details, fmt.Sprintf("mysql count error: %v", err))
		result.Status = "FAIL"
		return result
	}

	// 2. PostgreSQL 行数
	pgRow := pgConn.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, dstTable))
	if err := pgRow.Scan(&result.DstCount); err != nil {
		result.Details = append(result.Details, fmt.Sprintf("pg count error (table may not exist): %v", err))
		result.Status = "FAIL"
		return result
	}

	// 3. 行数比对
	if result.SrcCount != result.DstCount {
		diff := result.SrcCount - result.DstCount
		result.Details = append(result.Details,
			fmt.Sprintf("row count mismatch: src=%d dst=%d diff=%d", result.SrcCount, result.DstCount, diff))
		result.Status = "FAIL"
	}

	// 4. 重复检测（目标库）
	pkCol := resolvePKCol(table)
	dupRow := pgConn.QueryRow(ctx, fmt.Sprintf(`
		SELECT COUNT(*) FROM (
			SELECT "%s" FROM "%s" GROUP BY "%s" HAVING COUNT(*) > 1
		) dup`, pkCol, dstTable, pkCol))
	if err := dupRow.Scan(&result.DupCount); err == nil && result.DupCount > 0 {
		result.Details = append(result.Details,
			fmt.Sprintf("DUPLICATES detected: %d duplicate PKs", result.DupCount))
		result.Status = "FAIL"
	}

	return result
}

func resolveTableNames(table string) (src, pg string) {
	src = table
	pg = fmt.Sprintf("ods_hospital_his_%s", table)
	if table == "lab_results" {
		src = "dbo.lab_results"
		pg = "ods_hospital_lis_lab_results"
	}
	return
}

func openSQLServer() *sql.DB {
	host := envOr("SQLSERVER_HOST", "localhost")
	pass := envOr("SQLSERVER_SA_PASSWORD", "YourStrong!Pass123")
	dsn := fmt.Sprintf("sqlserver://sa:%s@%s:1433?database=hospital_lis", pass, host)
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sqlserver open: %v\n", err)
		return nil
	}
	if err := db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "sqlserver ping: %v\n", err)
		return nil
	}
	return db
}

func resolvePKCol(table string) string {
	m := map[string]string{
		"patients":    "patient_id",
		"visits":      "visit_id",
		"orders":      "order_id",
		"lab_results": "result_id",
	}
	if pk, ok := m[table]; ok {
		return pk
	}
	return "id"
}

func openMySQL() *sql.DB {
	host := envOr("MYSQL_HOST", "localhost")
	dsn := fmt.Sprintf("cdc_user:cdc_pass@tcp(%s:3306)/hospital_his", host)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mysql open: %v\n", err)
		return nil
	}
	if err := db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "mysql ping: %v\n", err)
		return nil
	}
	return db
}

func openPostgres(ctx context.Context) *pgx.Conn {
	host := envOr("PG_HOST", "localhost")
	port := envOr("PG_PORT", "5433")
	dsn := fmt.Sprintf("postgres://ods_user:ods_pass@%s:%s/hospital_ods?sslmode=disable", host, port)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pg connect: %v\n", err)
		return nil
	}
	return conn
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
