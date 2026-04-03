// seed — 模拟医院核心业务数据生成工具
//
// 用法:
//
//	go run ./scripts/seed/main.go --source mysql --records 10000
//	go run ./scripts/seed/main.go --source mysql --records 5000 --continuous --rate 100
//	go run ./scripts/seed/main.go --source sqlserver --records 5000 --mix insert:60,update:30,delete:10
package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/spf13/cobra"
)

var (
	flagSource     string
	flagRecords    int
	flagContinuous bool
	flagRate       int    // records/sec
	flagMix        string // e.g. "insert:60,update:30,delete:10"
	flagTables     string
)

func main() {
	root := &cobra.Command{
		Use:   "seed",
		Short: "模拟医院业务数据生成",
		RunE:  run,
	}
	root.Flags().StringVar(&flagSource, "source", "mysql", "数据源类型: mysql|sqlserver")
	root.Flags().IntVar(&flagRecords, "records", 1000, "生成记录数")
	root.Flags().BoolVar(&flagContinuous, "continuous", false, "持续写入模式")
	root.Flags().IntVar(&flagRate, "rate", 100, "持续模式: 每秒写入数")
	root.Flags().StringVar(&flagMix, "mix", "insert:100", "操作类型比例 insert:N,update:N,delete:N")
	root.Flags().StringVar(&flagTables, "tables", "patients,visits,orders,lab_results", "目标表列表")

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	ops := parseMix(flagMix)
	tables := strings.Split(flagTables, ",")

	switch flagSource {
	case "mysql":
		return seedMySQL(ops, tables)
	case "sqlserver":
		return seedSQLServer(ops, tables)
	default:
		return fmt.Errorf("unknown source: %s", flagSource)
	}
}

func seedMySQL(ops []string, tables []string) error {
	host := envOr("MYSQL_HOST", "localhost")
	user := envOr("MYSQL_SEED_USER", "root")
	pass := envOr("MYSQL_SEED_PASS", "root")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:3306)/hospital_his?parseTime=true", user, pass, host)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	patientIDs := make([]int64, 0, flagRecords)
	visitIDs := make([]int64, 0, flagRecords)

	start := time.Now()
	for i := 0; i < flagRecords || flagContinuous; i++ {
		if flagContinuous && flagRate > 0 {
			expected := time.Duration(float64(time.Second) / float64(flagRate))
			time.Sleep(expected)
		}

		op := ops[rng.Intn(len(ops))]
		table := tables[rng.Intn(len(tables))]

		switch table {
		case "visits":
			if len(patientIDs) == 0 {
				id, err := insertPatient(db, i)
				if err == nil {
					patientIDs = append(patientIDs, id)
				}
				continue
			}
			pid := patientIDs[rng.Intn(len(patientIDs))]
			id, err := insertVisit(db, i, pid, rng)
			if err != nil {
				fmt.Fprintf(os.Stderr, "insert visit: %v\n", err)
			} else {
				visitIDs = append(visitIDs, id)
			}
		case "orders":
			if len(visitIDs) == 0 {
				continue
			}
			vid := visitIDs[rng.Intn(len(visitIDs))]
			if err := insertOrder(db, i, vid, rng); err != nil {
				fmt.Fprintf(os.Stderr, "insert order: %v\n", err)
			}
		default: // patients
			switch op {
			case "update":
				if len(patientIDs) > 0 {
					id := patientIDs[rng.Intn(len(patientIDs))]
					if err := updatePatient(db, id); err != nil {
						fmt.Fprintf(os.Stderr, "update patient %d: %v\n", id, err)
					}
				}
			case "delete":
				if len(patientIDs) > 5 {
					idx := rng.Intn(len(patientIDs))
					id := patientIDs[idx]
					patientIDs = append(patientIDs[:idx], patientIDs[idx+1:]...)
					if err := deletePatient(db, id); err != nil {
						fmt.Fprintf(os.Stderr, "delete patient %d: %v\n", id, err)
					}
				}
			default:
				id, err := insertPatient(db, i)
				if err != nil {
					fmt.Fprintf(os.Stderr, "insert patient: %v\n", err)
				} else {
					patientIDs = append(patientIDs, id)
				}
			}
		}

		if (i+1)%500 == 0 {
			rate := float64(i+1) / time.Since(start).Seconds()
			fmt.Printf("progress: %d/%d (%.0f records/s)\n", i+1, flagRecords, rate)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("done: %d ops in %v (%.0f ops/s)\n",
		flagRecords, elapsed, float64(flagRecords)/elapsed.Seconds())
	return nil
}

func insertPatient(db *sql.DB, seq int) (int64, error) {
	genders := []string{"M", "F"}
	res, err := db.Exec(
		`INSERT INTO patients (name, gender, birth_date, address) VALUES (?, ?, ?, ?)`,
		fmt.Sprintf("患者%d", seq),
		genders[seq%2],
		randomDate(),
		fmt.Sprintf("北京市朝阳区%d号", seq%1000),
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func updatePatient(db *sql.DB, id int64) error {
	_, err := db.Exec(
		`UPDATE patients SET address = ?, updated_at = NOW() WHERE patient_id = ?`,
		fmt.Sprintf("上海市浦东新区%d号", id),
		id,
	)
	return err
}

func deletePatient(db *sql.DB, id int64) error {
	_, err := db.Exec(`DELETE FROM patients WHERE patient_id = ?`, id)
	return err
}

func insertVisit(db *sql.DB, seq int, patientID int64, rng *rand.Rand) (int64, error) {
	depts := []string{"内科", "外科", "儿科", "妇产科", "急诊科", "骨科"}
	types := []string{"outpatient", "inpatient", "emergency"}
	admit := time.Now().Add(-time.Duration(rng.Intn(30*24)) * time.Hour)
	res, err := db.Exec(
		`INSERT INTO visits (patient_id, dept, doctor, visit_type, admit_time, diagnosis) VALUES (?, ?, ?, ?, ?, ?)`,
		patientID,
		depts[seq%len(depts)],
		fmt.Sprintf("医生%d", seq%50),
		types[seq%len(types)],
		admit,
		fmt.Sprintf("诊断结果%d", seq%100),
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func insertOrder(db *sql.DB, seq int, visitID int64, rng *rand.Rand) error {
	routes := []string{"口服", "静脉注射", "肌肉注射", "外用"}
	drugs := []string{"阿莫西林", "布洛芬", "头孢克洛", "阿司匹林", "奥美拉唑", "甲硝唑"}
	_, err := db.Exec(
		`INSERT INTO orders (visit_id, drug_name, dosage, frequency, route, order_time, doctor, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		visitID,
		drugs[seq%len(drugs)],
		fmt.Sprintf("%.1fmg", float64(50+rng.Intn(450))),
		"每日三次",
		routes[seq%len(routes)],
		time.Now(),
		fmt.Sprintf("医生%d", seq%50),
		"pending",
	)
	return err
}

func seedSQLServer(ops []string, tables []string) error {
	host := envOr("SQLSERVER_HOST", "localhost")
	dsn := fmt.Sprintf("server=%s;port=1433;user id=sa;password=YourStrong!Pass123;database=hospital_lis", host)
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < flagRecords; i++ {
		_ = rng
		_, err := db.Exec(`
			INSERT INTO dbo.lab_results
				(visit_id, patient_id, item_code, item_name, value, unit, is_abnormal, result_time)
			VALUES (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8)`,
			int64(i%1000)+1, int64(i%500)+1,
			fmt.Sprintf("CBC%03d", i%20),
			"全血细胞计数",
			float64(50+rng.Intn(150))/10.0,
			"g/L",
			i%5 == 0, // 20% 异常
			time.Now().UTC(),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "insert lab_result %d: %v\n", i, err)
		}
	}
	fmt.Printf("seeded %d lab_results to SQL Server\n", flagRecords)
	return nil
}

func parseMix(mix string) []string {
	var ops []string
	for _, part := range strings.Split(mix, ",") {
		kv := strings.SplitN(part, ":", 2)
		if len(kv) != 2 {
			continue
		}
		n, _ := strconv.Atoi(kv[1])
		for i := 0; i < n; i++ {
			ops = append(ops, kv[0])
		}
	}
	if len(ops) == 0 {
		ops = []string{"insert"}
	}
	return ops
}

func randomDate() string {
	year := 1950 + rand.Intn(60)
	month := 1 + rand.Intn(12)
	day := 1 + rand.Intn(28)
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
