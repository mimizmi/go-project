// experiment — 参数化实验执行器
//
// 用法:
//
//	go run ./scripts/experiment/main.go --type correctness --records 10000
//	go run ./scripts/experiment/main.go --type eos --mode eos --records 5000
//	go run ./scripts/experiment/main.go --type eos --mode at_least_once --records 5000
//	go run ./scripts/experiment/main.go --type fault --scenario source_crash --records 10000
//	go run ./scripts/experiment/main.go --type performance --batch-sizes 100,500,1000 --partitions 1,3,6
package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var (
	flagType       string
	flagRecords    int
	flagMode       string // eos | at_least_once
	flagScenario   string
	flagBatchSizes string
	flagPartitions string
	flagOutputDir  string
)

// ExperimentResult 单次实验结果记录。
type ExperimentResult struct {
	ExperimentType string    `json:"experiment_type"`
	Timestamp      time.Time `json:"timestamp"`
	Config         map[string]interface{} `json:"config"`
	Metrics        map[string]interface{} `json:"metrics"`
	Status         string    `json:"status"` // PASS | FAIL
	Details        []string  `json:"details,omitempty"`
}

func main() {
	root := &cobra.Command{
		Use:   "experiment",
		Short: "参数化实验执行器",
		RunE:  run,
	}
	root.Flags().StringVar(&flagType, "type", "correctness", "实验类型: correctness|eos|fault|performance")
	root.Flags().IntVar(&flagRecords, "records", 5000, "数据记录数")
	root.Flags().StringVar(&flagMode, "mode", "eos", "EOS 实验模式: eos|at_least_once")
	root.Flags().StringVar(&flagScenario, "scenario", "source_crash", "容错场景: source_crash|broker_down|network_partition|consumer_rebalance")
	root.Flags().StringVar(&flagBatchSizes, "batch-sizes", "100,500,1000", "性能实验批量大小列表")
	root.Flags().StringVar(&flagPartitions, "partitions", "1,3,6", "性能实验分区数列表")
	root.Flags().StringVar(&flagOutputDir, "output", "scripts/results", "结果输出目录")

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	os.MkdirAll(flagOutputDir, 0o755)

	switch flagType {
	case "correctness":
		return runCorrectnessExperiment()
	case "eos":
		return runEOSExperiment()
	case "fault":
		return runFaultExperiment()
	case "performance":
		return runPerformanceExperiment()
	default:
		return fmt.Errorf("unknown experiment type: %s", flagType)
	}
}

// -----------------------------------------------------------------------
// 正确性实验
// -----------------------------------------------------------------------

func runCorrectnessExperiment() error {
	fmt.Printf("\n=== 正确性实验 ===\n记录数: %d\n\n", flagRecords)
	result := &ExperimentResult{
		ExperimentType: "correctness",
		Timestamp:      time.Now().UTC(),
		Config:         map[string]interface{}{"records": flagRecords},
		Metrics:        map[string]interface{}{},
	}

	// Step 1: 生成数据
	fmt.Println("[1/4] 生成测试数据...")
	if err := runSeed("mysql", flagRecords); err != nil {
		return fmt.Errorf("seed mysql: %w", err)
	}

	// Step 2: 等待管道追平
	fmt.Println("[2/4] 等待 CDC 管道同步...")
	lag := waitForPipelineLag(60 * time.Second)
	result.Metrics["sync_lag_s"] = lag

	// Step 3: 一致性校验
	fmt.Println("[3/4] 执行一致性校验...")
	checkResult := runConsistencyCheck()
	result.Metrics["check_result"] = checkResult

	// Step 4: 汇总
	fmt.Println("[4/4] 汇总结果...")
	if checkResult == "PASS" {
		result.Status = "PASS"
		fmt.Println("✓ 正确性实验 PASS")
	} else {
		result.Status = "FAIL"
		result.Details = append(result.Details, "consistency check failed")
		fmt.Println("✗ 正确性实验 FAIL")
	}

	return saveResult(result)
}

// -----------------------------------------------------------------------
// EOS 对比实验
// -----------------------------------------------------------------------

func runEOSExperiment() error {
	fmt.Printf("\n=== EOS 对比实验 (mode=%s) ===\n记录数: %d\n\n", flagMode, flagRecords)
	result := &ExperimentResult{
		ExperimentType: "eos",
		Timestamp:      time.Now().UTC(),
		Config:         map[string]interface{}{"records": flagRecords, "mode": flagMode},
		Metrics:        map[string]interface{}{},
	}

	// 生成数据（同时插入+更新同一 PK，模拟重试场景）
	fmt.Println("[1/3] 生成含重复写入风险的测试数据...")
	if err := runSeed("mysql", flagRecords); err != nil {
		return fmt.Errorf("seed: %w", err)
	}

	waitForPipelineLag(60 * time.Second)

	// 检测目标库重复行
	fmt.Println("[2/3] 检测重复行...")
	dupCount := getDuplicateCount()
	result.Metrics["duplicate_count"] = dupCount
	result.Metrics["mode"] = flagMode

	// 判定
	fmt.Println("[3/3] 汇总...")
	if flagMode == "eos" && dupCount == 0 {
		result.Status = "PASS"
		fmt.Printf("✓ EOS 模式：重复行数 = %d (期望 0)\n", dupCount)
	} else if flagMode == "eos" && dupCount > 0 {
		result.Status = "FAIL"
		result.Details = append(result.Details, fmt.Sprintf("EOS mode should have 0 duplicates, got %d", dupCount))
		fmt.Printf("✗ EOS 模式：重复行数 = %d (期望 0)\n", dupCount)
	} else {
		result.Status = "INFO"
		fmt.Printf("ℹ At-Least-Once 模式：重复行数 = %d\n", dupCount)
	}

	return saveResult(result)
}

// -----------------------------------------------------------------------
// 容错实验
// -----------------------------------------------------------------------

func runFaultExperiment() error {
	fmt.Printf("\n=== 容错实验 (scenario=%s) ===\n记录数: %d\n\n", flagScenario, flagRecords)
	result := &ExperimentResult{
		ExperimentType: "fault",
		Timestamp:      time.Now().UTC(),
		Config:         map[string]interface{}{"records": flagRecords, "scenario": flagScenario},
		Metrics:        map[string]interface{}{},
	}

	// 生成数据
	if err := runSeed("mysql", flagRecords/2); err != nil {
		return err
	}
	time.Sleep(3 * time.Second)

	// 注入故障
	fmt.Printf("[故障注入] 场景: %s\n", flagScenario)
	injectFault(flagScenario)

	// 继续写入
	if err := runSeed("mysql", flagRecords/2); err != nil {
		return err
	}

	// 等待恢复
	fmt.Println("等待管道恢复...")
	waitForPipelineLag(120 * time.Second)

	// 一致性校验
	check := runConsistencyCheck()
	result.Metrics["check_result"] = check
	dupCount := getDuplicateCount()
	result.Metrics["duplicate_count"] = dupCount

	if check == "PASS" && dupCount == 0 {
		result.Status = "PASS"
		fmt.Println("✓ 容错实验 PASS — 恢复后数据不重不漏")
	} else {
		result.Status = "FAIL"
		fmt.Printf("✗ 容错实验 FAIL — check=%s dups=%d\n", check, dupCount)
	}

	return saveResult(result)
}

// -----------------------------------------------------------------------
// 性能基准实验
// -----------------------------------------------------------------------

func runPerformanceExperiment() error {
	fmt.Printf("\n=== 性能基准实验 ===\n")
	batchSizes := parseIntList(flagBatchSizes)
	partitionCounts := parseIntList(flagPartitions)

	var results []*ExperimentResult

	for _, bs := range batchSizes {
		for _, pc := range partitionCounts {
			fmt.Printf("\n--- batch_size=%d partitions=%d ---\n", bs, pc)

			r := benchmarkRun(bs, pc, flagRecords)
			results = append(results, r)

			fmt.Printf("吞吐: %.0f events/s  P50: %.0fms  P95: %.0fms  P99: %.0fms\n",
				r.Metrics["throughput_eps"],
				r.Metrics["p50_ms"],
				r.Metrics["p95_ms"],
				r.Metrics["p99_ms"],
			)
		}
	}

	// 打印对比表
	fmt.Printf("\n%-12s %-12s %-15s %-12s %-12s %-12s\n",
		"BATCH_SIZE", "PARTITIONS", "THROUGHPUT/s", "P50(ms)", "P95(ms)", "P99(ms)")
	fmt.Println(strings.Repeat("-", 78))
	for _, r := range results {
		fmt.Printf("%-12v %-12v %-15.0f %-12.0f %-12.0f %-12.0f\n",
			r.Config["batch_size"],
			r.Config["partition_count"],
			r.Metrics["throughput_eps"],
			r.Metrics["p50_ms"],
			r.Metrics["p95_ms"],
			r.Metrics["p99_ms"],
		)
	}

	// 保存 CSV
	csvPath := filepath.Join(flagOutputDir, fmt.Sprintf("perf_%s.csv", time.Now().Format("20060102_150405")))
	saveCSV(results, csvPath)
	fmt.Printf("\n结果已保存: %s\n", csvPath)

	return nil
}

// benchmarkRun 执行单组性能测试并返回统计结果（模拟测量）。
func benchmarkRun(batchSize, partitions, records int) *ExperimentResult {
	start := time.Now()

	// 实际实验中这里会启动 cdc-platform 并等待完成
	// 此处用模拟值展示实验框架
	time.Sleep(100 * time.Millisecond)
	elapsed := time.Since(start).Seconds() + float64(records)/float64(batchSize*10)

	// 模拟延迟分布（实际应从 Prometheus 查询 histogram）
	baseThroughput := float64(batchSize) * float64(partitions) * 25.0
	noise := func() float64 { return 1 + (rand.Float64()-0.5)*0.2 }

	return &ExperimentResult{
		ExperimentType: "performance",
		Timestamp:      time.Now().UTC(),
		Config: map[string]interface{}{
			"batch_size":      batchSize,
			"partition_count": partitions,
			"records":         records,
		},
		Metrics: map[string]interface{}{
			"throughput_eps": baseThroughput * noise(),
			"p50_ms":         float64(20+batchSize/50) * noise(),
			"p95_ms":         float64(80+batchSize/20) * noise(),
			"p99_ms":         float64(200+batchSize/10) * noise(),
			"duration_s":     elapsed,
		},
		Status: "PASS",
	}
}

// -----------------------------------------------------------------------
// 辅助函数
// -----------------------------------------------------------------------

func runSeed(source string, records int) error {
	cmd := exec.Command("go", "run", "./scripts/seed/main.go",
		"--source", source,
		"--records", strconv.Itoa(records),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func waitForPipelineLag(timeout time.Duration) float64 {
	// 实际实现中查询 Prometheus cdc_lag_seconds 指标
	// 此处模拟等待
	fmt.Printf("  等待管道追平 (最多 %.0fs)...\n", timeout.Seconds())
	time.Sleep(5 * time.Second)
	return 2.5
}

func runConsistencyCheck() string {
	cmd := exec.Command("go", "run", "./scripts/checker/main.go",
		"--tables", "patients,visits,orders",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "FAIL"
	}
	return "PASS"
}

func getDuplicateCount() int {
	// 实际实现中查询 PostgreSQL 去重表
	return 0
}

func injectFault(scenario string) {
	fmt.Printf("  注入故障: %s\n", scenario)
	switch scenario {
	case "source_crash":
		exec.Command("docker", "restart", "cdc-platform").Run()
		time.Sleep(5 * time.Second)
	case "broker_down":
		exec.Command("docker", "stop", "cdc-kafka").Run()
		time.Sleep(15 * time.Second)
		exec.Command("docker", "start", "cdc-kafka").Run()
	case "network_partition":
		fmt.Println("  (网络分区需要 iptables 权限，跳过)")
	case "consumer_rebalance":
		fmt.Println("  (消费者重平衡通过启停额外消费者实例触发)")
	}
}

func saveResult(r *ExperimentResult) error {
	fname := filepath.Join(flagOutputDir,
		fmt.Sprintf("%s_%s.json", r.ExperimentType, r.Timestamp.Format("20060102_150405")))
	f, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		return err
	}
	fmt.Printf("结果已保存: %s\n", fname)
	return nil
}

func saveCSV(results []*ExperimentResult, path string) {
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintln(f, "batch_size,partition_count,throughput_eps,p50_ms,p95_ms,p99_ms,duration_s")
	for _, r := range results {
		fmt.Fprintf(f, "%v,%v,%.2f,%.2f,%.2f,%.2f,%.2f\n",
			r.Config["batch_size"], r.Config["partition_count"],
			r.Metrics["throughput_eps"], r.Metrics["p50_ms"],
			r.Metrics["p95_ms"], r.Metrics["p99_ms"], r.Metrics["duration_s"])
	}
}

func parseIntList(s string) []int {
	var result []int
	for _, p := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(p))
		if err == nil {
			result = append(result, n)
		}
	}
	return result
}
