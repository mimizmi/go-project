// CDC Platform CLI — 位点查询/重置/健康检查工具
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/mimizh/hospital-cdc-platform/internal/core"
	"github.com/mimizh/hospital-cdc-platform/internal/offset"
)

var dbPath string

var root = &cobra.Command{
	Use:   "cdc-cli",
	Short: "hospital-cdc-platform 运维 CLI",
}

var offsetCmd = &cobra.Command{
	Use:   "offset",
	Short: "位点管理",
}

var offsetListCmd = &cobra.Command{
	Use:   "list",
	Short: "列出所有数据源的当前位点",
	RunE: func(cmd *cobra.Command, args []string) error {
		store, err := offset.NewSQLiteOffsetStore(dbPath)
		if err != nil {
			return err
		}
		defer store.Close()

		ids, err := store.ListSources()
		if err != nil {
			return err
		}
		for _, id := range ids {
			pos, err := store.Load(id)
			if err != nil {
				fmt.Fprintf(os.Stderr, "load %s: %v\n", id, err)
				continue
			}
			b, _ := json.MarshalIndent(pos, "", "  ")
			fmt.Printf("=== %s ===\n%s\n\n", id, string(b))
		}
		return nil
	},
}

var offsetResetCmd = &cobra.Command{
	Use:   "reset <source_id>",
	Short: "重置指定数据源的位点（下次从最新位点开始）",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		sourceID := args[0]
		store, err := offset.NewSQLiteOffsetStore(dbPath)
		if err != nil {
			return err
		}
		defer store.Close()

		fmt.Printf("确认重置 %s 的位点? 输入 yes 继续: ", sourceID)
		var confirm string
		fmt.Scan(&confirm)
		if confirm != "yes" {
			fmt.Println("已取消")
			return nil
		}
		// 用空位点覆盖 — 下次启动视为全新（从当前最新 binlog 位置开始）
		emptyPos := &core.OffsetPosition{SourceID: sourceID}
		if err := store.Save(sourceID, emptyPos, ""); err != nil {
			return fmt.Errorf("reset offset: %w", err)
		}
		fmt.Printf("位点已重置: %s\n", sourceID)
		return nil
	},
}

func init() {
	root.PersistentFlags().StringVar(&dbPath, "db", "data/offsets/offsets.db", "位点数据库路径")
	offsetCmd.AddCommand(offsetListCmd, offsetResetCmd)
	root.AddCommand(offsetCmd)
}

func main() {
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
