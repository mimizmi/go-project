package engine

// builder.go — 从 config.SourceConfig 构建具体 ICdcSource 实例。
// 单独抽出避免 coordinator.go 过长，同时避免循环导入。

import (
	"go.uber.org/zap"

	"github.com/mimizh/hospital-cdc-platform/internal/cdc"
	"github.com/mimizh/hospital-cdc-platform/internal/config"
	"github.com/mimizh/hospital-cdc-platform/internal/core"
)

func buildMySQLSource(s config.SourceConfig, logger *zap.Logger) core.ICdcSource {
	serverID := s.ServerID
	if serverID == 0 {
		serverID = 1001
	}
	port := uint16(s.Port)
	if port == 0 {
		port = 3306
	}
	return cdc.NewMySQLCdcSource(cdc.MySQLConfig{
		SourceID:   s.ID,
		Host:       s.Host,
		Port:       port,
		User:       s.User,
		Password:   s.Password,
		Database:   s.Database,
		Tables:     s.Tables,
		ServerID:   serverID,
		MaskFields: s.MaskFields,
	}, logger)
}

func buildSQLServerSource(s config.SourceConfig, logger *zap.Logger) core.ICdcSource {
	port := s.Port
	if port == 0 {
		port = 1433
	}
	return cdc.NewSQLServerCdcSource(cdc.SQLServerConfig{
		SourceID:   s.ID,
		Host:       s.Host,
		Port:       port,
		User:       s.User,
		Password:   s.Password,
		Database:   s.Database,
		Tables:     s.Tables,
		MaskFields: s.MaskFields,
	}, logger)
}
