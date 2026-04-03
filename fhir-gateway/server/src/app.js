/**
 * 统一FHIR接口服务网关 - 主入口
 *
 * 架构定位：医院数据中台的核心枢纽，连接异构数据源与前端临床应用。
 * 核心流程：请求 → JWT认证 → 权限检查 → EMPI患者识别 → 数据聚合(med-insight) → FHIR映射转换 → 响应
 *
 * 依赖上游服务：
 *   - med-insight (localhost:8082): 提供ClickHouse聚合查询与FHIR数据
 *   - CDC平台: 通过 med-insight 间接消费实时CDC数据
 */
const express = require('express');
const cors = require('cors');
const morgan = require('morgan');
const config = require('./config');

// 中间件
const authMiddleware = require('./middleware/auth');

// 路由
const authRoutes = require('./routes/auth');
const fhirRoutes = require('./routes/fhir');
const mappingRoutes = require('./routes/mapping');
const systemRoutes = require('./routes/system');

const app = express();

// --- 基础中间件 ---
app.use(cors({ origin: config.corsOrigins, credentials: true }));
app.use(express.json({ limit: '10mb' }));
app.use(morgan('[:date[iso]] :method :url :status :response-time ms'));

// --- 健康检查（无需认证）---
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', service: 'fhir-gateway', timestamp: new Date().toISOString() });
});

// --- JWT认证中间件（白名单路径跳过）---
app.use(authMiddleware);

// --- 路由挂载 ---
app.use('/api/auth', authRoutes);
app.use('/api/fhir', fhirRoutes);
app.use('/api/mappings', mappingRoutes);
app.use('/api/system', systemRoutes);

// --- 错误处理 ---
app.use((err, req, res, _next) => {
  console.error('[Error]', err.message);
  res.status(500).json({ error: 'Internal Server Error', message: err.message });
});

// --- 启动服务 ---
app.listen(config.port, () => {
  console.log(`
╔══════════════════════════════════════════════════════════╗
║         统一FHIR接口服务网关 (FHIR Gateway)              ║
╠══════════════════════════════════════════════════════════╣
║  服务地址:  http://localhost:${config.port}                      ║
║  上游服务:  med-insight @ ${config.medInsight.baseUrl}    ║
║  认证方式:  JWT Bearer Token                             ║
║  EMPI模式:  模拟服务 (Simulated)                         ║
║  权限模式:  RBAC+ABAC混合 (Simulated)                    ║
╚══════════════════════════════════════════════════════════╝

API端点:
  POST /api/auth/login              - 用户登录
  GET  /api/fhir/Patient/:id        - 获取FHIR患者资源
  GET  /api/fhir/Patient/:id/$everything - 患者完整视图
  GET  /api/fhir/Encounter/:id      - 获取就诊记录
  GET  /api/mappings                - 查看映射规则
  PUT  /api/mappings/:type          - 更新映射规则
  GET  /api/system/status           - 系统状态
  GET  /api/system/empi             - EMPI记录
  `);
});

module.exports = app;
