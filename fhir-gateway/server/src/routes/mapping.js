/**
 * 映射规则管理路由
 *
 * 提供映射规则的CRUD接口，支持前端管理控制台动态修改映射规则，
 * 修改后自动热更新（无需重启服务）。
 */
const express = require('express');
const router = express.Router();
const mappingService = require('../services/mapping');

// GET /api/mappings - 获取所有映射规则概览
router.get('/', (req, res) => {
  res.json({ data: mappingService.listMappings() });
});

// GET /api/mappings/:resourceType - 获取指定资源的完整映射规则
router.get('/:resourceType', (req, res) => {
  const config = mappingService.getMappingConfig(req.params.resourceType);
  if (!config) {
    return res.status(404).json({ error: `未找到 ${req.params.resourceType} 的映射规则` });
  }
  res.json({ data: config });
});

// PUT /api/mappings/:resourceType - 更新映射规则
router.put('/:resourceType', (req, res) => {
  // 仅管理员可修改映射规则
  if (req.user && req.user.role !== 'role_admin') {
    return res.status(403).json({ error: '仅管理员可修改映射规则' });
  }

  const newConfig = req.body;
  if (!newConfig || !newConfig.resourceType || !newConfig.mappingRules) {
    return res.status(400).json({ error: '映射配置格式无效，需包含 resourceType 和 mappingRules' });
  }

  try {
    mappingService.updateMapping(req.params.resourceType, newConfig);
    res.json({ message: '映射规则更新成功', data: mappingService.getMappingConfig(req.params.resourceType) });
  } catch (err) {
    res.status(500).json({ error: '更新失败', detail: err.message });
  }
});

// POST /api/mappings/reload - 重新加载所有映射规则
router.post('/reload', (req, res) => {
  if (req.user && req.user.role !== 'role_admin') {
    return res.status(403).json({ error: '仅管理员可重载映射规则' });
  }

  mappingService.loadMappings();
  res.json({ message: '映射规则重载成功', data: mappingService.listMappings() });
});

// POST /api/mappings/test-transform - 测试映射转换（不保存）
router.post('/test-transform', (req, res) => {
  const { resourceType, sourceData } = req.body;
  if (!resourceType || !sourceData) {
    return res.status(400).json({ error: '需要提供 resourceType 和 sourceData' });
  }

  try {
    const result = mappingService.transform(resourceType, sourceData);
    res.json({ data: result });
  } catch (err) {
    res.status(400).json({ error: '转换失败', detail: err.message });
  }
});

module.exports = router;
