/**
 * 系统管理路由
 *
 * 提供系统状态、EMPI查询、权限管理等辅助接口。
 */
const express = require('express');
const router = express.Router();
const axios = require('axios');
const config = require('../config');
const empiService = require('../services/empi');
const permissionService = require('../services/permission');

// GET /api/system/status - 系统状态总览
router.get('/status', async (req, res) => {
  const status = {
    gateway: { status: 'running', uptime: process.uptime(), version: '1.0.0' },
    medInsight: { status: 'unknown' },
    services: {
      empi: { status: 'running', mode: 'simulated', recordCount: empiService.listAll().length },
      permission: { status: 'running', mode: 'simulated', roles: permissionService.listRoles().length, users: permissionService.listUsers().length },
    },
  };

  // 检测 med-insight 可达性
  try {
    const resp = await axios.get(`${config.medInsight.baseUrl}/healthz`, { timeout: 3000 });
    status.medInsight = { status: 'connected', url: config.medInsight.baseUrl };
  } catch {
    status.medInsight = { status: 'unreachable', url: config.medInsight.baseUrl, note: '使用模拟数据降级运行' };
  }

  res.json(status);
});

// --- EMPI 管理接口 ---

// GET /api/system/empi - 获取所有EMPI记录
router.get('/empi', (req, res) => {
  res.json({ data: empiService.listAll() });
});

// GET /api/system/empi/resolve?system=&id= - EMPI精确查询
router.get('/empi/resolve', (req, res) => {
  const { system, id } = req.query;
  if (!system || !id) {
    return res.status(400).json({ error: '需要提供 system 和 id 参数' });
  }
  const result = empiService.resolveBySourceId(system, id);
  if (!result) {
    return res.status(404).json({ error: '未找到匹配的EMPI记录' });
  }
  res.json({ data: result });
});

// POST /api/system/empi/match - EMPI模糊匹配
router.post('/empi/match', (req, res) => {
  const { name, gender, birthDate } = req.body;
  const results = empiService.fuzzyMatch({ name, gender, birthDate });
  res.json({ data: results, count: results.length });
});

// --- 权限管理接口 ---

// GET /api/system/permissions/roles - 获取所有角色
router.get('/permissions/roles', (req, res) => {
  res.json({ data: permissionService.listRoles() });
});

// GET /api/system/permissions/users - 获取所有用户
router.get('/permissions/users', (req, res) => {
  res.json({ data: permissionService.listUsers() });
});

// GET /api/system/permissions/policies - 获取所有访问策略
router.get('/permissions/policies', (req, res) => {
  res.json({ data: permissionService.listPolicies() });
});

// POST /api/system/permissions/check - 权限检查测试
router.post('/permissions/check', (req, res) => {
  const { userId, resourceType, targetDepartment } = req.body;
  const users = permissionService.listUsers();
  const user = users.find(u => u.userId === userId);
  if (!user) {
    return res.status(404).json({ error: '用户不存在' });
  }
  const result = permissionService.checkAccess(user, resourceType, targetDepartment);
  res.json({ data: { user, check: result } });
});

module.exports = router;
