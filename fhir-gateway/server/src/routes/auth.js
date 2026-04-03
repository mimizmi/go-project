/**
 * 认证路由
 */
const express = require('express');
const router = express.Router();
const permissionService = require('../services/permission');

// POST /api/auth/login - 用户登录
router.post('/login', (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return res.status(400).json({ error: '用户名和密码不能为空' });
  }

  const result = permissionService.authenticate(username, password);
  if (!result) {
    return res.status(401).json({ error: '用户名或密码错误' });
  }

  res.json({
    message: '登录成功',
    token: result.token,
    user: result.user,
  });
});

// GET /api/auth/profile - 获取当前用户信息
router.get('/profile', (req, res) => {
  if (!req.user) {
    return res.status(401).json({ error: '未认证' });
  }
  res.json({ user: req.user });
});

module.exports = router;
