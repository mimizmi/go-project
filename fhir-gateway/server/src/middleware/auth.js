/**
 * JWT认证中间件
 *
 * 从请求头 Authorization: Bearer <token> 中提取并验证JWT令牌。
 * 验证通过后将用户信息注入 req.user 供后续中间件和路由使用。
 */
const permissionService = require('../services/permission');

function authMiddleware(req, res, next) {
  // 白名单路径：不需要认证
  const whitelist = ['/api/auth/login', '/api/health', '/api/system/status'];
  if (whitelist.some(p => req.path.startsWith(p))) {
    return next();
  }

  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({
      error: 'Unauthorized',
      message: '缺少认证令牌，请先登录',
    });
  }

  const token = authHeader.slice(7);
  const decoded = permissionService.verifyToken(token);
  if (!decoded) {
    return res.status(401).json({
      error: 'Unauthorized',
      message: '令牌无效或已过期',
    });
  }

  req.user = decoded;
  next();
}

module.exports = authMiddleware;
