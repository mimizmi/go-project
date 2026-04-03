/**
 * 资源访问控制中间件
 *
 * 在FHIR资源请求前进行权限校验，确保请求者有权访问目标资源类型。
 * 实现 "权限检查→患者识别→数据聚合" 流程中的第一步。
 */
const permissionService = require('../services/permission');

/**
 * 创建资源访问控制中间件
 * @param {string} resourceType - FHIR资源类型
 */
function requireAccess(resourceType) {
  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({ error: 'Unauthorized', message: '未认证' });
    }

    const targetDept = req.query.dept || null;
    const result = permissionService.checkAccess(req.user, resourceType, targetDept);

    // 记录访问控制日志
    req.accessLog = {
      userId: req.user.userId,
      role: req.user.role,
      department: req.user.department,
      resourceType,
      targetDepartment: targetDept,
      allowed: result.allowed,
      reason: result.reason,
      timestamp: new Date().toISOString(),
    };

    if (!result.allowed) {
      return res.status(403).json({
        error: 'Forbidden',
        message: result.reason,
        accessLog: req.accessLog,
      });
    }

    req.allowedFields = result.allowedFields;
    next();
  };
}

module.exports = { requireAccess };
