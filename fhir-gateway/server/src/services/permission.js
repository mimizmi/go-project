/**
 * 模拟细粒度权限控制服务
 *
 * 实现RBAC（基于角色）+ ABAC（基于属性）混合访问控制模型。
 * 支持科室级、资源级和字段级的权限判定。
 * 生产环境中应替换为对接真实权限管理系统的适配器。
 */
const jwt = require('jsonwebtoken');
const config = require('../config');
const permData = require('../data/permissions.json');

// 索引
const userMap = new Map(permData.users.map(u => [u.username, u]));
const userIdMap = new Map(permData.users.map(u => [u.userId, u]));
const policyMap = new Map(permData.accessPolicies.map(p => [p.role, p]));

/**
 * 用户认证（模拟登录）
 * @param {string} username
 * @param {string} password
 * @returns {{ token, user } | null}
 */
function authenticate(username, password) {
  const user = userMap.get(username);
  if (!user || user.password !== password) return null;

  const token = jwt.sign(
    { userId: user.userId, username: user.username, role: user.role, department: user.department },
    config.jwtSecret,
    { expiresIn: config.jwtExpiresIn }
  );

  return {
    token,
    user: { userId: user.userId, name: user.name, role: user.role, department: user.department },
  };
}

/**
 * 验证JWT令牌
 * @param {string} token
 * @returns {object|null} 解码的用户信息
 */
function verifyToken(token) {
  try {
    return jwt.verify(token, config.jwtSecret);
  } catch {
    return null;
  }
}

/**
 * 检查资源访问权限
 * @param {object} user - { userId, role, department }
 * @param {string} resourceType - FHIR资源类型（如 Patient, Encounter）
 * @param {string} targetDepartment - 目标数据所属科室（可选）
 * @returns {{ allowed: boolean, reason: string, allowedFields: string[] }}
 */
function checkAccess(user, resourceType, targetDepartment) {
  const policy = policyMap.get(user.role);
  if (!policy) {
    return { allowed: false, reason: '未找到角色对应的访问策略', allowedFields: [] };
  }

  // 检查资源类型权限
  if (!policy.resources.includes(resourceType)) {
    return { allowed: false, reason: `角色 ${user.role} 无权访问 ${resourceType} 资源`, allowedFields: [] };
  }

  // 检查科室权限
  if (!policy.departments.includes('*')) {
    if (policy.departments.includes('$own') && targetDepartment && targetDepartment !== user.department) {
      return {
        allowed: false,
        reason: `仅允许访问本科室(${user.department})数据，目标科室: ${targetDepartment}`,
        allowedFields: [],
      };
    }
  }

  return {
    allowed: true,
    reason: '权限验证通过',
    allowedFields: policy.fields,
  };
}

/**
 * 根据允许字段过滤FHIR资源
 * 如果 allowedFields 包含 '*' 则返回完整资源
 */
function filterResource(resource, allowedFields) {
  if (!allowedFields || allowedFields.includes('*')) return resource;

  const filtered = { resourceType: resource.resourceType, id: resource.id };
  for (const field of allowedFields) {
    if (resource[field] !== undefined) {
      filtered[field] = resource[field];
    }
  }
  return filtered;
}

/**
 * 获取所有角色定义
 */
function listRoles() {
  return permData.roles;
}

/**
 * 获取所有用户（脱敏）
 */
function listUsers() {
  return permData.users.map(u => ({
    userId: u.userId,
    username: u.username,
    name: u.name,
    role: u.role,
    department: u.department,
  }));
}

/**
 * 获取所有访问策略
 */
function listPolicies() {
  return permData.accessPolicies;
}

module.exports = { authenticate, verifyToken, checkAccess, filterResource, listRoles, listUsers, listPolicies };
