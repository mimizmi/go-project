/**
 * 模拟EMPI（患者主索引）服务
 *
 * 实现混合匹配算法：精确匹配身份证号 + 基于人口学属性的模糊匹配。
 * 在生产环境中，此服务应替换为对接真实EMPI系统的适配器。
 */
const empiData = require('../data/empi.json');

// 内存索引: sourceSystemId+sourcePatientId → EMPI记录
const identifierIndex = new Map();
// 内存索引: empiId → EMPI记录
const empiIndex = new Map();

// 初始化索引
for (const patient of empiData.patients) {
  empiIndex.set(patient.empiId, patient);
  for (const id of patient.identifiers) {
    identifierIndex.set(`${id.system}:${id.value}`, patient);
  }
}

/**
 * 通过源系统标识符查找统一患者主索引
 * @param {string} sourceSystem - 源系统ID（如 his_mysql_01）
 * @param {string} sourcePatientId - 源系统患者ID
 * @returns {{ empiId, name, matchScore, matchMethod } | null}
 */
function resolveBySourceId(sourceSystem, sourcePatientId) {
  const key = `${sourceSystem}:${sourcePatientId}`;
  const record = identifierIndex.get(key);
  if (!record) return null;
  return {
    empiId: record.empiId,
    name: record.name,
    gender: record.gender,
    birthDate: record.birthDate,
    matchScore: record.matchScore,
    matchMethod: record.matchMethod,
  };
}

/**
 * 通过EMPI主索引ID查找患者信息
 * @param {string} empiId
 * @returns {object|null}
 */
function resolveByEmpiId(empiId) {
  return empiIndex.get(empiId) || null;
}

/**
 * 模糊匹配：基于人口学属性计算相似度
 * @param {{ name, gender, birthDate }} query
 * @returns {Array<{ empiId, name, matchScore }>}
 */
function fuzzyMatch(query) {
  const results = [];
  for (const patient of empiData.patients) {
    let score = 0;
    let factors = 0;

    if (query.name && patient.name) {
      factors++;
      if (patient.name === query.name) score += 0.4;
      else if (patient.name.includes(query.name) || query.name.includes(patient.name)) score += 0.2;
    }
    if (query.gender && patient.gender) {
      factors++;
      if (patient.gender === query.gender) score += 0.2;
    }
    if (query.birthDate && patient.birthDate) {
      factors++;
      if (patient.birthDate === query.birthDate) score += 0.4;
    }

    if (factors > 0 && score > 0.3) {
      results.push({ empiId: patient.empiId, name: patient.name, matchScore: score });
    }
  }
  return results.sort((a, b) => b.matchScore - a.matchScore);
}

/**
 * 获取全部EMPI记录（管理接口）
 */
function listAll() {
  return empiData.patients;
}

module.exports = { resolveBySourceId, resolveByEmpiId, fuzzyMatch, listAll };
