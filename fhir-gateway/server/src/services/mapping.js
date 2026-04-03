/**
 * FHIR数据映射规则引擎
 *
 * 核心功能：根据可配置的JSON映射规则，将异构源数据转换为标准FHIR R4资源。
 * 支持五种映射类型：direct（直接映射）、constant（常量）、mapping（值映射）、
 * template（模板）、conditional（条件映射）。
 */
const fs = require('fs');
const path = require('path');
const config = require('../config');

// 内存缓存：resourceType → 映射配置
let mappingCache = new Map();

/**
 * 加载所有映射规则文件到缓存
 */
function loadMappings() {
  mappingCache.clear();
  const dir = config.mappingDir;
  if (!fs.existsSync(dir)) {
    console.warn(`映射规则目录不存在: ${dir}`);
    return;
  }
  const files = fs.readdirSync(dir).filter(f => f.endsWith('.json'));
  for (const file of files) {
    try {
      const content = JSON.parse(fs.readFileSync(path.join(dir, file), 'utf-8'));
      mappingCache.set(content.resourceType, content);
      console.log(`[Mapping] 加载映射规则: ${content.resourceType} (${file})`);
    } catch (err) {
      console.error(`[Mapping] 加载失败 ${file}:`, err.message);
    }
  }
}

/**
 * 获取指定资源类型的映射配置
 */
function getMappingConfig(resourceType) {
  return mappingCache.get(resourceType) || null;
}

/**
 * 获取全部映射配置
 */
function listMappings() {
  const result = [];
  for (const [type, config] of mappingCache) {
    result.push({
      resourceType: type,
      description: config.description,
      sourceTable: config.sourceTable,
      sourceSystem: config.sourceSystem,
      version: config.version,
      rulesCount: config.mappingRules.length,
    });
  }
  return result;
}

/**
 * 更新映射规则（热更新）
 * @param {string} resourceType
 * @param {object} newConfig - 完整的映射配置JSON
 */
function updateMapping(resourceType, newConfig) {
  const fileName = resourceType.replace(/([A-Z])/g, (m, p, i) =>
    i > 0 ? '_' + m.toLowerCase() : m.toLowerCase()
  ) + '.json';
  const filePath = path.join(config.mappingDir, fileName);

  fs.writeFileSync(filePath, JSON.stringify(newConfig, null, 2), 'utf-8');
  mappingCache.set(resourceType, newConfig);
  console.log(`[Mapping] 更新映射规则: ${resourceType}`);
  return true;
}

/**
 * 在fhirPath指定的嵌套位置设置值
 * 支持 a.b.c 和 a[0].b 格式
 */
function setNestedValue(obj, fhirPath, value) {
  if (value === null || value === undefined) return;

  const parts = fhirPath.replace(/\[(\d+)\]/g, '.$1').split('.');
  let current = obj;

  for (let i = 0; i < parts.length - 1; i++) {
    const key = parts[i];
    const nextKey = parts[i + 1];
    const isNextArray = /^\d+$/.test(nextKey);

    if (current[key] === undefined) {
      current[key] = isNextArray ? [] : {};
    }
    current = current[key];
  }

  const lastKey = parts[parts.length - 1];
  current[lastKey] = value;
}

/**
 * 应用值变换
 */
function applyTransform(value, transform) {
  if (value === null || value === undefined) return value;
  switch (transform) {
    case 'toString': return String(value);
    case 'toNumber': return Number(value);
    case 'toDateString':
      if (value instanceof Date) return value.toISOString().split('T')[0];
      if (typeof value === 'string') return value.split('T')[0].split(' ')[0];
      return String(value);
    case 'toISOString':
      if (value instanceof Date) return value.toISOString();
      if (typeof value === 'string' && !value.endsWith('Z') && !value.includes('+')) {
        return new Date(value).toISOString();
      }
      return value;
    default: return value;
  }
}

/**
 * 核心转换方法：将源数据记录转换为FHIR资源
 * @param {string} resourceType - FHIR资源类型
 * @param {object} sourceData - 来自ODS/ClickHouse的源数据记录
 * @returns {object} FHIR R4资源
 */
function transform(resourceType, sourceData) {
  const mappingConfig = mappingCache.get(resourceType);
  if (!mappingConfig) {
    throw new Error(`未找到 ${resourceType} 的映射规则`);
  }

  const fhirResource = {};

  for (const rule of mappingConfig.mappingRules) {
    let value;

    switch (rule.type) {
      case 'constant':
        value = rule.value;
        break;

      case 'direct':
        value = sourceData[rule.sourceField];
        if (rule.transform) value = applyTransform(value, rule.transform);
        break;

      case 'mapping':
        value = sourceData[rule.sourceField];
        if (value !== null && value !== undefined) {
          value = rule.valueMap[String(value)] || rule.default || value;
        }
        break;

      case 'template':
        value = sourceData[rule.sourceField];
        if (value !== null && value !== undefined) {
          value = rule.template.replace('{value}', value);
        }
        break;

      case 'conditional':
        value = sourceData[rule.sourceField];
        if (rule.condition) {
          value = (value === null || value === undefined) ? rule.condition.ifNull : rule.condition.else;
        }
        break;

      default:
        continue;
    }

    if (value !== null && value !== undefined) {
      setNestedValue(fhirResource, rule.fhirPath, value);
    }
  }

  return fhirResource;
}

/**
 * 批量转换
 */
function transformBatch(resourceType, records) {
  return records.map(record => transform(resourceType, record));
}

// 启动时加载映射规则
loadMappings();

module.exports = {
  loadMappings,
  getMappingConfig,
  listMappings,
  updateMapping,
  transform,
  transformBatch,
};
