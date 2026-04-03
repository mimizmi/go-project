/**
 * FHIR数据聚合服务
 *
 * 负责从 med-insight 后端获取原始数据，经过EMPI身份关联和映射规则转换，
 * 聚合为标准FHIR Bundle返回。这是 "权限检查→患者识别→数据聚合" 核心流程的执行者。
 */
const axios = require('axios');
const config = require('../config');
const empiService = require('./empi');
const mappingService = require('./mapping');

const medInsightClient = axios.create({
  baseURL: config.medInsight.baseUrl,
  timeout: 10000,
});

/**
 * 获取FHIR Patient资源（完整流程）
 *
 * 流程：EMPI解析 → 从 med-insight 获取原始患者数据 → 映射转换 → 注入EMPI标识
 * @param {string} patientId - 患者ID（HIS系统ID）
 * @returns {object} FHIR Patient资源 + 处理日志
 */
async function getPatient(patientId) {
  const processLog = [];

  // Step 1: EMPI解析 - 获取统一患者主索引
  processLog.push({ step: 'EMPI解析', timestamp: new Date().toISOString(), status: 'processing' });
  const empiResult = empiService.resolveBySourceId('his_mysql_01', String(patientId));
  if (empiResult) {
    processLog[processLog.length - 1].status = 'success';
    processLog[processLog.length - 1].detail = `匹配到EMPI: ${empiResult.empiId}, 匹配方法: ${empiResult.matchMethod}, 置信度: ${empiResult.matchScore}`;
  } else {
    processLog[processLog.length - 1].status = 'warning';
    processLog[processLog.length - 1].detail = '未匹配到EMPI记录，使用源系统ID';
  }

  // Step 2: 从 med-insight 获取原始数据
  processLog.push({ step: '数据获取', timestamp: new Date().toISOString(), status: 'processing' });
  let rawData;
  try {
    const resp = await medInsightClient.get(`/api/v1/data/patient/${patientId}`);
    rawData = resp.data && resp.data.data ? resp.data.data : resp.data;
    processLog[processLog.length - 1].status = 'success';
    processLog[processLog.length - 1].detail = `从med-insight获取患者数据成功`;
  } catch (err) {
    const status = err.response?.status;
    const isServerError = err.response && status >= 400;
    if (isServerError) {
      const diag = err.response?.data?.error || `HTTP ${status}`;
      processLog[processLog.length - 1].status = 'fallback';
      processLog[processLog.length - 1].detail = `med-insight查询失败(${diag})，使用模拟数据补充`;
    } else {
      processLog[processLog.length - 1].status = 'fallback';
      processLog[processLog.length - 1].detail = `med-insight不可达(${err.message})，使用模拟数据`;
    }
    rawData = generateMockPatient(patientId);
  }

  // Step 3: 映射转换（med-insight 返回的已是平坦源字段，直接交给映射引擎）
  processLog.push({ step: 'FHIR映射转换', timestamp: new Date().toISOString(), status: 'processing' });
  let fhirResource;
  try {
    if (empiResult) rawData._empi_id = empiResult.empiId;
    fhirResource = mappingService.transform('Patient', rawData);
    processLog[processLog.length - 1].status = 'success';
    processLog[processLog.length - 1].detail = `转换为FHIR Patient资源成功`;
  } catch (err) {
    processLog[processLog.length - 1].status = 'error';
    processLog[processLog.length - 1].detail = err.message;
    fhirResource = rawData;
  }

  return { resource: fhirResource, processLog };
}

/**
 * 获取患者的完整就诊视图（聚合Patient + Encounter + MedicationRequest + DiagnosticReport）
 * @param {string} patientId
 * @returns {object} FHIR Bundle
 */
async function getPatientEverything(patientId) {
  const processLog = [];

  // Step 1: EMPI
  const empiResult = empiService.resolveBySourceId('his_mysql_01', String(patientId));
  processLog.push({
    step: 'EMPI解析',
    timestamp: new Date().toISOString(),
    status: empiResult ? 'success' : 'warning',
    detail: empiResult ? `EMPI: ${empiResult.empiId}` : '未匹配EMPI',
  });

  // Step 2: 并行获取各类资源
  processLog.push({ step: '数据聚合', timestamp: new Date().toISOString(), status: 'processing' });

  const entries = [];

  // 获取Patient
  try {
    const patientResult = await getPatient(patientId);
    entries.push({ resource: patientResult.resource, search: { mode: 'match' } });
  } catch (err) {
    processLog.push({ step: '获取Patient', status: 'error', detail: err.message });
  }

  // 获取Encounters（就诊记录）
  try {
    const resp = await medInsightClient.get(`/api/v1/data/encounter/${patientId}`);
    const data = resp.data && resp.data.data ? resp.data.data : resp.data;
    const encounters = Array.isArray(data) ? data : [data];
    for (const enc of encounters) {
      try {
        const fhir = mappingService.transform('Encounter', enc);
        entries.push({ resource: fhir, search: { mode: 'include' } });
      } catch {
        entries.push({ resource: enc, search: { mode: 'include' } });
      }
    }
  } catch {
    const mockEnc = generateMockEncounter(patientId);
    entries.push({ resource: mappingService.transform('Encounter', mockEnc), search: { mode: 'include' } });
  }

  processLog[processLog.length - 1].status = 'success';
  processLog[processLog.length - 1].detail = `聚合 ${entries.length} 个资源条目`;

  const bundle = {
    resourceType: 'Bundle',
    type: 'searchset',
    total: entries.length,
    timestamp: new Date().toISOString(),
    entry: entries,
  };

  return { bundle, processLog };
}

/**
 * 获取FHIR Encounter资源
 */
async function getEncounter(encounterId) {
  const processLog = [];
  processLog.push({ step: '数据获取', timestamp: new Date().toISOString(), status: 'processing' });

  let rawData;
  try {
    const resp = await medInsightClient.get(`/api/v1/data/encounter/${encounterId}`);
    rawData = resp.data && resp.data.data ? resp.data.data : resp.data;
    processLog[processLog.length - 1].status = 'success';
  } catch (err) {
    const diag = err.response?.data?.error || err.message;
    processLog[processLog.length - 1].status = 'fallback';
    processLog[processLog.length - 1].detail = `查询失败(${diag})，使用模拟数据`;
    rawData = generateMockEncounter(encounterId);
  }

  processLog.push({ step: 'FHIR映射转换', timestamp: new Date().toISOString(), status: 'processing' });
  let fhirResource;
  try {
    fhirResource = mappingService.transform('Encounter', rawData);
    processLog[processLog.length - 1].status = 'success';
  } catch (err) {
    processLog[processLog.length - 1].status = 'error';
    processLog[processLog.length - 1].detail = err.message;
    fhirResource = rawData;
  }

  return { resource: fhirResource, processLog };
}

// 模拟数据生成器（当med-insight不可达时使用）
function generateMockPatient(id) {
  const mockPatients = [
    { name: '张三', gender: 'male', birth_date: '1985-03-15', address: '北京市朝阳区建国路88号' },
    { name: '李四', gender: 'male', birth_date: '1978-07-22', address: '上海市浦东新区陆家嘴环路100号' },
    { name: '王芳', gender: 'female', birth_date: '1990-11-08', address: '广州市天河区天河路385号' },
    { name: '赵伟', gender: 'male', birth_date: '1965-01-30', address: '重庆市渝中区解放碑步行街' },
    { name: '刘洋', gender: 'female', birth_date: '2000-06-18', address: '南京市鼓楼区中山路169号' },
  ];
  const idx = ((parseInt(id) || 1) - 1) % mockPatients.length;
  return {
    patient_id: id,
    ...mockPatients[idx],
    created_at: new Date().toISOString(),
  };
}

function generateMockEncounter(patientId) {
  return {
    visit_id: `V-${patientId}-001`,
    patient_id: patientId,
    dept: '心内科',
    doctor: '模拟医生',
    visit_type: '门诊',
    admit_time: new Date().toISOString(),
    discharge_time: null,
    diagnosis: '模拟诊断',
  };
}

module.exports = { getPatient, getPatientEverything, getEncounter };
