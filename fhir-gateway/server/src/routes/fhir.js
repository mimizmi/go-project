/**
 * FHIR资源路由
 *
 * 实现标准FHIR RESTful API端点，完整演示：
 * "权限检查 → EMPI患者识别 → 数据聚合 → FHIR映射转换" 的核心业务链。
 */
const express = require('express');
const router = express.Router();
const { requireAccess } = require('../middleware/accessControl');
const fhirService = require('../services/fhir');
const permissionService = require('../services/permission');

// GET /api/fhir/Patient/:id - 获取患者资源
router.get('/Patient/:id', requireAccess('Patient'), async (req, res) => {
  try {
    const result = await fhirService.getPatient(req.params.id);

    // 字段级权限过滤
    const filtered = permissionService.filterResource(result.resource, req.allowedFields);

    res.json({
      data: filtered,
      processLog: result.processLog,
      accessLog: req.accessLog,
    });
  } catch (err) {
    res.status(500).json({ error: '获取患者数据失败', detail: err.message });
  }
});

// GET /api/fhir/Patient/:id/$everything - 获取患者完整视图
router.get('/Patient/:id/\\$everything', requireAccess('Patient'), async (req, res) => {
  try {
    const result = await fhirService.getPatientEverything(req.params.id);
    res.json({
      data: result.bundle,
      processLog: result.processLog,
      accessLog: req.accessLog,
    });
  } catch (err) {
    res.status(500).json({ error: '获取患者完整视图失败', detail: err.message });
  }
});

// GET /api/fhir/Encounter/:id - 获取就诊记录
router.get('/Encounter/:id', requireAccess('Encounter'), async (req, res) => {
  try {
    const result = await fhirService.getEncounter(req.params.id);
    const filtered = permissionService.filterResource(result.resource, req.allowedFields);
    res.json({
      data: filtered,
      processLog: result.processLog,
      accessLog: req.accessLog,
    });
  } catch (err) {
    res.status(500).json({ error: '获取就诊记录失败', detail: err.message });
  }
});

module.exports = router;
