<template>
  <div>
    <!-- 查询面板 -->
    <el-card style="margin-bottom:20px">
      <template #header><span>FHIR资源查询 - 完整业务流程演示</span></template>
      <el-form :inline="true" :model="query">
        <el-form-item label="资源类型">
          <el-select v-model="query.resourceType" style="width:200px">
            <el-option label="Patient (患者)" value="Patient" />
            <el-option label="Encounter (就诊)" value="Encounter" />
            <el-option label="Patient/$everything (完整视图)" value="PatientEverything" />
          </el-select>
        </el-form-item>
        <el-form-item label="资源ID">
          <el-input v-model="query.id" placeholder="如: 10001" style="width:160px" />
        </el-form-item>
        <el-form-item>
          <el-button type="primary" @click="doQuery" :loading="loading">
            <el-icon><Search /></el-icon> 查询
          </el-button>
        </el-form-item>
      </el-form>
      <div style="margin-top:8px">
        <el-text type="info" size="small">
          快捷测试：
          <el-link type="primary" @click="quickTest('Patient', '1')">Patient/1</el-link> |
          <el-link type="primary" @click="quickTest('Patient', '2')">Patient/2</el-link> |
          <el-link type="primary" @click="quickTest('Patient', '3')">Patient/3</el-link> |
          <el-link type="primary" @click="quickTest('Encounter', '1')">Encounter/1</el-link> |
          <el-link type="primary" @click="quickTest('PatientEverything', '1')">Patient/1/$everything</el-link>
        </el-text>
      </div>
    </el-card>

    <!-- 流程日志 -->
    <el-card v-if="processLog.length" style="margin-bottom:20px">
      <template #header><span>处理流程日志 (权限检查 → EMPI识别 → 数据聚合 → FHIR转换)</span></template>
      <el-timeline>
        <el-timeline-item
          v-for="(log, i) in fullLog"
          :key="i"
          :type="logType(log.status)"
          :timestamp="log.timestamp"
          placement="top"
        >
          <el-card shadow="never" style="padding:8px 16px">
            <div style="display:flex;align-items:center;gap:8px">
              <el-tag :type="logType(log.status)" size="small">{{ log.step }}</el-tag>
              <span style="font-size:13px;color:#606266">{{ log.detail || log.status }}</span>
            </div>
          </el-card>
        </el-timeline-item>
      </el-timeline>
    </el-card>

    <!-- FHIR资源结果 -->
    <el-card v-if="result">
      <template #header>
        <div style="display:flex;justify-content:space-between;align-items:center">
          <span>FHIR R4 资源响应</span>
          <el-tag type="success">{{ result.resourceType || 'Bundle' }}</el-tag>
        </div>
      </template>
      <el-tabs>
        <el-tab-pane label="格式化视图">
          <div v-if="result.resourceType === 'Patient'" class="fhir-detail">
            <el-descriptions :column="2" border>
              <el-descriptions-item label="患者ID">{{ result.id }}</el-descriptions-item>
              <el-descriptions-item label="姓名">{{ result.name?.[0]?.text }}</el-descriptions-item>
              <el-descriptions-item label="性别">{{ result.gender }}</el-descriptions-item>
              <el-descriptions-item label="出生日期">{{ result.birthDate }}</el-descriptions-item>
              <el-descriptions-item label="地址">{{ result.address?.[0]?.text }}</el-descriptions-item>
              <el-descriptions-item label="EMPI ID">
                {{ result.identifier?.find(i => i.system === 'urn:hospital:empi')?.value || '-' }}
              </el-descriptions-item>
            </el-descriptions>
          </div>
          <div v-else-if="result.resourceType === 'Encounter'" class="fhir-detail">
            <el-descriptions :column="2" border>
              <el-descriptions-item label="就诊ID">{{ result.id }}</el-descriptions-item>
              <el-descriptions-item label="状态">{{ result.status }}</el-descriptions-item>
              <el-descriptions-item label="类型">{{ result.class?.display }}</el-descriptions-item>
              <el-descriptions-item label="科室">{{ result.serviceProvider?.display }}</el-descriptions-item>
              <el-descriptions-item label="医生">{{ result.participant?.[0]?.individual?.display }}</el-descriptions-item>
              <el-descriptions-item label="诊断">{{ result.reasonCode?.[0]?.text }}</el-descriptions-item>
              <el-descriptions-item label="入院时间">{{ result.period?.start }}</el-descriptions-item>
              <el-descriptions-item label="出院时间">{{ result.period?.end || '在院' }}</el-descriptions-item>
            </el-descriptions>
          </div>
          <div v-else-if="result.resourceType === 'Bundle'">
            <el-tag style="margin-bottom:12px">共 {{ result.total }} 个资源条目</el-tag>
            <el-collapse>
              <el-collapse-item v-for="(entry, i) in result.entry" :key="i" :title="`${entry.resource?.resourceType} / ${entry.resource?.id}`">
                <pre class="json-view">{{ JSON.stringify(entry.resource, null, 2) }}</pre>
              </el-collapse-item>
            </el-collapse>
          </div>
        </el-tab-pane>
        <el-tab-pane label="JSON原始数据">
          <pre class="json-view">{{ JSON.stringify(result, null, 2) }}</pre>
        </el-tab-pane>
      </el-tabs>
    </el-card>
  </div>
</template>

<script setup>
import { reactive, ref } from 'vue'
import { Search } from '@element-plus/icons-vue'
import { getPatient, getEncounter, getPatientEverything } from '../api'

const query = reactive({ resourceType: 'Patient', id: '1' })
const loading = ref(false)
const result = ref(null)
const processLog = ref([])
const accessLog = ref(null)

const fullLog = ref([])

function logType(status) {
  if (status === 'success') return 'success'
  if (status === 'error') return 'danger'
  if (status === 'warning' || status === 'fallback') return 'warning'
  return 'primary'
}

function quickTest(type, id) {
  query.resourceType = type
  query.id = id
  doQuery()
}

async function doQuery() {
  if (!query.id) return
  loading.value = true
  result.value = null
  processLog.value = []
  fullLog.value = []

  try {
    let res
    if (query.resourceType === 'Patient') {
      res = await getPatient(query.id)
    } else if (query.resourceType === 'Encounter') {
      res = await getEncounter(query.id)
    } else {
      res = await getPatientEverything(query.id)
    }

    result.value = res.data
    processLog.value = res.processLog || []
    accessLog.value = res.accessLog || null

    // 构建完整日志
    const logs = []
    if (accessLog.value) {
      logs.push({
        step: '权限检查',
        timestamp: accessLog.value.timestamp,
        status: accessLog.value.allowed ? 'success' : 'error',
        detail: `${accessLog.value.role} → ${accessLog.value.resourceType}: ${accessLog.value.reason}`,
      })
    }
    logs.push(...processLog.value)
    fullLog.value = logs
  } catch (e) {
    console.error(e)
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
.json-view {
  background: #fafafa;
  border: 1px solid #ebeef5;
  border-radius: 4px;
  padding: 16px;
  font-size: 13px;
  line-height: 1.6;
  overflow-x: auto;
  max-height: 600px;
  white-space: pre-wrap;
  word-break: break-all;
}
.fhir-detail { margin: 8px 0; }
</style>
