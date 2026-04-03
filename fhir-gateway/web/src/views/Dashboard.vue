<template>
  <div>
    <!-- 系统状态卡片 -->
    <el-row :gutter="20" style="margin-bottom:20px">
      <el-col :span="6">
        <el-card shadow="hover">
          <template #header><span>网关服务</span></template>
          <div class="stat-value">
            <el-tag :type="status.gateway?.status === 'running' ? 'success' : 'danger'" size="large">
              {{ status.gateway?.status || '...' }}
            </el-tag>
          </div>
          <div class="stat-label">运行时间: {{ formatUptime(status.gateway?.uptime) }}</div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="hover">
          <template #header><span>Med-Insight</span></template>
          <div class="stat-value">
            <el-tag :type="status.medInsight?.status === 'connected' ? 'success' : 'warning'" size="large">
              {{ status.medInsight?.status || '...' }}
            </el-tag>
          </div>
          <div class="stat-label">{{ status.medInsight?.url || '-' }}</div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="hover">
          <template #header><span>EMPI服务</span></template>
          <div class="stat-value">
            <span class="num">{{ status.services?.empi?.recordCount || 0 }}</span>
          </div>
          <div class="stat-label">患者主索引记录 ({{ status.services?.empi?.mode }})</div>
        </el-card>
      </el-col>
      <el-col :span="6">
        <el-card shadow="hover">
          <template #header><span>权限服务</span></template>
          <div class="stat-value">
            <span class="num">{{ status.services?.permission?.roles || 0 }}</span> 角色 /
            <span class="num">{{ status.services?.permission?.users || 0 }}</span> 用户
          </div>
          <div class="stat-label">RBAC+ABAC 混合模型 ({{ status.services?.permission?.mode }})</div>
        </el-card>
      </el-col>
    </el-row>

    <!-- 架构流程图 -->
    <el-card style="margin-bottom:20px">
      <template #header><span>数据中台架构 - 核心数据流</span></template>
      <div class="arch-flow">
        <div class="arch-node source">
          <div class="arch-title">数据采集层 (CDC)</div>
          <div class="arch-items">
            <div>MySQL HIS (binlog)</div>
            <div>SQL Server LIS (CDC)</div>
          </div>
        </div>
        <div class="arch-arrow">Kafka<br/>(Exactly-Once)</div>
        <div class="arch-node transport">
          <div class="arch-title">ODS层</div>
          <div class="arch-items">
            <div>PostgreSQL</div>
            <div>实时同步</div>
          </div>
        </div>
        <div class="arch-arrow">增量同步<br/>(Waterline)</div>
        <div class="arch-node olap">
          <div class="arch-title">分析层 (Med-Insight)</div>
          <div class="arch-items">
            <div>ClickHouse OLAP</div>
            <div>L1/L2 多级缓存</div>
          </div>
        </div>
        <div class="arch-arrow">REST API</div>
        <div class="arch-node gateway">
          <div class="arch-title">服务层 (FHIR Gateway)</div>
          <div class="arch-items">
            <div>EMPI 患者识别</div>
            <div>权限管控</div>
            <div>FHIR 映射转换</div>
          </div>
        </div>
        <div class="arch-arrow">FHIR R4</div>
        <div class="arch-node app">
          <div class="arch-title">应用层</div>
          <div class="arch-items">
            <div>管理控制台</div>
            <div>电子病历</div>
            <div>临床决策</div>
          </div>
        </div>
      </div>
    </el-card>

    <!-- 映射规则概览 -->
    <el-card>
      <template #header><span>FHIR映射规则概览</span></template>
      <el-table :data="mappings" stripe>
        <el-table-column prop="resourceType" label="FHIR资源类型" width="200" />
        <el-table-column prop="sourceTable" label="源数据表" width="280" />
        <el-table-column prop="sourceSystem" label="源系统" width="180" />
        <el-table-column prop="rulesCount" label="映射规则数" width="120" />
        <el-table-column prop="version" label="版本" width="100" />
        <el-table-column prop="description" label="描述" />
      </el-table>
    </el-card>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { getSystemStatus, listMappings } from '../api'

const status = ref({})
const mappings = ref([])

onMounted(async () => {
  try {
    const [s, m] = await Promise.all([getSystemStatus(), listMappings()])
    status.value = s
    mappings.value = m.data
  } catch (e) {
    console.error(e)
  }
})

function formatUptime(seconds) {
  if (!seconds) return '-'
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = Math.floor(seconds % 60)
  return `${h}h ${m}m ${s}s`
}
</script>

<style scoped>
.stat-value { font-size: 24px; font-weight: 700; color: #303133; margin: 8px 0; }
.stat-value .num { color: #409EFF; }
.stat-label { font-size: 13px; color: #909399; }

.arch-flow {
  display: flex;
  align-items: center;
  gap: 8px;
  overflow-x: auto;
  padding: 16px 0;
}
.arch-node {
  min-width: 150px;
  padding: 16px;
  border-radius: 8px;
  text-align: center;
  flex-shrink: 0;
}
.arch-title { font-weight: 600; margin-bottom: 8px; font-size: 14px; }
.arch-items { font-size: 12px; color: #606266; line-height: 1.8; }
.arch-arrow {
  color: #909399;
  font-size: 12px;
  text-align: center;
  min-width: 60px;
  flex-shrink: 0;
}
.arch-arrow::before { content: '→ '; font-size: 16px; }
.source { background: #fdf6ec; border: 1px solid #e6a23c; }
.transport { background: #f0f9eb; border: 1px solid #67c23a; }
.olap { background: #ecf5ff; border: 1px solid #409eff; }
.gateway { background: #f4ecfe; border: 1px solid #a855f7; }
.app { background: #fef0f0; border: 1px solid #f56c6c; }
</style>
