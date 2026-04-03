<template>
  <div>
    <el-row :gutter="20">
      <!-- 左侧：映射规则列表 -->
      <el-col :span="8">
        <el-card>
          <template #header>
            <div style="display:flex;justify-content:space-between;align-items:center">
              <span>映射规则列表</span>
              <el-button size="small" @click="reloadAll" :loading="reloading">重载规则</el-button>
            </div>
          </template>
          <div v-for="m in mappings" :key="m.resourceType"
            :class="['mapping-item', { active: selected === m.resourceType }]"
            @click="selectMapping(m.resourceType)"
          >
            <div class="mapping-name">{{ m.resourceType }}</div>
            <div class="mapping-meta">
              <el-tag size="small" type="info">{{ m.sourceTable }}</el-tag>
              <el-text size="small" type="info">{{ m.rulesCount }} 条规则</el-text>
            </div>
          </div>
        </el-card>
      </el-col>

      <!-- 右侧：编辑器 -->
      <el-col :span="16">
        <el-card v-if="currentConfig">
          <template #header>
            <div style="display:flex;justify-content:space-between;align-items:center">
              <span>编辑: {{ currentConfig.resourceType }} 映射规则</span>
              <div>
                <el-button size="small" @click="testTransformDialog = true" type="warning">测试转换</el-button>
                <el-button size="small" @click="saveMapping" type="primary" :loading="saving">保存</el-button>
              </div>
            </div>
          </template>
          <div style="margin-bottom:12px">
            <el-descriptions :column="3" border size="small">
              <el-descriptions-item label="资源类型">{{ currentConfig.resourceType }}</el-descriptions-item>
              <el-descriptions-item label="源数据表">{{ currentConfig.sourceTable }}</el-descriptions-item>
              <el-descriptions-item label="源系统">{{ currentConfig.sourceSystem }}</el-descriptions-item>
            </el-descriptions>
          </div>
          <textarea
            v-model="editorContent"
            class="json-editor"
            spellcheck="false"
          />
        </el-card>
        <el-empty v-else description="请选择一个映射规则进行编辑" />
      </el-col>
    </el-row>

    <!-- 测试转换对话框 -->
    <el-dialog v-model="testTransformDialog" title="测试映射转换" width="700px">
      <el-form label-position="top">
        <el-form-item label="源数据 (JSON)">
          <textarea v-model="testInput" class="json-editor" style="height:150px" placeholder='如: {"patient_id":"10001","name":"测试","gender":"M","birth_date":"1990-01-01"}' />
        </el-form-item>
      </el-form>
      <el-button type="primary" @click="doTestTransform" :loading="testing">执行转换</el-button>
      <div v-if="testResult" style="margin-top:16px">
        <el-divider>转换结果 (FHIR R4)</el-divider>
        <pre class="json-view">{{ JSON.stringify(testResult, null, 2) }}</pre>
      </div>
      <template #footer>
        <el-button @click="testTransformDialog = false">关闭</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { listMappings, getMapping, updateMapping, reloadMappings, testTransform } from '../api'

const mappings = ref([])
const selected = ref('')
const currentConfig = ref(null)
const editorContent = ref('')
const saving = ref(false)
const reloading = ref(false)
const testTransformDialog = ref(false)
const testInput = ref('')
const testResult = ref(null)
const testing = ref(false)

onMounted(loadList)

async function loadList() {
  const res = await listMappings()
  mappings.value = res.data
}

async function selectMapping(type) {
  selected.value = type
  const res = await getMapping(type)
  currentConfig.value = res.data
  editorContent.value = JSON.stringify(res.data, null, 2)
}

async function saveMapping() {
  try {
    const parsed = JSON.parse(editorContent.value)
    saving.value = true
    await updateMapping(selected.value, parsed)
    ElMessage.success('映射规则保存成功（已热更新）')
    currentConfig.value = parsed
    await loadList()
  } catch (e) {
    ElMessage.error('JSON格式错误: ' + e.message)
  } finally {
    saving.value = false
  }
}

async function reloadAll() {
  reloading.value = true
  try {
    await reloadMappings()
    await loadList()
    ElMessage.success('所有映射规则已重载')
  } finally {
    reloading.value = false
  }
}

async function doTestTransform() {
  try {
    const sourceData = JSON.parse(testInput.value)
    testing.value = true
    const res = await testTransform(selected.value, sourceData)
    testResult.value = res.data
  } catch (e) {
    ElMessage.error('转换失败: ' + e.message)
  } finally {
    testing.value = false
  }
}
</script>

<style scoped>
.mapping-item {
  padding: 12px;
  border-radius: 6px;
  cursor: pointer;
  margin-bottom: 8px;
  border: 1px solid #ebeef5;
  transition: all 0.2s;
}
.mapping-item:hover { border-color: #409eff; }
.mapping-item.active { border-color: #409eff; background: #ecf5ff; }
.mapping-name { font-weight: 600; margin-bottom: 4px; }
.mapping-meta { display: flex; gap: 8px; align-items: center; }
.json-editor {
  width: 100%;
  height: 400px;
  font-family: 'Courier New', monospace;
  font-size: 13px;
  line-height: 1.6;
  padding: 12px;
  border: 1px solid #dcdfe6;
  border-radius: 4px;
  resize: vertical;
  box-sizing: border-box;
}
.json-view {
  background: #fafafa;
  border: 1px solid #ebeef5;
  border-radius: 4px;
  padding: 12px;
  font-size: 13px;
  overflow-x: auto;
  white-space: pre-wrap;
}
</style>
