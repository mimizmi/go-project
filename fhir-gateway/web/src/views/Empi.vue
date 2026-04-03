<template>
  <div>
    <el-row :gutter="20">
      <!-- EMPI记录列表 -->
      <el-col :span="16">
        <el-card>
          <template #header><span>患者主索引 (EMPI) 记录</span></template>
          <el-table :data="empiList" stripe>
            <el-table-column prop="empiId" label="EMPI ID" width="120" />
            <el-table-column prop="name" label="姓名" width="100" />
            <el-table-column prop="gender" label="性别" width="80">
              <template #default="{ row }">
                <el-tag :type="row.gender === 'male' ? 'primary' : 'danger'" size="small">
                  {{ row.gender === 'male' ? '男' : '女' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column prop="birthDate" label="出生日期" width="120" />
            <el-table-column prop="idCard" label="身份证号" width="180" />
            <el-table-column prop="matchMethod" label="匹配方法" width="160">
              <template #default="{ row }">
                <el-tag :type="row.matchMethod === 'exact_id_card' ? 'success' : 'warning'" size="small">
                  {{ row.matchMethod === 'exact_id_card' ? '精确匹配' : '混合匹配' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="关联系统">
              <template #default="{ row }">
                <el-tag v-for="id in row.identifiers" :key="id.system" size="small" style="margin:2px">
                  {{ id.system }}: {{ id.value }}
                </el-tag>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>

      <!-- 右侧：EMPI查询测试 -->
      <el-col :span="8">
        <el-card style="margin-bottom:20px">
          <template #header><span>精确查询</span></template>
          <el-form :model="exactQuery" label-position="top" size="small">
            <el-form-item label="源系统">
              <el-select v-model="exactQuery.system" style="width:100%">
                <el-option label="HIS (his_mysql_01)" value="his_mysql_01" />
                <el-option label="LIS (lis_sqlserver_01)" value="lis_sqlserver_01" />
              </el-select>
            </el-form-item>
            <el-form-item label="源患者ID">
              <el-input v-model="exactQuery.id" placeholder="如: HIS-P-10001" />
            </el-form-item>
            <el-button type="primary" @click="doExactQuery" size="small">查询</el-button>
          </el-form>
          <div v-if="exactResult" style="margin-top:12px">
            <el-descriptions :column="1" border size="small">
              <el-descriptions-item label="EMPI ID">{{ exactResult.empiId }}</el-descriptions-item>
              <el-descriptions-item label="姓名">{{ exactResult.name }}</el-descriptions-item>
              <el-descriptions-item label="匹配得分">{{ exactResult.matchScore }}</el-descriptions-item>
              <el-descriptions-item label="匹配方法">{{ exactResult.matchMethod }}</el-descriptions-item>
            </el-descriptions>
          </div>
        </el-card>

        <el-card>
          <template #header><span>模糊匹配 (人口学属性)</span></template>
          <el-form :model="fuzzyQuery" label-position="top" size="small">
            <el-form-item label="姓名">
              <el-input v-model="fuzzyQuery.name" placeholder="如: 张三" />
            </el-form-item>
            <el-form-item label="性别">
              <el-select v-model="fuzzyQuery.gender" clearable style="width:100%">
                <el-option label="男" value="male" />
                <el-option label="女" value="female" />
              </el-select>
            </el-form-item>
            <el-form-item label="出生日期">
              <el-input v-model="fuzzyQuery.birthDate" placeholder="1985-03-15" />
            </el-form-item>
            <el-button type="warning" @click="doFuzzyMatch" size="small">模糊匹配</el-button>
          </el-form>
          <div v-if="fuzzyResults.length" style="margin-top:12px">
            <div v-for="r in fuzzyResults" :key="r.empiId" style="padding:6px 0;border-bottom:1px solid #ebeef5">
              <el-text>{{ r.empiId }} - {{ r.name }}</el-text>
              <el-tag size="small" type="success" style="float:right">得分: {{ r.matchScore }}</el-tag>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { getEmpiList, resolveEmpi, empiMatch } from '../api'

const empiList = ref([])
const exactQuery = reactive({ system: 'his_mysql_01', id: '1' })
const exactResult = ref(null)
const fuzzyQuery = reactive({ name: '', gender: '', birthDate: '' })
const fuzzyResults = ref([])

onMounted(async () => {
  const res = await getEmpiList()
  empiList.value = res.data
})

async function doExactQuery() {
  try {
    const res = await resolveEmpi(exactQuery.system, exactQuery.id)
    exactResult.value = res.data
  } catch { exactResult.value = null }
}

async function doFuzzyMatch() {
  const res = await empiMatch(fuzzyQuery)
  fuzzyResults.value = res.data
}
</script>
