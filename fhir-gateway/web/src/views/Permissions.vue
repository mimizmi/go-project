<template>
  <div>
    <el-row :gutter="20">
      <!-- 角色与策略 -->
      <el-col :span="12">
        <el-card style="margin-bottom:20px">
          <template #header><span>角色定义</span></template>
          <el-table :data="roles" stripe size="small">
            <el-table-column prop="id" label="角色ID" width="140" />
            <el-table-column prop="name" label="角色名称" width="120" />
            <el-table-column prop="description" label="描述" />
          </el-table>
        </el-card>

        <el-card>
          <template #header><span>访问控制策略 (RBAC+ABAC)</span></template>
          <el-table :data="policies" stripe size="small">
            <el-table-column prop="role" label="角色" width="140" />
            <el-table-column label="可访问资源">
              <template #default="{ row }">
                <el-tag v-for="r in row.resources" :key="r" size="small" style="margin:2px">{{ r }}</el-tag>
              </template>
            </el-table-column>
            <el-table-column label="科室范围" width="100">
              <template #default="{ row }">
                <el-tag :type="row.departments.includes('*') ? 'success' : 'warning'" size="small">
                  {{ row.departments.includes('*') ? '全部' : '本科室' }}
                </el-tag>
              </template>
            </el-table-column>
            <el-table-column label="操作" width="140">
              <template #default="{ row }">
                <el-tag v-for="a in row.actions" :key="a" size="small" style="margin:2px" type="info">{{ a }}</el-tag>
              </template>
            </el-table-column>
          </el-table>
        </el-card>
      </el-col>

      <!-- 用户与权限测试 -->
      <el-col :span="12">
        <el-card style="margin-bottom:20px">
          <template #header><span>用户列表</span></template>
          <el-table :data="users" stripe size="small">
            <el-table-column prop="userId" label="用户ID" width="80" />
            <el-table-column prop="name" label="姓名" width="100" />
            <el-table-column prop="username" label="用户名" width="140" />
            <el-table-column prop="role" label="角色" width="140" />
            <el-table-column prop="department" label="科室" />
          </el-table>
        </el-card>

        <el-card>
          <template #header><span>权限检查测试</span></template>
          <el-form :model="checkForm" label-position="top" size="small">
            <el-form-item label="用户">
              <el-select v-model="checkForm.userId" style="width:100%">
                <el-option v-for="u in users" :key="u.userId" :label="`${u.name} (${u.role})`" :value="u.userId" />
              </el-select>
            </el-form-item>
            <el-form-item label="FHIR资源类型">
              <el-select v-model="checkForm.resourceType" style="width:100%">
                <el-option label="Patient" value="Patient" />
                <el-option label="Encounter" value="Encounter" />
                <el-option label="MedicationRequest" value="MedicationRequest" />
                <el-option label="DiagnosticReport" value="DiagnosticReport" />
                <el-option label="Observation" value="Observation" />
              </el-select>
            </el-form-item>
            <el-form-item label="目标科室 (可选)">
              <el-input v-model="checkForm.targetDepartment" placeholder="如: 心内科" />
            </el-form-item>
            <el-button type="primary" @click="doCheck" size="small">验证权限</el-button>
          </el-form>

          <div v-if="checkResult" style="margin-top:16px">
            <el-alert
              :title="checkResult.check.allowed ? '权限验证通过' : '权限验证拒绝'"
              :type="checkResult.check.allowed ? 'success' : 'error'"
              :description="checkResult.check.reason"
              show-icon
              :closable="false"
            />
            <div v-if="checkResult.check.allowed && checkResult.check.allowedFields" style="margin-top:8px">
              <el-text size="small" type="info">允许访问的字段：</el-text>
              <el-tag v-for="f in checkResult.check.allowedFields" :key="f" size="small" style="margin:2px">{{ f }}</el-tag>
            </div>
          </div>
        </el-card>
      </el-col>
    </el-row>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { getRoles, getUsers, getPolicies, checkPermission } from '../api'

const roles = ref([])
const users = ref([])
const policies = ref([])
const checkForm = reactive({ userId: 'U001', resourceType: 'Patient', targetDepartment: '' })
const checkResult = ref(null)

onMounted(async () => {
  const [r, u, p] = await Promise.all([getRoles(), getUsers(), getPolicies()])
  roles.value = r.data
  users.value = u.data
  policies.value = p.data
})

async function doCheck() {
  const res = await checkPermission(checkForm)
  checkResult.value = res.data
}
</script>
