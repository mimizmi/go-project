<template>
  <div class="login-container">
    <div class="login-card">
      <div class="login-header">
        <el-icon :size="48" color="#409EFF"><Monitor /></el-icon>
        <h1>FHIR数据中台</h1>
        <p>统一接口服务管理控制台</p>
      </div>
      <el-form :model="form" @submit.prevent="handleLogin" class="login-form">
        <el-form-item>
          <el-input v-model="form.username" placeholder="用户名" :prefix-icon="User" size="large" />
        </el-form-item>
        <el-form-item>
          <el-input v-model="form.password" placeholder="密码" type="password" :prefix-icon="Lock" size="large" show-password />
        </el-form-item>
        <el-button type="primary" size="large" :loading="loading" native-type="submit" style="width:100%">登 录</el-button>
      </el-form>
      <div class="login-accounts">
        <p>演示账号：</p>
        <el-tag v-for="u in demoUsers" :key="u.username" @click="fillDemo(u)" style="cursor:pointer;margin:4px" effect="plain">
          {{ u.label }} ({{ u.username }})
        </el-tag>
      </div>
    </div>
  </div>
</template>

<script setup>
import { reactive, ref, shallowRef } from 'vue'
import { useRouter } from 'vue-router'
import { User, Lock, Monitor } from '@element-plus/icons-vue'
import { useUserStore } from '../stores/user'
import { login } from '../api'

const router = useRouter()
const userStore = useUserStore()
const loading = ref(false)
const form = reactive({ username: '', password: '' })

const demoUsers = [
  { label: '管理员', username: 'admin', password: 'admin123' },
  { label: '医生', username: 'doctor_zhang', password: '123456' },
  { label: '护士', username: 'nurse_li', password: '123456' },
  { label: '检验技师', username: 'lab_wang', password: '123456' },
]

function fillDemo(u) {
  form.username = u.username
  form.password = u.password
}

async function handleLogin() {
  if (!form.username || !form.password) return
  loading.value = true
  try {
    const res = await login(form.username, form.password)
    userStore.setAuth(res.token, res.user)
    router.push('/')
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
.login-container {
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}
.login-card {
  width: 420px;
  padding: 40px;
  background: #fff;
  border-radius: 12px;
  box-shadow: 0 20px 60px rgba(0,0,0,0.3);
}
.login-header {
  text-align: center;
  margin-bottom: 30px;
}
.login-header h1 {
  margin: 12px 0 4px;
  font-size: 24px;
  color: #303133;
}
.login-header p {
  color: #909399;
  font-size: 14px;
}
.login-form {
  margin-bottom: 20px;
}
.login-accounts {
  text-align: center;
  color: #909399;
  font-size: 13px;
}
.login-accounts p {
  margin-bottom: 8px;
}
</style>
