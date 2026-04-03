<template>
  <el-container style="min-height:100vh">
    <el-aside width="220px" style="background:#304156">
      <div class="logo">
        <el-icon :size="24" color="#409EFF"><Monitor /></el-icon>
        <span>FHIR数据中台</span>
      </div>
      <el-menu
        :default-active="route.path"
        router
        background-color="#304156"
        text-color="#bfcbd9"
        active-text-color="#409EFF"
      >
        <el-menu-item index="/dashboard">
          <el-icon><DataAnalysis /></el-icon>
          <span>系统总览</span>
        </el-menu-item>
        <el-menu-item index="/fhir-test">
          <el-icon><Search /></el-icon>
          <span>接口测试</span>
        </el-menu-item>
        <el-menu-item index="/mapping">
          <el-icon><Setting /></el-icon>
          <span>映射配置</span>
        </el-menu-item>
        <el-menu-item index="/empi">
          <el-icon><Connection /></el-icon>
          <span>EMPI管理</span>
        </el-menu-item>
        <el-menu-item index="/permissions">
          <el-icon><Lock /></el-icon>
          <span>权限管理</span>
        </el-menu-item>
      </el-menu>
    </el-aside>
    <el-container>
      <el-header style="display:flex;align-items:center;justify-content:space-between;background:#fff;box-shadow:0 1px 4px rgba(0,0,0,.08)">
        <span style="font-size:18px;font-weight:600;color:#303133">{{ route.meta.title }}</span>
        <div style="display:flex;align-items:center;gap:16px">
          <el-tag>{{ userStore.user?.name }}</el-tag>
          <el-tag type="info">{{ userStore.user?.department }}</el-tag>
          <el-button text @click="handleLogout">退出登录</el-button>
        </div>
      </el-header>
      <el-main style="background:#f0f2f5;padding:20px">
        <router-view />
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup>
import { useRoute, useRouter } from 'vue-router'
import { Monitor, DataAnalysis, Search, Setting, Connection, Lock } from '@element-plus/icons-vue'
import { useUserStore } from '../stores/user'

const route = useRoute()
const router = useRouter()
const userStore = useUserStore()

function handleLogout() {
  userStore.logout()
  router.push('/login')
}
</script>

<style scoped>
.logo {
  height: 60px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  color: #fff;
  font-size: 16px;
  font-weight: 600;
  border-bottom: 1px solid rgba(255,255,255,0.1);
}
</style>
