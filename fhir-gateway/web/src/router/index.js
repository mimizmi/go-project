import { createRouter, createWebHashHistory } from 'vue-router'

const routes = [
  {
    path: '/login',
    name: 'Login',
    component: () => import('../views/Login.vue'),
    meta: { public: true },
  },
  {
    path: '/',
    component: () => import('../views/Layout.vue'),
    redirect: '/dashboard',
    children: [
      { path: 'dashboard', name: 'Dashboard', component: () => import('../views/Dashboard.vue'), meta: { title: '系统总览' } },
      { path: 'fhir-test', name: 'FhirTest', component: () => import('../views/FhirTest.vue'), meta: { title: '接口测试' } },
      { path: 'mapping', name: 'MappingConfig', component: () => import('../views/MappingConfig.vue'), meta: { title: '映射配置' } },
      { path: 'empi', name: 'Empi', component: () => import('../views/Empi.vue'), meta: { title: 'EMPI管理' } },
      { path: 'permissions', name: 'Permissions', component: () => import('../views/Permissions.vue'), meta: { title: '权限管理' } },
    ],
  },
]

const router = createRouter({
  history: createWebHashHistory(),
  routes,
})

router.beforeEach((to, from, next) => {
  if (to.meta.public) return next()
  const token = localStorage.getItem('token')
  if (!token) return next('/login')
  next()
})

export default router
