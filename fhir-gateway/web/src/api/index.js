import axios from 'axios'
import { ElMessage } from 'element-plus'

const api = axios.create({
  baseURL: '/api',
  timeout: 15000,
})

// 请求拦截：注入JWT
api.interceptors.request.use(config => {
  const token = localStorage.getItem('token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

// 响应拦截：统一错误处理
api.interceptors.response.use(
  res => res.data,
  err => {
    const msg = err.response?.data?.message || err.response?.data?.error || err.message
    if (err.response?.status === 401) {
      localStorage.removeItem('token')
      window.location.hash = '#/login'
      ElMessage.error('认证已过期，请重新登录')
    } else {
      ElMessage.error(msg)
    }
    return Promise.reject(err)
  }
)

// --- Auth ---
export const login = (username, password) =>
  api.post('/auth/login', { username, password })

export const getProfile = () => api.get('/auth/profile')

// --- FHIR ---
export const getPatient = (id) => api.get(`/fhir/Patient/${id}`)
export const getPatientEverything = (id) => api.get(`/fhir/Patient/${id}/$everything`)
export const getEncounter = (id) => api.get(`/fhir/Encounter/${id}`)

// --- Mapping ---
export const listMappings = () => api.get('/mappings')
export const getMapping = (type) => api.get(`/mappings/${type}`)
export const updateMapping = (type, data) => api.put(`/mappings/${type}`, data)
export const reloadMappings = () => api.post('/mappings/reload')
export const testTransform = (resourceType, sourceData) =>
  api.post('/mappings/test-transform', { resourceType, sourceData })

// --- System ---
export const getSystemStatus = () => api.get('/system/status')
export const getEmpiList = () => api.get('/system/empi')
export const resolveEmpi = (system, id) => api.get('/system/empi/resolve', { params: { system, id } })
export const empiMatch = (data) => api.post('/system/empi/match', data)
export const getRoles = () => api.get('/system/permissions/roles')
export const getUsers = () => api.get('/system/permissions/users')
export const getPolicies = () => api.get('/system/permissions/policies')
export const checkPermission = (data) => api.post('/system/permissions/check', data)

export default api
