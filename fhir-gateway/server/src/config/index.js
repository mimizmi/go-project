// 网关配置
module.exports = {
  port: process.env.PORT || 3001,
  jwtSecret: process.env.JWT_SECRET || 'fhir-gateway-secret-key-2024',
  jwtExpiresIn: '8h',

  // med-insight 查询服务地址
  medInsight: {
    baseUrl: process.env.MED_INSIGHT_URL || 'http://localhost:8082',
  },

  // 映射规则文件路径
  mappingDir: process.env.MAPPING_DIR || require('path').join(__dirname, '..', 'data', 'mappings'),

  // CORS 允许的前端地址
  corsOrigins: (process.env.CORS_ORIGINS || 'http://localhost:5173').split(','),
};
