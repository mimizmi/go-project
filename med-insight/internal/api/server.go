// Package api 提供 HTTP 查询服务，暴露原始数据接口供 FHIR Gateway 消费。
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/mimizh/med-insight/internal/metrics"
	"github.com/mimizh/med-insight/internal/query"
)

// Server HTTP 服务封装。
type Server struct {
	engine  *query.Engine
	metrics *metrics.Metrics
	logger  *zap.Logger
	mux     *http.ServeMux
	httpSrv *http.Server
}

// New 创建并注册所有路由。
func New(engine *query.Engine, m *metrics.Metrics, logger *zap.Logger, port int) *Server {
	s := &Server{
		engine:  engine,
		metrics: m,
		logger:  logger,
		mux:     http.NewServeMux(),
	}
	s.registerRoutes()
	s.httpSrv = &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      s.mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	return s
}

// registerRoutes 注册所有 API 路由。
func (s *Server) registerRoutes() {
	// 健康检查
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)

	// Prometheus 指标
	s.mux.Handle("GET /metrics", metrics.Handler())

	// ── 原始数据接口（供 FHIR Gateway 消费）──────────────────────────────
	// 患者原始数据：GET /api/v1/data/patient/{id}
	s.mux.HandleFunc("GET /api/v1/data/patient/{id}", s.instrument("data-patient", s.handleDataPatient))

	// 就诊原始数据：GET /api/v1/data/encounter/{id}
	s.mux.HandleFunc("GET /api/v1/data/encounter/{id}", s.instrument("data-encounter", s.handleDataEncounter))
}

// Start 启动 HTTP 服务（阻塞直到 ctx 取消）。
func (s *Server) Start(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("HTTP server started", zap.String("addr", s.httpSrv.Addr))
		if err := s.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.httpSrv.Shutdown(shutCtx)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 通用工具
// ─────────────────────────────────────────────────────────────────────────────

// instrument 包装 handler，记录延迟与请求数 Prometheus 指标。
func (s *Server) instrument(endpoint string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w, status: 200}
		h(rw, r)
		elapsed := time.Since(start).Seconds()

		status := "ok"
		if rw.status >= 400 {
			status = "error"
		}
		s.metrics.QueryRequests.WithLabelValues(endpoint, status).Inc()
		s.metrics.QueryDuration.WithLabelValues(endpoint).Observe(elapsed)
	}
}

// responseWriter 捕获 HTTP 状态码。
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

// writeJSON 序列化并写入 JSON 响应。
func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// errResp 统一错误响应结构。
type errResp struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, errResp{Error: msg})
}

// ─────────────────────────────────────────────────────────────────────────────
// Handlers
// ─────────────────────────────────────────────────────────────────────────────

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

// handleDataPatient 返回患者原始数据（供 FHIR Gateway 映射转换）。
func (s *Server) handleDataPatient(w http.ResponseWriter, r *http.Request) {
	patientID := r.PathValue("id")
	if patientID == "" {
		writeError(w, 400, "patient id is required")
		return
	}

	p, err := s.engine.GetPatient(r.Context(), patientID)
	if err != nil {
		s.logger.Error("GetPatient", zap.String("id", patientID), zap.Error(err))
		writeError(w, 500, err.Error())
		return
	}
	if p == nil {
		writeError(w, 404, fmt.Sprintf("patient %s not found", patientID))
		return
	}
	writeJSON(w, 200, map[string]interface{}{"data": p})
}

// handleDataEncounter 返回就诊原始数据（供 FHIR Gateway 映射转换）。
func (s *Server) handleDataEncounter(w http.ResponseWriter, r *http.Request) {
	visitID := r.PathValue("id")
	if visitID == "" {
		writeError(w, 400, "encounter id is required")
		return
	}

	v, err := s.engine.GetVisit(r.Context(), visitID)
	if err != nil {
		s.logger.Error("GetVisit", zap.String("id", visitID), zap.Error(err))
		writeError(w, 500, err.Error())
		return
	}
	if v == nil {
		writeError(w, 404, fmt.Sprintf("encounter %s not found", visitID))
		return
	}
	writeJSON(w, 200, map[string]interface{}{"data": v})
}
