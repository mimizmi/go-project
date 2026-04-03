.PHONY: install dev-server dev-web dev build up down clean

# 安装所有依赖
install:
	cd server && npm install
	cd web && npm install

# 开发模式：启动后端
dev-server:
	cd server && node src/app.js

# 开发模式：启动前端 (Vite dev server)
dev-web:
	cd web && npx vite --host

# 同时启动前后端（开发模式）
dev:
	@echo "请在两个终端分别执行:"
	@echo "  Terminal 1: make dev-server"
	@echo "  Terminal 2: make dev-web"

# 生产构建
build:
	cd web && npx vite build

# Docker 编排
up:
	docker compose up -d --build

down:
	docker compose down

clean:
	rm -rf server/node_modules web/node_modules web/dist
