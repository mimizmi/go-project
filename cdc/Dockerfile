# 多阶段构建：构建阶段
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache gcc musl-dev sqlite-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -tags musl -o /app/cdc-platform ./cmd/platform
RUN CGO_ENABLED=1 GOOS=linux go build -tags musl -o /app/cdc-cli ./cmd/cli

# 运行阶段
FROM alpine:3.19
RUN apk add --no-cache ca-certificates sqlite-libs tzdata

WORKDIR /app
COPY --from=builder /app/cdc-platform /app/cdc-platform
COPY --from=builder /app/cdc-cli /app/cdc-cli
COPY configs/ /app/configs/

RUN mkdir -p /app/data/offsets

EXPOSE 8000
ENTRYPOINT ["/app/cdc-platform"]
