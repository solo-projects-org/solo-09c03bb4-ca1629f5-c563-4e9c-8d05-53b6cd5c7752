#!/usr/bin/env bash
# 本地一键启动（无 Docker 环境时使用）：模拟源 + web + 两个独立采集进程
set -euo pipefail
cd "$(dirname "$0")"

export PYTHONPATH="$(pwd)"
export QC_DB_PATH="$(pwd)/data/qc.db"
export MOCK_BASE_URL="http://127.0.0.1:9000"
export WEB_PUBLIC_BASE_URL="http://127.0.0.1:8080"
export MOCK_HOST="127.0.0.1" MOCK_PORT="9000" QC_HOST="127.0.0.1" QC_PORT="8080"
export REFRESH_LEASE_SECONDS="${REFRESH_LEASE_SECONDS:-15}"

PIDS=()
cleanup() {
  echo ""
  echo "正在停止本地服务..."
  kill "${PIDS[@]}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

mkdir -p data
echo "▶ 启动检测机构模拟源  http://127.0.0.1:9000/docs"
python3 -m mock_lab.server & PIDS+=($!)
sleep 1.5
echo "▶ 启动质控库网页      http://127.0.0.1:8080"
python3 -m app.web & PIDS+=($!)
sleep 1.5
echo "▶ 启动采集进程 A / B（共用同一 OAuth2 账户与数据卷 $QC_DB_PATH）"
WORKER_NAME=collector-a python3 -m app.worker A & PIDS+=($!)
WORKER_NAME=collector-b python3 -m app.worker B & PIDS+=($!)

echo ""
echo "✅ 全部就绪。浏览器打开 http://127.0.0.1:8080 ，先点「发起授权」。"
echo "   Ctrl+C 停止全部进程。"
wait
