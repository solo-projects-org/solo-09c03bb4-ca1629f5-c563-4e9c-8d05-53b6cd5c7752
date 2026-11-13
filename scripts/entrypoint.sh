#!/bin/sh
# 容器入口：按参数启动不同角色
set -e
case "$1" in
  mock)
    exec python -m mock_lab.server
    ;;
  web)
    exec python -m app.web
    ;;
  worker)
    exec python -m app.worker "$2"
    ;;
  *)
    echo "usage: entrypoint.sh mock|web|worker [A|B]" >&2
    exit 2
    ;;
esac
