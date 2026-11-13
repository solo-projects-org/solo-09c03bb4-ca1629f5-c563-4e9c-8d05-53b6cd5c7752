"""统一的异步桥接。

所有 SQLite 同步操作都通过本模块的独立线程池执行，而不是 anyio 默认的
40 线程容量限流器，原因：

- worker 忙轮询会持续占用线程执行带 busy_timeout 的短事务；
- 若这些线程在等锁，而新的 DB 任务又排在同一个容量受限的池里，
  池耗尽后会出现“线程等锁、任务等线程”的自死锁；
- 独立线程池 + 每个任务自己的短连接，保证 web 回调不会被 worker 饿死。
"""
from __future__ import annotations

import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, TypeVar

T = TypeVar("T")

# 独立 DB 线程池：线程均为短事务任务，容量给足，避免与网络/其它线程任务互相饿死
_DB_POOL = ThreadPoolExecutor(max_workers=32, thread_name_prefix="qc-db")


async def run_db(func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
    """在独立 DB 线程池中运行同步函数。"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        _DB_POOL, functools.partial(func, *args, **kwargs)
    )
