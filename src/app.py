"""服务入口。

用法：
    python3 -m src.app --host 127.0.0.1 --port 8080 --log ./run/events.jsonl [--seed]

--seed 启动时登记豫园水上舞台与外滩露台（已存在则跳过）。
日志为只追加 JSONL；删除日志即得到一个干净可重放的环境。
"""
from __future__ import annotations

import argparse
import signal
import sys
import threading

from .httpapi import build_service, create_server, seed_venues


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="双场域演出版本衔接服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--log", default="./run/events.jsonl", help="JSONL 事件日志路径；传空串表示纯内存")
    parser.add_argument("--seed", action="store_true", help="登记两个标准场地")
    args = parser.parse_args(argv)

    log_path = args.log or None
    store, service = build_service(log_path)
    if args.seed:
        seed_venues(service)
    httpd = create_server(service, args.host, args.port)

    stop_event = threading.Event()

    def _request_stop(_signum: int, _frame: object) -> None:
        # 仅唤醒主线程；shutdown() 不能在 serve_forever 自身线程调用（会死锁）。
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _request_stop)

    server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    server_thread.start()
    print(f"lineage service on http://{args.host}:{args.port} (log={log_path or 'memory'})", flush=True)
    try:
        stop_event.wait()
    finally:
        httpd.shutdown()
        httpd.server_close()
        server_thread.join(timeout=3)
        store.close()
        print("lineage service stopped", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
