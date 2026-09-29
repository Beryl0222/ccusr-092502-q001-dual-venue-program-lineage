"""服务入口：python3 -m src.server [--host 127.0.0.1] [--port 8080] [--store data/events.jsonl]"""
from __future__ import annotations

import argparse

from .httpapi import build_server


def main() -> None:
    parser = argparse.ArgumentParser(description="双场域演出版本衔接服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--store", default="data/events.jsonl")
    parser.add_argument("--access-log", action="store_true")
    args = parser.parse_args()

    server = build_server(args.host, args.port, args.store, access_log=args.access_log)
    print(f"服务已启动: http://{args.host}:{args.port}  事件日志: {args.store}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
