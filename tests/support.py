"""测试支撑：在随机端口启动真实 HTTP 服务（JSONL 落临时目录）。"""
from __future__ import annotations

import http.client
import json
import tempfile
import threading
from pathlib import Path
from typing import Any
from urllib.parse import quote

from src.httpapi import build_server


class Harness:
    def __init__(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store_path = str(Path(self.tmp.name) / "events.jsonl")
        self.server = build_server("127.0.0.1", 0, self.store_path)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def restart(self) -> None:
        """关闭后用同一事件日志重启，验证可重放。"""
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.server = build_server("127.0.0.1", 0, self.store_path)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.tmp.cleanup()

    def request(self, method: str, path: str, token: str | None = None,
                body: dict[str, Any] | None = None,
                headers: dict[str, str] | None = None) -> tuple[int, dict[str, str], Any]:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        hdr = dict(headers or {})
        if token is not None:
            hdr["Authorization"] = f"Bearer {token}"
        payload = None
        if body is not None:
            payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
            hdr["Content-Type"] = "application/json; charset=utf-8"
        conn.request(method, path, body=payload, headers=hdr)
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8")
        resp_headers = {k: v for k, v in resp.getheaders()}
        conn.close()
        try:
            parsed = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            parsed = raw
        return resp.status, resp_headers, parsed


# 常用令牌
COORD = "coordinator"
PROD_A = "producer:A"
PROD_B = "producer:B"
VENUE1 = "venue:v-yuyuan"
VENUE2 = "venue:v-bund"
WEB = "channel:web"
WECHAT = "channel:wechat"
