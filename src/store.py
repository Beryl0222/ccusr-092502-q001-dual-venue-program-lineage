"""只追加的事件存储与重放。

业务事实一旦写入即不可原地修改，状态更正只能由后续事件表达。
每个聚合流拥有独立的单调版本号；写入时以期望版本做乐观并发控制，
并发确认同一场次时只会有一方成功。

日志为单行 JSON（JSONL），每行是一条 event 或一条幂等回执记录，
重启后按顺序重放即可同时重建事件流与幂等结果，保证重复请求语义可重放。
"""
from __future__ import annotations

import json
import os
import threading
from collections.abc import Callable, Iterable
from typing import Any


class DomainError(Exception):
    """业务规则冲突（含并发版本冲突）。"""

    def __init__(self, code: str, message: str, status: int = 422) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


class EventStore:
    def __init__(self, log_path: str | os.PathLike[str] | None = None) -> None:
        self._lock = threading.RLock()
        self._global: list[dict[str, Any]] = []
        self._streams: dict[str, list[dict[str, Any]]] = {}
        self._idempotency: dict[str, dict[str, dict[str, Any]]] = {}
        self._listeners: list[Callable[[dict[str, Any]], None]] = []
        self._log_path = os.fspath(log_path) if log_path else None
        self._log_file = None
        if self._log_path:
            os.makedirs(os.path.dirname(os.path.abspath(self._log_path)), exist_ok=True)
            if os.path.exists(self._log_path):
                self._replay()
            self._log_file = open(self._log_path, "a", encoding="utf-8", buffering=1)

    # ---------- 订阅与重放 ----------
    def subscribe(self, handler: Callable[[dict[str, Any]], None]) -> None:
        """注册投影处理器；历史事件立即重放，之后每次写入同步调用。"""
        with self._lock:
            for event in self._global:
                handler(event)
            self._listeners.append(handler)

    def _replay(self) -> None:
        assert self._log_path is not None
        with open(self._log_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if record["kind"] == "event":
                    self._ingest_event(record["stream"], record["event"])
                elif record["kind"] == "idem":
                    self._idempotency.setdefault(record["scope"], {})[record["key"]] = record["response"]

    def _ingest_event(self, stream_id: str, event: dict[str, Any]) -> None:
        self._global.append(event)
        self._streams.setdefault(stream_id, []).append(event)

    # ---------- 写入 ----------
    def commit(
        self,
        entries: Iterable[tuple[str, int, dict[str, Any]]],
        remember: tuple[str, str, dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """在一个临界区内完成：校验多个流的期望版本、分配版本号、
        将事件与幂等回执一并写盘（单次 fsync）、更新投影。

        任一流版本不符则整体失败、不落任何记录。
        remember 为 (scope, key, response)，response 可引用条目中预生成的 event_id。
        """
        with self._lock:
            entries = list(entries)
            planned: list[tuple[str, dict[str, Any]]] = []
            delta: dict[str, int] = {}
            for stream_id, expected_version, event in entries:
                base = len(self._streams.get(stream_id, ()))
                will_be = base + delta.get(stream_id, 0)
                if will_be != expected_version:
                    raise DomainError(
                        "VERSION_CONFLICT",
                        f"流 {stream_id} 版本已变化：期望 {expected_version}，当前 {will_be}",
                        status=409,
                    )
                event = dict(event)
                event["version"] = will_be + 1
                delta[stream_id] = delta.get(stream_id, 0) + 1
                planned.append((stream_id, event))
            records: list[dict[str, Any]] = [
                {"kind": "event", "stream": stream_id, "event": event}
                for stream_id, event in planned
            ]
            committed = [event for _stream_id, event in planned]
            if remember is not None:
                scope, key, response = remember
                if self._idempotency.get(scope, {}).get(key) is not None:
                    raise DomainError("IDEMPOTENCY_RACE", "幂等键并发冲突", status=409)
                records.append(
                    {"kind": "idem", "scope": scope, "key": key, "response": response}
                )
            for record in records:
                if self._log_file is not None:
                    self._log_file.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            if self._log_file is not None:
                self._log_file.flush()
                os.fsync(self._log_file.fileno())
            for stream_id, event in planned:
                self._ingest_event(stream_id, event)
            if remember is not None:
                scope, key, response = remember
                self._idempotency.setdefault(scope, {})[key] = response
            for event in committed:
                for handler in list(self._listeners):
                    handler(event)
            return committed

    def append(
        self,
        stream_id: str,
        expected_version: int,
        event: dict[str, Any],
    ) -> dict[str, Any]:
        """单流追加的便捷封装。"""
        return self.commit([(stream_id, expected_version, event)])[0]

    # ---------- 读取 ----------
    def version(self, stream_id: str) -> int:
        with self._lock:
            return len(self._streams.get(stream_id, ()))

    def events(self, stream_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._streams.get(stream_id, ()))

    def all_events(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._global)

    def exists(self, stream_id: str) -> bool:
        with self._lock:
            return stream_id in self._streams

    # ---------- 幂等回执（与事件同一日志，重放后保持一致） ----------
    def remembered_response(self, scope: str, key: str) -> dict[str, Any] | None:
        with self._lock:
            return self._idempotency.get(scope, {}).get(key)

    def remember_response(
        self, scope: str, key: str, response: dict[str, Any]
    ) -> None:
        """仅记录幂等回执（无伴随事件时使用）。"""
        with self._lock:
            if self._idempotency.get(scope, {}).get(key) is not None:
                return
            record = {"kind": "idem", "scope": scope, "key": key, "response": response}
            if self._log_file is not None:
                self._log_file.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
                self._log_file.flush()
                os.fsync(self._log_file.fileno())
            self._idempotency.setdefault(scope, {})[key] = response

    def close(self) -> None:
        with self._lock:
            if self._log_file is not None:
                self._log_file.close()
                self._log_file = None
