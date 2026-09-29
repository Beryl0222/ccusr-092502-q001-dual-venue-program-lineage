"""仅追加（append-only）的事件存储。

每个事件属于一个 *stream*（聚合标识），并在该流上拥有从 1 开始的单调版本号。
事件以 JSONL 落盘，重启后通过重放重建状态；同一条 HTTP 命令携带的
``Idempotency-Key`` 记录在侧车文件中，保证重复请求/重复回调返回同一结果、
不会二次追加事件（因此不会二次预留场地或重复计费）。

一条命令可在同一临界区内原子提交多个流上的事件（例如确认场次同时生成计费、
露天限制同时重排多个受影响场次）：任一期望版本不匹配则整批不写入。
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Callable, Iterable, NamedTuple


class ConflictError(Exception):
    """期望版本与流当前版本不一致（乐观并发失败）。"""


_MISSING = object()


class CommitItem(NamedTuple):
    stream_id: str
    expected_version: int
    event: dict[str, Any]


class IdempotentReplay(Exception):
    """命中幂等键；:attr result 为首次成功调用缓存的 JSON 结果。"""

    def __init__(self, result: Any) -> None:
        super().__init__("幂等重放")
        self.result = result


class EventStore:
    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._listeners: list[Callable[[dict[str, Any]], None]] = []
        self._streams: dict[str, list[dict[str, Any]]] = {}
        self._global_index: list[dict[str, Any]] = []
        # 幂等键 -> 缓存的命令结果
        self._idem: dict[str, Any] = {}
        self._replay()

    # ---- 重放 -----------------------------------------------------------

    def subscribe(self, listener: Callable[[dict[str, Any]], None]) -> None:
        """注册投影监听器；注册时立即用历史事件重放一次。"""
        with self._lock:
            self._listeners.append(listener)
            for event in self._global_index:
                listener(event)

    def _replay(self) -> None:
        if self.path.exists():
            for line in self.path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    self._index(json.loads(line))
        idem_path = self._idem_path()
        if idem_path.exists():
            for line in idem_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    row = json.loads(line)
                    self._idem[row["key"]] = row["result"]

    def _idem_path(self) -> Path:
        return self.path.with_suffix(".idem.jsonl")

    def _index(self, event: dict[str, Any]) -> None:
        self._streams.setdefault(event["stream_id"], []).append(event)
        self._global_index.append(event)

    # ---- 读取 -----------------------------------------------------------

    def stream_version(self, stream_id: str) -> int:
        with self._lock:
            return len(self._streams.get(stream_id, ()))

    def read_stream(self, stream_id: str) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._streams.get(stream_id, ()))

    def read_all(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._global_index)

    def idempotent_result(self, idem_key: str | None) -> Any:
        """返回幂等键的首次结果；键为空或未登记时返回哨兵 ``_MISSING``。"""
        if not idem_key:
            return _MISSING
        with self._lock:
            return self._idem.get(idem_key, _MISSING)

    def raise_if_replayed(self, idem_key: str | None) -> None:
        """状态校验前调用：命中幂等键则直接抛 :class:`IdempotentReplay`。"""
        result = self.idempotent_result(idem_key)
        if result is not _MISSING:
            raise IdempotentReplay(result)

    def replay_into(self, listener: Callable[[dict[str, Any]], None]) -> None:
        with self._lock:
            for event in self._global_index:
                listener(event)

    # ---- 提交 -----------------------------------------------------------

    def commit(
        self,
        items: Iterable[CommitItem | tuple[str, int, dict[str, Any]]],
        *,
        idem_key: str | None = None,
        result: Any = None,
    ) -> list[dict[str, Any]]:
        """原子提交一批事件。

        先在锁内检查幂等键与全部期望版本，再统一落盘与分发投影。
        命中幂等键时抛出 :class:`IdempotentReplay`（携带首次结果），
        保证重复回调不会产生第二条事件。
        """
        normalized = [item if isinstance(item, CommitItem) else CommitItem(*item)
                      for item in items]
        with self._lock:
            if idem_key and idem_key in self._idem:
                raise IdempotentReplay(self._idem[idem_key])
            for item in normalized:
                current = len(self._streams.get(item.stream_id, ()))
                if current != item.expected_version:
                    raise ConflictError(
                        f"流 {item.stream_id} 版本冲突: 期望 {item.expected_version}, 实际 {current}"
                    )
            stored_events: list[dict[str, Any]] = []
            with self.path.open("a", encoding="utf-8") as fh:
                for item in normalized:
                    stored = dict(item.event)
                    stored["stream_id"] = item.stream_id
                    stored["version"] = len(self._streams.get(item.stream_id, ())) + 1
                    fh.write(json.dumps(stored, ensure_ascii=False, sort_keys=True) + "\n")
                    self._index(stored)
                    stored_events.append(stored)
            if idem_key:
                row = {"key": idem_key, "result": result}
                with self._idem_path().open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
                self._idem[idem_key] = result
            for stored in stored_events:
                for listener in list(self._listeners):
                    listener(stored)
            return stored_events
