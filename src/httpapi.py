"""HTTP JSON 接口与最小角色鉴权。

令牌方案（Bearer）形如 ``<角色>[:主体]``：

* ``coordinator`` 演出统筹：全链路追溯、限制申报、受限重排、技审判定、执行登记；
* ``producer:<姓名>`` 制作人：提案、修订、授权、合同、确认、发布、换人；
* ``venue:<venue_id>`` 场地方：只能查看本场地完成职责所需的信息；
* ``channel:<渠道名>`` 渠道：只能查看/回执本渠道节目单。

重复请求可带 ``Idempotency-Key``；渠道回执、确认、执行等还内置派生幂等键，
因此重复回调绝不产生第二条事件。
"""
from __future__ import annotations

import json
import re
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import urlparse

from . import domain as D
from .app import AppService
from .eventstore import ConflictError, EventStore, IdempotentReplay

# 路由: (方法, 正则) -> (处理函数, 允许角色)
Route = tuple[str, re.Pattern[str], Callable[..., Any], frozenset[str]]


def _build_routes() -> list[Route]:
    r = lambda p: re.compile(p)
    P, C, V, Ch = "producer", "coordinator", "venue", "channel"
    return [
        ("POST", r(r"^/venues$"), "register_venue", frozenset({C})),
        ("GET", r(r"^/venues/(?P<venue_id>[\w-]+)/view$"), "venue_view",
         frozenset({C, V})),
        ("POST", r(r"^/restrictions$"), "declare_restriction", frozenset({C})),
        ("GET", r(r"^/restrictions/(?P<rid>[\w-]+)/affected$"), "affected",
         frozenset({C})),
        ("POST", r(r"^/restrictions/(?P<rid>[\w-]+)/replan$"), "replan",
         frozenset({C})),

        ("POST", r(r"^/programs$"), "propose_program", frozenset({P, C})),
        ("GET", r(r"^/programs/(?P<program_id>[\w-]+)$"), "get_program",
         frozenset({P, C})),
        ("POST", r(r"^/programs/(?P<program_id>[\w-]+)/revisions$"),
         "revise_program", frozenset({P, C})),
        ("POST", r(r"^/programs/(?P<program_id>[\w-]+)/slots$"),
         "request_slot", frozenset({P, C})),
        ("POST", r(r"^/programs/(?P<program_id>[\w-]+)/rights$"),
         "clear_rights", frozenset({P, C})),

        ("POST", r(r"^/slots/(?P<slot_id>[\w-]+)/confirm$"), "confirm_slot",
         frozenset({P, C})),
        ("POST", r(r"^/slots/(?P<slot_id>[\w-]+)/tech-reviews$"),
         "submit_tech", frozenset({P, C})),
        ("POST", r(r"^/slots/(?P<slot_id>[\w-]+)/tech-review/decision$"),
         "decide_tech", frozenset({C})),
        ("POST", r(r"^/slots/(?P<slot_id>[\w-]+)/contracts$"), "scope_contract",
         frozenset({P, C})),
        ("POST", r(r"^/slots/(?P<slot_id>[\w-]+)/replace-performer$"),
         "replace_performer", frozenset({P, C})),
        ("POST", r(r"^/slots/(?P<slot_id>[\w-]+)/execution$"), "record_execution",
         frozenset({C})),

        ("POST", r(r"^/rights/(?P<grant_id>[\w-]+)/narrow$"), "narrow_rights",
         frozenset({P, C})),

        ("POST", r(r"^/releases$"), "publish_release", frozenset({P, C})),
        ("GET", r(r"^/releases/(?P<release_id>[\w-]+)/trace$"), "release_trace",
         frozenset({C, Ch})),
        ("POST", r(r"^/releases/(?P<release_id>[\w-]+)/receipts$"), "ack_receipt",
         frozenset({Ch})),
        ("GET", r(r"^/channels/me$"), "channel_view", frozenset({Ch})),

        ("GET", r(r"^/overview$"), "overview", frozenset({C})),
        ("GET", r(r"^/events$"), "events", frozenset({C})),
        ("GET", r(r"^/health$"), "health", frozenset()),
    ]


ROUTES = _build_routes()


class Handler(BaseHTTPRequestHandler):
    server_version = "DualVenueLineage/1.0"

    # -- 鉴权 -------------------------------------------------------------

    def _principal(self) -> tuple[str, str] | None:
        """返回 (角色, 主体)。"""
        header = self.headers.get("Authorization", "")
        if not header.startswith("Bearer "):
            return None
        token = header[len("Bearer "):].strip()
        if token == "coordinator":
            return "coordinator", "coordinator"
        if ":" in token:
            role, subject = token.split(":", 1)
            if role in ("producer", "venue", "channel") and subject:
                return role, subject
        return None

    # -- 基础读写 ---------------------------------------------------------

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        try:
            value = json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise D.DomainError("请求体必须是 JSON 对象")
        if not isinstance(value, dict):
            raise D.DomainError("请求体必须是 JSON 对象")
        value.setdefault("idempotency_key", self.headers.get("Idempotency-Key"))
        return value

    def _send(self, status: int, body: Any, extra_headers: dict[str, str] | None = None) -> None:
        data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def _error(self, status: int, code: str, message: str) -> None:
        self._send(status, {"error": {"code": code, "message": message}})

    def log_message(self, fmt: str, *args: Any) -> None:  # 安静的测试输出
        if getattr(self.server, "access_log", False):
            super().log_message(fmt, *args)

    # -- 路由 -------------------------------------------------------------

    def do_GET(self) -> None:
        self._dispatch()

    def do_POST(self) -> None:
        self._dispatch()

    def _dispatch(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        principal = self._principal()
        for method, pattern, action, roles in ROUTES:
            if method != self.command:
                continue
            m = pattern.match(path)
            if not m:
                continue
            if roles and principal is None:
                self._error(HTTPStatus.UNAUTHORIZED, "unauthorized",
                            "缺少或无法识别的 Bearer 令牌")
                return
            if roles and principal[0] not in roles:
                self._error(HTTPStatus.FORBIDDEN, "forbidden",
                            f"角色 {principal[0]} 无权执行该操作")
                return
            handler = getattr(self, f"_action_{action}")
            try:
                handler(principal, m.groupdict())
            except IdempotentReplay as exc:
                body = {"idempotent_replay": True, **exc.result}
                self._send(HTTPStatus.OK, body, {"Idempotent-Replay": "true"})
            except D.NotFoundError as exc:
                self._error(HTTPStatus.NOT_FOUND, "not_found", str(exc))
            except (D.DomainError, ValueError) as exc:
                self._error(HTTPStatus.BAD_REQUEST, "domain_error", str(exc))
            except D.ConflictStateError as exc:
                self._error(HTTPStatus.CONFLICT, "conflict", str(exc))
            except ConflictError as exc:
                self._error(HTTPStatus.CONFLICT, "version_conflict", str(exc))
            return
        self._error(HTTPStatus.NOT_FOUND, "not_found", f"无此路由: {self.command} {path}")

    @property
    def app(self) -> AppService:
        return self.server.app  # type: ignore[attr-defined]

    # -- 动作 -------------------------------------------------------------

    def _body(self) -> dict[str, Any]:
        return self._read_json()

    def _action_health(self, principal, groups) -> None:
        self._send(HTTPStatus.OK, {"status": "ok"})

    def _action_register_venue(self, principal, groups) -> None:
        self._send(HTTPStatus.CREATED, self.app.register_venue(self._body()))

    def _action_declare_restriction(self, principal, groups) -> None:
        self._send(HTTPStatus.CREATED, self.app.declare_restriction(self._body()))

    def _action_affected(self, principal, groups) -> None:
        self._send(HTTPStatus.OK,
                   {"restriction_id": groups["rid"],
                    "affected_slots": self.app.affected_slots(groups["rid"])})

    def _action_replan(self, principal, groups) -> None:
        result = self.app.replan_for_restriction(groups["rid"], self._body())
        self._send(HTTPStatus.OK if result.get("change_id") is None
                   else HTTPStatus.CREATED, result)

    def _action_propose_program(self, principal, groups) -> None:
        self._send(HTTPStatus.CREATED,
                   self.app.propose_program(self._body(), principal[1]))

    def _action_get_program(self, principal, groups) -> None:
        prog = self.app.rm.programs.get(groups["program_id"])
        if not prog:
            raise D.NotFoundError(f"节目不存在: {groups['program_id']}")
        self._send(HTTPStatus.OK, prog)

    def _action_revise_program(self, principal, groups) -> None:
        self._send(HTTPStatus.CREATED,
                   self.app.revise_program(groups["program_id"], self._body()))

    def _action_request_slot(self, principal, groups) -> None:
        self._send(HTTPStatus.CREATED,
                   self.app.request_slot(groups["program_id"], self._body()))

    def _action_clear_rights(self, principal, groups) -> None:
        self._send(HTTPStatus.CREATED,
                   self.app.clear_rights(groups["program_id"], self._body()))

    def _action_confirm_slot(self, principal, groups) -> None:
        self._send(HTTPStatus.CREATED,
                   self.app.confirm_slot(
                       groups["slot_id"], principal[1],
                       self.headers.get("Idempotency-Key")))

    def _action_submit_tech(self, principal, groups) -> None:
        body = self._body()
        body["submitted_by"] = principal[1]
        self._send(HTTPStatus.CREATED,
                   self.app.submit_tech_review(groups["slot_id"], body))

    def _action_decide_tech(self, principal, groups) -> None:
        body = self._body()
        body["reviewed_by"] = principal[1]
        self._send(HTTPStatus.OK,
                   self.app.decide_tech_review(groups["slot_id"], body))

    def _action_scope_contract(self, principal, groups) -> None:
        self._send(HTTPStatus.CREATED,
                   self.app.scope_contract(groups["slot_id"], self._body()))

    def _action_replace_performer(self, principal, groups) -> None:
        self._send(HTTPStatus.CREATED,
                   self.app.replace_performer(groups["slot_id"], self._body()))

    def _action_record_execution(self, principal, groups) -> None:
        body = self._body()
        body["recorded_by"] = principal[1]
        self._send(HTTPStatus.CREATED,
                   self.app.record_execution(groups["slot_id"], body))

    def _action_narrow_rights(self, principal, groups) -> None:
        self._send(HTTPStatus.CREATED,
                   self.app.narrow_rights(groups["grant_id"], self._body()))

    def _action_publish_release(self, principal, groups) -> None:
        body = self._body()
        self._send(HTTPStatus.CREATED,
                   self.app.publish_release(body, principal[1]))

    def _action_release_trace(self, principal, groups) -> None:
        trace = self.app.release_trace(groups["release_id"])
        if principal[0] == "channel" and trace["channel"] != principal[1]:
            self._error(HTTPStatus.FORBIDDEN, "forbidden",
                        "渠道只能追溯本渠道节目单")
            return
        self._send(HTTPStatus.OK, trace)

    def _action_ack_receipt(self, principal, groups) -> None:
        self._send(HTTPStatus.CREATED,
                   self.app.ack_channel_receipt(
                       groups["release_id"], self._body(), principal[1]))

    def _action_channel_view(self, principal, groups) -> None:
        self._send(HTTPStatus.OK, self.app.channel_view(principal[1]))

    def _action_venue_view(self, principal, groups) -> None:
        if principal[0] == "venue" and principal[1] != groups["venue_id"]:
            self._error(HTTPStatus.FORBIDDEN, "forbidden",
                        "场地方只能查看本场地视图")
            return
        self._send(HTTPStatus.OK, self.app.venue_view(groups["venue_id"]))

    def _action_overview(self, principal, groups) -> None:
        self._send(HTTPStatus.OK, self.app.coordinator_overview())

    def _action_events(self, principal, groups) -> None:
        events = self.app.store.read_all()
        self._send(HTTPStatus.OK, {"count": len(events), "events": events})


def build_server(host: str, port: int, store_path: str,
                 access_log: bool = False) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), Handler)
    server.app = AppService(EventStore(store_path))  # type: ignore[attr-defined]
    server.access_log = access_log  # type: ignore[attr-defined]
    return server
