"""HTTP JSON 接口层。

- 角色：X-Role 取 coordinator（演出统筹）/ venue（场地方）/ public（公开节目单）/ gateway（支付与渠道回调）。
- 场地方另用 X-Venue-Id 限定只能看自己场地。
- 写接口接受 Idempotency-Key，重复请求回放首次结果。
- 所有错误使用统一信封 {"error": {...}} 并带 X-Request-Id，便于跨团队排障。
"""
from __future__ import annotations

import json
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .service import LineageService
from .store import DomainError, EventStore

COORDINATOR = "coordinator"
VENUE = "venue"
PUBLIC = "public"
GATEWAY = "gateway"


def create_server(service: LineageService, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    handler = _make_handler(service)
    server = ThreadingHTTPServer((host, port), handler)
    server.daemon_threads = True
    return server


def _make_handler(service: LineageService) -> type[BaseHTTPRequestHandler]:
    class ApiHandler(BaseHTTPRequestHandler):
        server_version = "LineageHTTP/1.0"

        def log_message(self, fmt: str, *args: Any) -> None:  # 静默默认访问日志
            return

        # ---------- 基础收发 ----------
        def _send_json(self, status: int, body: Any, extra_headers: dict[str, str] | None = None) -> None:
            raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            for k, v in (extra_headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(raw)

        def _request_id(self) -> str:
            return self.headers.get("X-Request-Id") or uuid.uuid4().hex[:12]

        def _read_json(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0:
                return {}
            try:
                value = json.loads(self.rfile.read(length).decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise DomainError("BAD_JSON", f"请求体不是有效 JSON：{exc}", 400) from exc
            if not isinstance(value, dict):
                raise DomainError("BAD_REQUEST", "请求体必须是 JSON 对象", 400)
            return value

        def _idem(self) -> str | None:
            return self.headers.get("Idempotency-Key") or None

        def _require_role(self, *roles: str) -> str:
            role = self.headers.get("X-Role", PUBLIC)
            if role not in roles:
                raise DomainError("FORBIDDEN", f"该接口需要角色：{'/'.join(roles)}（当前 {role}）", 403)
            return role

        def _handle(self) -> None:
            request_id = self._request_id()
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query)
            try:
                body = self._read_json() if self.command == "POST" else {}
                status, payload = self._route(self.command, path, query, body)
                self._send_json(status, payload, {"X-Request-Id": request_id})
            except DomainError as exc:
                self._send_json(
                    exc.status,
                    {"error": {"code": exc.code, "message": str(exc), "request_id": request_id}},
                    {"X-Request-Id": request_id},
                )
            except Exception as exc:  # noqa: BLE001 - 不让连接裸崩，统一信封
                self._send_json(
                    500,
                    {"error": {"code": "INTERNAL", "message": str(exc), "request_id": request_id}},
                    {"X-Request-Id": request_id},
                )

        def do_GET(self) -> None:  # noqa: N802
            self._handle()

        def do_POST(self) -> None:  # noqa: N802
            self._handle()

        # ---------- 路由 ----------
        def _route(
            self, method: str, path: str, query: dict[str, list[str]], body: dict[str, Any]
        ) -> tuple[int, Any]:
            p = [x for x in path.split("/") if x]
            svc = service
            idem = self._idem()

            if method == "GET" and path == "/health":
                return 200, {"status": "ok"}

            if method == "GET" and path == "/events":
                self._require_role(COORDINATOR)
                stream = (query.get("stream") or [None])[0]
                events = svc.store.events(stream) if stream else svc.store.all_events()
                return 200, {"stream": stream, "count": len(events), "events": events}

            if method == "POST" and path == "/admin/venues":
                self._require_role(COORDINATOR)
                return 201, svc.register_venue(body, idem)

            if method == "GET" and path == "/public/programs":
                self._require_role(PUBLIC, COORDINATOR, VENUE, GATEWAY)
                return 200, svc.public_listing()

            if method == "POST" and path == "/programs":
                self._require_role(COORDINATOR)
                return 201, svc.propose_program(body, idem)

            if len(p) == 2 and p[0] == "programs":
                program_id = p[1]
                if method == "GET":
                    self._require_role(COORDINATOR)
                    return 200, svc.coordinator_program(program_id)

            if len(p) == 3 and p[0] == "programs" and p[2] == "revisions":
                self._require_role(COORDINATOR)
                return 201, svc.revise_program(p[1], body, idem)

            if len(p) == 4 and p[0] == "programs" and p[2] == "performers" and p[3] == "replace":
                self._require_role(COORDINATOR)
                return 201, svc.replace_performer(p[1], body, idem)

            if len(p) == 3 and p[0] == "programs" and p[2] == "contracts":
                self._require_role(COORDINATOR)
                return 201, svc.scope_contract(p[1], body, idem)

            if len(p) == 3 and p[0] == "programs" and p[2] == "tech-reviews":
                self._require_role(COORDINATOR)
                return 201, svc.submit_tech_review(p[1], body, idem)

            if len(p) == 3 and p[0] == "programs" and p[2] == "rights":
                self._require_role(COORDINATOR)
                return 201, svc.clear_rights(p[1], body, idem)

            if len(p) == 5 and p[0] == "programs" and p[2] == "rights" and p[4] == "narrow":
                self._require_role(COORDINATOR)
                return 201, svc.narrow_rights(p[1], p[3], body, idem)

            if method == "POST" and path == "/slots/holds":
                self._require_role(COORDINATOR)
                return 201, svc.hold_slot(body, idem)

            if len(p) == 3 and p[0] == "slots" and p[2] == "confirm":
                self._require_role(COORDINATOR)
                return 200, svc.confirm_slot(p[1], body, idem)

            if len(p) == 3 and p[0] == "slots" and p[2] == "reschedule":
                self._require_role(COORDINATOR)
                return 200, svc.reschedule_slot(p[1], body, idem)

            if len(p) == 3 and p[0] == "slots" and p[2] == "release":
                self._require_role(COORDINATOR)
                return 200, svc.release_slot(p[1], body, idem)

            if len(p) == 3 and p[0] == "slots" and p[2] == "settle":
                self._require_role(COORDINATOR, GATEWAY)
                return 200, svc.settle_slot(p[1], body, idem)

            if method == "POST" and path == "/releases":
                self._require_role(COORDINATOR)
                return 201, svc.publish_release(body, idem)

            if len(p) >= 2 and p[0] == "releases":
                # release_id 为不透明标识（release-<program>-<channel>），取 /releases/ 后的完整剩余路径。
                release_id = "/".join(p[1:])
                if len(p) >= 2 and p[-1] == "receipts":
                    rid = "/".join(p[1:-1])
                    self._require_role(COORDINATOR, GATEWAY)
                    return 200, svc.record_release_receipt(rid, body, idem)
                if len(p) >= 2 and p[-1] == "retract":
                    rid = "/".join(p[1:-1])
                    self._require_role(COORDINATOR)
                    return 200, svc.retract_release(rid, body, idem)
                if method == "GET":
                    self._require_role(COORDINATOR)
                    return 200, svc.release_trace(release_id)

            if len(p) == 3 and p[0] == "venues" and p[2] == "schedule":
                role = self._require_role(VENUE, COORDINATOR)
                if role == VENUE and self.headers.get("X-Venue-Id") != p[1]:
                    raise DomainError("FORBIDDEN", "场地方只能查看本场地日程", 403)
                return 200, svc.venue_schedule(p[1])

            raise DomainError("NOT_FOUND", f"无此接口：{method} {path}", 404)

    return ApiHandler


def build_service(log_path: str | None = None) -> tuple[EventStore, LineageService]:
    store = EventStore(log_path)
    service = LineageService(store)
    return store, service


def seed_venues(service: LineageService) -> None:
    """登记双场域（只在缺失时登记，重放后不会重复）。"""
    venues = [
        {
            "venue_id": "yuyuan-water-stage",
            "name": "豫园水上舞台",
            "stage_type": "waterfront",
            "open_air": True,
            "business_hours": {"open": "10:00", "close": "22:00"},
        },
        {
            "venue_id": "bund-terrace",
            "name": "外滩露台",
            "stage_type": "terrace",
            "open_air": True,
            "business_hours": {"open": "10:00", "close": "22:00"},
        },
    ]
    for venue in venues:
        if venue["venue_id"] not in service.venue_ids:
            service.register_venue(venue, idem_key=f"seed-venue:{venue['venue_id']}")
