"""领域服务：节目提案沿同一版本链驱动合同、技术、版权、场地、发布与结算。

核心不变量：
- 事实只追加：改期、替换表演者、收窄授权都产生新版本事件，绝不原地改写。
- 出场条件门：合同范围、技术审查、版权授权必须同时覆盖目标版本与场地/渠道。
- 临时露天限制只重排受影响场次（按单场次操作，返回受影响清单，不波及其他场次）。
- 并发确认同一场次由事件存储的乐观锁保证唯一生效；回调按 callback_id 幂等，
  不会二次预留或重复计费。
- 已发布内容不可静默改写：同一版本内容变更必须先开新版本，发布形成新版本事件。
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from .store import DomainError, EventStore

GENRES = ("opera", "electronic")

# 戏曲与电子音乐的排练与场地能力要求不同。
REHEARSAL_RULES: dict[str, dict[str, Any]] = {
    "opera": {
        "label": "戏曲",
        "rehearsal_hours": 6,
        "capabilities": ["遮雨", "独立化妆间", "固定扩声"],
    },
    "electronic": {
        "label": "电子音乐",
        "rehearsal_hours": 3,
        "capabilities": ["电力冗余", "低音承载", "防雨设备"],
    },
}


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _parse_ts(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise DomainError("BAD_TIME", f"时间格式无效：{value}", 400) from exc
    if parsed.tzinfo is None:
        raise DomainError("BAD_TIME", "时间必须包含时区", 400)
    return parsed


class LineageService:
    def __init__(self, store: EventStore) -> None:
        self.store = store
        self.venue_ids: set[str] = set()
        self.slot_ids: set[str] = set()
        self.billing_ids: set[str] = set()
        store.subscribe(self._index)

    # ================= 事件构造 =================
    def _event(
        self,
        event_type: str,
        aggregate_id: str,
        payload: dict[str, Any],
        causation_id: str | None = None,
        correlation_id: str | None = None,
    ) -> dict[str, Any]:
        evt: dict[str, Any] = {
            "event_id": _new_id("evt"),
            "event_type": event_type,
            "occurred_at": _now(),
            "aggregate_id": aggregate_id,
            "payload": payload,
        }
        if causation_id:
            evt["causation_id"] = causation_id
        if correlation_id:
            evt["correlation_id"] = correlation_id
        return evt

    def _index(self, event: dict[str, Any]) -> None:
        etype = event["event_type"]
        aid = event["aggregate_id"]
        if etype == "VENUE_REGISTERED":
            self.venue_ids.add(aid)
        elif etype in ("SLOT_HELD", "SLOT_CONFIRMED"):
            self.slot_ids.add(aid)
        elif etype == "BILLING_SETTLED":
            self.billing_ids.add(aid)

    # ================= 折叠读模型 =================
    def _venue(self, venue_id: str) -> dict[str, Any]:
        state: dict[str, Any] | None = None
        for e in self.store.events("venues"):
            if e["event_type"] == "VENUE_REGISTERED" and e["aggregate_id"] == venue_id:
                state = {"venue_id": venue_id, **e["payload"]}
        if state is None:
            raise DomainError("VENUE_NOT_FOUND", f"场地不存在：{venue_id}", 404)
        return state

    def _program(self, program_id: str) -> dict[str, Any]:
        stream = f"program-{program_id}"
        state: dict[str, Any] | None = None
        for e in self.store.events(stream):
            p = e["payload"]
            etype = e["event_type"]
            if etype == "PROGRAM_PROPOSED":
                state = {
                    "program_id": program_id,
                    "title": p["title"],
                    "fee": p["fee"],
                    "lineup": [dict(x) for x in p["lineup"]],
                    "current_revision": 1,
                    "status": "proposed",
                    "revisions": {
                        1: {
                            "revision": 1,
                            "title": p["title"],
                            "fee": p["fee"],
                            "lineup": [dict(x) for x in p["lineup"]],
                            "change": "proposed",
                            "supersedes": None,
                            "event_id": e["event_id"],
                        }
                    },
                    "lineage": [
                        {"revision": 1, "event_id": e["event_id"], "change": "proposed", "supersedes": None}
                    ],
                    "contracts": {},
                    "tech": {},
                    "rights": {},
                }
            elif state is None:
                continue
            elif etype == "PROGRAM_REVISED":
                rev = e["version"]
                snapshot = {
                    "revision": p["revision"],
                    "title": p.get("title", state["title"]),
                    "fee": p.get("fee", state["fee"]),
                    "lineup": [dict(x) for x in p["lineup"]],
                    "change": p["change"],
                    "supersedes": p["supersedes"],
                    "event_id": e["event_id"],
                    "reason": p.get("reason"),
                }
                state["title"] = snapshot["title"]
                state["fee"] = snapshot["fee"]
                state["lineup"] = [dict(x) for x in p["lineup"]]
                state["current_revision"] = p["revision"]
                state["revisions"][p["revision"]] = snapshot
                state["lineage"].append(
                    {
                        "revision": p["revision"],
                        "event_id": e["event_id"],
                        "change": p["change"],
                        "supersedes": p["supersedes"],
                        "reason": p.get("reason"),
                    }
                )
            elif etype == "CONTRACT_SCOPED":
                state["contracts"][p["contract_id"]] = dict(p, event_id=e["event_id"])
            elif etype == "TECH_REVIEWED":
                state["tech"][p["revision"]] = dict(p, event_id=e["event_id"])
            elif etype == "RIGHTS_CLEARED":
                state["rights"][p["rights_id"]] = {
                    "rights_id": p["rights_id"],
                    "work_id": p["work_id"],
                    "revision": p["revision"],
                    "all_revisions": bool(p.get("all_revisions", True)),
                    "scope": dict(p["scope"]),
                    "status": "active",
                    "supersedes": None,
                    "event_id": e["event_id"],
                }
            elif etype == "RIGHTS_NARROWED":
                if p["supersedes"] in state["rights"]:
                    state["rights"][p["supersedes"]]["status"] = "narrowed"
                state["rights"][p["rights_id"]] = {
                    "rights_id": p["rights_id"],
                    "work_id": p["work_id"],
                    "revision": p["revision"],
                    "all_revisions": bool(p.get("all_revisions", False)),
                    "scope": dict(p["scope"]),
                    "status": "active",
                    "supersedes": p["supersedes"],
                    "reason": p.get("reason"),
                    "event_id": e["event_id"],
                }
            elif etype == "PROGRAM_CANCELLED":
                state["status"] = "cancelled"
        if state is None:
            raise DomainError("PROGRAM_NOT_FOUND", f"节目不存在：{program_id}", 404)
        return state

    def _slot(self, slot_id: str) -> dict[str, Any]:
        state: dict[str, Any] | None = None
        for e in self.store.events(f"slot-{slot_id}"):
            p = e["payload"]
            etype = e["event_type"]
            if etype == "SLOT_HELD":
                if state is None:
                    state = {
                        "slot_id": slot_id,
                        "venue_id": p["venue_id"],
                        "starts_at": p["starts_at"],
                        "ends_at": p["ends_at"],
                        "status": "provisional",
                        "program_id": p["program_id"],
                        "revision": p["revision"],
                        "holds": [],
                        "history": [],
                    }
                state["holds"].append(
                    {"program_id": p["program_id"], "revision": p["revision"], "event_id": e["event_id"]}
                )
            elif state is None:
                continue
            elif etype == "SLOT_CONFIRMED":
                state["status"] = "confirmed"
                state["program_id"] = p["program_id"]
                state["revision"] = p["revision"]
                state["holds"] = []
                state["history"].append({"kind": "confirmed", "event_id": e["event_id"], **p})
            elif etype == "SLOT_RESCHEDULED":
                state["starts_at"] = p["starts_at"]
                state["ends_at"] = p["ends_at"]
                state["revision"] = p["revision"]
                state["history"].append({"kind": "rescheduled", "event_id": e["event_id"], **p})
            elif etype == "SLOT_RELEASED":
                state["status"] = "released"
                state["program_id"] = None
                state["revision"] = None
                state["holds"] = []
                state["history"].append({"kind": "released", "event_id": e["event_id"], **p})
        if state is None:
            raise DomainError("SLOT_NOT_FOUND", f"场次不存在：{slot_id}", 404)
        return state

    def _release(self, release_id: str) -> dict[str, Any]:
        state: dict[str, Any] | None = None
        for e in self.store.events(release_id):
            p = e["payload"]
            etype = e["event_type"]
            if etype == "RELEASE_PUBLISHED":
                if state is None:
                    state = {
                        "release_id": release_id,
                        "program_id": p["program_id"],
                        "channel": p["channel"],
                        "status": "published",
                        "receipts": {},
                        "publications": [],
                    }
                state["status"] = "published"
                state["revision"] = p["revision"]
                state["content"] = dict(p["content"])
                state["publications"].append(dict(p, event_id=e["event_id"]))
            elif state is None:
                continue
            elif etype == "RELEASE_RECEIPTED":
                state["receipts"][p["channel"]] = dict(p, event_id=e["event_id"])
            elif etype == "RELEASE_RETRACTED":
                state["status"] = "retracted"
                state["retraction"] = dict(p, event_id=e["event_id"])
        if state is None:
            raise DomainError("RELEASE_NOT_FOUND", f"发布不存在：{release_id}", 404)
        return state

    def _billing(self, billing_id: str) -> dict[str, Any]:
        state: dict[str, Any] | None = None
        for e in self.store.events(billing_id):
            p = e["payload"]
            if e["event_type"] == "BILLING_SETTLED":
                state = {
                    "billing_id": billing_id,
                    "slot_id": p["slot_id"],
                    "venue_id": p["venue_id"],
                    "program_id": p["program_id"],
                    "amount": p["amount"],
                    "currency": p["currency"],
                    "status": "settled",
                    "payments": [],
                    "settle_event_id": e["event_id"],
                }
                state["payments"].append(dict(p, event_id=e["event_id"]))
            elif state is not None and e["event_type"] == "BILLING_REFUNDED":
                state["status"] = "refunded"
                state["refund"] = dict(p, event_id=e["event_id"])
        if state is None:
            raise DomainError("BILLING_NOT_FOUND", f"结算不存在：{billing_id}", 404)
        return state

    # ================= 规则 =================
    def _genres(self, lineup: list[dict[str, Any]]) -> list[str]:
        return sorted({entry["genre"] for entry in lineup if entry["genre"] in GENRES})

    def _requirements(self, lineup: list[dict[str, Any]]) -> dict[str, Any]:
        genres = self._genres(lineup)  # type: ignore[arg-type]
        caps: set[str] = set()
        hours = 0
        for genre in genres:
            rule = REHEARSAL_RULES[genre]
            caps.update(rule["capabilities"])
            hours = max(hours, rule["rehearsal_hours"])
        return {"genres": genres, "rehearsal_hours": hours, "capabilities": sorted(caps)}

    def _evaluate_tech(self, program: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
        reqs = self._requirements(program["revisions"][payload["revision"]]["lineup"])
        provided = set(payload.get("capabilities", ()))
        missing = [c for c in reqs["capabilities"] if c not in provided]
        reasons: list[str] = []
        if payload.get("rehearsal_hours", 0) < reqs["rehearsal_hours"]:
            reasons.append(
                f"排练时长不足：需要 {reqs['rehearsal_hours']} 小时，"
                f"申报 {payload.get('rehearsal_hours', 0)} 小时"
            )
        reasons.extend(f"缺少能力：{c}" for c in missing)
        return {
            "revision": payload["revision"],
            "reviewer": payload["reviewer"],
            "venue_id": payload.get("venue_id"),
            "open_air": bool(payload.get("open_air", False)),
            "rehearsal_hours": payload.get("rehearsal_hours", 0),
            "capabilities": sorted(provided),
            "required": reqs,
            "status": "approved" if not reasons else "rejected",
            "reasons": reasons,
            "note": payload.get("note"),
            "reviewed_at": _now(),
        }

    def _not_ready(self, blockers: list[str], where: str) -> None:
        raise DomainError("NOT_READY", f"{where}：" + "；".join(blockers), 422)

    def _blockers(self, program: dict[str, Any], revision: int, venue_id: str, channel: str | None = None) -> list[str]:
        blockers: list[str] = []
        rev_snapshot = program["revisions"].get(revision)
        if rev_snapshot is None:
            return [f"版本 {revision} 不存在"]
        cast_ids = {x["performer_id"] for x in rev_snapshot["lineup"]}
        # 合同：按表演者集合与场地覆盖；换人后新表演者不在合同内即阻断（与具体版本号无关）。
        contract_ok = any(
            c["status"] == "active"
            and venue_id in c["venues"]
            and (c["performer_ids"] == ["*"] or cast_ids.issubset(set(c["performer_ids"])))
            for c in program["contracts"].values()
        )
        if not contract_ok:
            blockers.append("合同范围未覆盖该版本与场地（或表演者不全）")
        # 技术：某次审查通过时的演出要求（剧种/排练时长/能力）与目标版本一致即沿用；
        # 同阵容只改节目备注不需重审，换剧种/换人导致要求变化则需重审。
        target_reqs = self._requirements(rev_snapshot["lineup"])
        tech_ok = any(
            t["status"] == "approved" and t.get("required") == target_reqs
            for t in program["tech"].values()
        )
        if not tech_ok:
            blockers.append("技术审查未通过该版本")
        # 版权：active 授权在其签发版本之后沿用（节目备注类变更不破坏授权），
        # 收窄后旧授权失效，新版本需新授权。
        active_rights = [
            r for r in program["rights"].values()
            if r["status"] == "active" and revision >= r["revision"]
            and (r.get("all_revisions", True) or r["revision"] == revision)
        ]
        if not active_rights:
            blockers.append("版权授权未覆盖该版本")
        elif not any(venue_id in r["scope"].get("venues", []) for r in active_rights):
            blockers.append("版权授权未覆盖该场地")
        if channel is not None and not any(channel in r["scope"].get("channels", []) for r in active_rights):
            blockers.append("版权授权未覆盖该宣传渠道")
        return blockers

    def _within_business_hours(self, venue: dict[str, Any], starts_at: str, ends_at: str) -> bool:
        start = _parse_ts(starts_at)
        end = _parse_ts(ends_at)
        if end <= start:
            raise DomainError("BAD_TIME", "结束时间必须晚于开始时间", 400)
        hours = venue.get("business_hours") or {}
        open_t, close_t = hours.get("open", "00:00"), hours.get("close", "23:59")
        return open_t <= start.strftime("%H:%M") and end.strftime("%H:%M") <= close_t

    def _overlaps_confirmed(self, venue_id: str, starts_at: str, ends_at: str, exclude_slot: str | None = None) -> dict[str, Any] | None:
        new_start, new_end = _parse_ts(starts_at), _parse_ts(ends_at)
        for sid in self.slot_ids:
            if sid == exclude_slot:
                continue
            slot = self._slot(sid)
            if slot["venue_id"] != venue_id or slot["status"] != "confirmed":
                continue
            if new_start < _parse_ts(slot["ends_at"]) and _parse_ts(slot["starts_at"]) < new_end:
                return slot
        return None

    def _program_slots(self, program_id: str) -> list[dict[str, Any]]:
        result = []
        for sid in self.slot_ids:
            slot = self._slot(sid)
            if slot.get("program_id") == program_id or any(h["program_id"] == program_id for h in slot["holds"]):
                result.append(slot)
        return result

    def _run_idem(
        self, scope: str, key: str | None, build: Any
    ) -> tuple[dict[str, Any], list[tuple[str, int, dict[str, Any]]]]:
        if key:
            remembered = self.store.remembered_response(scope, key)
            if remembered is not None:
                return remembered, []
        try:
            response, entries = build()
            self.store.commit(entries, remember=(scope, key, response) if key else None)
        except DomainError as exc:
            # 同幂等键并发：败者回放胜者已落盘的回执，语义等同重试。
            if key and exc.code == "IDEMPOTENCY_RACE":
                remembered = self.store.remembered_response(scope, key)
                if remembered is not None:
                    return remembered, []
            raise
        return response, [e for _s, _v, e in entries]

    # ================= 命令：场地与节目 =================
    def register_venue(self, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        if idem_key:
            remembered = self.store.remembered_response("venue:register", idem_key)
            if remembered is not None:
                return remembered
        for field in ("venue_id", "name", "stage_type"):
            if not data.get(field):
                raise DomainError("BAD_REQUEST", f"缺少字段：{field}", 400)
        if data["venue_id"] in self.venue_ids:
            raise DomainError("VENUE_EXISTS", "场地已登记", 409)

        def build() -> Any:
            event = self._event(
                "VENUE_REGISTERED",
                data["venue_id"],
                {
                    "name": data["name"],
                    "stage_type": data["stage_type"],
                    "open_air": bool(data.get("open_air", False)),
                    "business_hours": data.get("business_hours", {"open": "00:00", "close": "23:59"}),
                },
            )
            resp = {"venue_id": data["venue_id"], "event_id": event["event_id"], "version": 1}
            return resp, [("venues", self.store.version("venues"), event)]

        resp, _ = self._run_idem("venue:register", idem_key, build)
        return resp

    def propose_program(self, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        if not data.get("program_id"):
            raise DomainError("BAD_REQUEST", "缺少字段：program_id", 400)
        if not data.get("title"):
            raise DomainError("BAD_REQUEST", "缺少字段：title", 400)
        lineup = data.get("lineup") or []
        if not lineup:
            raise DomainError("BAD_REQUEST", "lineup 不能为空", 400)
        for entry in lineup:
            for field in ("entry_id", "performer_id", "performer_name", "genre", "act_title"):
                if not entry.get(field):
                    raise DomainError("BAD_REQUEST", f"lineup 条目缺少字段：{field}", 400)
            if entry["genre"] not in GENRES:
                raise DomainError("BAD_REQUEST", f"未知剧种：{entry['genre']}", 400)
        fee = data.get("fee", 0)
        if self.store.exists(f"program-{data['program_id']}"):
            raise DomainError("PROGRAM_EXISTS", "节目已存在", 409)

        def build() -> Any:
            event = self._event(
                "PROGRAM_PROPOSED",
                data["program_id"],
                {"title": data["title"], "fee": fee, "lineup": lineup, "proposed_by": data.get("proposed_by")},
            )
            resp = {
                "program_id": data["program_id"],
                "revision": 1,
                "event_id": event["event_id"],
                "version": 1,
            }
            return resp, [(f"program-{data['program_id']}", 0, event)]

        resp, _ = self._run_idem("program:propose", idem_key, build)
        return resp

    def _bump_revision(
        self, program_id: str, change: str, reason: str | None, lineup: list[dict[str, Any]],
        title: str | None = None, fee: float | None = None,
    ) -> tuple[dict[str, Any], list[tuple[str, int, dict[str, Any]]]]:
        """构造节目新版本条目；expected_version 取事件流实际长度（流版本 != 业务版本号）。"""
        program = self._program(program_id)
        stream = f"program-{program_id}"
        stream_version = self.store.version(stream)
        new_rev = program["current_revision"] + 1
        old_rev = program["current_revision"]
        old_event = program["revisions"][old_rev]["event_id"]
        event = self._event(
            "PROGRAM_REVISED",
            program_id,
            {
                "revision": new_rev,
                "supersedes": old_rev,
                "change": change,
                "reason": reason,
                "title": title if title is not None else program["title"],
                "fee": fee if fee is not None else program["fee"],
                "lineup": lineup,
            },
            causation_id=old_event,
            correlation_id=f"program-{program_id}",
        )
        resp = {
            "program_id": program_id,
            "revision": new_rev,
            "supersedes": old_rev,
            "change": change,
            "event_id": event["event_id"],
        }
        return resp, [(stream, stream_version, event)]

    def revise_program(self, program_id: str, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        program = self._program(program_id)
        lineup = data.get("lineup")
        if lineup is not None:
            for entry in lineup:
                if entry.get("genre") not in GENRES:
                    raise DomainError("BAD_REQUEST", f"未知剧种：{entry.get('genre')}", 400)
        lineup = lineup or program["lineup"]

        def build() -> Any:
            resp, entries = self._bump_revision(
                program_id, data.get("change", "lineup"), data.get("reason"),
                lineup, data.get("title"), data.get("fee"),
            )
            resp["affected_slots"] = self._affected_after_revision(program_id, entries)
            return resp, entries

        resp, _ = self._run_idem(f"program:revise:{program_id}", idem_key, build)
        return resp

    def replace_performer(self, program_id: str, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        old_id = data.get("performer_id")
        replacement = data.get("replacement") or {}
        if not old_id or not replacement.get("performer_id") or not replacement.get("performer_name"):
            raise DomainError("BAD_REQUEST", "需要 performer_id 与 replacement", 400)
        program = self._program(program_id)
        if not any(e["performer_id"] == old_id for e in program["lineup"]):
            raise DomainError("PERFORMER_NOT_FOUND", "原表演者不在节目中", 404)
        new_lineup = []
        for entry in program["lineup"]:
            entry = dict(entry)
            if entry["performer_id"] == old_id:
                entry["performer_id"] = replacement["performer_id"]
                entry["performer_name"] = replacement["performer_name"]
                if replacement.get("genre"):
                    entry["genre"] = replacement["genre"]
                if replacement.get("act_title"):
                    entry["act_title"] = replacement["act_title"]
            new_lineup.append(entry)

        def build() -> Any:
            resp, entries = self._bump_revision(
                program_id, "performer_replaced",
                data.get("reason", f"替换表演者 {old_id} -> {replacement['performer_id']}"),
                new_lineup,
            )
            resp["affected_slots"] = self._affected_after_revision(program_id, entries)
            return resp, entries

        resp, _ = self._run_idem(f"program:replace:{program_id}:{old_id}", idem_key, build)
        return resp

    def _affected_after_revision(
        self, program_id: str, entries: list[tuple[str, int, dict[str, Any]]]
    ) -> list[dict[str, Any]]:
        """在尚未提交的条目上模拟提交后投影，计算会失去出场条件的已确认场次。"""
        program = self._program(program_id)
        payload = entries[0][2]["payload"]
        snapshot = {
            "revision": payload["revision"],
            "title": payload["title"],
            "fee": payload["fee"],
            "lineup": payload["lineup"],
            "change": payload["change"],
            "supersedes": payload["supersedes"],
            "event_id": entries[0][2]["event_id"],
        }
        sim = self._simulate_revision(program, snapshot)
        # 若同批还有 RIGHTS_NARROWED（收窄授权），把旧授权置窄、挂上新授权。
        for _stream, _ver, event in entries[1:]:
            if event["event_type"] == "RIGHTS_NARROWED":
                rp = event["payload"]
                if rp["supersedes"] in sim["rights"]:
                    sim["rights"][rp["supersedes"]]["status"] = "narrowed"
                sim["rights"][rp["rights_id"]] = {
                    "rights_id": rp["rights_id"],
                    "work_id": rp["work_id"],
                    "revision": rp["revision"],
                    "scope": dict(rp["scope"]),
                    "status": "active",
                    "supersedes": rp["supersedes"],
                    "event_id": event["event_id"],
                }
        return self._affected_slots(sim, snapshot["revision"])

    def _affected_slots(self, program: dict[str, Any], revision: int) -> list[dict[str, Any]]:
        """已确认但在新版本下不再满足出场条件的场次（露天限制只影响这些场次）。"""
        out = []
        for slot in self._program_slots(program["program_id"]):
            if slot["status"] != "confirmed":
                continue
            blockers = self._blockers(program, revision, slot["venue_id"])
            if blockers:
                out.append(
                    {
                        "slot_id": slot["slot_id"],
                        "venue_id": slot["venue_id"],
                        "starts_at": slot["starts_at"],
                        "ends_at": slot["ends_at"],
                        "blockers": blockers,
                    }
                )
        return out

    @staticmethod
    def _simulate_revision(program: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
        import copy

        sim = copy.deepcopy(program)
        rev = snapshot["revision"]
        sim["current_revision"] = rev
        sim["title"] = snapshot["title"]
        sim["fee"] = snapshot["fee"]
        sim["lineup"] = [dict(x) for x in snapshot["lineup"]]
        sim["revisions"][rev] = copy.deepcopy(snapshot)
        return sim

    # ================= 命令：合同 / 技术 / 版权 =================
    def scope_contract(self, program_id: str, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        program = self._program(program_id)
        cid = data.get("contract_id") or _new_id("contract")
        if cid in program["contracts"]:
            raise DomainError("CONTRACT_EXISTS", "合同已存在", 409)
        for venue_id in data.get("venues", []):
            if venue_id not in self.venue_ids:
                raise DomainError("VENUE_NOT_FOUND", f"场地不存在：{venue_id}", 404)
        revisions = data.get("revisions", [program["current_revision"]])
        if revisions == ["*"]:
            revisions = [program["current_revision"]]
        payload = {
            "contract_id": cid,
            "performer_ids": data.get("performer_ids", ["*"]),
            "venues": data.get("venues", []),
            "revisions": revisions,
            "scope_text": data.get("scope_text", ""),
            "status": data.get("status", "active"),
            "contracted_at": _now(),
        }

        def build() -> Any:
            event = self._event("CONTRACT_SCOPED", program_id, payload)
            return {"contract_id": cid, "event_id": event["event_id"], "version": self.store.version(f"program-{program_id}") + 1}, [
                (f"program-{program_id}", self.store.version(f"program-{program_id}"), event)
            ]

        resp, _ = self._run_idem(f"contract:{cid}", idem_key, build)
        return resp

    def submit_tech_review(self, program_id: str, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        program = self._program(program_id)
        revision = data.get("revision", program["current_revision"])
        if revision not in program["revisions"]:
            raise DomainError("REVISION_NOT_FOUND", f"版本不存在：{revision}", 404)
        if not data.get("reviewer"):
            raise DomainError("BAD_REQUEST", "缺少 reviewer", 400)
        result = self._evaluate_tech(program, {**data, "revision": revision})

        def build() -> Any:
            event = self._event("TECH_REVIEWED", program_id, result)
            return {
                "rights": None,
                "tech_review": {"revision": revision, "status": result["status"], "reasons": result["reasons"]},
                "event_id": event["event_id"],
            }, [(f"program-{program_id}", self.store.version(f"program-{program_id}"), event)]

        resp, _ = self._run_idem(f"tech:{program_id}:{revision}:{data['reviewer']}", idem_key, build)
        return {"tech_review": resp["tech_review"], "event_id": resp["event_id"]}

    def clear_rights(self, program_id: str, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        program = self._program(program_id)
        rid = data.get("rights_id") or _new_id("rights")
        revision = data.get("revision", program["current_revision"])
        if revision not in program["revisions"]:
            raise DomainError("REVISION_NOT_FOUND", f"版本不存在：{revision}", 404)
        scope = data.get("scope") or {}
        payload = {
            "rights_id": rid,
            "work_id": data["work_id"],
            "revision": revision,
            "licensor": data.get("licensor", ""),
            "all_revisions": bool(data.get("all_revisions", True)),
            "scope": {
                "channels": scope.get("channels", []),
                "venues": scope.get("venues", []),
                "uses": scope.get("uses", []),
            },
            "cleared_at": _now(),
        }

        def build() -> Any:
            event = self._event("RIGHTS_CLEARED", program_id, payload)
            return {"rights_id": rid, "event_id": event["event_id"]}, [
                (f"program-{program_id}", self.store.version(f"program-{program_id}"), event)
            ]

        resp, _ = self._run_idem(f"rights:{rid}", idem_key, build)
        return resp

    def narrow_rights(self, program_id: str, rights_id: str, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        """收窄授权：旧授权标记 narrowed，同时节目产生新版本，新授权挂在新版本上。"""
        program = self._program(program_id)
        old = program["rights"].get(rights_id)
        if old is None or old["status"] != "active":
            raise DomainError("RIGHTS_NOT_FOUND", "有效授权不存在", 404)
        new_rid = data.get("new_rights_id") or _new_id("rights")
        new_scope = {
            "channels": data.get("channels", old["scope"]["channels"]),
            "venues": data.get("venues", old["scope"]["venues"]),
            "uses": data.get("uses", old["scope"]["uses"]),
        }

        def build() -> Any:
            rev_resp, rev_entries = self._bump_revision(
                program_id, "rights_narrowed", data.get("reason", "收窄授权"),
                program["lineup"],
            )
            new_rev = rev_resp["revision"]
            stream_version = rev_entries[0][1] + 1
            narrowed = self._event(
                "RIGHTS_NARROWED",
                program_id,
                {
                    "rights_id": new_rid,
                    "work_id": old["work_id"],
                    "revision": new_rev,
                    "scope": new_scope,
                    "supersedes": rights_id,
                    "reason": data.get("reason"),
                    "narrowed_at": _now(),
                },
                correlation_id=f"program-{program_id}",
            )
            rev_entries.append((f"program-{program_id}", stream_version, narrowed))
            rev_resp.update({"rights_id": new_rid, "supersedes_rights": rights_id, "scope": new_scope})
            rev_resp["affected_slots"] = self._affected_after_revision(program_id, rev_entries)
            return rev_resp, rev_entries

        resp, _ = self._run_idem(f"rights-narrow:{new_rid}", idem_key, build)
        return resp

    # ================= 命令：场地时段 =================
    def hold_slot(self, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        slot_id = data["slot_id"]
        program = self._program(data["program_id"])
        venue = self._venue(data["venue_id"])
        revision = data.get("revision", program["current_revision"])
        if revision not in program["revisions"]:
            raise DomainError("REVISION_NOT_FOUND", f"版本不存在：{revision}", 404)
        if not self._within_business_hours(venue, data["starts_at"], data["ends_at"]):
            raise DomainError("OUTSIDE_BUSINESS_HOURS", "时段超出商圈营业时段", 422)
        # 多个团队可对同一窗口临时预留（允许重叠抢占）；最终冲突在 confirm 时裁决。
        payload = {
            "venue_id": venue["venue_id"],
            "starts_at": data["starts_at"],
            "ends_at": data["ends_at"],
            "program_id": program["program_id"],
            "revision": revision,
            "note": data.get("note"),
            "held_at": _now(),
        }

        def build() -> Any:
            event = self._event("SLOT_HELD", slot_id, payload)
            version = self.store.version(f"slot-{slot_id}") + 1
            return {"slot_id": slot_id, "status": "provisional", "event_id": event["event_id"], "version": version}, [
                (f"slot-{slot_id}", self.store.version(f"slot-{slot_id}"), event)
            ]

        resp, _ = self._run_idem(f"slot-hold:{slot_id}:{program['program_id']}:{revision}", idem_key, build)
        return resp

    def confirm_slot(self, slot_id: str, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        slot = self._slot(slot_id)
        program = self._program(data["program_id"])
        revision = data.get("revision", program["current_revision"])
        held = any(h["program_id"] == program["program_id"] and h["revision"] == revision for h in slot["holds"])
        if slot["status"] == "confirmed":
            raise DomainError("SLOT_TAKEN", f"场次已由 {slot['program_id']} 确认", 409)
        if slot["status"] == "released":
            raise DomainError("SLOT_RELEASED", "场次已释放，不能确认", 422)
        if not held:
            raise DomainError("NO_HOLD", "该节目未临时预留此场次", 422)
        blockers = self._blockers(program, revision, slot["venue_id"])
        if blockers:
            self._not_ready(blockers, "出场条件不满足")
        clash = self._overlaps_confirmed(slot["venue_id"], slot["starts_at"], slot["ends_at"], slot_id)
        if clash:
            raise DomainError("SLOT_OVERLAP", f"与已确认场次 {clash['slot_id']} 冲突", 409)
        payload = {
            "program_id": program["program_id"],
            "revision": revision,
            "confirmed_by": data.get("confirmed_by"),
            "confirmed_at": _now(),
        }

        def build() -> Any:
            event = self._event("SLOT_CONFIRMED", slot_id, payload)
            version = self.store.version(f"slot-{slot_id}") + 1
            return {
                "slot_id": slot_id,
                "status": "confirmed",
                "program_id": program["program_id"],
                "revision": revision,
                "event_id": event["event_id"],
                "version": version,
            }, [(f"slot-{slot_id}", self.store.version(f"slot-{slot_id}"), event)]

        # 并发确认：不同制作人各自的幂等键不同，最终由流版本乐观锁决出唯一胜者。
        resp, _ = self._run_idem(f"slot-confirm:{slot_id}:{program['program_id']}:{revision}", idem_key, build)
        return resp

    def reschedule_slot(self, slot_id: str, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        """改期：仅作用于这一个场次（露天环境受影响范围），产生新版本事件。"""
        slot = self._slot(slot_id)
        if slot["status"] != "confirmed":
            raise DomainError("SLOT_NOT_CONFIRMED", "只有已确认场次可以改期", 422)
        program = self._program(slot["program_id"])
        revision = data.get("revision", slot["revision"])
        venue = self._venue(slot["venue_id"])
        if not self._within_business_hours(venue, data["starts_at"], data["ends_at"]):
            raise DomainError("OUTSIDE_BUSINESS_HOURS", "新时段超出商圈营业时段", 422)
        blockers = self._blockers(program, revision, slot["venue_id"])
        if blockers:
            self._not_ready(blockers, "改期后出场条件不满足")
        clash = self._overlaps_confirmed(slot["venue_id"], data["starts_at"], data["ends_at"], slot_id)
        if clash:
            raise DomainError("SLOT_OVERLAP", f"与已确认场次 {clash['slot_id']} 冲突", 409)
        payload = {
            "program_id": slot["program_id"],
            "revision": revision,
            "starts_at": data["starts_at"],
            "ends_at": data["ends_at"],
            "reason": data.get("reason"),
            "open_air_only": venue.get("open_air", False),
            "rescheduled_at": _now(),
        }

        def build() -> Any:
            event = self._event("SLOT_RESCHEDULED", slot_id, payload, correlation_id=f"slot-{slot_id}")
            return {
                "slot_id": slot_id,
                "status": "confirmed",
                "starts_at": data["starts_at"],
                "ends_at": data["ends_at"],
                "revision": revision,
                "event_id": event["event_id"],
                "version": self.store.version(f"slot-{slot_id}") + 1,
            }, [(f"slot-{slot_id}", self.store.version(f"slot-{slot_id}"), event)]

        resp, _ = self._run_idem(f"slot-reschedule:{slot_id}:{data['starts_at']}", idem_key, build)
        return resp

    def release_slot(self, slot_id: str, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        slot = self._slot(slot_id)
        if slot["status"] == "released":
            raise DomainError("SLOT_RELEASED", "场次已释放", 422)
        was_confirmed = slot["status"] == "confirmed"
        entries: list[tuple[str, int, dict[str, Any]]] = []
        billing_id = f"bill-{slot_id}"
        refund_event_id: str | None = None
        if was_confirmed and self.store.exists(billing_id):
            billing = self._billing(billing_id)
            if billing["status"] == "settled":
                refund = self._event(
                    "BILLING_REFUNDED",
                    billing_id,
                    {
                        "slot_id": slot_id,
                        "amount": billing["amount"],
                        "currency": billing["currency"],
                        "reason": data.get("reason", "场次释放"),
                        "refunded_at": _now(),
                    },
                )
                refund_event_id = refund["event_id"]
                entries.append((billing_id, self.store.version(billing_id), refund))
        release = self._event(
            "SLOT_RELEASED",
            slot_id,
            {"reason": data.get("reason"), "released_by": data.get("released_by"), "released_at": _now()},
        )
        entries.append((f"slot-{slot_id}", self.store.version(f"slot-{slot_id}"), release))

        def build() -> Any:
            return {
                "slot_id": slot_id,
                "status": "released",
                "refunded": refund_event_id is not None,
                "refund_event_id": refund_event_id,
                "event_id": release["event_id"],
            }, entries

        resp, _ = self._run_idem(f"slot-release:{slot_id}", idem_key, build)
        return resp

    # ================= 命令：宣传发布与渠道回执 =================
    def publish_release(self, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        program = self._program(data["program_id"])
        revision = data.get("revision", program["current_revision"])
        channel = data.get("channel")
        content = data.get("content") or {}
        if not channel or not content.get("title") or not content.get("blurb"):
            raise DomainError("BAD_REQUEST", "需要 channel 与 content.title/blurb", 400)
        release_id = f"release-{program['program_id']}-{channel}"
        # 已发布内容不可静默改写：同一版本文案不同即拒绝；完全相同则回放已有发布（天然幂等）。
        if self.store.exists(release_id):
            existing = self._release(release_id)
            if existing["status"] == "retracted":
                raise DomainError("RELEASE_RETRACTED", "该渠道发布已撤回", 422)
            if existing["revision"] == revision:
                if existing["content"] != content:
                    raise DomainError("CONTENT_IMMUTABLE", "已发布内容不可静默改写，请先创建节目新版本", 422)
                last = existing["publications"][-1]
                return {
                    "release_id": release_id,
                    "revision": revision,
                    "channel": channel,
                    "event_id": last["event_id"],
                    "version": self.store.version(release_id),
                    "deduplicated": True,
                }
        # 发布同样过出场条件门，并额外要求版权覆盖该渠道。
        venue_id = data.get("venue_id") or self._any_contract_venue(program, revision)
        blockers = self._blockers(program, revision, venue_id, channel=channel)
        if blockers:
            self._not_ready(blockers, "发布条件不满足")
        payload = {
            "program_id": program["program_id"],
            "revision": revision,
            "channel": channel,
            "content": content,
            "supersedes_revision": self._release(release_id)["revision"] if self.store.exists(release_id) else None,
            "published_at": _now(),
        }

        def build() -> Any:
            event = self._event("RELEASE_PUBLISHED", release_id, payload, correlation_id=f"program-{program['program_id']}")
            return {
                "release_id": release_id,
                "revision": revision,
                "channel": channel,
                "event_id": event["event_id"],
                "version": self.store.version(release_id) + 1,
            }, [(release_id, self.store.version(release_id), event)]

        resp, _ = self._run_idem(f"publish:{release_id}:{revision}", idem_key, build)
        return resp

    def _any_contract_venue(self, program: dict[str, Any], revision: int) -> str:
        for c in program["contracts"].values():
            if c["status"] == "active" and c["venues"]:
                return c["venues"][0]
        return ""

    def record_release_receipt(self, release_id: str, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        release = self._release(release_id)
        channel = data.get("channel", release["channel"])
        callback_id = data.get("callback_id")
        if not callback_id:
            raise DomainError("BAD_REQUEST", "缺少 callback_id", 400)
        # 重复回调：同一 callback_id 只生效一次，返回首次回执，不重复计费/不重复变更。
        remembered = self.store.remembered_response("channel-receipt", callback_id)
        if remembered is not None:
            return dict(remembered, replay=True)
        payload = {
            "channel": channel,
            "callback_id": callback_id,
            "status": data.get("status", "delivered"),
            "detail": data.get("detail", ""),
            "received_at": _now(),
        }
        event = self._event("RELEASE_RECEIPTED", release_id, payload)
        resp = {
            "release_id": release_id,
            "channel": channel,
            "callback_id": callback_id,
            "status": payload["status"],
            "event_id": event["event_id"],
            "replay": False,
        }
        self.store.commit(
            [(release_id, self.store.version(release_id), event)],
            remember=("channel-receipt", callback_id, {k: v for k, v in resp.items() if k != "replay"}),
        )
        return resp

    def retract_release(self, release_id: str, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        release = self._release(release_id)
        if release["status"] == "retracted":
            raise DomainError("RELEASE_RETRACTED", "已撤回", 422)
        payload = {"reason": data.get("reason"), "retracted_at": _now()}

        def build() -> Any:
            event = self._event("RELEASE_RETRACTED", release_id, payload)
            return {"release_id": release_id, "status": "retracted", "event_id": event["event_id"]}, [
                (release_id, self.store.version(release_id), event)
            ]

        resp, _ = self._run_idem(f"retract:{release_id}", idem_key, build)
        return resp

    # ================= 命令：结算 =================
    def settle_slot(self, slot_id: str, data: dict[str, Any], idem_key: str | None = None) -> dict[str, Any]:
        """按支付回调结算。重复 callback_id 永远返回首次结果，不重复计费。"""
        callback_id = data.get("callback_id")
        if not callback_id:
            raise DomainError("BAD_REQUEST", "缺少 callback_id", 400)
        remembered = self.store.remembered_response("payment", callback_id)
        if remembered is not None:
            return dict(remembered, replay=True)
        slot = self._slot(slot_id)
        if slot["status"] != "confirmed":
            raise DomainError("SLOT_NOT_CONFIRMED", "只有已确认场次可以结算", 422)
        billing_id = f"bill-{slot_id}"
        if self.store.exists(billing_id) and self._billing(billing_id)["status"] == "settled":
            raise DomainError("ALREADY_SETTLED", "该场次已结算，请勿重复扣款", 409)
        program = self._program(slot["program_id"])
        fee = program["revisions"][slot["revision"]]["fee"]
        amount = data.get("amount", fee)
        payload = {
            "slot_id": slot_id,
            "venue_id": slot["venue_id"],
            "program_id": slot["program_id"],
            "revision": slot["revision"],
            "callback_id": callback_id,
            "amount": amount,
            "currency": data.get("currency", "CNY"),
            "channel": data.get("channel", "payment-gateway"),
            "settled_at": _now(),
        }
        event = self._event("BILLING_SETTLED", billing_id, payload)
        resp = {
            "billing_id": billing_id,
            "slot_id": slot_id,
            "amount": amount,
            "currency": payload["currency"],
            "status": "settled",
            "event_id": event["event_id"],
            "replay": False,
        }
        self.store.commit(
            [(billing_id, 0, event)],
            remember=("payment", callback_id, {k: v for k, v in resp.items() if k != "replay"}),
        )
        return resp

    # ================= 查询与角色视图 =================
    def _tech_for(self, program: dict[str, Any], revision: int) -> dict[str, Any] | None:
        rev_snapshot = program["revisions"].get(revision)
        if rev_snapshot is None:
            return None
        reqs = self._requirements(rev_snapshot["lineup"])
        return next(
            (t for t in program["tech"].values() if t["status"] == "approved" and t.get("required") == reqs),
            None,
        )

    def _rights_for(self, program: dict[str, Any], revision: int) -> list[dict[str, Any]]:
        return [
            r for r in program["rights"].values()
            if r["status"] == "active" and revision >= r["revision"]
            and (r.get("all_revisions", True) or r["revision"] == revision)
        ]

    def _slot_evidence(self, slot: dict[str, Any]) -> dict[str, Any]:
        if not slot.get("program_id"):
            return {}
        program = self._program(slot["program_id"])
        rev = slot["revision"]
        billing_id = f"bill-{slot['slot_id']}"
        billing = self._billing(billing_id) if self.store.exists(billing_id) else None
        return {
            "program_id": slot["program_id"],
            "revision": rev,
            "contracts": [
                {"contract_id": c["contract_id"], "status": c["status"], "venues": c["venues"],
                 "performer_ids": c["performer_ids"], "event_id": c["event_id"]}
                for c in program["contracts"].values()
                if c["status"] == "active"
            ],
            "tech_review": self._tech_for(program, rev),
            "rights": [
                {"rights_id": r["rights_id"], "status": r["status"], "scope": r["scope"],
                 "revision": r["revision"], "event_id": r["event_id"]}
                for r in self._rights_for(program, rev)
            ],
            "billing": billing,
            "blockers": self._blockers(program, rev, slot["venue_id"]),
        }

    def coordinator_program(self, program_id: str) -> dict[str, Any]:
        program = self._program(program_id)
        slots = self._program_slots(program_id)
        releases = []
        for channel in self._program_channels(program_id):
            rid = f"release-{program_id}-{channel}"
            if self.store.exists(rid):
                releases.append(self._release(rid))
        rev = program["current_revision"]
        return {
            "program_id": program_id,
            "title": program["title"],
            "status": program["status"],
            "current_revision": rev,
            "requirements": self._requirements(program["lineup"]),
            "lineage": program["lineage"],
            "lineup": program["lineup"],
            "fee": program["fee"],
            "contracts": program["contracts"],
            "tech_reviews": program["tech"],
            "rights": program["rights"],
            "slots": [
                {
                    "slot_id": s["slot_id"],
                    "venue_id": s["venue_id"],
                    "starts_at": s["starts_at"],
                    "ends_at": s["ends_at"],
                    "status": s["status"],
                    "revision": s.get("revision"),
                    "evidence": self._slot_evidence(s) if s["status"] == "confirmed" else None,
                }
                for s in slots
            ],
            "releases": [
                {
                    "release_id": r["release_id"],
                    "channel": r["channel"],
                    "status": r["status"],
                    "revision": r.get("revision"),
                    "content": r.get("content"),
                    "receipts": r["receipts"],
                    "publications": [{"revision": x["revision"], "event_id": x["event_id"]} for x in r["publications"]],
                }
                for r in releases
            ],
            "current_blockers": {
                s["slot_id"]: self._blockers(program, rev, s["venue_id"])
                for s in slots
                if s["status"] in ("confirmed", "provisional") and s.get("program_id") == program_id
            },
        }

    def _program_channels(self, program_id: str) -> set[str]:
        channels: set[str] = set()
        for e in self.store.all_events():
            if e["event_type"] == "RELEASE_PUBLISHED" and e["payload"]["program_id"] == program_id:
                channels.add(e["payload"]["channel"])
        return channels

    def public_listing(self) -> dict[str, Any]:
        """公开节目单：只暴露已发布版本；每个条目可据 release_id 让统筹追溯完整链路。"""
        items = []
        for e in self.store.all_events():
            if e["event_type"] != "RELEASE_PUBLISHED":
                continue
            p = e["payload"]
            rid = e["aggregate_id"]
            release = self._release(rid)
            if release["status"] != "published":
                continue
            # 只展示该发布流的最新一次发布。
            if release["publications"][-1]["event_id"] != e["event_id"]:
                continue
            program = self._program(p["program_id"])
            slot_info = []
            for sid in self.slot_ids:
                s = self._slot(sid)
                if s.get("program_id") == p["program_id"] and s["status"] == "confirmed":
                    slot_info.append(
                        {"slot_id": sid, "venue_id": s["venue_id"], "starts_at": s["starts_at"], "ends_at": s["ends_at"]}
                    )
            items.append(
                {
                    "program_id": p["program_id"],
                    "title": p["content"]["title"],
                    "blurb": p["content"]["blurb"],
                    "revision": p["revision"],
                    "channel": p["channel"],
                    "release_id": rid,
                    "published_event_id": e["event_id"],
                    "performances": slot_info,
                }
            )
        return {"programs": items}

    def venue_schedule(self, venue_id: str) -> dict[str, Any]:
        """场地方视图：只看完成职责所需——时间、装台要求与对接人，无合同/费用/版权细节。"""
        self._venue(venue_id)
        entries = []
        for sid in sorted(self.slot_ids):
            s = self._slot(sid)
            if s["venue_id"] != venue_id or s["status"] not in ("provisional", "confirmed"):
                continue
            item: dict[str, Any] = {
                "slot_id": sid,
                "starts_at": s["starts_at"],
                "ends_at": s["ends_at"],
                "status": s["status"],
            }
            if s.get("program_id"):
                program = self._program(s["program_id"])
                rev = program["revisions"][s["revision"]]
                item.update(
                    {
                        "program_title": rev["title"],
                        "setup": self._requirements(rev["lineup"]),
                    }
                )
            entries.append(item)
        return {"venue_id": venue_id, "schedule": entries}

    def release_trace(self, release_id: str) -> dict[str, Any]:
        """从任一公开发布追到审批、授权与实际执行结果。"""
        release = self._release(release_id)
        program = self._program(release["program_id"])
        rev = release["revision"]
        slots = []
        for sid in self.slot_ids:
            s = self._slot(sid)
            if s.get("program_id") == program["program_id"] and s["status"] == "confirmed":
                billing_id = f"bill-{sid}"
                slots.append(
                    {
                        "slot_id": sid,
                        "venue_id": s["venue_id"],
                        "starts_at": s["starts_at"],
                        "ends_at": s["ends_at"],
                        "revision": s["revision"],
                        "billing": self._billing(billing_id)
                        if self.store.exists(billing_id)
                        else None,
                    }
                )
        return {
            "release_id": release_id,
            "program_id": program["program_id"],
            "revision": rev,
            "content": release["content"],
            "tech_review": self._tech_for(program, rev),
            "contracts": [c for c in program["contracts"].values() if c["status"] == "active"],
            "rights": self._rights_for(program, rev),
            "receipts": list(release["receipts"].values()),
            "performances": slots,
        }
