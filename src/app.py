"""应用服务：沿同一条事件链流转节目提案、场地时段、合同、技审、宣传与结算。

所有状态改变都以事件提交到 :class:`~src.eventstore.EventStore`，读模型完全由
事件重放得到。关键承诺：

* 已发布内容不可原地改写——改期/换人/收窄产生新版本（新的 release 聚合），
  旧 release 收到 RELEASE_SUPERSEDED 指引但内容原样保留；
* 多位制作人并发确认同一场次，只有一方的期望版本匹配并生效；
* 确认与计费在同一原子提交内，重复回调命中幂等记录，不会二次预留或重复计费；
* 露天临时限制按剧种/场地/时间窗命中，只重排受影响场次。

为了让“新版本节目单”的快照反映同一次提交里改期/换人/收窄后的状态，构建
命令时先把暂存事件应用到读模型的深拷贝（scratch），全部事件再一次性原子提交。
"""
from __future__ import annotations

import copy
import uuid
from datetime import datetime, timedelta
from typing import Any

from . import domain as D
from .eventstore import CommitItem, EventStore


def _eid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _event(event_type: str, aggregate_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": _eid("evt"),
        "event_type": event_type,
        "occurred_at": D.now_iso(),
        "aggregate_id": aggregate_id,
        "payload": payload,
    }


# --------------------------------------------------------------------------- #
# 读模型
# --------------------------------------------------------------------------- #


class ReadModel:
    """由事件流重放得到的全部投影。"""

    def __init__(self) -> None:
        self.venues: dict[str, dict[str, Any]] = {}
        self.restrictions: list[dict[str, Any]] = []
        self.programs: dict[str, dict[str, Any]] = {}
        self.slots: dict[str, dict[str, Any]] = {}
        self.rights: dict[str, dict[str, Any]] = {}
        self.contracts: dict[str, dict[str, Any]] = {}
        self.techs: dict[str, dict[str, Any]] = {}
        self.releases: dict[str, dict[str, Any]] = {}
        self.settlements: dict[str, dict[str, Any]] = {}
        self.changes: dict[str, dict[str, Any]] = {}

    def apply(self, event: dict[str, Any]) -> None:
        et, p, v = event["event_type"], event["payload"], event["version"]
        at = event["occurred_at"]
        if et == "VENUE_REGISTERED":
            self.venues[p["id"]] = {**p, "version": v}
        elif et == "RESTRICTION_DECLARED":
            self.restrictions.append({**p, "version": v})
        elif et == "PROGRAM_PROPOSED":
            self.programs[p["id"]] = {
                **p, "version": v, "history": [{
                    "version": v, "at": at, "reason": "立项",
                    "title": p["title"], "artists": p["artists"], "works": p["works"],
                }],
                "slots": [], "rights": [], "contracts": [],
            }
        elif et == "PROGRAM_REVISED":
            prog = self.programs[p["id"]]
            for field in ("title", "synopsis", "artists", "works"):
                if field in p:
                    prog[field] = p[field]
            prog["version"] = v
            prog["history"].append({
                "version": v, "at": at, "reason": p.get("change_reason", "修订"),
                "title": prog["title"], "artists": prog["artists"], "works": prog["works"],
            })
        elif et == "SLOT_REQUESTED":
            self.slots[p["id"]] = {
                **p, "version": v, "status": "requested",
                "history": [{"version": v, "at": at, "status": "requested",
                             "start": p["start"], "end": p["end"]}],
            }
            self.programs[p["program_id"]]["slots"].append(p["id"])
        elif et in ("SLOT_CONFIRMED", "SLOT_RESCHEDULED", "SLOT_CANCELLED",
                    "SLOT_PERFORMER_REPLACED"):
            slot = self.slots[p["id"]]
            slot["version"] = v
            if et == "SLOT_CONFIRMED":
                slot["status"] = "confirmed"
                slot["confirmed_by"] = p.get("confirmed_by")
                slot["confirmed_at"] = at
            elif et == "SLOT_RESCHEDULED":
                slot["start"], slot["end"] = p["new_start"], p["new_end"]
                if slot["status"] != "confirmed":
                    slot["status"] = "requested"
                slot.setdefault("reschedules", []).append(
                    {"version": v, "at": at, "from": p.get("previous_start"),
                     "to": p["new_start"], "reason": p.get("reason"),
                     "change_id": p.get("change_id")})
            elif et == "SLOT_PERFORMER_REPLACED":
                slot.setdefault("performer_history", []).append(
                    {"version": v, "at": at,
                     "from": slot.get("performer_artist_id"), "to": p["new_artist_id"],
                     "reason": p.get("reason"), "change_id": p.get("change_id")})
                slot["performer_artist_id"] = p["new_artist_id"]
            else:  # SLOT_CANCELLED
                slot["status"] = "cancelled"
            slot["history"].append({"version": v, "at": at, "status": slot["status"],
                                    "start": slot["start"], "end": slot["end"]})
        elif et == "RIGHTS_CLEARED":
            self.rights[p["id"]] = {**p, "version": v,
                                    "history": [{"version": v, "at": at, "scope": _scope_of(p)}]}
            self.programs[p["program_id"]]["rights"].append(p["id"])
        elif et == "RIGHTS_NARROWED":
            grant = self.rights[p["id"]]
            grant.update(p["scope"])
            grant["version"] = v
            grant["history"].append({"version": v, "at": at, "reason": p.get("reason"),
                                     "scope": dict(p["scope"])})
        elif et == "CONTRACT_SCOPED":
            self.contracts[p["id"]] = {**p, "version": v}
            self.programs[p["program_id"]]["contracts"].append(p["id"])
        elif et == "TECH_REVIEW_SUBMITTED":
            self.techs[p["id"]] = {**p, "version": v, "status": "submitted"}
            self.slots[p["slot_id"]]["tech_review_id"] = p["id"]
        elif et in ("TECH_REVIEW_APPROVED", "TECH_REVIEW_REJECTED"):
            tech = self.techs[p["id"]]
            tech["version"] = v
            tech["status"] = "approved" if et == "TECH_REVIEW_APPROVED" else "rejected"
            tech["review_note"] = p.get("review_note")
            tech["reviewed_by"] = p.get("reviewed_by")
            tech["reviewed_at"] = at
        elif et == "RELEASE_PUBLISHED":
            self.releases[p["id"]] = {
                **p, "version": v, "status": "published",
                "published_at": at, "receipts": {},
            }
        elif et == "RELEASE_SUPERSEDED":
            old = self.releases[p["id"]]
            old["status"] = "superseded"
            old["superseded_by"] = p["new_release_id"]
            old["superseded_reason"] = p.get("reason")
            old["superseded_change"] = p.get("change_id")
        elif et == "CHANNEL_RECEIPT_ACKED":
            rel = self.releases[p["release_id"]]
            rel["receipts"][p["receipt_id"]] = {
                "channel": p["channel"], "status": p["status"],
                "at": p.get("at", at), "detail": p.get("detail"),
            }
        elif et == "CHANGE_RECONCILED":
            self.changes[p["id"]] = {**p, "version": v, "at": at}
        elif et == "PERFORMANCE_EXECUTED":
            slot = self.slots[p["slot_id"]]
            slot["execution"] = {**p, "version": v, "at": at}
        elif et == "SETTLEMENT_RECORDED":
            self.settlements[p["id"]] = {**p, "version": v, "status": "recorded", "at": at}
            self.slots[p["slot_id"]].setdefault("settlement_ids", []).append(p["id"])


def _scope_of(p: dict[str, Any]) -> dict[str, Any]:
    return {k: p[k] for k in ("works", "channels", "territories", "valid_from", "valid_until")}


# --------------------------------------------------------------------------- #
# 暂存批次
# --------------------------------------------------------------------------- #


class Staging:
    """构建一次原子提交：事件先落到 scratch 投影，快照即可反映提交后状态。"""

    def __init__(self, app: "AppService") -> None:
        self.app = app
        self.scratch: ReadModel = copy.deepcopy(app.rm)
        self.items: list[CommitItem] = []
        self._staged_count: dict[str, int] = {}

    def add(self, stream_id: str, event: dict[str, Any]) -> dict[str, Any]:
        staged = self._staged_count.get(stream_id, 0)
        expected = self.app.store.stream_version(stream_id) + staged
        self.items.append(CommitItem(stream_id, expected, event))
        self._staged_count[stream_id] = staged + 1
        projected = dict(event, stream_id=stream_id, version=expected + 1)
        self.scratch.apply(projected)
        return projected


# --------------------------------------------------------------------------- #
# 应用服务
# --------------------------------------------------------------------------- #


class AppService:
    def __init__(self, store: EventStore) -> None:
        self.store = store
        self.rm = ReadModel()
        store.subscribe(self.rm.apply)

    # -- 工具 -------------------------------------------------------------

    @staticmethod
    def _program_in(model: ReadModel, program_id: str) -> dict[str, Any]:
        prog = model.programs.get(program_id)
        if not prog:
            raise D.NotFoundError(f"节目不存在: {program_id}")
        return prog

    @staticmethod
    def _slot_in(model: ReadModel, slot_id: str) -> dict[str, Any]:
        slot = model.slots.get(slot_id)
        if not slot:
            raise D.NotFoundError(f"场次不存在: {slot_id}")
        return slot

    def _program(self, program_id: str) -> dict[str, Any]:
        return self._program_in(self.rm, program_id)

    def _slot(self, slot_id: str) -> dict[str, Any]:
        return self._slot_in(self.rm, slot_id)

    def _commit(self, staging: Staging, idem_key: str | None,
                result: dict[str, Any]) -> dict[str, Any]:
        events = self.store.commit(staging.items, idem_key=idem_key, result=result)
        return {"events": [e["event_id"] for e in events], **result}

    # -- 场地与限制 -------------------------------------------------------

    def register_venue(self, data: dict[str, Any]) -> dict[str, Any]:
        for f in ("name", "business_open", "business_close"):
            if not data.get(f):
                raise D.DomainError(f"缺少字段: {f}")
        D.parse_hhmm(data["business_open"])
        D.parse_hhmm(data["business_close"])
        venue_id = data.get("id") or _eid("venue")
        payload = {
            "id": venue_id, "name": data["name"],
            "business_open": data["business_open"],
            "business_close": data["business_close"],
            "open_air": bool(data.get("open_air", False)),
            "power_kw": int(data.get("power_kw", 0)),
            "noise_limit_db": int(data.get("noise_limit_db", 120)),
            "has_quiet_room": bool(data.get("has_quiet_room", False)),
            "settlement_amount": int(data.get("settlement_amount", 10000)),
        }
        st = Staging(self)
        st.add(f"venue-{venue_id}", _event("VENUE_REGISTERED", f"venue-{venue_id}", payload))
        return self._commit(st, data.get("idempotency_key"), {"venue_id": venue_id})

    def declare_restriction(self, data: dict[str, Any]) -> dict[str, Any]:
        for f in ("start", "end", "reason"):
            if not data.get(f):
                raise D.DomainError(f"缺少字段: {f}")
        D.TimeWindow.from_iso(data["start"], data["end"])
        rid = data.get("id") or _eid("restriction")
        payload = {
            "id": rid, "venue_ids": data.get("venue_ids") or [],
            "open_air_only": bool(data.get("open_air_only", False)),
            "genres": data.get("genres") or [],
            "start": data["start"], "end": data["end"], "reason": data["reason"],
        }
        st = Staging(self)
        st.add(f"restriction-{rid}",
               _event("RESTRICTION_DECLARED", f"restriction-{rid}", payload))
        return self._commit(st, data.get("idempotency_key"), {"restriction_id": rid})

    # -- 节目提案与修订 ---------------------------------------------------

    def propose_program(self, data: dict[str, Any], producer: str) -> dict[str, Any]:
        for f in ("title", "genre"):
            if not data.get(f):
                raise D.DomainError(f"缺少字段: {f}")
        if data["genre"] not in D.GENRES:
            raise D.DomainError(f"未知剧种，可选: {','.join(D.GENRES)}")
        pid = data.get("id") or _eid("program")
        payload = {
            "id": pid, "title": data["title"], "genre": data["genre"],
            "synopsis": data.get("synopsis", ""),
            "works": list(data.get("works", [])),
            "artists": list(data.get("artists", [])),
            "proposed_by": producer,
        }
        st = Staging(self)
        st.add(f"program-{pid}", _event("PROGRAM_PROPOSED", f"program-{pid}", payload))
        return self._commit(st, data.get("idempotency_key"),
                            {"program_id": pid, "version": 1})

    def revise_program(self, program_id: str, data: dict[str, Any]) -> dict[str, Any]:
        prog = self._program(program_id)
        changes = {k: data[k] for k in ("title", "synopsis", "artists", "works") if k in data}
        if not changes:
            raise D.DomainError("没有需要修订的字段")
        reason = data.get("change_reason", "节目修订")
        change_id = _eid("change")
        st = Staging(self)
        st.add(f"program-{program_id}",
               _event("PROGRAM_REVISED", f"program-{program_id}",
                      {"id": program_id, **changes, "change_reason": reason}))
        new_releases = self._supersede_in(
            st, program_id, None, change_id,
            reason=f"节目修订: {reason}", change_kind="program_revised",
            extra={"program_id": program_id})
        result = {"program_id": program_id, "change_id": change_id,
                  "new_versions": [r["new_release_id"] for r in new_releases]}
        st.add(f"change-{change_id}",
               _event("CHANGE_RECONCILED", f"change-{change_id}",
                      {"id": change_id, "kind": "program_revised", "reason": reason,
                       "program_id": program_id, "new_releases": new_releases}))
        return self._commit(st, data.get("idempotency_key"), result)

    # -- 场次 -------------------------------------------------------------

    def request_slot(self, program_id: str, data: dict[str, Any]) -> dict[str, Any]:
        prog = self._program(program_id)
        venue = self.rm.venues.get(data["venue_id"])
        if not venue:
            raise D.NotFoundError(f"场地不存在: {data.get('venue_id')}")
        win = D.TimeWindow.from_iso(data["start"], data["end"])
        if not D.within_business_hours(win, venue["business_open"], venue["business_close"]):
            raise D.DomainError("演出时段超出商圈营业时段")
        if not D.before_curfew(win, D.GENRE_RULES[prog["genre"]]["curfew"]):
            raise D.DomainError(
                f"结束时间晚于该剧种噪音宵禁 {D.GENRE_RULES[prog['genre']]['curfew']}")
        artist_id = data.get("performer_artist_id")
        if prog["artists"] and artist_id and artist_id not in {a["id"] for a in prog["artists"]}:
            raise D.DomainError("表演者不在节目演职名单内")
        sid = data.get("id") or _eid("slot")
        payload = {"id": sid, "program_id": program_id, "venue_id": venue["id"],
                   "genre": prog["genre"], "start": data["start"], "end": data["end"],
                   "performer_artist_id": artist_id}
        st = Staging(self)
        st.add(f"slot-{sid}", _event("SLOT_REQUESTED", f"slot-{sid}", payload))
        return self._commit(st, data.get("idempotency_key"), {"slot_id": sid})

    def confirm_slot(self, slot_id: str, producer: str, idem_key: str | None) -> dict[str, Any]:
        # 默认按“场次+制作人”派生：同一制作人的重复回调重放首次结果；
        # 不同制作人并发时输家得到 409，只有一个确认生效。
        idem_key = idem_key or f"confirm-{slot_id}-{producer}"
        self.store.raise_if_replayed(idem_key)
        slot = self._slot(slot_id)
        prog = self._program(slot["program_id"])
        venue = self.rm.venues[slot["venue_id"]]
        if slot["status"] == "confirmed":
            raise D.ConflictStateError(
                f"场次已由 {slot.get('confirmed_by')} 确认，并发确认只有一个结果生效")
        if slot["status"] == "cancelled":
            raise D.ConflictStateError("场次已取消，不可确认")
        problems = self._readiness_problems(self.rm, prog, slot, venue)
        if problems:
            raise D.DomainError("场次尚不可确认: " + "；".join(problems))

        settlement_id = _eid("settlement")
        st = Staging(self)
        st.add(f"slot-{slot_id}", _event("SLOT_CONFIRMED", f"slot-{slot_id}",
                                          {"id": slot_id, "confirmed_by": producer}))
        st.add(f"settlement-{settlement_id}",
               _event("SETTLEMENT_RECORDED", f"settlement-{settlement_id}", {
                   "id": settlement_id, "slot_id": slot_id, "venue_id": venue["id"],
                   "program_id": prog["id"], "amount": venue["settlement_amount"],
                   "currency": "CNY", "basis": "场地时段确认一次性场租"}))
        result = {"slot_id": slot_id, "status": "confirmed",
                  "settlement_id": settlement_id, "amount": venue["settlement_amount"],
                  "confirmed_by": producer}
        return self._commit(st, idem_key, result)

    def _readiness_problems(self, model: ReadModel, prog: dict[str, Any],
                            slot: dict[str, Any], venue: dict[str, Any]) -> list[str]:
        problems: list[str] = []
        win = D.TimeWindow.from_iso(slot["start"], slot["end"])
        for other in model.slots.values():
            if other["id"] == slot["id"] or other["venue_id"] != venue["id"]:
                continue
            if other["status"] != "confirmed" or other.get("execution"):
                continue
            if win.overlaps(D.TimeWindow.from_iso(other["start"], other["end"])):
                problems.append(f"与同场地已确认场次 {other['id']} 时间冲突")
        tech = model.techs.get(slot.get("tech_review_id", ""))
        if not tech or tech["status"] != "approved":
            problems.append("技术审查尚未通过")
        needed = {"works": prog["works"], "channels": ["live"],
                  "territory": "CN", "slot_start": slot["start"], "slot_end": slot["end"]}
        if prog["works"] and all(D.scope_covers(model.rights[gid], needed)
                                 for gid in prog["rights"]):
            problems.append("缺少覆盖现场演出的音乐授权")
        contracted = {c["artist_id"] for c in model.contracts.values()
                      if c.get("slot_id") == slot["id"]}
        performer_id = slot.get("performer_artist_id")
        if performer_id and performer_id not in contracted:
            problems.append(f"该场次表演者缺少演职合同: {performer_id}")
        return problems

    # -- 技术审查 ---------------------------------------------------------

    def submit_tech_review(self, slot_id: str, data: dict[str, Any]) -> dict[str, Any]:
        slot = self._slot(slot_id)
        venue = self.rm.venues[slot["venue_id"]]
        plan = dict(data.get("plan", {}))
        plan.setdefault("slot_start", slot["start"])
        plan.setdefault("slot_end", slot["end"])
        problems = D.tech_compliance(slot["genre"], venue, plan)
        review_id = data.get("id") or _eid("tech")
        payload = {"id": review_id, "slot_id": slot_id, "program_id": slot["program_id"],
                   "venue_id": venue["id"], "genre": slot["genre"], "plan": plan,
                   "matrix_problems": problems,
                   "submitted_by": data.get("submitted_by", "producer")}
        st = Staging(self)
        st.add(f"tech-{review_id}",
               _event("TECH_REVIEW_SUBMITTED", f"tech-{review_id}", payload))
        return self._commit(st, data.get("idempotency_key"),
                            {"tech_review_id": review_id, "matrix_problems": problems})

    def decide_tech_review(self, slot_id: str, data: dict[str, Any]) -> dict[str, Any]:
        slot = self._slot(slot_id)
        tech = self.rm.techs.get(slot.get("tech_review_id", ""))
        if not tech:
            raise D.ConflictStateError("该场次尚未提交技术审查")
        if tech["status"] in ("approved", "rejected"):
            raise D.ConflictStateError(f"技术审查已有结论: {tech['status']}，结论不可改写")
        decision = data.get("decision")
        if decision not in ("approved", "rejected"):
            raise D.DomainError("decision 必须是 approved 或 rejected")
        if decision == "approved" and tech.get("matrix_problems"):
            raise D.DomainError(
                "技术审查存在未消除的硬性违规，不可批准: "
                + "；".join(tech["matrix_problems"]))
        if decision == "rejected" and not data.get("review_note"):
            raise D.DomainError("驳回必须给出 review_note")
        et = "TECH_REVIEW_APPROVED" if decision == "approved" else "TECH_REVIEW_REJECTED"
        st = Staging(self)
        st.add(f"tech-{tech['id']}", _event(et, f"tech-{tech['id']}", {
            "id": tech["id"], "slot_id": slot_id,
            "review_note": data.get("review_note", ""),
            "reviewed_by": data.get("reviewed_by", "coordinator")}))
        return self._commit(st, data.get("idempotency_key"),
                            {"tech_review_id": tech["id"], "status": decision})

    # -- 授权与合同 -------------------------------------------------------

    def clear_rights(self, program_id: str, data: dict[str, Any]) -> dict[str, Any]:
        self._program(program_id)
        scope = self._scope_payload(data)
        gid = data.get("id") or _eid("rights")
        payload = {"id": gid, "program_id": program_id, **scope,
                   "cleared_by": data.get("cleared_by", "producer")}
        st = Staging(self)
        st.add(f"rights-{gid}", _event("RIGHTS_CLEARED", f"rights-{gid}", payload))
        return self._commit(st, data.get("idempotency_key"),
                            {"rights_id": gid, "version": 1})

    def narrow_rights(self, grant_id: str, data: dict[str, Any]) -> dict[str, Any]:
        grant = self.rm.rights.get(grant_id)
        if not grant:
            raise D.NotFoundError(f"授权不存在: {grant_id}")
        new_scope = self._scope_payload(data)
        problems = D.is_subset_scope(new_scope, grant)
        if problems:
            raise D.DomainError("授权收窄被拒绝: " + "；".join(problems))
        reason = data.get("reason", "收窄授权")
        change_id = _eid("change")
        st = Staging(self)
        st.add(f"rights-{grant_id}",
               _event("RIGHTS_NARROWED", f"rights-{grant_id}",
                      {"id": grant_id, "scope": new_scope, "reason": reason}))
        affected = [rel["id"] for rel in st.scratch.releases.values()
                    if rel["status"] == "published"
                    and rel["program_id"] == grant["program_id"]
                    and not self._release_coverable(st.scratch, rel)]
        new_releases = self._supersede_in(
            st, grant["program_id"], affected or None, change_id,
            reason=f"授权收窄: {reason}", change_kind="rights_narrowed")
        result = {"rights_id": grant_id, "change_id": change_id,
                  "affected_releases": affected,
                  "new_versions": [r["new_release_id"] for r in new_releases]}
        st.add(f"change-{change_id}",
               _event("CHANGE_RECONCILED", f"change-{change_id}",
                      {"id": change_id, "kind": "rights_narrowed", "reason": reason,
                       "rights_id": grant_id, "affected_releases": affected,
                       "new_releases": new_releases}))
        return self._commit(st, data.get("idempotency_key"), result)

    @staticmethod
    def _scope_payload(data: dict[str, Any]) -> dict[str, Any]:
        for f in ("works", "channels", "territories", "valid_from", "valid_until"):
            if f not in data:
                raise D.DomainError(f"授权缺少字段: {f}")
        scope = {"works": list(data["works"]), "channels": list(data["channels"]),
                 "territories": list(data["territories"]),
                 "valid_from": data["valid_from"], "valid_until": data["valid_until"]}
        D.parse_iso(scope["valid_from"])
        D.parse_iso(scope["valid_until"])
        return scope

    def scope_contract(self, slot_id: str, data: dict[str, Any]) -> dict[str, Any]:
        slot = self._slot(slot_id)
        prog = self._program(slot["program_id"])
        for f in ("artist_id", "works"):
            if not data.get(f):
                raise D.DomainError(f"合同缺少字段: {f}")
        if data["artist_id"] not in {a["id"] for a in prog["artists"]}:
            raise D.DomainError("合同艺人不在节目演职名单内")
        cid = data.get("id") or _eid("contract")
        payload = {"id": cid, "program_id": prog["id"], "slot_id": slot_id,
                   "artist_id": data["artist_id"], "works": list(data["works"]),
                   "scope_start": data.get("scope_start", slot["start"]),
                   "scope_end": data.get("scope_end", slot["end"]),
                   "role": data.get("role", "performer")}
        st = Staging(self)
        st.add(f"contract-{cid}", _event("CONTRACT_SCOPED", f"contract-{cid}", payload))
        return self._commit(st, data.get("idempotency_key"), {"contract_id": cid})

    # -- 发布与渠道回执（不可变） -----------------------------------------

    def publish_release(self, data: dict[str, Any], publisher: str) -> dict[str, Any]:
        prog = self._program(data["program_id"])
        channel = data.get("channel")
        if not channel:
            raise D.DomainError("缺少 channel")
        territory = data.get("territory", "CN")
        snapshot, problems = self._build_snapshot(self.rm, prog, channel, territory)
        if problems:
            raise D.DomainError("节目单尚不满足发布条件: " + "；".join(problems))
        rid = data.get("id") or _eid("release")
        # 同一节目+渠道再次发布：旧在版节目单被新版本替代（内容保留、仅加指引）。
        previous = next((r for r in self.rm.releases.values()
                         if r["status"] == "published"
                         and r["program_id"] == prog["id"]
                         and r["channel"] == channel), None)
        st = Staging(self)
        if previous is not None:
            payload_supersedes = previous["id"]
        else:
            payload_supersedes = None
        payload = {"id": rid, "program_id": prog["id"], "channel": channel,
                   "territory": territory,
                   "content": data.get("content", {"title": prog["title"],
                                                   "synopsis": prog.get("synopsis", "")}),
                   "snapshot": snapshot, "published_by": publisher,
                   "supersedes": payload_supersedes}
        st.add(f"release-{rid}", _event("RELEASE_PUBLISHED", f"release-{rid}", payload))
        if previous is not None:
            st.add(f"release-{previous['id']}",
                   _event("RELEASE_SUPERSEDED", f"release-{previous['id']}", {
                       "id": previous["id"], "new_release_id": rid,
                       "reason": data.get("change_reason", "同渠道重新发布")}))
        result = {"release_id": rid, "status": "published", "channel": channel,
                  "supersedes": payload_supersedes}
        return self._commit(st, data.get("idempotency_key"), result)

    def _build_snapshot(self, model: ReadModel, prog: dict[str, Any], channel: str,
                        territory: str) -> tuple[dict[str, Any], list[str]]:
        problems: list[str] = []
        slot_rows = []
        for sid in prog["slots"]:
            slot = model.slots[sid]
            if slot["status"] == "cancelled":
                continue
            if slot["status"] != "confirmed":
                problems.append(f"场次 {sid} 尚未确认")
                continue
            tech = model.techs.get(slot.get("tech_review_id", ""))
            if not tech or tech["status"] != "approved":
                problems.append(f"场次 {sid} 技术审查未通过")
            contract = next((c for c in model.contracts.values()
                             if c.get("slot_id") == sid
                             and c["artist_id"] == slot.get("performer_artist_id")), None)
            if slot.get("performer_artist_id") and not contract:
                problems.append(f"场次 {sid} 的表演者缺少演职合同")
            artist = next((a for a in prog["artists"]
                           if a["id"] == slot.get("performer_artist_id")), None)
            slot_rows.append({
                "slot_id": sid, "version": slot["version"],
                "venue_id": slot["venue_id"],
                "venue_name": model.venues[slot["venue_id"]]["name"],
                "start": slot["start"], "end": slot["end"], "genre": slot["genre"],
                "artist_id": slot.get("performer_artist_id"),
                "artist_name": artist["name"] if artist else None,
                "tech_review_id": slot.get("tech_review_id"),
                "tech_status": tech["status"] if tech else None,
                "settlement_ids": slot.get("settlement_ids", []),
            })
        rights_rows = [{"grant_id": gid, "version": model.rights[gid]["version"],
                        "works": model.rights[gid]["works"],
                        "channels": model.rights[gid]["channels"],
                        "territories": model.rights[gid]["territories"],
                        "valid_from": model.rights[gid]["valid_from"],
                        "valid_until": model.rights[gid]["valid_until"]}
                       for gid in prog["rights"]]
        contracts_rows = [{"contract_id": cid,
                           "artist_id": model.contracts[cid]["artist_id"],
                           "works": model.contracts[cid]["works"]}
                          for cid in prog["contracts"] if cid in model.contracts]
        for row in slot_rows:
            needed = {"works": prog["works"], "channels": [channel],
                      "territory": territory, "slot_start": row["start"],
                      "slot_end": row["end"]}
            if prog["works"] and all(D.scope_covers(model.rights[gid], needed)
                                     for gid in prog["rights"]):
                problems.append(
                    f"场次 {row['slot_id']} 缺少覆盖渠道 {channel}/{territory} 的授权")
        snapshot = {
            "program": {"id": prog["id"], "version": prog["version"],
                        "title": prog["title"], "genre": prog["genre"],
                        "artists": prog["artists"], "works": prog["works"]},
            "slots": slot_rows, "rights": rights_rows, "contracts": contracts_rows,
        }
        return snapshot, problems

    def _release_coverable(self, model: ReadModel, rel: dict[str, Any]) -> bool:
        """发布快照中的授权覆盖，在当前（可能已收窄的）授权下是否仍然成立。"""
        prog = model.programs[rel["program_id"]]
        for row in rel["snapshot"]["slots"]:
            needed = {"works": prog["works"], "channels": [rel["channel"]],
                      "territory": rel["territory"],
                      "slot_start": row["start"], "slot_end": row["end"]}
            ok = False
            for grow in rel["snapshot"]["rights"]:
                current = model.rights.get(grow["grant_id"])
                if current and not D.scope_covers(current, needed):
                    ok = True
                    break
            if prog["works"] and not ok:
                return False
        return True

    def ack_channel_receipt(self, release_id: str, data: dict[str, Any],
                            channel: str) -> dict[str, Any]:
        # release+receipt 派生幂等键：渠道重复回调绝不产生第二条回执，
        # 且必须在任何状态校验之前命中（旧节目单被替代后重复回调仍返回同一结果）。
        receipt_id = data.get("receipt_id")
        idem_key = f"receipt-{release_id}-{receipt_id}" if receipt_id else None
        self.store.raise_if_replayed(idem_key)
        rel = self.rm.releases.get(release_id)
        if not rel:
            raise D.NotFoundError(f"节目单不存在: {release_id}")
        if rel["channel"] != channel:
            raise D.DomainError("该节目单不属于本渠道，渠道只能回执自己的节目单")
        if not receipt_id:
            raise D.DomainError("缺少 receipt_id")
        payload = {"release_id": release_id, "receipt_id": receipt_id,
                   "channel": channel, "status": data.get("status", "delivered"),
                   "at": data.get("at", D.now_iso()), "detail": data.get("detail")}
        st = Staging(self)
        st.add(f"release-{release_id}",
               _event("CHANNEL_RECEIPT_ACKED", f"release-{release_id}", payload))
        result = {"release_id": release_id, "receipt_id": receipt_id,
                  "status": payload["status"]}
        return self._commit(st, idem_key, result)

    # -- 新版本：替代已发布节目单 -----------------------------------------

    def _supersede_in(self, st: Staging, program_id: str,
                      only_release_ids: list[str] | None, change_id: str,
                      reason: str, change_kind: str,
                      extra: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """在暂存批次中为受影响的在版节目单生成新版本，并给旧版本留替代指引。

        旧 RELEASE_PUBLISHED 的内容永不修改；新节目单是携带提交后快照的新聚合。
        """
        created: list[dict[str, Any]] = []
        model = st.scratch
        for rel in list(model.releases.values()):
            if rel["program_id"] != program_id or rel["status"] != "published":
                continue
            if only_release_ids is not None and rel["id"] not in only_release_ids:
                continue
            prog = self._program_in(model, program_id)
            snapshot, problems = self._build_snapshot(
                model, prog, rel["channel"], rel["territory"])
            new_id = _eid("release")
            st.add(f"release-{new_id}",
                   _event("RELEASE_PUBLISHED", f"release-{new_id}", {
                       "id": new_id, "program_id": program_id,
                       "channel": rel["channel"], "territory": rel["territory"],
                       "content": dict(rel["content"]), "snapshot": snapshot,
                       "supersedes": rel["id"], "change_id": change_id,
                       "published_by": "system-reconcile",
                       "reconcile_problems": problems}))
            st.add(f"release-{rel['id']}",
                   _event("RELEASE_SUPERSEDED", f"release-{rel['id']}", {
                       "id": rel["id"], "new_release_id": new_id,
                       "change_id": change_id, "reason": reason}))
            created.append({"old_release_id": rel["id"], "new_release_id": new_id,
                            "channel": rel["channel"], "ready": not problems,
                            "blocked_by": problems})
        return created

    # -- 替换表演者（产生新版本） -----------------------------------------

    def replace_performer(self, slot_id: str, data: dict[str, Any]) -> dict[str, Any]:
        slot = self._slot(slot_id)
        prog = self._program(slot["program_id"])
        new_artist_id = data.get("new_artist_id")
        if not new_artist_id:
            raise D.DomainError("缺少 new_artist_id")
        if new_artist_id not in {a["id"] for a in prog["artists"]}:
            raise D.DomainError("替换者不在节目演职名单内，请先修订节目名单")
        if new_artist_id == slot.get("performer_artist_id"):
            raise D.DomainError("新表演者与当前表演者相同")
        reason = data.get("reason", "替换表演者")
        change_id = _eid("change")
        st = Staging(self)
        st.add(f"slot-{slot_id}",
               _event("SLOT_PERFORMER_REPLACED", f"slot-{slot_id}", {
                   "id": slot_id, "new_artist_id": new_artist_id,
                   "previous_artist_id": slot.get("performer_artist_id"),
                   "reason": reason, "change_id": change_id}))
        affected = [r["id"] for r in st.scratch.releases.values()
                    if r["status"] == "published" and r["program_id"] == prog["id"]
                    and {s["slot_id"] for s in r["snapshot"]["slots"]} >= {slot_id}]
        new_releases = self._supersede_in(
            st, prog["id"], affected or None, change_id,
            reason=f"替换表演者: {reason}", change_kind="performer_replaced")
        result = {"slot_id": slot_id, "change_id": change_id,
                  "affected_releases": affected,
                  "new_versions": [r["new_release_id"] for r in new_releases]}
        st.add(f"change-{change_id}",
               _event("CHANGE_RECONCILED", f"change-{change_id}",
                      {"id": change_id, "kind": "performer_replaced", "reason": reason,
                       "slot_id": slot_id, "new_artist_id": new_artist_id,
                       "affected_releases": affected, "new_releases": new_releases}))
        return self._commit(st, data.get("idempotency_key"), result)

    # -- 露天临时限制与受限重排 -------------------------------------------

    def affected_slots(self, restriction_id: str) -> list[dict[str, Any]]:
        restriction = next((r for r in self.rm.restrictions if r["id"] == restriction_id), None)
        if not restriction:
            raise D.NotFoundError(f"限制不存在: {restriction_id}")
        hits = []
        for slot in self.rm.slots.values():
            if slot["status"] not in ("requested", "confirmed"):
                continue
            if D.restriction_hits(slot, self.rm.venues[slot["venue_id"]], restriction):
                hits.append({"slot_id": slot["id"], "venue_id": slot["venue_id"],
                             "program_id": slot["program_id"],
                             "start": slot["start"], "end": slot["end"],
                             "status": slot["status"]})
        return hits

    def replan_for_restriction(self, restriction_id: str,
                               data: dict[str, Any] | None) -> dict[str, Any]:
        data = data or {}
        hits = self.affected_slots(restriction_id)
        if data.get("dry_run"):
            return {"restriction_id": restriction_id, "dry_run": True,
                    "affected_slots": hits, "rescheduled": [], "new_versions": []}
        restriction = next(r for r in self.rm.restrictions if r["id"] == restriction_id)
        change_id = _eid("change")
        st = Staging(self)
        rescheduled: list[dict[str, Any]] = []
        # 在 scratch 上逐日改期，避免新档期与同批刚挪动的场次互撞
        for h in hits:
            slot = self._slot_in(st.scratch, h["slot_id"])
            new_start, new_end = self._shift_out_of_restriction(st.scratch, slot, restriction)
            st.add(f"slot-{slot['id']}",
                   _event("SLOT_RESCHEDULED", f"slot-{slot['id']}", {
                       "id": slot["id"], "previous_start": slot["start"],
                       "previous_end": slot["end"],
                       "new_start": new_start.isoformat(), "new_end": new_end.isoformat(),
                       "reason": f"露天临时限制: {restriction['reason']}",
                       "restriction_id": restriction_id, "change_id": change_id}))
            rescheduled.append({"slot_id": slot["id"], "new_start": new_start.isoformat(),
                                "new_end": new_end.isoformat()})
        affected_slot_ids = [h["slot_id"] for h in hits]
        touched_programs = {h["program_id"] for h in hits}
        new_releases: list[dict[str, Any]] = []
        for pid in touched_programs:
            live = [r["id"] for r in st.scratch.releases.values()
                    if r["status"] == "published" and r["program_id"] == pid
                    and {s["slot_id"] for s in r["snapshot"]["slots"]} & set(affected_slot_ids)]
            new_releases += self._supersede_in(
                st, pid, live, change_id,
                reason=f"露天临时限制改期: {restriction['reason']}",
                change_kind="restriction_reschedule")
        result = {"restriction_id": restriction_id, "change_id": change_id,
                  "affected_slots": affected_slot_ids,
                  "rescheduled": rescheduled,
                  "new_versions": [r["new_release_id"] for r in new_releases]}
        st.add(f"change-{change_id}",
               _event("CHANGE_RECONCILED", f"change-{change_id}",
                      {"id": change_id, "kind": "restriction_reschedule",
                       "restriction_id": restriction_id, "reason": restriction["reason"],
                       "affected_slots": affected_slot_ids,
                       "rescheduled": rescheduled, "new_releases": new_releases}))
        return self._commit(st, data.get("idempotency_key"), result)

    def _shift_out_of_restriction(self, model: ReadModel, slot: dict[str, Any],
                                  restriction: dict[str, Any]) -> tuple[datetime, datetime]:
        venue = model.venues[slot["venue_id"]]
        rules = D.GENRE_RULES[slot["genre"]]
        start = D.parse_iso(slot["start"])
        duration = D.parse_iso(slot["end"]) - start
        rwin = D.TimeWindow.from_iso(restriction["start"], restriction["end"])
        candidate = start + timedelta(days=1)
        for _ in range(30):
            win = D.TimeWindow(candidate, candidate + duration)
            if (not rwin.overlaps(win)
                    and D.within_business_hours(win, venue["business_open"],
                                                venue["business_close"])
                    and D.before_curfew(win, rules["curfew"])
                    and not self._overlaps_other_confirmed(model, slot, win)):
                return candidate, candidate + duration
            candidate += timedelta(days=1)
        raise D.DomainError(f"场次 {slot['id']} 在 30 天内找不到可改期的空档")

    def _overlaps_other_confirmed(self, model: ReadModel, slot: dict[str, Any],
                                  win: D.TimeWindow) -> bool:
        for other in model.slots.values():
            if other["id"] == slot["id"] or other["venue_id"] != slot["venue_id"]:
                continue
            if other["status"] != "confirmed":
                continue
            if win.overlaps(D.TimeWindow.from_iso(other["start"], other["end"])):
                return True
        return False

    # -- 执行结果 ---------------------------------------------------------

    def record_execution(self, slot_id: str, data: dict[str, Any]) -> dict[str, Any]:
        idem_key = f"exec-{slot_id}"
        self.store.raise_if_replayed(idem_key)
        slot = self._slot(slot_id)
        if slot["status"] != "confirmed":
            raise D.ConflictStateError("只有已确认场次可以登记执行结果")
        status = data.get("status")
        if status not in ("performed", "interrupted", "no_show"):
            raise D.DomainError("status 必须是 performed/interrupted/no_show")
        st = Staging(self)
        st.add(f"slot-{slot_id}", _event("PERFORMANCE_EXECUTED", f"slot-{slot_id}", {
            "slot_id": slot_id, "status": status,
            "actual_start": data.get("actual_start", slot["start"]),
            "actual_end": data.get("actual_end", slot["end"]),
            "note": data.get("note", ""),
            "recorded_by": data.get("recorded_by", "coordinator")}))
        result = {"slot_id": slot_id, "execution_status": status}
        return self._commit(st, f"exec-{slot_id}", result)

    # -- 追溯与视图 -------------------------------------------------------

    def release_trace(self, release_id: str) -> dict[str, Any]:
        rel = self.rm.releases.get(release_id)
        if not rel:
            raise D.NotFoundError(f"节目单不存在: {release_id}")
        slot_traces = []
        for row in rel["snapshot"]["slots"]:
            slot = self.rm.slots.get(row["slot_id"])
            tech = self.rm.techs.get(row["tech_review_id"]) if slot else None
            slot_traces.append({
                "snapshot": row,
                "current": None if not slot else {
                    "version": slot["version"], "status": slot["status"],
                    "start": slot["start"], "end": slot["end"],
                    "performer_artist_id": slot.get("performer_artist_id"),
                    "performer_history": slot.get("performer_history", []),
                    "reschedules": slot.get("reschedules", []),
                    "execution": slot.get("execution"),
                    "settlements": [self.rm.settlements[s]
                                    for s in slot.get("settlement_ids", [])
                                    if s in self.rm.settlements],
                },
                "tech_review": None if not tech else {
                    "id": tech["id"], "version": tech["version"], "status": tech["status"],
                    "matrix_problems": tech.get("matrix_problems"),
                    "review_note": tech.get("review_note")},
            })
        right_traces = []
        for grow in rel["snapshot"]["rights"]:
            current = self.rm.rights.get(grow["grant_id"])
            right_traces.append({"snapshot": grow,
                                 "current_version": None if not current else current["version"],
                                 "narrowed": bool(current and current["version"] > grow["version"]),
                                 "history": current["history"] if current else []})
        return {"release_id": release_id, "status": rel["status"], "channel": rel["channel"],
                "territory": rel["territory"], "published_at": rel["published_at"],
                "content": rel["content"], "snapshot": rel["snapshot"],
                "receipts": rel["receipts"],
                "superseded_by": rel.get("superseded_by"),
                "superseded_reason": rel.get("superseded_reason"),
                "supersedes": rel.get("supersedes"),
                "version_chain": self._version_chain(release_id),
                "slots": slot_traces, "rights": right_traces}

    def _version_chain(self, release_id: str) -> list[dict[str, Any]]:
        chain = []
        cur = self.rm.releases.get(release_id)
        while cur:
            chain.append({"release_id": cur["id"], "status": cur["status"],
                          "channel": cur["channel"], "published_at": cur["published_at"],
                          "supersedes": cur.get("supersedes")})
            cur = self.rm.releases.get(cur["supersedes"]) if cur.get("supersedes") else None
        return chain

    def venue_view(self, venue_id: str) -> dict[str, Any]:
        """场地方最小可见：仅本场地、完成职责所需的字段。"""
        venue = self.rm.venues.get(venue_id)
        if not venue:
            raise D.NotFoundError(f"场地不存在: {venue_id}")
        rows = []
        for slot in self.rm.slots.values():
            if slot["venue_id"] != venue_id:
                continue
            prog = self.rm.programs[slot["program_id"]]
            tech = self.rm.techs.get(slot.get("tech_review_id", ""))
            rows.append({
                "slot_id": slot["id"], "start": slot["start"], "end": slot["end"],
                "status": slot["status"], "genre": slot["genre"],
                "program_id": prog["id"], "program_title": prog["title"],
                "performer": next((a["name"] for a in prog["artists"]
                                   if a["id"] == slot.get("performer_artist_id")), None),
                "load_in_minutes": tech["plan"].get("load_in_minutes") if tech else None,
                "power_kw": tech["plan"].get("power_kw") if tech else None,
                "tech_status": tech["status"] if tech else None,
                "reschedules": slot.get("reschedules", []),
                "execution": slot.get("execution", {}).get("status"),
            })
        return {"venue": {"id": venue["id"], "name": venue["name"],
                          "business_open": venue["business_open"],
                          "business_close": venue["business_close"]},
                "slots": rows}

    def channel_view(self, channel: str) -> dict[str, Any]:
        live, superseded = [], []
        for rel in self.rm.releases.values():
            if rel["channel"] != channel:
                continue
            row = {"release_id": rel["id"], "program_id": rel["program_id"],
                   "status": rel["status"], "published_at": rel["published_at"],
                   "receipts": rel["receipts"], "superseded_by": rel.get("superseded_by"),
                   "supersedes": rel.get("supersedes"),
                   "ready": not rel.get("reconcile_problems"),
                   "blocked_by": rel.get("reconcile_problems", [])}
            (live if rel["status"] == "published" else superseded).append(row)
        return {"channel": channel, "live": live, "superseded": superseded}

    def coordinator_overview(self) -> dict[str, Any]:
        return {
            "venues": list(self.rm.venues.values()),
            "restrictions": self.rm.restrictions,
            "programs": list(self.rm.programs.values()),
            "slots": list(self.rm.slots.values()),
            "rights": list(self.rm.rights.values()),
            "contracts": list(self.rm.contracts.values()),
            "tech_reviews": list(self.rm.techs.values()),
            "releases": list(self.rm.releases.values()),
            "settlements": list(self.rm.settlements.values()),
            "changes": list(self.rm.changes.values()),
        }
