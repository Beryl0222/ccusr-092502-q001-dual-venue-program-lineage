"""可重放的 HTTP 级自动化测试。

每个用例在临时端口启动真实 HTTP 服务（ThreadingHTTPServer + JSONL 日志），
通过 urllib 走完整网络栈。重放用例还会在同一日志上重启服务，校验投影一致。
"""
from __future__ import annotations

import json
import os
import socket
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from src.httpapi import build_service, create_server, seed_venues
from src.service import REHEARSAL_RULES

COORD = {"X-Role": "coordinator"}
PUB = {"X-Role": "public"}
GW = {"X-Role": "gateway"}


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Api:
    def __init__(self, base: str) -> None:
        self.base = base

    def call(self, method: str, path: str, body: dict | None = None, headers: dict | None = None,
             idem: str | None = None) -> tuple[int, dict]:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        req.add_header("Content-Type", "application/json; charset=utf-8")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        if idem:
            req.add_header("Idempotency-Key", idem)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def get(self, path: str, headers: dict | None = None) -> tuple[int, dict]:
        return self.call("GET", path, None, headers)

    def post(self, path: str, body: dict, headers: dict | None = None, idem: str | None = None) -> tuple[int, dict]:
        return self.call("POST", path, body, headers, idem)


class ServerHarness:
    def __init__(self, log_path: str | None = None, seed: bool = True) -> None:
        self.tmp: tempfile.TemporaryDirectory[str] | None = None
        if log_path is None:
            self.tmp = tempfile.TemporaryDirectory()
            log_path = os.path.join(self.tmp.name, "events.jsonl")
        self.log_path = log_path
        self.store, self.service = build_service(log_path)
        if seed:
            seed_venues(self.service)
        self.port = free_port()
        self.httpd = create_server(self.service, "127.0.0.1", self.port)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.api = Api(f"http://127.0.0.1:{self.port}")

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=3)
        self.store.close()
        if self.tmp:
            self.tmp.cleanup()


class HttpApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.h = ServerHarness()
        self.api = self.h.api

    def tearDown(self) -> None:
        self.h.stop()

    # ---------- 夹具 ----------
    def make_ready_program(
        self, pid: str = "p1", genre: str = "opera", venue: str = "yuyuan-water-stage",
        channels: list[str] | None = None, fee: int = 50000,
    ) -> str:
        """提案并打通合同/技术/版权，返回 venue。"""
        channels = channels or ["billboard"]
        lineup = [{
            "entry_id": "e1", "performer_id": "artist-1", "performer_name": "梨园社",
            "genre": genre, "act_title": "牡丹亭·惊梦" if genre == "opera" else "电音水秀",
        }]
        st, body = self.api.post("/programs", {
            "program_id": pid, "title": "非遗音乐季开幕场", "fee": fee, "lineup": lineup}, headers=COORD)
        self.assertEqual(st, 201, body)
        caps = REHEARSAL_RULES[genre]["capabilities"]
        self.api.post(f"/programs/{pid}/contracts", {
            "performer_ids": ["artist-1"], "venues": [venue]}, headers=COORD)
        st, body = self.api.post(f"/programs/{pid}/tech-reviews", {
            "reviewer": "tech-a", "venue_id": venue, "open_air": True,
            "rehearsal_hours": REHEARSAL_RULES[genre]["rehearsal_hours"],
            "capabilities": caps}, headers=COORD)
        self.assertEqual(st, 201, body)
        self.assertEqual(body["tech_review"]["status"], "approved")
        self.api.post(f"/programs/{pid}/rights", {
            "work_id": "work-1", "scope": {"channels": channels, "venues": [venue], "uses": ["live", "promo"]}},
            headers=COORD)
        return venue

    def hold_and_confirm(self, slot: str, pid: str, venue: str, start: str, end: str) -> dict:
        st, body = self.api.post("/slots/holds", {
            "slot_id": slot, "program_id": pid, "venue_id": venue,
            "starts_at": start, "ends_at": end}, headers=COORD)
        self.assertEqual(st, 201, body)
        st, body = self.api.post(f"/slots/{slot}/confirm", {"program_id": pid}, headers=COORD)
        self.assertEqual(st, 200, body)
        return body

    # ---------- 测试 ----------
    def test_health(self) -> None:
        st, body = self.api.get("/health")
        self.assertEqual((st, body["status"]), (200, "ok"))

    def test_gate_blocks_confirmation_until_all_clear(self) -> None:
        venue = "yuyuan-water-stage"
        # 提案后不打通任何条件
        lineup = [{"entry_id": "e1", "performer_id": "a1", "performer_name": "社班",
                   "genre": "opera", "act_title": "x"}]
        st, _ = self.api.post("/programs", {"program_id": "pg", "title": "t", "lineup": lineup}, headers=COORD)
        self.assertEqual(st, 201)
        st, _ = self.api.post("/slots/holds", {
            "slot_id": "s1", "program_id": "pg", "venue_id": venue,
            "starts_at": "2026-10-01T19:00:00+08:00", "ends_at": "2026-10-01T21:00:00+08:00"}, headers=COORD)
        self.assertEqual(st, 201)
        st, body = self.api.post("/slots/s1/confirm", {"program_id": "pg"}, headers=COORD)
        self.assertEqual(st, 422)
        self.assertEqual(body["error"]["code"], "NOT_READY")
        self.assertIn("合同", body["error"]["message"] + "".join([]))

    def test_opera_vs_electronic_rehearsal_requirements(self) -> None:
        # 戏曲 6 小时+固定扩声；电子 3 小时+低音承载——要求不同，不能互换通过。
        for genre, hours in (("opera", 3), ("electronic", 1)):
            pid = f"p-{genre}-bad"
            self.api.post("/programs", {"program_id": pid, "title": genre, "lineup": [{
                "entry_id": "e", "performer_id": "a", "performer_name": "n",
                "genre": genre, "act_title": "t"}]}, headers=COORD)
            st, body = self.api.post(f"/programs/{pid}/tech-reviews", {
                "reviewer": "r", "rehearsal_hours": hours, "capabilities": []}, headers=COORD)
            self.assertEqual(st, 201)
            self.assertEqual(body["tech_review"]["status"], "rejected")
            self.assertTrue(any("排练时长" in r for r in body["tech_review"]["reasons"]))
            self.assertTrue(any("缺少能力" in r for r in body["tech_review"]["reasons"]))
        # 达标后通过
        self.make_ready_program("p-opera-ok", "opera")
        st, prog = self.api.get("/programs/p-opera-ok", headers=COORD)
        self.assertEqual(st, 200)
        self.assertEqual(prog["requirements"]["rehearsal_hours"], 6)

    def test_business_hours_and_overlap(self) -> None:
        self.make_ready_program("p1")
        st, body = self.api.post("/slots/holds", {
            "slot_id": "s-late", "program_id": "p1", "venue_id": "yuyuan-water-stage",
            "starts_at": "2026-10-02T21:30:00+08:00", "ends_at": "2026-10-02T23:00:00+08:00"}, headers=COORD)
        self.assertEqual((st, body["error"]["code"]), (422, "OUTSIDE_BUSINESS_HOURS"))
        self.hold_and_confirm("s1", "p1", "yuyuan-water-stage",
                              "2026-10-03T19:00:00+08:00", "2026-10-03T21:00:00+08:00")
        # 同时段另一场地不受影响
        self.make_ready_program("p2", genre="electronic", venue="bund-terrace")
        st, _ = self.api.post("/slots/holds", {
            "slot_id": "s2", "program_id": "p2", "venue_id": "bund-terrace",
            "starts_at": "2026-10-03T19:00:00+08:00", "ends_at": "2026-10-03T21:00:00+08:00"}, headers=COORD)
        self.assertEqual(st, 201)
        # 同场地重叠的第二场无法确认
        self.api.post("/slots/holds", {
            "slot_id": "s3", "program_id": "p2", "venue_id": "yuyuan-water-stage",
            "starts_at": "2026-10-03T20:00:00+08:00", "ends_at": "2026-10-03T22:00:00+08:00"}, headers=COORD)
        # p2 的版权/合同不覆盖 yuyuan，先补覆盖，确保真正撞在时间冲突上
        self.api.post("/programs/p2/contracts", {
            "contract_id": "c-p2-yy", "performer_ids": ["artist-1"], "venues": ["yuyuan-water-stage"]},
            headers=COORD)
        self.api.post("/programs/p2/rights", {
            "rights_id": "r-p2-yy", "work_id": "w", "scope": {"channels": ["billboard"],
            "venues": ["yuyuan-water-stage"]}}, headers=COORD)
        st, body = self.api.post(f"/slots/s3/confirm", {"program_id": "p2"}, headers=COORD)
        self.assertEqual((st, body["error"]["code"]), (409, "SLOT_OVERLAP"))

    def test_concurrent_confirmation_single_winner(self) -> None:
        self.make_ready_program("pa")
        self.make_ready_program("pb", genre="electronic")
        window = ("2026-10-04T19:00:00+08:00", "2026-10-04T21:00:00+08:00")
        for pid in ("pa", "pb"):
            st, _ = self.api.post("/slots/holds", {
                "slot_id": "srace", "program_id": pid, "venue_id": "yuyuan-water-stage",
                "starts_at": window[0], "ends_at": window[1]}, headers=COORD)
            self.assertEqual(st, 201)
        results: list[tuple[int, dict]] = []
        barrier = threading.Barrier(2)

        def attempt(pid: str) -> None:
            barrier.wait()
            results.append(self.api.post(f"/slots/srace/confirm", {"program_id": pid},
                                         headers=COORD, idem=f"confirm-{pid}"))

        t1 = threading.Thread(target=attempt, args=("pa",))
        t2 = threading.Thread(target=attempt, args=("pb",))
        t1.start(); t2.start(); t1.join(); t2.join()
        codes = sorted(r[0] for r in results)
        self.assertEqual(codes, [200, 409])
        winner = next(r[1]["program_id"] for r in results if r[0] == 200)
        # 败者重试仍然失败——唯一生效结果
        st, body = self.api.post("/slots/srace/confirm", {"program_id": "pb" if winner == "pa" else "pa"},
                                 headers=COORD)
        self.assertEqual((st, body["error"]["code"]), (409, "SLOT_TAKEN"))
        st, slot_events = self.api.get("/events?stream=slot-srace", headers=COORD)
        self.assertEqual(st, 200)
        confirms = [e for e in slot_events["events"] if e["event_type"] == "SLOT_CONFIRMED"]
        self.assertEqual(len(confirms), 1)

    def test_published_content_immutable_and_new_version_chain(self) -> None:
        self.make_ready_program("p1")
        self.hold_and_confirm("s1", "p1", "yuyuan-water-stage",
                              "2026-10-05T19:00:00+08:00", "2026-10-05T21:00:00+08:00")
        content = {"title": "开幕之夜", "blurb": "水上戏台实景"}
        st, pub = self.api.post("/releases", {
            "program_id": "p1", "channel": "billboard", "content": content}, headers=COORD)
        self.assertEqual(st, 201, pub)
        # 同版本静默改文案被拒绝
        st, body = self.api.post("/releases", {
            "program_id": "p1", "channel": "billboard",
            "content": {"title": "被篡改的标题", "blurb": "x"}}, headers=COORD)
        self.assertEqual((st, body["error"]["code"]), (422, "CONTENT_IMMUTABLE"))
        # 同内容重放（幂等键）返回首次发布事件
        st, again = self.api.post("/releases", {
            "program_id": "p1", "channel": "billboard", "content": content},
            headers=COORD, idem="pub-1")
        self.assertEqual(st, 201)
        self.assertEqual(again["event_id"], pub["event_id"])
        # 节目出新版本 -> 渠道发布形成新版本事件而非覆盖
        st, rev = self.api.post("/programs/p1/revisions", {"change": "program_note", "reason": "增补谢幕"},
                                headers=COORD)
        self.assertEqual(st, 201)
        self.assertEqual(rev["revision"], 2)
        st, pub2 = self.api.post("/releases", {
            "program_id": "p1", "channel": "billboard", "revision": 2,
            "content": {"title": "开幕之夜（含谢幕）", "blurb": "水上戏台实景"}}, headers=COORD)
        self.assertEqual(st, 201)
        self.assertNotEqual(pub2["event_id"], pub["event_id"])
        st, trace = self.api.get(f"/releases/{pub2['release_id']}", headers=COORD)
        self.assertEqual(st, 200)
        self.assertEqual(trace["revision"], 2)
        # 同一发布流保留两次发布事件，构成版本链
        st, rel_events = self.api.get(f"/events?stream={pub2['release_id']}", headers=COORD)
        pubs = [e for e in rel_events["events"] if e["event_type"] == "RELEASE_PUBLISHED"]
        self.assertEqual([e["payload"]["revision"] for e in pubs], [1, 2])
        # 公开节目单只出现该渠道一次，且指向最新版本
        st, listing = self.api.get("/public/programs", headers=PUB)
        self.assertEqual(st, 200)
        items = [i for i in listing["programs"] if i["release_id"] == pub2["release_id"]]
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["revision"], 2)

    def test_replace_performer_marks_only_affected_slot(self) -> None:
        # 两个场地各一场已确认；替换表演者后合同不再覆盖新表演者——只影响两场（合同场地维度），
        # 而未受波及的另一个节目场次完全不动。
        self.make_ready_program("p1", venue="yuyuan-water-stage", channels=["billboard"])
        self.api.post("/programs/p1/contracts", {
            "contract_id": "c2", "performer_ids": ["artist-1"], "venues": ["bund-terrace"]}, headers=COORD)
        self.api.post("/programs/p1/rights", {
            "rights_id": "r2", "work_id": "w",
            "scope": {"channels": ["billboard"], "venues": ["bund-terrace"]}}, headers=COORD)
        self.hold_and_confirm("sy", "p1", "yuyuan-water-stage",
                              "2026-10-06T19:00:00+08:00", "2026-10-06T20:30:00+08:00")
        self.hold_and_confirm("sb", "p1", "bund-terrace",
                              "2026-10-07T19:00:00+08:00", "2026-10-07T20:30:00+08:00")
        # 无关节目场次
        self.make_ready_program("p2", genre="electronic", venue="bund-terrace", channels=["billboard"])
        self.hold_and_confirm("sother", "p2", "bund-terrace",
                              "2026-10-08T19:00:00+08:00", "2026-10-08T20:30:00+08:00")
        st, body = self.api.post("/programs/p1/performers/replace", {
            "performer_id": "artist-1",
            "replacement": {"performer_id": "artist-9", "performer_name": "特邀名角", "genre": "opera"}},
            headers=COORD)
        self.assertEqual(st, 201, body)
        affected = {x["slot_id"] for x in body["affected_slots"]}
        self.assertEqual(affected, {"sy", "sb"})
        # 无关场次不动
        st, other = self.api.get("/programs/p2", headers=COORD)
        self.assertEqual(other["slots"][0]["status"], "confirmed")

    def test_open_air_reschedule_is_scoped_to_single_slot(self) -> None:
        # 突发降雨：只改受影响的那一场，另一场时间不变。
        self.make_ready_program("p1", venue="yuyuan-water-stage")
        self.hold_and_confirm("s1", "p1", "yuyuan-water-stage",
                              "2026-10-09T19:00:00+08:00", "2026-10-09T20:30:00+08:00")
        self.make_ready_program("p2", genre="electronic", venue="bund-terrace")
        self.hold_and_confirm("s2", "p2", "bund-terrace",
                              "2026-10-09T19:00:00+08:00", "2026-10-09T20:30:00+08:00")
        st, body = self.api.post("/slots/s1/reschedule", {
            "starts_at": "2026-10-10T19:00:00+08:00", "ends_at": "2026-10-10T20:30:00+08:00",
            "reason": "临时露天降雨"}, headers=COORD)
        self.assertEqual(st, 200, body)
        st, s1 = self.api.get("/events?stream=slot-s1", headers=COORD)
        self.assertTrue(any(e["event_type"] == "SLOT_RESCHEDULED" for e in s1["events"]))
        st, s2 = self.api.get("/events?stream=slot-s2", headers=COORD)
        self.assertFalse(any(e["event_type"] == "SLOT_RESCHEDULED" for e in s2["events"]))
        # 改期事件保留版本链关联
        ev = [e for e in s1["events"] if e["event_type"] == "SLOT_RESCHEDULED"][0]
        self.assertEqual(ev["payload"]["reason"], "临时露天降雨")

    def test_duplicate_callbacks_do_not_double_charge_or_double_receipt(self) -> None:
        self.make_ready_program("p1", fee=68000)
        self.hold_and_confirm("s1", "p1", "yuyuan-water-stage",
                              "2026-10-11T19:00:00+08:00", "2026-10-11T21:00:00+08:00")
        self.api.post("/releases", {"program_id": "p1", "channel": "billboard",
                                    "content": {"title": "t", "blurb": "b"}}, headers=COORD)
        rid = "release-p1-billboard"
        payload = {"callback_id": "cb-77", "status": "delivered", "detail": "ok"}
        st, r1 = self.api.post(f"/releases/{rid}/receipts", payload, headers=GW)
        self.assertEqual((st, r1["replay"]), (200, False))
        st, r2 = self.api.post(f"/releases/{rid}/receipts", payload, headers=GW)
        self.assertEqual((st, r2["replay"]), (200, True))
        self.assertEqual(r1["event_id"], r2["event_id"])
        # 支付回调：重复不重复计费
        pay = {"callback_id": "pay-77", "amount": 68000}
        st, b1 = self.api.post("/slots/s1/settle", pay, headers=GW)
        self.assertEqual((st, b1["status"], b1["replay"]), (200, "settled", False))
        st, b2 = self.api.post("/slots/s1/settle", pay, headers=GW)
        self.assertEqual((st, b2["replay"], b2["event_id"]), (200, True, b1["event_id"]))
        st, ev = self.api.get("/events?stream=bill-s1", headers=COORD)
        self.assertEqual(len([e for e in ev["events"] if e["event_type"] == "BILLING_SETTLED"]), 1)

    def test_trace_from_public_listing_to_approvals_and_execution(self) -> None:
        self.make_ready_program("p1", fee=42000, channels=["billboard"])
        self.hold_and_confirm("s1", "p1", "yuyuan-water-stage",
                              "2026-10-12T19:00:00+08:00", "2026-10-12T21:00:00+08:00")
        self.api.post("/releases", {"program_id": "p1", "channel": "billboard",
                                    "content": {"title": "溯源场", "blurb": "b"}}, headers=COORD)
        self.api.post("/slots/s1/settle", {"callback_id": "pay-1", "amount": 42000}, headers=GW)
        st, listing = self.api.get("/public/programs", headers=PUB)
        release_id = listing["programs"][0]["release_id"]
        st, trace = self.api.get(f"/releases/{release_id}", headers=COORD)
        self.assertEqual(st, 200)
        self.assertTrue(trace["tech_review"])
        self.assertEqual(trace["tech_review"]["status"], "approved")
        self.assertTrue(trace["contracts"])
        self.assertTrue(trace["rights"])
        self.assertEqual(trace["performances"][0]["billing"]["status"], "settled")

    def test_role_minimum_visibility(self) -> None:
        self.make_ready_program("p1", fee=42000)
        self.hold_and_confirm("s1", "p1", "yuyuan-water-stage",
                              "2026-10-13T19:00:00+08:00", "2026-10-13T21:00:00+08:00")
        self.api.post("/releases", {"program_id": "p1", "channel": "billboard",
                                    "content": {"title": "公开", "blurb": "b"}}, headers=COORD)
        # 公开节目单不含费用/合同/版权
        st, listing = self.api.get("/public/programs", headers=PUB)
        self.assertEqual(st, 200)
        item = listing["programs"][0]
        self.assertNotIn("fee", item)
        self.assertNotIn("contracts", item)
        # 公众不能读统筹视图
        st, body = self.api.get("/programs/p1", headers=PUB)
        self.assertEqual((st, body["error"]["code"]), (403, "FORBIDDEN"))
        # 场地方只能看自己场地，且只见装台要求，不见费用/版权
        st, sched = self.api.get("/venues/yuyuan-water-stage/schedule",
                                 headers={"X-Role": "venue", "X-Venue-Id": "yuyuan-water-stage"})
        self.assertEqual(st, 200)
        row = sched["schedule"][0]
        self.assertIn("setup", row)
        self.assertNotIn("fee", row)
        st, body = self.api.get("/venues/bund-terrace/schedule",
                                headers={"X-Role": "venue", "X-Venue-Id": "yuyuan-water-stage"})
        self.assertEqual((st, body["error"]["code"]), (403, "FORBIDDEN"))
        # 无角色默认公众，管理接口拒绝
        st, body = self.api.post("/programs", {"program_id": "x", "title": "t", "lineup": []})
        self.assertEqual((st, body["error"]["code"]), (403, "FORBIDDEN"))

    def test_narrow_rights_creates_revision_and_retracks(self) -> None:
        self.make_ready_program("p1", channels=["billboard", "social"])
        st, prog = self.api.get("/programs/p1", headers=COORD)
        rights_id = [r["rights_id"] for r in prog["rights"].values()]
        # 取覆盖 social 的授权
        st, body = self.api.post(f"/programs/p1/rights/{rights_id[0]}/narrow", {
            "channels": ["billboard"], "venues": ["yuyuan-water-stage"], "reason": "渠道授权到期"},
            headers=COORD)
        self.assertEqual(st, 201, body)
        self.assertEqual(body["revision"], 2)
        st, prog = self.api.get("/programs/p1", headers=COORD)
        narrowed = [r for r in prog["rights"].values() if r["status"] == "narrowed"]
        self.assertEqual(len(narrowed), 1)
        # 旧版本可追溯，新版本为当前
        self.assertEqual(prog["current_revision"], 2)
        chain = [x["change"] for x in prog["lineage"]]
        self.assertEqual(chain, ["proposed", "rights_narrowed"])

    def test_idempotency_key_replays_first_result(self) -> None:
        payload = {"venue_id": "v9", "name": "第三空间", "stage_type": "indoor",
                   "business_hours": {"open": "09:00", "close": "22:00"}}
        st, r1 = self.api.post("/admin/venues", payload, headers=COORD, idem="venue-9")
        self.assertEqual(st, 201)
        st, r2 = self.api.post("/admin/venues", payload, headers=COORD, idem="venue-9")
        self.assertEqual((st, r2["event_id"]), (201, r1["event_id"]))


class ReplayTest(unittest.TestCase):
    """关闭服务并用同一 JSONL 重启：投影与幂等语义必须可重放。"""

    def test_state_and_idempotency_replay_across_restart(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        log = os.path.join(tmp.name, "events.jsonl")

        def boot(seed: bool = True) -> ServerHarness:
            return ServerHarness(log_path=log, seed=seed)

        h = boot()
        api = h.api
        # 直接走 HTTP 建一个完整链路
        st, _ = api.post("/programs", {"program_id": "p1", "title": "重启场", "fee": 1000, "lineup": [{
            "entry_id": "e", "performer_id": "artist-1", "performer_name": "社",
            "genre": "opera", "act_title": "t"}]}, headers={"X-Role": "coordinator"})
        self.assertEqual(st, 201)
        api.post("/programs/p1/contracts", {"performer_ids": ["artist-1"],
                                             "venues": ["yuyuan-water-stage"]}, headers=COORD)
        api.post("/programs/p1/tech-reviews", {"reviewer": "r", "rehearsal_hours": 6,
                                               "capabilities": REHEARSAL_RULES["opera"]["capabilities"]},
                 headers=COORD)
        api.post("/programs/p1/rights", {"work_id": "w", "scope": {"channels": ["billboard"],
                                                                   "venues": ["yuyuan-water-stage"]}},
                 headers=COORD)
        api.post("/slots/holds", {"slot_id": "s1", "program_id": "p1", "venue_id": "yuyuan-water-stage",
                                  "starts_at": "2026-10-14T19:00:00+08:00",
                                  "ends_at": "2026-10-14T21:00:00+08:00"}, headers=COORD)
        api.post("/slots/s1/confirm", {"program_id": "p1"}, headers=COORD)
        st, settle1 = api.post("/slots/s1/settle", {"callback_id": "pay-x", "amount": 1000}, headers=GW)
        self.assertEqual((st, settle1["replay"]), (200, False))
        before = api.get("/events", headers=COORD)[1]
        h.stop()

        # 等待端口释放
        time.sleep(0.2)
        h2 = boot()
        api2 = h2.api
        after = api2.get("/events", headers=COORD)[1]
        self.assertEqual(len(before["events"]), len(after["events"]))
        self.assertEqual(
            [e["event_id"] for e in before["events"]],
            [e["event_id"] for e in after["events"]],
        )
        st, prog = api2.get("/programs/p1", headers=COORD)
        self.assertEqual(prog["current_revision"], 1)
        self.assertEqual(prog["slots"][0]["status"], "confirmed")
        self.assertEqual(prog["slots"][0]["evidence"]["billing"]["status"], "settled")
        # 幂等回执同样重放：重复回调不再产生事件
        st, settle2 = api2.post("/slots/s1/settle", {"callback_id": "pay-x", "amount": 1000}, headers=GW)
        self.assertEqual((st, settle2["replay"], settle2["event_id"]),
                         (200, True, settle1["event_id"]))
        again = api2.get("/events", headers=COORD)[1]
        self.assertEqual(len(after["events"]), len(again["events"]))
        h2.stop()


if __name__ == "__main__":
    unittest.main()
