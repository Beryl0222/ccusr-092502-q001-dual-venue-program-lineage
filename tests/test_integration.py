"""端到端可重放测试：覆盖版本链、并发确认、幂等回调、受限重排与最小可见。"""
from __future__ import annotations

import json
import threading
import unittest

from tests.support import (COORD, PROD_A, PROD_B, VENUE1, VENUE2, WEB, WECHAT,
                          Harness)


def create_venues(h: Harness) -> None:
    # 豫园水上舞台：露天、有扮戏间、供电一般
    status, _, body = h.request("POST", "/venues", COORD, {
        "id": "v-yuyuan", "name": "豫园水上舞台",
        "business_open": "10:00", "business_close": "22:00",
        "open_air": True, "power_kw": 120, "noise_limit_db": 90,
        "has_quiet_room": True, "settlement_amount": 12000})
    assert status == 201, body
    # 外滩露台：露天、大功率、无安静间
    status, _, body = h.request("POST", "/venues", COORD, {
        "id": "v-bund", "name": "外滩露台",
        "business_open": "11:00", "business_close": "23:00",
        "open_air": True, "power_kw": 300, "noise_limit_db": 100,
        "has_quiet_room": False, "settlement_amount": 20000})
    assert status == 201, body


def ready_program(h: Harness, *, program="p1", venue="v-yuyuan", slot="s1",
                  start="2026-10-03T19:00:00+08:00",
                  end="2026-10-03T20:30:00+08:00", genre="opera",
                  works=("w-mudan",), artist=("a1", "张三"),
                  tech_plan=None, auto_approve=True) -> None:
    """提案 -> 场次 -> 技审 -> 授权 -> 合同，使场次达到可确认状态。"""
    aid, aname = artist
    plan = tech_plan or {"rehearsal_hours": 2, "power_kw": 10,
                         "sound_db": 80, "load_in_minutes": 60}
    s, _, b = h.request("POST", "/programs", PROD_A, {
        "id": program, "title": "牡丹亭·电音夜", "genre": genre,
        "synopsis": "戏曲与电子的跨界现场", "works": list(works),
        "artists": [{"id": aid, "name": aname},
                    {"id": "a2", "name": "李四"}]})
    assert s == 201, b
    s, _, b = h.request("POST", f"/programs/{program}/slots", PROD_A, {
        "id": slot, "venue_id": venue, "start": start, "end": end,
        "performer_artist_id": aid})
    assert s == 201, b
    s, _, b = h.request("POST", f"/slots/{slot}/tech-reviews", PROD_A,
                        {"plan": plan})
    assert s == 201, b
    if auto_approve:
        s, _, b = h.request("POST", f"/slots/{slot}/tech-review/decision", COORD,
                            {"decision": "approved"})
        assert s == 200, b
    s, _, b = h.request("POST", f"/programs/{program}/rights", PROD_A, {
        "id": f"rights-{program}", "works": list(works),
        "channels": ["live", "web", "wechat"], "territories": ["CN"],
        "valid_from": "2026-09-01T00:00:00+08:00",
        "valid_until": "2026-12-31T00:00:00+08:00"})
    assert s == 201, b
    s, _, b = h.request("POST", f"/slots/{slot}/contracts", PROD_A, {
        "id": f"contract-{slot}", "artist_id": aid, "works": list(works)})
    assert s == 201, b


class VersionChainTest(unittest.TestCase):
    def setUp(self) -> None:
        self.h = Harness()
        create_venues(self.h)

    def tearDown(self) -> None:
        self.h.close()

    def test_full_lineage_from_public_program_to_execution(self) -> None:
        h = self.h
        ready_program(h)
        # 确认
        s, _, conf = h.request("POST", "/slots/s1/confirm", PROD_A)
        self.assertEqual(s, 201, conf)
        self.assertEqual(conf["status"], "confirmed")
        # 发布节目单
        s, _, rel = h.request("POST", "/releases", PROD_A,
                              {"id": "rel1", "program_id": "p1", "channel": "web"})
        self.assertEqual(s, 201, rel)
        # 渠道回执
        s, _, rcp = h.request("POST", "/releases/rel1/receipts", WEB,
                              {"receipt_id": "rcp-1", "status": "delivered"})
        self.assertEqual(s, 201, rcp)
        # 执行结果
        s, _, exe = h.request("POST", "/slots/s1/execution", COORD,
                              {"status": "performed"})
        self.assertEqual(s, 201, exe)
        # 统筹可从任一公开节目单追到审批、授权、执行与结算
        s, _, trace = h.request("GET", "/releases/rel1/trace", COORD)
        self.assertEqual(s, 200, trace)
        self.assertEqual(trace["slots"][0]["tech_review"]["status"], "approved")
        self.assertTrue(trace["slots"][0]["current"]["execution"])
        self.assertEqual(trace["slots"][0]["current"]["execution"]["status"],
                         "performed")
        self.assertEqual(trace["rights"][0]["narrowed"], False)
        self.assertEqual(len(trace["slots"][0]["current"]["settlements"]), 1)
        self.assertEqual(trace["receipts"]["rcp-1"]["status"], "delivered")

    def test_published_content_is_never_silently_rewritten(self) -> None:
        h = self.h
        ready_program(h)
        h.request("POST", "/slots/s1/confirm", PROD_A)
        s, _, rel = h.request("POST", "/releases", PROD_A,
                              {"id": "rel1", "program_id": "p1", "channel": "web"})
        self.assertEqual(s, 201, rel)
        original_content = json.dumps(rel)  # noqa: F841

        # 申报露天台风限制并重排 -> 产生新版本，旧版本内容保留
        s, _, _ = h.request("POST", "/restrictions", COORD, {
            "id": "r-typhoon", "open_air_only": True,
            "start": "2026-10-03T00:00:00+08:00",
            "end": "2026-10-04T00:00:00+08:00", "reason": "台风预警"})
        self.assertEqual(s, 201)
        s, _, affected = h.request("GET", "/restrictions/r-typhoon/affected", COORD)
        self.assertEqual(s, 200)
        self.assertEqual([x["slot_id"] for x in affected["affected_slots"]], ["s1"])
        s, _, replan = h.request("POST", "/restrictions/r-typhoon/replan", COORD,
                                 {})
        self.assertEqual(s, 201, replan)
        self.assertEqual(replan["affected_slots"], ["s1"])
        self.assertEqual(len(replan["new_versions"]), 1)
        new_id = replan["new_versions"][0]

        s, _, old = h.request("GET", "/releases/rel1/trace", COORD)
        self.assertEqual(old["status"], "superseded")
        self.assertEqual(old["superseded_by"], new_id)
        # 旧快照时间原样保留（未静默改写）
        self.assertEqual(old["snapshot"]["slots"][0]["start"],
                         "2026-10-03T19:00:00+08:00")
        # 新版本快照反映改期，且沿版本链可回溯
        s, _, new = h.request("GET", f"/releases/{new_id}/trace", COORD)
        self.assertEqual(new["snapshot"]["slots"][0]["start"],
                         "2026-10-04T19:00:00+08:00")
        self.assertEqual(new["supersedes"], "rel1")
        chain = [c["release_id"] for c in new["version_chain"]]
        self.assertEqual(chain, [new_id, "rel1"])

    def test_only_affected_slots_are_replanned(self) -> None:
        h = self.h
        # 第一场：10-03 豫园（命中台风），第二场：10-10 豫园（不命中）
        ready_program(h, slot="s1", start="2026-10-03T19:00:00+08:00",
                      end="2026-10-03T20:30:00+08:00")
        ready_program(h, program="p2", slot="s2",
                      start="2026-10-10T19:00:00+08:00",
                      end="2026-10-10T20:30:00+08:00")
        h.request("POST", "/slots/s1/confirm", PROD_A)
        h.request("POST", "/slots/s2/confirm", PROD_A)
        h.request("POST", "/releases", PROD_A,
                  {"id": "rel-a", "program_id": "p1", "channel": "web"})
        h.request("POST", "/releases", PROD_A,
                  {"id": "rel-b", "program_id": "p2", "channel": "web"})
        h.request("POST", "/restrictions", COORD, {
            "id": "r2", "open_air_only": True,
            "start": "2026-10-03T00:00:00+08:00",
            "end": "2026-10-04T00:00:00+08:00", "reason": "大风"})
        s, _, replan = h.request("POST", "/restrictions/r2/replan", COORD, {})
        self.assertEqual(s, 201, replan)
        self.assertEqual(replan["affected_slots"], ["s1"])
        # s2 未被挪动
        s, _, ov = h.request("GET", "/overview", COORD)
        s2 = next(x for x in ov["slots"] if x["id"] == "s2")
        self.assertEqual(s2["start"], "2026-10-10T19:00:00+08:00")
        # 只有 rel-a 产生新版本，rel-b 仍在版
        self.assertNotIn("rel-b", replan["new_versions"])
        s, _, relb = h.request("GET", "/releases/rel-b/trace", COORD)
        self.assertEqual(relb["status"], "published")

    def test_restriction_scoped_by_genre_and_indoor_venue(self) -> None:
        h = self.h
        # 室内场地不受“露天”限制影响
        s, _, body = h.request("POST", "/venues", COORD, {
            "id": "v-indoor", "name": "室内小剧场",
            "business_open": "10:00", "business_close": "22:00",
            "open_air": False, "power_kw": 200, "noise_limit_db": 95,
            "has_quiet_room": True})
        self.assertEqual(s, 201, body)
        ready_program(h, venue="v-indoor", slot="s-in",
                      start="2026-10-03T19:00:00+08:00",
                      end="2026-10-03T20:30:00+08:00")
        h.request("POST", "/restrictions", COORD, {
            "id": "r-open", "open_air_only": True,
            "start": "2026-10-03T00:00:00+08:00",
            "end": "2026-10-04T00:00:00+08:00", "reason": "露天管制"})
        s, _, affected = h.request("GET", "/restrictions/r-open/affected", COORD)
        self.assertEqual(affected["affected_slots"], [])
        # 仅限电子音乐的限制不命中戏曲场
        h.request("POST", "/restrictions", COORD, {
            "id": "r-elec", "genres": ["electronic"],
            "start": "2026-10-03T00:00:00+08:00",
            "end": "2026-10-04T00:00:00+08:00", "reason": "电音噪音管制"})
        s, _, affected = h.request("GET", "/restrictions/r-elec/affected", COORD)
        self.assertEqual(affected["affected_slots"], [])


class ConcurrencyAndIdempotencyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.h = Harness()
        create_venues(self.h)
        ready_program(self.h)

    def tearDown(self) -> None:
        self.h.close()

    def test_concurrent_confirmation_has_single_winner(self) -> None:
        h = self.h
        outcomes: list[tuple[str, int]] = []

        def confirm(token: str) -> None:
            s, _, body = h.request("POST", "/slots/s1/confirm", token)
            outcomes.append((token, s))

        threads = [threading.Thread(target=confirm, args=(t,))
                   for t in (PROD_A, PROD_B)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        statuses = sorted(s for _, s in outcomes)
        self.assertEqual(statuses, [201, 409])
        s, _, ov = h.request("GET", "/overview", COORD)
        slot = next(x for x in ov["slots"] if x["id"] == "s1")
        self.assertEqual(slot["status"], "confirmed")
        # 只产生一笔结算
        self.assertEqual(len(ov["settlements"]), 1)

    def test_duplicate_confirmation_callback_does_not_double_charge(self) -> None:
        h = self.h
        s, _, first = h.request("POST", "/slots/s1/confirm", PROD_A, {},
                                 {"Idempotency-Key": "cb-confirm-1"})
        self.assertEqual(s, 201, first)
        s, headers, second = h.request("POST", "/slots/s1/confirm", PROD_A, {},
                                       {"Idempotency-Key": "cb-confirm-1"})
        self.assertEqual(s, 200)
        self.assertEqual(headers.get("Idempotent-Replay"), "true")
        self.assertTrue(second["idempotent_replay"])
        self.assertEqual(second["settlement_id"], first["settlement_id"])
        s, _, ov = h.request("GET", "/overview", COORD)
        self.assertEqual(len(ov["settlements"]), 1)

    def test_duplicate_channel_receipts_recorded_once(self) -> None:
        h = self.h
        h.request("POST", "/slots/s1/confirm", PROD_A)
        h.request("POST", "/releases", PROD_A,
                  {"id": "rel1", "program_id": "p1", "channel": "web"})
        payload = {"receipt_id": "rcp-x", "status": "delivered"}
        s, _, first = h.request("POST", "/releases/rel1/receipts", WEB, payload)
        self.assertEqual(s, 201, first)
        # 渠道重复推送同一回执（哪怕状态字段变化）返回首次结果，不产生新事件
        s, headers, second = h.request(
            "POST", "/releases/rel1/receipts", WEB,
            {"receipt_id": "rcp-x", "status": "failed"})
        self.assertEqual(s, 200)
        self.assertEqual(headers.get("Idempotent-Replay"), "true")
        self.assertEqual(second["status"], "delivered")
        s, _, trace = h.request("GET", "/releases/rel1/trace", COORD)
        self.assertEqual(list(trace["receipts"].keys()), ["rcp-x"])

    def test_duplicate_execution_callback_replays(self) -> None:
        h = self.h
        h.request("POST", "/slots/s1/confirm", PROD_A)
        s, _, first = h.request("POST", "/slots/s1/execution", COORD,
                                {"status": "performed"})
        self.assertEqual(s, 201)
        s, _, second = h.request("POST", "/slots/s1/execution", COORD,
                                 {"status": "no_show"})
        self.assertEqual(s, 200)
        self.assertTrue(second["idempotent_replay"])
        self.assertEqual(second["execution_status"], "performed")

    def test_restriction_replan_is_idempotent(self) -> None:
        h = self.h
        h.request("POST", "/slots/s1/confirm", PROD_A)
        h.request("POST", "/restrictions", COORD, {
            "id": "rr", "open_air_only": True,
            "start": "2026-10-03T00:00:00+08:00",
            "end": "2026-10-04T00:00:00+08:00", "reason": "暴雨"})
        s, _, first = h.request("POST", "/restrictions/rr/replan", COORD, {},
                                {"Idempotency-Key": "replan-rr"})
        self.assertEqual(s, 201)
        new_start = first["rescheduled"][0]["new_start"]
        s, _, second = h.request("POST", "/restrictions/rr/replan", COORD, {},
                                 {"Idempotency-Key": "replan-rr"})
        self.assertEqual(s, 200)
        self.assertEqual(second["rescheduled"][0]["new_start"], new_start)
        s, _, ov = h.request("GET", "/overview", COORD)
        # 只改期一次：slot 版本为 requested(1)->confirmed(2)->rescheduled(3)
        slot = next(x for x in ov["slots"] if x["id"] == "s1")
        self.assertEqual(slot["version"], 3)


class RightsAndPerformerVersioningTest(unittest.TestCase):
    def setUp(self) -> None:
        self.h = Harness()
        create_venues(self.h)
        ready_program(self.h)
        self.h.request("POST", "/slots/s1/confirm", PROD_A)
        self.h.request("POST", "/releases", PROD_A,
                       {"id": "rel1", "program_id": "p1", "channel": "web"})

    def tearDown(self) -> None:
        self.h.close()

    def test_narrowing_rights_creates_new_version(self) -> None:
        h = self.h
        # 收窄为仅现场（去掉 web）-> 已发布 web 节目单失去渠道覆盖，需新版本
        s, _, body = h.request("POST", "/rights/rights-p1/narrow", PROD_A, {
            "works": ["w-mudan"], "channels": ["live"],
            "territories": ["CN"],
            "valid_from": "2026-09-01T00:00:00+08:00",
            "valid_until": "2026-12-31T00:00:00+08:00",
            "reason": "线上授权收回"})
        self.assertEqual(s, 201, body)
        self.assertEqual(len(body["new_versions"]), 1)
        s, _, old = h.request("GET", "/releases/rel1/trace", COORD)
        self.assertEqual(old["status"], "superseded")
        self.assertEqual(old["rights"][0]["narrowed"], True)

    def test_widening_scope_is_rejected(self) -> None:
        h = self.h
        s, _, body = h.request("POST", "/rights/rights-p1/narrow", PROD_A, {
            "works": ["w-mudan", "w-extra"], "channels": ["live", "web"],
            "territories": ["CN", "US"],
            "valid_from": "2026-09-01T00:00:00+08:00",
            "valid_until": "2027-12-31T00:00:00+08:00",
            "reason": "试图借机扩大"})
        self.assertEqual(s, 400)
        self.assertIn("范围之外", body["error"]["message"])

    def test_identical_scope_is_not_a_narrowing(self) -> None:
        h = self.h
        s, _, body = h.request("POST", "/rights/rights-p1/narrow", PROD_A, {
            "works": ["w-mudan"], "channels": ["live", "web", "wechat"],
            "territories": ["CN"],
            "valid_from": "2026-09-01T00:00:00+08:00",
            "valid_until": "2026-12-31T00:00:00+08:00"})
        self.assertEqual(s, 400)
        self.assertIn("不构成收窄", body["error"]["message"])

    def test_replace_performer_creates_new_version(self) -> None:
        h = self.h
        s, _, body = h.request("POST", "/slots/s1/replace-performer", PROD_A, {
            "new_artist_id": "a2", "reason": "主演失声"})
        self.assertEqual(s, 201, body)
        self.assertEqual(len(body["new_versions"]), 1)
        new_id = body["new_versions"][0]
        s, _, new = h.request("GET", f"/releases/{new_id}/trace", COORD)
        self.assertEqual(new["snapshot"]["slots"][0]["artist_id"], "a2")
        s, _, old = h.request("GET", "/releases/rel1/trace", COORD)
        self.assertEqual(old["status"], "superseded")
        self.assertEqual(old["snapshot"]["slots"][0]["artist_id"], "a1")

    def test_replacement_without_contract_marks_new_version_blocked(self) -> None:
        h = self.h
        # a2 没有该场次合同：新版本必须存在（旧版已被替代），但被标记为不可对外
        s, _, body = h.request("POST", "/slots/s1/replace-performer", PROD_A, {
            "new_artist_id": "a2", "reason": "主演失声"})
        self.assertEqual(s, 201, body)
        new_id = body["new_versions"][0]
        s, _, view = h.request("GET", "/channels/me", WEB)
        row = next(x for x in view["live"] if x["release_id"] == new_id)
        self.assertFalse(row["ready"])
        self.assertTrue(any("合同" in p for p in row["blocked_by"]))

    def test_republish_same_channel_supersedes_previous(self) -> None:
        h = self.h
        s, _, second = h.request("POST", "/releases", PROD_A, {
            "id": "rel2", "program_id": "p1", "channel": "web",
            "change_reason": "文案勘误后重发"})
        self.assertEqual(s, 201, second)
        self.assertEqual(second["supersedes"], "rel1")
        s, _, old = h.request("GET", "/releases/rel1/trace", COORD)
        self.assertEqual(old["status"], "superseded")
        self.assertEqual(old["superseded_by"], "rel2")
        s, _, view = h.request("GET", "/channels/me", WEB)
        self.assertEqual([x["release_id"] for x in view["live"]], ["rel2"])
        self.assertEqual([x["release_id"] for x in view["superseded"]], ["rel1"])

    def test_every_emitted_event_is_registered_in_contract(self) -> None:
        import json as _json
        from pathlib import Path
        root = Path(__file__).resolve().parents[1]
        allowed = set(_json.loads((root / "contracts" / "domain.json")
                                  .read_text(encoding="utf-8"))["events"])
        h = self.h
        h.request("POST", "/restrictions", COORD, {
            "id": "rr", "open_air_only": True,
            "start": "2026-10-03T00:00:00+08:00",
            "end": "2026-10-04T00:00:00+08:00", "reason": "暴雨"})
        h.request("POST", "/restrictions/rr/replan", COORD)
        s, _, feed = h.request("GET", "/events", COORD)
        self.assertEqual(s, 200)
        emitted = {e["event_type"] for e in feed["events"]}
        unknown = emitted - allowed
        self.assertEqual(unknown, set())


class GenreMatrixAndBusinessHoursTest(unittest.TestCase):
    def setUp(self) -> None:
        self.h = Harness()
        create_venues(self.h)

    def tearDown(self) -> None:
        self.h.close()

    def test_opera_needs_quiet_room_and_rehearsal(self) -> None:
        h = self.h
        # 外滩露台无安静间 -> 戏曲技审必然列出违规，且不可批准
        ready_program(h, venue="v-bund", slot="s1",
                      start="2026-10-03T19:00:00+08:00",
                      end="2026-10-03T20:30:00+08:00", auto_approve=False)
        s, _, ov = h.request("GET", "/overview", COORD)
        tech = ov["tech_reviews"][0]
        self.assertTrue(any("扮戏" in p for p in tech["matrix_problems"]))
        s, _, body = h.request("POST", "/slots/s1/tech-review/decision", COORD,
                               {"decision": "approved"})
        self.assertEqual(s, 400, body)
        self.assertIn("硬性违规", body["error"]["message"])

    def test_electronic_requires_power_and_respects_curfew(self) -> None:
        h = self.h
        # 豫园供电 120kW 足够；但方案要 200kW -> 违规
        ready_program(h, genre="electronic",
                      artist=("a1", "DJ"), works=("w-elec",),
                      start="2026-10-03T19:00:00+08:00",
                      end="2026-10-03T20:30:00+08:00",
                      tech_plan={"rehearsal_hours": 1, "power_kw": 200,
                                 "sound_db": 90, "load_in_minutes": 90},
                      auto_approve=False)
        s, _, ov = h.request("GET", "/overview", COORD)
        tech = ov["tech_reviews"][0]
        self.assertTrue(any("供电" in p for p in tech["matrix_problems"]))
        s, _, body = h.request("POST", "/slots/s1/tech-review/decision", COORD,
                               {"decision": "approved"})
        self.assertEqual(s, 400)
        self.assertIn("硬性违规", body["error"]["message"])
        # 22:30 结束晚于电子音乐 22:00 宵禁
        h.request("POST", "/programs", PROD_A, {
            "id": "p2", "title": "夜场电音", "genre": "electronic",
            "works": ["w2"], "artists": [{"id": "a1", "name": "DJ"}]})
        s, _, body = h.request("POST", "/programs/p2/slots", PROD_A, {
            "id": "s-late", "venue_id": "v-bund",
            "start": "2026-10-03T21:30:00+08:00",
            "end": "2026-10-03T22:30:00+08:00", "performer_artist_id": "a1"})
        self.assertEqual(s, 400)
        self.assertIn("宵禁", body["error"]["message"])

    def test_slot_outside_business_hours_rejected(self) -> None:
        h = self.h
        h.request("POST", "/programs", PROD_A, {
            "id": "p1", "title": "晨排", "genre": "opera",
            "works": ["w"], "artists": [{"id": "a1", "name": "张三"}]})
        s, _, body = h.request("POST", "/programs/p1/slots", PROD_A, {
            "id": "s1", "venue_id": "v-yuyuan",
            "start": "2026-10-03T08:00:00+08:00",
            "end": "2026-10-03T09:30:00+08:00", "performer_artist_id": "a1"})
        self.assertEqual(s, 400)
        self.assertIn("营业时段", body["error"]["message"])


class VisibilityAndAuthTest(unittest.TestCase):
    def setUp(self) -> None:
        self.h = Harness()
        create_venues(self.h)

    def tearDown(self) -> None:
        self.h.close()

    def test_venue_sees_only_own_minimal_view(self) -> None:
        h = self.h
        ready_program(h, venue="v-yuyuan", slot="s1")
        ready_program(h, program="p2", venue="v-bund", slot="s2",
                      start="2026-10-05T19:00:00+08:00",
                      end="2026-10-05T20:30:00+08:00", auto_approve=False)
        # v-yuyuan 令牌只能看豫园，且看不到另一场地、版权与结算明细
        s, _, view = h.request("GET", "/venues/v-yuyuan/view", VENUE1)
        self.assertEqual(s, 200, view)
        ids = {x["slot_id"] for x in view["slots"]}
        self.assertEqual(ids, {"s1"})
        # 视图不含授权/结算敏感字段
        self.assertNotIn("settlements", view["slots"][0])
        self.assertNotIn("rights", view)
        # 越权查看其他场地
        s, _, body = h.request("GET", "/venues/v-bund/view", VENUE1)
        self.assertEqual(s, 403)
        # 场地方不能调统筹接口
        s, _, body = h.request("GET", "/overview", VENUE1)
        self.assertEqual(s, 403)

    def test_channel_scoped_to_own_releases(self) -> None:
        h = self.h
        ready_program(h)
        h.request("POST", "/slots/s1/confirm", PROD_A)
        h.request("POST", "/releases", PROD_A,
                  {"id": "r-web", "program_id": "p1", "channel": "web"})
        # wechat 渠道看不到/不能回执 web 的节目单
        s, _, body = h.request("GET", "/releases/r-web/trace", WECHAT)
        self.assertEqual(s, 403)
        s, _, body = h.request("POST", "/releases/r-web/receipts", WECHAT,
                               {"receipt_id": "x"})
        self.assertEqual(s, 400)
        s, _, mine = h.request("GET", "/channels/me", WEB)
        self.assertEqual([x["release_id"] for x in mine["live"]], ["r-web"])

    def test_missing_or_bad_token_unauthorized(self) -> None:
        h = self.h
        s, _, _ = h.request("GET", "/overview")
        self.assertEqual(s, 401)
        s, _, _ = h.request("GET", "/overview", "Bearer nonsense" if False else "nonsense")
        self.assertEqual(s, 401)


class ReplayPersistenceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.h = Harness()
        create_venues(self.h)

    def tearDown(self) -> None:
        self.h.close()

    def test_state_rebuilds_from_event_log_after_restart(self) -> None:
        h = self.h
        ready_program(h)
        h.request("POST", "/slots/s1/confirm", PROD_A)
        h.request("POST", "/releases", PROD_A,
                  {"id": "rel1", "program_id": "p1", "channel": "web"})
        h.request("POST", "/releases/rel1/receipts", WEB,
                  {"receipt_id": "rcp1", "status": "delivered"})
        h.request("POST", "/slots/s1/execution", COORD, {"status": "performed"})
        events_before = h.request("GET", "/events", COORD)[2]["count"]
        self.assertGreater(events_before, 0)

        h.restart()
        # 健康检查 + 读模型完整恢复
        s, _, health = h.request("GET", "/health")
        self.assertEqual(s, 200)
        s, _, trace = h.request("GET", "/releases/rel1/trace", COORD)
        self.assertEqual(s, 200)
        self.assertEqual(trace["slots"][0]["current"]["execution"]["status"],
                         "performed")
        self.assertEqual(trace["receipts"]["rcp1"]["status"], "delivered")
        self.assertEqual(len(trace["slots"][0]["current"]["settlements"]), 1)
        # 重启后幂等记录仍生效：重复回执不二次记录
        s, _, replay = h.request("POST", "/releases/rel1/receipts", WEB,
                                 {"receipt_id": "rcp1", "status": "delivered"})
        self.assertEqual(s, 200)
        self.assertTrue(replay["idempotent_replay"])
        # 事件数未增加
        self.assertEqual(h.request("GET", "/events", COORD)[2]["count"],
                         events_before)


if __name__ == "__main__":
    unittest.main()
