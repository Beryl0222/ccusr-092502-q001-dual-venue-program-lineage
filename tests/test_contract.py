"""基础事件合同测试。"""
from __future__ import annotations

import json
import unittest
from pathlib import Path

from src.domain import EVENT_TYPES
from src.envelope import validate_event


ROOT = Path(__file__).resolve().parents[1]


class EventContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.contract = json.loads((ROOT / "contracts" / "domain.json").read_text(encoding="utf-8"))
        self.sample = json.loads((ROOT / "data" / "sample.json").read_text(encoding="utf-8"))

    def test_sample_matches_contract(self) -> None:
        self.assertEqual(validate_event(self.sample, set(self.contract["events"])), [])

    def test_unknown_event_is_rejected(self) -> None:
        changed = dict(self.sample, event_type="UNKNOWN")
        self.assertIn("未知的 event_type", validate_event(changed, set(self.contract["events"])))

    def test_timezone_is_required(self) -> None:
        changed = dict(self.sample, occurred_at="2026-09-24T09:00:00")
        self.assertIn("occurred_at 必须包含时区", validate_event(changed, set(self.contract["events"])))

    def test_code_event_set_matches_contract(self) -> None:
        # 服务代码实际使用的事件集合必须与对外合同严格一致，防止两边漂移
        self.assertEqual(EVENT_TYPES, set(self.contract["events"]))

    def test_sample_event_type_still_registered(self) -> None:
        self.assertIn(self.sample["event_type"], EVENT_TYPES)


if __name__ == "__main__":
    unittest.main()
