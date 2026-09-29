"""领域策略：剧种排练/技术要求、场地能力矩阵、营业时段与露天限制。

这里不保存任何状态，只表达“什么组合可行”的确定规则，供应用服务在
技术审查批准、场次确认、受限重排时调用。戏曲与电子音乐的排练要求不同，
因此规则按剧种分开；露天临时限制只判定“命中”的场次。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any

# 与 contracts/domain.json 保持一致的事件集合
EVENT_TYPES = frozenset({
    "PROGRAM_PROPOSED",
    "PROGRAM_REVISED",
    "VENUE_REGISTERED",
    "RESTRICTION_DECLARED",
    "SLOT_REQUESTED",
    "SLOT_CONFIRMED",
    "SLOT_RESCHEDULED",
    "SLOT_PERFORMER_REPLACED",
    "SLOT_CANCELLED",
    "RIGHTS_CLEARED",
    "RIGHTS_NARROWED",
    "CONTRACT_SCOPED",
    "TECH_REVIEW_SUBMITTED",
    "TECH_REVIEW_APPROVED",
    "TECH_REVIEW_REJECTED",
    "RELEASE_PUBLISHED",
    "RELEASE_SUPERSEDED",
    "CHANNEL_RECEIPT_ACKED",
    "CHANGE_RECONCILED",
    "PERFORMANCE_EXECUTED",
    "SETTLEMENT_RECORDED",
})

GENRES = ("opera", "electronic")

# 戏曲：合乐排练至少 2 小时、需安静的热身/扮戏间、声压上限较低；
# 电子音乐：需大功率供电、声压上限较高、受夜间噪音宵禁约束。
GENRE_RULES: dict[str, dict[str, Any]] = {
    "opera": {
        "label": "戏曲",
        "rehearsal_hours_min": 2,
        "requires_quiet_room": True,
        "requires_power_kw": 0,
        "sound_limit_db": 85,
        "curfew": None,
        "load_in_minutes_min": 60,
    },
    "electronic": {
        "label": "电子音乐",
        "rehearsal_hours_min": 1,
        "requires_quiet_room": False,
        "requires_power_kw": 150,
        "sound_limit_db": 95,
        "curfew": "22:00",
        "load_in_minutes_min": 90,
    },
}

TZ = timezone(timedelta(hours=8))


def now_iso() -> str:
    return datetime.now(TZ).replace(microsecond=0).isoformat()


def parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("时间必须包含时区")
    return parsed


def parse_hhmm(value: str) -> time:
    hour, minute = value.split(":")
    return time(int(hour), int(minute))


class DomainError(Exception):
    """请求可执行但违反领域规则（400）。"""


class NotFoundError(Exception):
    """引用的聚合不存在（404）。"""


class ConflictStateError(Exception):
    """聚合当前状态不允许该操作（409），区别于乐观锁版本冲突。"""


@dataclass(frozen=True)
class TimeWindow:
    start: datetime
    end: datetime

    @classmethod
    def from_iso(cls, start: str, end: str) -> "TimeWindow":
        s, e = parse_iso(start), parse_iso(end)
        if e <= s:
            raise DomainError("结束时间必须晚于开始时间")
        return cls(s, e)

    def overlaps(self, other: "TimeWindow") -> bool:
        return self.start < other.end and other.start < self.end


def within_business_hours(win: TimeWindow, open_hhmm: str, close_hhmm: str) -> bool:
    """窗口必须被每日营业段无缝覆盖（不支持跨午夜营业）。"""
    opens, closes = parse_hhmm(open_hhmm), parse_hhmm(close_hhmm)
    if closes <= opens:
        return False
    tz = win.start.tzinfo
    cursor = win.start
    day = win.start.date()
    while cursor < win.end:
        day_open = datetime.combine(day, opens, tz)
        day_close = datetime.combine(day, closes, tz)
        if not (day_open <= cursor < day_close):
            return False  # 落在闭店间隙
        if win.end <= day_close:
            return True
        cursor = day_close
        day += timedelta(days=1)
    return True


def before_curfew(win: TimeWindow, curfew_hhmm: str | None) -> bool:
    if curfew_hhmm is None:
        return True
    return win.end.timetz().replace(tzinfo=None) <= parse_hhmm(curfew_hhmm)


def restriction_hits(
    slot: dict[str, Any], venue: dict[str, Any], restriction: dict[str, Any]
) -> bool:
    """判定一场演出是否被露天临时限制命中。"""
    if restriction.get("venue_ids") and venue["id"] not in restriction["venue_ids"]:
        return False
    if restriction.get("open_air_only") and not venue.get("open_air"):
        return False
    if restriction.get("genres") and slot.get("genre") not in restriction["genres"]:
        return False
    return TimeWindow.from_iso(restriction["start"], restriction["end"]).overlaps(
        TimeWindow.from_iso(slot["start"], slot["end"])
    )


def tech_compliance(
    genre: str, venue: dict[str, Any], plan: dict[str, Any]
) -> list[str]:
    """技术审查批准前的硬性矩阵校验，返回违规说明（空列表表示通过）。"""
    if genre not in GENRE_RULES:
        return [f"未知剧种: {genre}"]
    rules = GENRE_RULES[genre]
    problems: list[str] = []

    hours = float(plan.get("rehearsal_hours", 0))
    if hours < rules["rehearsal_hours_min"]:
        problems.append(
            f"{rules['label']}合乐排练不得少于 {rules['rehearsal_hours_min']} 小时"
        )
    if rules["requires_quiet_room"] and not venue.get("has_quiet_room"):
        problems.append("该剧种要求安静的扮戏/热身间，场地不具备")
    if rules["requires_power_kw"]:
        if float(venue.get("power_kw", 0)) < rules["requires_power_kw"]:
            problems.append(
                f"场地供电 {venue.get('power_kw')}kW 低于要求 {rules['requires_power_kw']}kW"
            )
        if float(plan.get("power_kw", 0)) > float(venue.get("power_kw", 0)):
            problems.append("技术方案用电超过场地供电能力")
    sound_limit = min(rules["sound_limit_db"], int(venue.get("noise_limit_db", 10**9)))
    if int(plan.get("sound_db", 0)) > sound_limit:
        problems.append(f"方案声压 {plan.get('sound_db')}dB 超过场地/剧种上限 {sound_limit}dB")
    if int(plan.get("load_in_minutes", 0)) < rules["load_in_minutes_min"]:
        problems.append(
            f"装台时间不得少于 {rules['load_in_minutes_min']} 分钟"
        )
    win = TimeWindow.from_iso(plan["slot_start"], plan["slot_end"])
    if not within_business_hours(win, venue["business_open"], venue["business_close"]):
        problems.append("演出时段超出商圈营业时段")
    if not before_curfew(win, rules["curfew"]):
        problems.append(f"结束时间晚于该剧种噪音宵禁 {rules['curfew']}")
    return problems


def scope_covers(grant_scope: dict[str, Any], needed: dict[str, Any]) -> list[str]:
    """判定授权范围是否覆盖所需作品、渠道、地域与有效期。"""
    problems: list[str] = []
    missing_works = sorted(set(needed["works"]) - set(grant_scope.get("works", [])))
    if missing_works:
        problems.append(f"授权未覆盖作品: {','.join(missing_works)}")
    missing_channels = sorted(set(needed["channels"]) - set(grant_scope.get("channels", [])))
    if missing_channels:
        problems.append(f"授权未覆盖渠道: {','.join(missing_channels)}")
    if needed.get("territory") and needed["territory"] not in grant_scope.get("territories", []):
        problems.append(f"授权未覆盖地域: {needed['territory']}")
    valid_from = parse_iso(grant_scope["valid_from"])
    valid_until = parse_iso(grant_scope["valid_until"])
    slot_start, slot_end = parse_iso(needed["slot_start"]), parse_iso(needed["slot_end"])
    if slot_start < valid_from or slot_end > valid_until:
        problems.append("授权有效期未覆盖演出时段")
    return problems


def is_subset_scope(narrow: dict[str, Any], current: dict[str, Any]) -> list[str]:
    """收窄授权：新范围必须是当前范围的真子集，不允许借机扩大或改写。"""
    problems: list[str] = []
    for key in ("works", "channels", "territories"):
        extra = sorted(set(narrow.get(key, [])) - set(current.get(key, [])))
        if extra:
            problems.append(f"收窄后的 {key} 出现原范围之外的项: {','.join(extra)}")
    nf, nu = parse_iso(narrow["valid_from"]), parse_iso(narrow["valid_until"])
    cf, cu = parse_iso(current["valid_from"]), parse_iso(current["valid_until"])
    if nf < cf or nu > cu:
        problems.append("收窄后的有效期超出当前授权有效期")
    same = (
        set(narrow.get("works", [])) == set(current.get("works", []))
        and set(narrow.get("channels", [])) == set(current.get("channels", []))
        and set(narrow.get("territories", [])) == set(current.get("territories", []))
        and nf == cf and nu == cu
    )
    if same:
        problems.append("新授权范围与当前范围完全相同，不构成收窄")
    return problems
