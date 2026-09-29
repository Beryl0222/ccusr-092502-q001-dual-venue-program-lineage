# 双场域演出版本衔接服务

豫园水上舞台与外滩露台共用的服务端系统。节目提案、场地时段、演职合同、技术审查、
宣传文案与结算依据沿**同一版本链**流转；事实只追加，任何更正都是新版本事件。

## 解决了什么

- **一个对外版本**：节目是单一版本链（`revision` 单调递增，带 `supersedes`/`causation_id`），
  提案、合同范围、技术审查、版权授权、发布、结算都挂在节目流上，换场后不再出现两个版本。
- **已发布内容不可静默改写**：同一版本直接改文案返回 `422 CONTENT_IMMUTABLE`；
  改期、替换表演者、收窄授权都产生新版本，渠道再发布是新的 `RELEASE_PUBLISHED` 事件，并记录渠道回执。
- **戏曲 / 电子差异排练要求**：戏曲需 6 小时排练、遮雨、独立化妆间、固定扩声；
  电子需 3 小时、电力冗余、低音承载、防雨设备。技术审查按"演出要求集合"判定，换剧种需重审。
- **露天限制只重排受影响场次**：改期只作用于单个场次；替换表演者/收窄授权的响应直接返回
  `affected_slots`（失去出场条件的已确认场次清单），其余场次不动。
- **并发确认唯一生效**：场次流用乐观版本锁，两位制作人同时确认只有一人 `200`，另一人 `409`；
  支付/渠道回调按 `callback_id` 幂等，重复回调回放首次结果，**不二次预留、不重复计费**。
- **可追溯 + 最小知情**：`GET /public/programs` 是公开节目单，统筹可由任一 `release_id`
  追到审批、授权、场次执行与结算；场地方只看到自己场地的时间与装台要求，看不到费用、合同、版权。

## 架构

- `src/store.py` — 只追加事件存储。按流的单调 `version` + 乐观并发；JSONL 落盘，
  启动重放同时重建事件流与幂等回执；多事件与回执在同一临界区单次 fsync 提交。
- `src/service.py` — 领域规则与折叠读模型（出场条件门、版本链、选择性受影响场次、角色视图）。
- `src/httpapi.py` — `ThreadingHTTPServer` 的 HTTP JSON 接口、角色校验、统一错误信封。
- `src/app.py` — 入口。
- `contracts/domain.json` — 实体、事件、流命名与信封字段登记。

## 运行

```bash
python3 -m src.app --host 127.0.0.1 --port 8080 --log ./run/events.jsonl --seed
```

`--seed` 登记豫园水上舞台、外滩露台（营业时段 10:00–22:00，均为露天）。
删除 JSONL 即得到干净环境；不传 `--log` 为纯内存模式。

## 角色与约定

- 请求头 `X-Role: coordinator | venue | gateway | public`（默认 public）；
  场地方另需 `X-Venue-Id`，且只能读自己场地。
- 写接口可带 `Idempotency-Key`，重放返回首次结果。
- 时间必须带时区；错误统一为 `{"error":{"code","message","request_id"}}`，响应带 `X-Request-Id`。

## HTTP 接口

| 方法 | 路径 | 角色 | 说明 |
|---|---|---|---|
| GET | `/health` | 任意 | 存活检查 |
| GET | `/events?stream=<流>` | coordinator | 可观察事件流（不传 stream 为全量） |
| POST | `/admin/venues` | coordinator | 登记场地（营业时段/露天标志） |
| POST | `/programs` | coordinator | 提案节目（revision=1） |
| GET | `/programs/{id}` | coordinator | 版本链、条件门、场次证据、发布全览 |
| POST | `/programs/{id}/revisions` | coordinator | 改备注/阵容/费用 → 新版本，返回 affected_slots |
| POST | `/programs/{id}/performers/replace` | coordinator | 替换表演者 → 新版本 |
| POST | `/programs/{id}/contracts` | coordinator | 演职合同范围（表演者+场地） |
| POST | `/programs/{id}/tech-reviews` | coordinator | 技术审查（按剧种要求自动判定） |
| POST | `/programs/{id}/rights` | coordinator | 版权授权（渠道+场地+用途） |
| POST | `/programs/{id}/rights/{rid}/narrow` | coordinator | 收窄授权 → 新版本，旧授权置 narrowed |
| POST | `/slots/holds` | coordinator | 临时预留（允许重叠抢占） |
| POST | `/slots/{id}/confirm` | coordinator | 确认（过条件门+时间冲突裁决，乐观锁唯一胜者） |
| POST | `/slots/{id}/reschedule` | coordinator | 改期（仅该场次） |
| POST | `/slots/{id}/release` | coordinator | 释放（已结算则产生退款事件） |
| POST | `/slots/{id}/settle` | coordinator/gateway | 支付回调结算（callback_id 幂等） |
| POST | `/releases` | coordinator | 渠道发布（同版本改文案被拒；新版本再发布） |
| POST | `/releases/{id}/receipts` | coordinator/gateway | 渠道回执（callback_id 幂等，重复返回 replay:true） |
| POST | `/releases/{id}/retract` | coordinator | 撤回发布 |
| GET | `/releases/{id}` | coordinator | 从发布追到审批/授权/执行/结算 |
| GET | `/public/programs` | 任意 | 公开节目单（只含已发布最新版本） |
| GET | `/venues/{id}/schedule` | venue/coordinator | 场地视图（时间+装台要求，无商务字段） |

## 出场条件门

确认场次或发布宣传前，必须同时满足：

1. 有效合同覆盖该版本全部表演者与目标场地（换人后旧合同不覆盖新表演者）；
2. 技术审查通过时的演出要求（剧种→排练时长与场地能力）与目标版本一致；
3. 有效版权授权覆盖目标版本、场地；发布时还要覆盖对应渠道；
4. 时段在商圈营业时段内，且不与同场地已确认场次重叠。

不满足时返回 `422 NOT_READY`，message 列出具体阻断项。

## 测试

```bash
python3 -m unittest discover -s tests
python3 -m compileall -q src tests
```

`tests/test_http_api.py` 在临时端口启动真实 HTTP 服务，覆盖：条件门、剧种差异要求、
营业时段与冲突、并发确认唯一胜者、发布不可改写与版本链、替换表演者只影响相关场次、
露天单场改期、重复回执/支付不重复、公开节目单溯源、角色最小可见、收窄授权新版本，
以及关闭后用同一 JSONL 重启的**状态与幂等重放**。仅依赖 Python 标准库。
