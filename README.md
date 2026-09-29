# 双场域演出版本衔接

非遗音乐季同时使用**豫园水上舞台**与**外滩露台**。节目提案、场地时段、演职合同、
技术审查、宣传文案与结算依据曾被多支团队分别修改，一次换场留下两个对外版本。
本服务让这些事实沿**同一条仅追加事件链**流转：已发布内容不可静默改写，改期、
替换表演者或收窄授权一律产生新版本，并追踪渠道回执。

仅依赖 Python 3.11+ 标准库（`http.server` + JSONL 事件日志），无外部数据库。

## 核心承诺

| 承诺 | 实现方式 |
| --- | --- |
| 一条版本链 | 所有状态改变都是事件；按聚合（stream）单调版本号，JSONL 落盘 |
| 已发布内容不可改写 | 改期/换人/收窄生成**新 release 聚合**；旧 release 仅追加 `RELEASE_SUPERSEDED` 指引，快照原样保留 |
| 并发确认只有一个生效 | 场次确认是乐观并发提交；输家得到 `409 version_conflict/conflict`，赢家同时原子生成**唯一一笔**结算 |
| 重复回调不二次预留/计费 | 确认、执行、回执、重排均带幂等键（可显式传 `Idempotency-Key`，否则按业务键派生）；重复请求返回 `200` + `Idempotent-Replay: true` 与首次结果 |
| 只重排受影响场次 | 露天限制按 *场地/露天标记 × 剧种 × 时间窗* 命中；仅命中场次顺延，未命中不动 |
| 剧种差异化技审 | 戏曲需安静扮戏间与 ≥2h 合乐排练；电子音乐需 ≥150kW 供电、装台 ≥90min、22:00 噪音宵禁 |
| 商圈营业时段 | 场次窗口必须被场地每日营业段无缝覆盖 |
| 授权收窄 | 新范围必须是当前范围的**真子集**；扩大/原样都被拒绝；收窄导致已发布节目单失去覆盖时自动出新版 |
| 可追溯 | 从任一公开节目单可追到技审结论、授权版本、合同、改期/换人记录、执行结果与结算 |
| 最小可见 | 场地方只看本场地执行所需字段；渠道只看/回执本渠道节目单；统筹看全链路 |
| 可重放 | 重启服务从事件日志 + 幂等侧车重建全部读模型与幂等记录 |

## 启动

```bash
python3 -m src.server --host 127.0.0.1 --port 8080 --store data/events.jsonl
```

## 鉴权（Bearer 令牌）

| 令牌 | 角色 | 能力 |
| --- | --- | --- |
| `coordinator` | 演出统筹 | 注册场地、申报限制、受限重排、技审批准/驳回、登记执行、全链路追溯 |
| `producer:<姓名>` | 制作人 | 提案/修订、场次申请与确认、授权与合同、发布、替换表演者 |
| `venue:<venue_id>` | 场地方 | 仅 `GET /venues/<自己>/view` |
| `channel:<渠道名>` | 渠道方 | 仅查看本渠道节目单、回执本渠道节目单 |

写接口建议带 `Idempotency-Key`；不带时确认/执行/回执/重放按业务键自动派生。

## 主要 HTTP 接口

```
POST /venues                                   注册场地（含营业时段/供电/声压/露天）
POST /restrictions                             申报临时限制（venue_ids/open_air_only/genres/时间窗）
GET  /restrictions/{id}/affected               预览命中场次（不落事件）
POST /restrictions/{id}/replan                 受限重排（body {"dry_run": true} 仅预览）

POST /programs                                 提案
POST /programs/{id}/revisions                  修订（标题/简介/名单/作品）
POST /programs/{id}/slots                      申请场次（校验营业时段、宵禁）
POST /programs/{id}/rights                     音乐授权确权
POST /slots/{id}/tech-reviews                  提交技审方案（返回剧种×场地矩阵违规）
POST /slots/{id}/tech-review/decision          批准/驳回（有硬性违规不可批准）
POST /slots/{id}/contracts                     按场次签订演职合同范围
POST /slots/{id}/confirm                       确认场次（原子生成唯一结算）
POST /slots/{id}/replace-performer             替换表演者（产生新版本）
POST /slots/{id}/execution                     登记实际执行结果（performed/interrupted/no_show）

POST /rights/{id}/narrow                       收窄授权（必须为真子集；受影响节目单出新版本）
POST /releases                                 发布节目单（同节目同渠道再发自动替代旧版）
GET  /releases/{id}/trace                      版本链 + 技审/授权/执行/结算全追溯
POST /releases/{id}/receipts                   渠道回执（重复回执只记一次）
GET  /channels/me                              本渠道在版/已替代节目单
GET  /venues/{id}/view                         场地方最小视图
GET  /overview                                 统筹全量读模型
GET  /events                                   原始事件流
```

### 版本链示例

改期后 `GET /releases/{新版}/trace` 的 `version_chain`：

```json
[
  {"release_id": "release-xxxx", "status": "published", "supersedes": "rel1"},
  {"release_id": "rel1",        "status": "superseded", "supersedes": null}
]
```

旧版 `snapshot.slots[].start` 保持发布当时的值；新版快照反映改期并携带
`supersedes`。若新版本前置条件尚未补齐（如替换者无合同、技审未重过），
新版本仍会生成以保证旧版下线，但在渠道视图中 `ready=false` 且列出 `blocked_by`。

## 领域事件

合同见 `contracts/domain.json`，代码内事件集合与其严格一致（有测试守护）：

```
PROGRAM_PROPOSED / PROGRAM_REVISED
VENUE_REGISTERED / RESTRICTION_DECLARED
SLOT_REQUESTED / SLOT_CONFIRMED / SLOT_RESCHEDULED /
SLOT_PERFORMER_REPLACED / SLOT_CANCELLED
RIGHTS_CLEARED / RIGHTS_NARROWED
CONTRACT_SCOPED
TECH_REVIEW_SUBMITTED / TECH_REVIEW_APPROVED / TECH_REVIEW_REJECTED
RELEASE_PUBLISHED / RELEASE_SUPERSEDED / CHANNEL_RECEIPT_ACKED
CHANGE_RECONCILED
PERFORMANCE_EXECUTED / SETTLEMENT_RECORDED
```

事件信封字段见 `src/envelope.py`：`event_id / event_type / occurred_at（含时区）/
aggregate_id / version / payload`，存储时附 `stream_id`。

## 测试

```bash
python3 -m unittest discover -s tests -v
python3 -m compileall -q src tests
```

测试在随机端口启动真实 HTTP 服务（临时 JSONL），并覆盖：

* 提案→技审→授权→合同→确认→发布→回执→执行→结算的完整版本链与追溯；
* 露天限制按剧种/室内外/时间窗只重排受影响场次；
* 已发布内容改期/换人/收窄后旧版保留、新版沿链可回溯；
* 两位制作人并发确认恰有一个 201、一个 409，且只有一笔结算；
* 重复确认/回执/执行/重排回调返回同一结果，不新增事件、不重复计费；
* 戏曲安静间/排练、电子供电/装台/宵禁、商圈营业时段等矩阵拦截；
* 场地方越权 403 且视图最小化、渠道隔离、无令牌 401；
* 杀掉服务后用同一事件日志重启，读模型与幂等记录完整恢复。

## 目录

```
contracts/domain.json   聚合与事件登记（对外合同）
data/sample.json        信封联调样例
src/envelope.py         共用事件信封校验
src/eventstore.py       仅追加事件存储、乐观并发、幂等侧车、重放
src/domain.py           剧种/场地/营业时段/授权范围等无状态领域规则
src/app.py              读模型投影与全部版本链命令
src/httpapi.py          HTTP JSON 路由与角色鉴权
src/server.py           启动入口
tests/                  合同测试 + 端到端可重放集成测试
```
