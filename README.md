# Device Event Session Merger

乱序设备事件会话归并 API（Python + FastAPI + SQLite）。

同一设备的事件，若相邻事件（按 `occurred_at` 排序）间隔不超过可配置阈值，则按传递关系归入同一会话。会话在读取时从全量有序事件实时归并，因此**事件到达顺序不影响结果**，迟到事件会自动落到排序位置并可能**桥接**原先分开的两个会话。

本服务还内置**版本化告警规则**：按设备创建不可变版本规则，匹配 `event_type` 与 payload 中的 JSON 路径值，在时间窗口内统计连续命中次数并按事件时间冷却；事件写入时在同一事务内按 `occurred_at` 把事件归入对应**版本时代**并重放受影响版本（包括已被取代的冻结版本），持久化命中进度与告警实例（见[告警规则](#告警规则)）。

设备还可启用一套**不可变状态机版本投影**：注册由初始状态、状态集合与按 `event_type` / JSON Pointer 条件匹配的转移组成的版本；启用后，事件写入须在**原事务内按到达序**执行唯一匹配的转移，非法（零匹配）或多匹配事件返回 `409` 且不落库，重复 `event_id` 幂等；当前状态与转移日志持久化，重启后可查询状态及按游标分页的日志（见[设备状态机事件投影](#设备状态机事件投影)）。

设备还可以注册**采集流（collection stream）**做采集完整性管理：`PUT /devices/{device_id}/streams/{stream_id}` 注册采集流，流内事件以设备自分配的 `source_seq`（**从 1 开始**，允许乱序补传）经 `.../streams/{stream_id}/events` 提交，并在现有摄取事务内一并落库；`(device_id, stream_id, source_seq)` 只能绑定一个 `event_id`，同内容重放幂等，序号或 `event_id` 冲突返回 `409`。状态接口返回 `highest_seen`、`contiguous_through` 与压缩后的 `missing_ranges`；检查点仅在 `1..through_seq` 无缺口时保存按 `source_seq`、`event_id` 计算的 SHA-256，否则 `409`，重复创建返回原结果。清理事件后，序号账本与检查点仍保留（见[采集完整性](#采集完整性)）。

服务还内置**设备时钟校准**：设备上报 `(anchor_id, observed_seq, device_time, reference_time)` 锚点，`anchor_id` 内容一致时幂等、`observed_seq` 必须递增，`device_time` 不前进（回退或相等）即开启新 epoch；同一 epoch 内用相邻锚点线性插值把设备时间归一化到参考时间，漂移超过 `CLOCK_DRIFT_PPM` 或 `reference_time` 不递增的锚点返回 `409` 且不落库，锚点支持区间外仅在配置的外推范围内线性外推。单事件归一化接口返回 `corrected_at`、epoch、计算方式与不确定区间；跨设备时间线按 `corrected_at` 排序，并在首个游标中冻结各设备锚点高水位，使翻页过程中新写入的锚点不会造成重复或漏项（见[设备时钟校准](#设备时钟校准)）。

服务还提供**会话演变快照**：`POST /devices/{device_id}/session-snapshots` 在同一写事务内冻结设备当前的事件单调序列高水位（`events.seq`）并按现有 gap 配置把当时的会话与**有序成员完整复制**入库；`snapshot_id` 全局唯一，同设备重复提交幂等返回原快照，跨设备复用返回 `409`。两快照差异接口以共享 `event_id` 构建旧/新会话二部图，按连通分量判定 `unchanged`、`changed`、`merged`、`split`、`created`、`removed`，多对多分量标为 `reorganized`，输出双方会话 ID、增删成员并稳定排序；gap 配置不同的快照拒绝比较。迟到事件与保留清理都不会改写旧快照（见[会话演变快照](#会话演变快照)）。

服务为每个**首次成功写入**的事件维护一棵**透明回执 Merkle 日志**（RFC 6962 风格）：叶在原 `BEGIN IMMEDIATE` 事务内按 `events.seq` 追加，规范化绑定 `seq`、`device_id`、`event_id`、`occurred_at`、`event_type`、`schema_version` 与 `payload`；幂等重放与失败请求绝不新增叶。支持创建**不可变检查点**、按 `event_id` 取**成员证明**、以及两个检查点间的**一致性证明**，所有证明仅凭返回数据即可离线复算；非法树大小返回 `422`。保留清理只删事件正文，回执叶与旧检查点仍可验证；迁移旧库时按现有 seq 一次性回填且结果可复现（见[透明事件回执](#透明事件回执)）。

## 启动

```bash
docker compose up --build
```

单条命令（无需克隆后手工构建等价物）：

```bash
docker run --rm -p 8000:8000 -v events-data:/data \
  -e DATABASE_PATH=/data/events.db "$(docker build -q .)"
```

服务监听 **8000** 端口（Compose 配置与端口保持不变）：

- 交互式文档：http://localhost:8000/docs
- 健康检查：http://localhost:8000/healthz

### 配置

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `SESSION_GAP_SECONDS` | `300` | 相邻事件归并阈值（秒），支持小数 |
| `CLOCK_DRIFT_PPM` | `50000` | 时钟锚点允许的最大速率偏差（ppm，50000 = 5%） |
| `CLOCK_MAX_EXTRAPOLATION_SECONDS` | `3600` | 锚点支持区间外允许的最大外推秒数 |
| `DATABASE_PATH` | `/data/events.db`（Compose 中） | SQLite 文件路径 |

## 接口

### 注册事件载荷契约 `PUT /schemas/{event_type}/{version}`

为某种 `event_type` 注册一份**不可变**的 [JSON Schema](https://json-schema.org/)（Draft 2020-12）。契约必须自包含：禁止任何外部引用（`$ref` / `$dynamicRef` / `$recursiveRef` 只能是 `#` 开头的文档内引用），注册时即做元校验。

- `201 Created`：注册成功。
- `200 OK`：相同文档的重复 PUT（幂等回放），不产生副作用。
- `409 Conflict`：同一 `(event_type, version)` 已注册但文档或 `supersedes` 不同——契约不可改写。
- `422 Unprocessable Entity`：Schema 本身不合法、引用了外部资源、`supersedes` 指向未注册版本或形成环；`errors` 中每项带 JSON Pointer（如 `/supersedes`、`/properties/a/$ref`）。

通过可选的 `supersedes` 为同一事件类型建立**版本链**（如 `2.0` 取代 `1.0`）。前序版本必须已经注册且属于同一事件类型，链上不允许出现环。

```bash
# v1（链的根版本）
curl -s -X PUT http://localhost:8000/schemas/telemetry/1.0 \
  -H 'Content-Type: application/json' \
  -d '{
    "schema": {
      "type": "object",
      "additionalProperties": false,
      "required": ["reading", "tags"],
      "properties": {
        "reading": {
          "type": "object",
          "required": ["value", "unit"],
          "properties": {
            "value": {"type": "number", "minimum": 0},
            "unit":  {"type": "string", "enum": ["c", "f"]}
          }
        },
        "tags": {"type": "array", "items": {"type": "string"}}
      }
    }
  }'

# v2 声明取代 v1
curl -s -X PUT http://localhost:8000/schemas/telemetry/2.0 \
  -H 'Content-Type: application/json' \
  -d '{
    "supersedes": "1.0",
    "schema": {
      "type": "object",
      "required": ["reading"],
      "properties": {
        "reading": {
          "type": "object",
          "required": ["value"],
          "properties": {"value": {"type": "number"}}
        }
      }
    }
  }'
```

### 查询契约 `GET /schemas/{event_type}/{version}`

```bash
curl -s http://localhost:8000/schemas/telemetry/1.0
# { "event_type": "telemetry", "schema_version": "1.0",
#   "schema": { ... }, "supersedes": null, "created_at": "..." }
```

未注册时返回 `404`。

### 提交事件 `POST /events`

#### 带契约的载荷事件

在请求中同时提供 `event_type`、`schema_version`、`payload`（三者要么全给，要么全不给）。服务在**任何入库或会话计算之前**按指定契约校验载荷：

- `201 Created`：校验通过并入库，返回该事件当前所属会话。
- `200 OK`：与已存在事件完全一致的重复提交（含这三个字段的逐项比较），幂等返回（`replay: true`）。
- `409 Conflict`：`event_id` 已存在，但 `device_id`、`occurred_at`、`event_type`、`schema_version` 或 `payload` 不同。
- `422 Unprocessable Entity`：契约版本不存在，或载荷不满足契约。响应形如：

```json
{
  "detail": {
    "message": "payload does not satisfy the registered schema",
    "errors": [
      {"path": "/reading/value", "error": "-3 is less than the minimum of 0"},
      {"path": "/tags/1", "error": "7 is not of type 'string'"}
    ]
  }
}
```

`path` 为定位错误项的 JSON Pointer：多层对象逐段拼接（`/reading/value`），数组项带下标（`/tags/1`），根级错误为空串。载荷以规范化 JSON 持久化，因此回放时键的顺序不影响幂等判定。

```bash
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
    "device_id": "dev-1",
    "event_id": "e1",
    "occurred_at": "2026-09-20T10:00:00Z",
    "event_type": "telemetry",
    "schema_version": "1.0",
    "payload": {
      "reading": {"value": 21.5, "unit": "c"},
      "tags": ["warm", "lab"]
    }
  }'
```

#### 无载荷事件（向后兼容）

不带这三个字段的请求行为与旧版完全一致，仍然只按 `device_id` / `event_id` / `occurred_at` 归并；无载荷事件与载荷事件可以混合归并，归并规则与会话标识算法不变。

```json
{ "device_id": "dev-1", "event_id": "e1", "occurred_at": "2026-09-20T10:00:00Z" }
```

```bash
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"dev-1","event_id":"e1","occurred_at":"2026-09-20T10:00:00Z"}'
```

### 查询设备的全部会话 `GET /devices/{device_id}/sessions`

```bash
curl -s http://localhost:8000/devices/dev-1/sessions
```

返回按开始时间排序的会话数组，每个会话包含 `session_id`、`started_at`、`ended_at`、`event_count` 和按 `(occurred_at, event_id)` 排序的 `members`；载荷事件的成员额外带 `event_type`、`schema_version`、`payload`。

### 查询单个会话 `GET /devices/{device_id}/sessions/{session_id}`

```bash
curl -s http://localhost:8000/devices/dev-1/sessions/<session_id>
```

不存在时返回 `404`。

## 会话演变快照

会话是读取时从全量有序事件实时归并的：迟到事件可能桥接两个会话，保留清理可能删除事件。**会话演变快照**把某一时刻设备的会话连同有序成员一起**冻结**，并支持对比任意两个快照，得到会话的产生、消失、合并、拆分与重组。

### 创建快照 `POST /devices/{device_id}/session-snapshots`

请求体 `{"snapshot_id": "<客户端选择的 id>"}`，`snapshot_id` 非空、不含 `/`、最长 256，且**全局唯一**。创建在**单个写事务（`BEGIN IMMEDIATE`）**内完成：先冻结设备当前的事件单调序列高水位（持久、不复用的 `events.seq`），再严格从 `seq <= 高水位` 的事件按现有 `SESSION_GAP_SECONDS` 配置归并出会话，最后把每个会话及其按 `(occurred_at, event_id)` 排序的成员**完整复制**到快照表（含 `event_type` / `schema_version` / `payload`）。

- `201 Created`：冻结成功。响应含 `event_high_water`、`gap_seconds`、`session_count`、`sessions`（结构与会话查询接口一致：`session_id`、`started_at`、`ended_at`、`event_count`、`members`）与 `created_at`。
- `200 OK`：同一设备、同一 `snapshot_id` 的重复提交——**幂等返回首次的原快照**（相同高水位、会话、成员与 `created_at`），即使此后迟到事件已让实时会话演变、或部分事件已被清理，也不重新冻结。
- `409 Conflict`：该 `snapshot_id` 已属于**另一台设备**（`reason: "snapshot_id_device_conflict"`，响应给出 `bound_device_id`）；快照 id 不能跨设备复用，本次不写入任何内容。

```bash
BASE=http://localhost:8000

# 两个相隔 480s（> 默认 gap 300s）的事件 -> 两个会话
curl -s -X POST $BASE/events -H 'Content-Type: application/json' \
  -d '{"device_id":"d","event_id":"a","occurred_at":"2026-09-20T10:00:00Z"}'
curl -s -X POST $BASE/events -H 'Content-Type: application/json' \
  -d '{"device_id":"d","event_id":"b","occurred_at":"2026-09-20T10:08:00Z"}'

curl -s -X POST $BASE/devices/d/session-snapshots \
  -H 'Content-Type: application/json' -d '{"snapshot_id":"snap-before"}'
# 201 {
#   "snapshot_id":"snap-before","device_id":"d","event_high_water":2,
#   "gap_seconds":300.0,"session_count":2,
#   "sessions":[{"session_id":"…","event_count":1,"members":[{"event_id":"a", ...}]},
#               {"session_id":"…","event_count":1,"members":[{"event_id":"b", ...}]}],
#   "created_at":"…"
# }

# 同一 id 重复提交 -> 200，返回原快照
curl -s -o /dev/null -w '%{http_code}\n' -X POST $BASE/devices/d/session-snapshots \
  -H 'Content-Type: application/json' -d '{"snapshot_id":"snap-before"}'   # => 200

# 跨设备复用同一 id -> 409
curl -s -X POST $BASE/devices/other/session-snapshots \
  -H 'Content-Type: application/json' -d '{"snapshot_id":"snap-before"}'
# 409 {"detail":{"reason":"snapshot_id_device_conflict",
#               "bound_device_id":"d","conflicting_device_id":"other", ...}}
```

### 查询快照

- `GET /devices/{device_id}/session-snapshots`：按创建时间升序列出该设备的快照摘要（`snapshot_id`、`event_high_water`、`gap_seconds`、`session_count`、各会话的 `session_id` / `started_at` / `ended_at` / `event_count` 与 `created_at`）。
- `GET /devices/{device_id}/session-snapshots/{snapshot_id}`：取单个快照的完整内容（含全部冻结成员）。快照不存在、或该 id 属于其它设备时返回 `404`。

### 两快照差异 `POST /devices/{device_id}/session-snapshots/diff`

请求体：

```json
{"old_snapshot_id": "snap-before", "new_snapshot_id": "snap-after"}
```

对比**同一设备**的两个冻结快照（方向为 old → new）。算法以**共享 `event_id`** 构建旧会话（左）与新会话（右）的二部图：一个旧会话与一个新会话只要共享至少一个成员就连边；每个连通分量按下表分类：

| `change` | 判定 |
|---|---|
| `unchanged` | 1 个旧会话 ↔ 1 个新会话，成员集合完全相同 |
| `changed` | 1 个旧会话 ↔ 1 个新会话，成员集合不同（有增/删成员但未拆分合并） |
| `merged` | 多个旧会话 → 1 个新会话（如迟到事件桥接） |
| `split` | 1 个旧会话 → 多个新会话（如中间事件被清理后链条断开） |
| `reorganized` | 多个旧会话 ↔ 多个新会话的多对多连通分量 |
| `created` | 分量只含新会话（其成员在旧快照中不存在） |
| `removed` | 分量只含旧会话（其成员在新快照中不存在） |

每个分量输出：

- `old_session_ids` / `new_session_ids`：双方会话 ID（不存在的一侧为 `[]`）；
- `added_member_event_ids`：新侧有、旧侧无的成员 ID，按 `(occurred_at, event_id)` 稳定排序；
- `removed_member_event_ids`：旧侧有、新侧无的成员 ID，同样稳定排序。

各分量按起始时间稳定排序。**对比只读取冻结副本，不读实时事件表**，因此迟到事件与清理都不会改变既有快照之间的对比结果。

- `200 OK`：返回 `device_id`、两个快照 id、`gap_seconds` 与 `changes` 数组。
- `404 Not Found`：任一快照不存在或不属于路径中的设备（含两 id 分属不同设备的情况）。
- `409 Conflict`：两个快照冻结时使用的 gap 配置不同（`reason: "gap_configuration_mismatch"`，响应给出两侧 `gap_seconds`）；会话边界依赖 gap，配置不同不可比较。

```bash
# 迟到事件 c 落在中间（两侧各 240s <= 300s）-> 两个旧会话合并为一个
curl -s -X POST $BASE/events -H 'Content-Type: application/json' \
  -d '{"device_id":"d","event_id":"c","occurred_at":"2026-09-20T10:04:00Z"}'
# 再晚些来一个独立事件 d -> 另一个新会话
curl -s -X POST $BASE/events -H 'Content-Type: application/json' \
  -d '{"device_id":"d","event_id":"d","occurred_at":"2026-09-20T11:00:00Z"}'

curl -s -X POST $BASE/devices/d/session-snapshots \
  -H 'Content-Type: application/json' -d '{"snapshot_id":"snap-after"}'

curl -s -X POST $BASE/devices/d/session-snapshots/diff \
  -H 'Content-Type: application/json' \
  -d '{"old_snapshot_id":"snap-before","new_snapshot_id":"snap-after"}'
# {
#   "device_id":"d","old_snapshot_id":"snap-before",
#   "new_snapshot_id":"snap-after","gap_seconds":300.0,
#   "changes":[
#     {"change":"merged",
#      "old_session_ids":["<hash(a)>","<hash(b)>"],
#      "new_session_ids":["<hash(a,c,b)>"],
#      "added_member_event_ids":["c"],"removed_member_event_ids":[]},
#     {"change":"created","old_session_ids":[],
#      "new_session_ids":["<hash(d)>"],
#      "added_member_event_ids":["d"],"removed_member_event_ids":[]}
#   ]
# }
```

### 不可变性、与清理的关系、存储与迁移

- **迟到事件不改写旧快照**：事件按其插入时拿到的单调 `seq` 与快照冻结的高水位比较；高水位之后到达的事件（即使事件时间落在旧区间）只影响实时会话与之后创建的新快照，旧快照的会话、成员、`session_id` 与高水位均原样保留。
- **清理不改写旧快照**：快照成员是**完整复制**（不是按 `event_id` 引用 `events` 行），且快照表对 `events` **不建外键**。[可确认的保留清理](#可确认的保留清理)删除事件后，旧快照照常可读取、其差异结果照常可复算；被删事件在差异中表现为 `removed` / `split`。
- 快照头部与会话分别持久化在 SQLite 的 `session_snapshots`（`snapshot_id` 全局主键、`device_id`、冻结的 `event_high_water` 与 `gap_seconds`、`session_count`、`created_at`）与 `session_snapshot_sessions`（`snapshot_id` + 有序 `session_index`、`session_id`、起止时间、规范化 JSON 的有序成员副本）表。
- 升级到本版本时服务启动以 `CREATE TABLE IF NOT EXISTS` **自动、幂等**建表（旧库无需手工脚本，Docker Compose 启动方式、端口与既有接口完全不变）；新接口与同一套 WAL + `BEGIN IMMEDIATE` + `busy_timeout` 并发模型一致：高水位读取、事件窗口读取与快照行插入在同一把单写者锁下完成，并发创建不会把彼此的事件冻结进高水位。



每台设备拥有一个**严格按创建序列（FIFO）交付**的持久化下行命令队列：提交时携带 `command_id` 与**任意 JSON payload**；设备端通过 `claim` 领取**队首**并获得一个不可预测、带有效期的 `lease_token`，用它完成 `ack`（成功）或 `nack`（失败重试）。队列状态全部持久化在 SQLite 的 `device_commands` 表，服务重启后租约、退避与 attempt 计数都原样保留。

命令的派生交付状态有四种：

| 状态 | 含义 |
|---|---|
| `queued` | 等待领取（含租约已过期、退避已结束的命令） |
| `leased` | 队首被当前未过期租约持有 |
| `retry_wait` | 被 nack，处于 `retry_after_seconds` 退避期 |
| `acked` | 终态：已完成，离开交付队列 |

### 提交命令 `PUT /devices/{device_id}/commands/{command_id}`

请求体为 `{"payload": <任意 JSON>}`，`payload` 键必须存在（值可以是对象、数组、字符串、数字、布尔或 `null`）。命令标识为 **`(device_id, command_id)`**，不同设备可复用同一 id：

- `201 Created`：入队成功，返回当前状态（`queued`、`attempts: 0`）。
- `200 OK`：同一设备、同一 `command_id` 且 **payload 内容相同**的重复提交——幂等回放，不会重复入队、不改变排队位置、不重置已有的领取计数；对象键顺序不同但语义相同也算相同（payload 以规范化 JSON 比较）。
- `409 Conflict`：同一 `(device_id, command_id)` 已存在但 **payload 内容不同**，已存命令原样保留，绝不覆盖。
- `422`：请求体缺少 `payload` 键。

对一条已经 `acked` 的命令做相同内容的重复提交仍是 `200` 幂等回放，不会重新入队。

```bash
curl -s -X PUT http://localhost:8000/devices/sensor-7/commands/reboot \
  -H 'Content-Type: application/json' \
  -d '{"payload": {"zone": "a", "delay": 5}}'
# 201 {"command_id":"reboot","status":"queued","attempts":0, ...}

# 内容相同（键顺序无关）-> 200 幂等回放
curl -s -X PUT http://localhost:8000/devices/sensor-7/commands/reboot \
  -H 'Content-Type: application/json' \
  -d '{"payload": {"delay": 5, "zone": "a"}}'

# 内容不同 -> 409，原命令不受影响
curl -s -X PUT http://localhost:8000/devices/sensor-7/commands/reboot \
  -H 'Content-Type: application/json' -d '{"payload": {"delay": 9}}'
```

### 领取队首 `POST /devices/{device_id}/commands/claim`

请求体 `{"lease_seconds": 120}`，取值范围 **1～300 秒**（越界返回 `422`）。在**单个写事务（`BEGIN IMMEDIATE`）内**领取该设备按 `seq` 排序的最老未完成命令：

- `201 Created`：领取成功，返回命令 `payload`、`attempts`（首次为 `1`）、不可预测的 `lease_token`（32 字节随机量）、`leased_at` 与 `lease_expires_at`。
- `409 Conflict`：队列为空，或**队首仍在租约期 / nack 退避期**。**绝不越过队首**：即使后面已有其他命令，只要队首不可领取就返回 409。
- 租约**过期后**队首可被重新领取：签发**新的不可预测 token**、`attempts` 递增，旧 token 立即作废。
- **并发领取最多一人成功**：并发 claim 在 SQLite 单写者锁上串行化，先到者得到 `201`，其余全部看到"队首已被租约持有"而得到 `409`。

```bash
curl -s -X POST http://localhost:8000/devices/sensor-7/commands/claim \
  -H 'Content-Type: application/json' -d '{"lease_seconds": 120}'
# {
#   "command_id": "reboot",
#   "payload": {"delay": 5, "zone": "a"},
#   "attempts": 1,
#   "lease_token": "lCuFDq0cPiCLSJxGq5rjDb9-h7zweUgP9qFSfp_ZRwo",
#   "leased_at": "2026-09-21T14:30:30.937181Z",
#   "lease_expires_at": "2026-09-21T14:32:30.937181Z",
#   ...
# }
```

### 确认完成 `POST /devices/{device_id}/commands/{command_id}/ack`

请求体 `{"lease_token": "<token>"}`，只接受**当前且未过期**的 token：

- `200 OK`：命令置为终态 `acked`（记录 `acked_at`），离开交付队列，后面的命令才能成为新队首。
- `409 Conflict`：命令已 ack、token 不是当前 token（含从未领取、被 nack 作废、已被新租约轮换的旧 token），或 token 曾有效但**已过期**（过期后需重新 claim 获取新 token）。重复 ack 同样是 `409`。
- `404 Not Found`：该设备下没有此命令。

ack 与 nack 的结算与 claim 一样在**单个写事务（`BEGIN IMMEDIATE`）**内完成：命令查找（未知命令 `404`）、token 与到期校验、以及状态更新都在同一把单写者锁下，且最终的 `UPDATE` 本身带原子条件——`state='pending'` **且**存储中的 `lease_token` 与请求 token 相同**且** `expires_at` 仍晚于当前时刻，命中行数不为 1 即返回 `409` 且不改动任何状态。因此同一个有效 token 被两个请求并发提交时，后到者在锁上排队后看到的已是结算结果，只能得到 `409`，**同一租约有且仅有一个结算请求成功**；先 nack 后到的旧 token ack 不会把命令"复活"成 acked，先 ack 后到的旧 token nack 也不会把终态重新打开。

### 否定并安排重试 `POST /devices/{device_id}/commands/{command_id}/nack`

请求体 `{"lease_token": "<token>", "retry_after_seconds": 2}`，`retry_after_seconds` 为非负数（默认 `0`，负数返回 `422`）：

- `200 OK`：**立即作废当前 token**（之后用它 ack/nack 都是 `409`）、清空租约，命令在 `retry_after_seconds` 秒后重新开放（`0` 表示立即开放）。退避期间状态为 `retry_wait`、`not_before` 给出重新开放时刻。
- 退避期内该命令仍是队首：claim 返回 `409` 且**不会越过它**交付后面的命令；退避结束后重新领取，`attempts` 递增、签发新 token。
- token 校验规则与 ack 一致：错误/过期 token 返回 `409`，未知命令返回 `404`。
- nack 与 ack 共用同一套原子结算：并发的 ack/ack、nack/nack、ack/nack（无论先后）都在单写者锁上串行化，**只有一个请求返回 200**，其余 `409` 且不改状态；后到的 nack 不会覆盖先到 nack 已写入的退避截止时刻。

```bash
TOKEN='lCuFDq0cPiCLSJxGq5rjDb9-h7zweUgP9qFSfp_ZRwo'

curl -s -X POST http://localhost:8000/devices/sensor-7/commands/reboot/nack \
  -H 'Content-Type: application/json' \
  -d "{\"lease_token\": \"$TOKEN\", \"retry_after_seconds\": 2}"
# 200 {"status":"retry_wait","not_before":"...","attempts":1, ...}

# 退避期内：409，且后面的命令不会被越过交付
curl -s -X POST http://localhost:8000/devices/sensor-7/commands/claim \
  -H 'Content-Type: application/json' -d '{"lease_seconds": 60}'

sleep 3
# 退避结束：重新领取，attempts=2，得到新 lease_token
curl -s -X POST http://localhost:8000/devices/sensor-7/commands/claim \
  -H 'Content-Type: application/json' -d '{"lease_seconds": 120}'
```

### 查询单条命令状态 `GET /devices/{device_id}/commands/{command_id}`

返回该命令当前的完整状态（派生状态按当前时刻实时计算，因此租约过期会自动从 `leased` 变为 `queued`、退避结束会自动从 `retry_wait` 变为 `queued`，无需后台清理任务）。**任何查询接口都不会返回 `lease_token`**。未知命令返回 `404`。

```bash
curl -s http://localhost:8000/devices/sensor-7/commands/reboot
# {
#   "command_id": "reboot", "status": "leased", "attempts": 1,
#   "leased_at": "...", "lease_expires_at": "...",
#   "not_before": null, "acked_at": null, "created_at": "...", "updated_at": "..."
# }
```

### 完整调用示例：两条命令的严格 FIFO 交付

```bash
BASE=http://localhost:8000

# 1) 提交两条命令
curl -s -X PUT $BASE/devices/dev-1/commands/c1 -H 'Content-Type: application/json' -d '{"payload":{"op":"open"}}'
curl -s -X PUT $BASE/devices/dev-1/commands/c2 -H 'Content-Type: application/json' -d '{"payload":{"op":"close"}}'

# 2) 领取 -> 必定是队首 c1，租期 60s
C1=$(curl -s -X POST $BASE/devices/dev-1/commands/claim -H 'Content-Type: application/json' -d '{"lease_seconds":60}')
T1=$(python3 -c 'import sys,json;print(json.load(sys.stdin)["lease_token"])' <<<"$C1")

# 3) 租约期内再领 -> 409（不越过 c1）；并发 claim 同样只有一人成功
curl -s -o /dev/null -w '%{http_code}\n' -X POST $BASE/devices/dev-1/commands/claim \
  -H 'Content-Type: application/json' -d '{"lease_seconds":60}'   # => 409

# 4) c1 执行失败 -> nack，10s 后重试；旧 token 立即作废
curl -s -X POST $BASE/devices/dev-1/commands/c1/nack -H 'Content-Type: application/json' \
  -d "{\"lease_token\":\"$T1\",\"retry_after_seconds\":10}"
curl -s -o /dev/null -w '%{http_code}\n' -X POST $BASE/devices/dev-1/commands/c1/ack \
  -H 'Content-Type: application/json' -d "{\"lease_token\":\"$T1\"}"   # => 409 旧token

# 5) 退避结束后重新领取 -> c1，attempts=2，新 token
sleep 11
C1=$(curl -s -X POST $BASE/devices/dev-1/commands/claim -H 'Content-Type: application/json' -d '{"lease_seconds":60}')
T1=$(python3 -c 'import sys,json;print(json.load(sys.stdin)["lease_token"])' <<<"$C1")

# 6) ack c1 -> 队列出队；再次领取才得到 c2
curl -s -X POST $BASE/devices/dev-1/commands/c1/ack -H 'Content-Type: application/json' -d "{\"lease_token\":\"$T1\"}"
C2=$(curl -s -X POST $BASE/devices/dev-1/commands/claim -H 'Content-Type: application/json' -d '{"lease_seconds":60}')
T2=$(python3 -c 'import sys,json;print(json.load(sys.stdin)["lease_token"])' <<<"$C2")
curl -s -X POST $BASE/devices/dev-1/commands/c2/ack -H 'Content-Type: application/json' -d "{\"lease_token\":\"$T2\"}"

# 7) 队列已空 -> 409
curl -s -o /dev/null -w '%{http_code}\n' -X POST $BASE/devices/dev-1/commands/claim \
  -H 'Content-Type: application/json' -d '{"lease_seconds":60}'   # => 409
```

### 存储、迁移与并发

队列保存在 SQLite `device_commands` 表：`seq INTEGER PRIMARY KEY AUTOINCREMENT`（与 `events` 相同的**持久、单调、不复用**序列）固定每设备的严格创建次序，`UNIQUE (device_id, command_id)` 同时支撑幂等回放与 409 冲突判定；`attempts`、当前 `lease_token`、`leased_at`、`expires_at`、退避 `not_before` 与终态 `acked_at` 均在同一行原地更新。

升级到本版本时，服务启动会以 `CREATE TABLE IF NOT EXISTS` **自动、幂等**地建表（旧版数据库无需任何迁移脚本，Docker Compose 启动方式、端口与现有事件接口完全不变）；既有的 `events` / `schemas` / `cleanup_plans` 数据不受影响。`lease_token` 由 `secrets.token_urlsafe(32)` 生成，不可预测；状态判定与单写者事务模型沿用既有约定（WAL + `BEGIN IMMEDIATE` + `busy_timeout`），因此并发提交的 `seq` 唯一递增、并发 claim 严格最多一人成功。ack/nack 的结算同样在 `BEGIN IMMEDIATE` 内完成，且 UPDATE 以"当前 token 仍相同且未到期"为条件并校验命中行数：并发 ack/ack、nack/nack、ack/nack 以及与租约到期重领交错时，同一租约只有一个结算请求成功，落败方一律 `409` 且不改变命令状态（未知命令仍为 `404`，错误/过期 token 仍为 `409`）。

## 告警规则

每台设备可拥有若干条**版本化、不可变**的告警规则（标识为 `(device_id, rule_id)`）。规则版本只能追加（首个版本必须为 `1`，之后每次只能注册"当前最新版本 + 1"），已注册版本的内容永不修改；只有每条规则的**最新版本处于生效状态**，但旧版本会继续守护自己的事件时间**时代**（见下）。规则定义包含：

| 字段 | 说明 |
|---|---|
| `event_type` | 精确匹配事件类型；无载荷事件永不命中 |
| `conditions` | 最多 5 个 AND 载荷条件，语法与 `POST /events/search` 完全一致（RFC 6901 JSON Pointer + `exists`/`eq`/`lt`/`lte`/`gt`/`gte`）；留空表示匹配该类型的任意载荷事件 |
| `window_seconds` | 滑动时间窗口（> 0）；连续命中必须都落在以当前事件为终点的窗口内 |
| `consecutive` | 触发所需的**连续**命中次数（≥ 1）；任一不匹配事件都会清零连击 |
| `cooldown_seconds` | 触发后在**事件时间**上的冷却时长（≥ 0） |

### 事件时间语义与乱序处理

所有判定都以事件的 `occurred_at`（事件时间）为准，与写入/到达时刻无关：事件始终按 `(occurred_at, event_id)` 排序处理；窗口外的旧命中滑出窗口，任何不匹配事件重置连击。事件写入时，规则评估与事件插入在**同一个写事务（`BEGIN IMMEDIATE`）**内完成，因此并发写入在 SQLite 单写者锁上串行化；乱序（迟到）事件到达后，设备的**全部规则版本**（含已冻结的旧版本）基于有序事件集重放，结果与"事件按时间顺序到达"完全一致。

- **版本按 `occurred_at` 划分事件时间时代**：创建版本 v（v≥2）时，记录该设备当前事件集合的事件时间前沿 `F = max(occurred_at, event_id)`（创建时刻 `created_seq` 高水位对应的数据快照）。v 的下界严格高于 `F`，v 的前一版本同时在 `F` 处冻结（其上界含 `F`）；最新版本的时代上界开放。每个事件（受下面的不回填约束）按其事件时间键恰好归属一个版本，因此**版本切换之后才到达、但事件时间落在旧版本时代的迟到事件会被重放到那个冻结版本**（例如旧版本连击因此补齐并触发 v1 告警），而不是只喂给最新版本——最终告警集合与到达顺序无关。
- **创建时刻不回填（`created_seq`）**：版本只评估"创建之后到达"的事件（`seq > created_seq`）；创建时已经存在的历史事件即使事件时间落在该版本时代内也不会回填。设备上尚无任何事件时连续创建多个版本，则中间版本的时代永远为空，只有最新版本会对后续事件触发。
- 同一 `event_id` 的重放重跑同一份确定性评估，告警以 `(规则版本, 触发事件 event_id)` 的确定性 `trigger_key` 去重——**重放绝不重复触发**。
- 冷却内只保留**一条**打开的告警：冷却以事件时间计算（`fired_at + cooldown_seconds`）；冷却结束后，后续的新连击可以再次触发。
- 若乱序重算使一条已产生的触发不再成立（例如迟到的不匹配事件插入了连击中间），该告警被置为终态 `invalidated`；**已确认（acked）的历史始终保留**，不会被改为失效。
- 规则命中进度（当前滑动窗口内的连击）与抑制门状态持久化在 `alarm_rule_progress` 表，服务重启后原样继续。

### 创建规则版本 `PUT /devices/{device_id}/alert-rules/{rule_id}/versions/{version}`

- `201 Created`：版本注册成功（首个版本必须为 1，之后必须连续递增）。
- `409 Conflict`：版本号不是"最新 + 1"（跳号、重复注册不可变版本）。并发创建同一版本时只有一个请求成功，其余在写锁内重新读到最新版本后得到 `409`。
- `422`：路径版本与请求体 `version` 不一致、窗口/次数/冷却取值非法、指针或条件值非法。

```bash
curl -s -X PUT http://localhost:8000/devices/dev-1/alert-rules/overheat/versions/1 \
  -H 'Content-Type: application/json' \
  -d '{
    "version": 1,
    "event_type": "telemetry",
    "conditions": [
      {"pointer": "/reading/value", "operator": "gte", "value": 80}
    ],
    "window_seconds": 300,
    "consecutive": 3,
    "cooldown_seconds": 600
  }'
# {
#   "device_id": "dev-1", "rule_id": "overheat", "version": 1,
#   "event_type": "telemetry", "conditions": [ ... ],
#   "window_seconds": 300.0, "consecutive": 3, "cooldown_seconds": 600.0,
#   "active": true, "created_seq": 0, "created_at": "..."
# }
```

随后任意顺序提交三条窗口内连续命中的事件，即产生一条告警；即使三条以逆序到达，触发事件仍为事件时间最晚的那条，连击成员按 `(occurred_at, event_id)` 排列。

### 查询规则

- `GET /devices/{device_id}/alert-rules`：列出该设备全部版本（含 `active` 标记）。
- `GET /devices/{device_id}/alert-rules/{rule_id}/versions/{version}`：取单个版本，未知返回 `404`。

### 告警列表 `GET /devices/{device_id}/alarms`

按 `fired_at` 倒序返回告警实例。可用 `?status=` 过滤：`active` / `expired` / `acked` / `invalidated`，非法取值返回 `422`。

告警状态：

| 状态 | 含义 |
|---|---|
| `active` | 未确认且仍在事件时间冷却内 |
| `expired` | 未确认但冷却（按当前墙钟时间衡量）已过；规则可再次触发 |
| `acked` | 终态：已确认 |
| `invalidated` | 终态：乱序重算后该触发不再成立（已确认的告警不会进入此状态） |

每个告警包含 `rule_id` / `rule_version`、`trigger_event_id`、按事件时间有序的 `member_event_ids`、`fired_at`、`first_match_at`、`cooldown_until`（事件时间上的重新开放时刻）以及 `acked_at` / `invalidated_at`。

> 冷却门以**事件时间**抑制触发（乱序安全），而列表中的 `active`/`expired` 是按**当前墙钟时间**对 `cooldown_until` 的派生展示，无需后台清理任务。

### 确认告警（带状态/版本条件）`POST /devices/{device_id}/alarms/{alarm_id}/ack`

请求体必须同时给出乐观条件 `expected_status` 与 `expected_rule_version`：

```json
{"expected_status": "active", "expected_rule_version": 1}
```

- `200 OK`：当前状态与版本都匹配，告警置为终态 `acked`（记录 `acked_at`）。确认会把**当前事件到达高水位**写为 `acked_seq`：确认时刻已存在的事件不可能再触发该规则；**确认之后到达**的事件不受剩余事件时间冷却限制，可再次触发（"过期或确认后才允许再次触发"）。
- `200 OK`（幂等回放）：对已 `acked` 的告警以 `expected_status: "acked"` 重复确认，返回首次确认的同一份结果（相同 `acked_at`）。
- `409 Conflict`：当前派生状态或规则版本与条件不符，或告警已被乱序重算置为 `invalidated`；不改任何状态。
- `422`：`expected_status` 不是 `active` / `expired` / `acked` 之一（`invalidated` 不可确认）。
- `404`：设备下无此告警。

查找、条件校验与带条件的 `UPDATE ... WHERE state='active'` 在同一个 `BEGIN IMMEDIATE` 事务内完成并校验命中行数，因此对同一告警的并发确认只有一个请求完成状态迁移，其余得到 `409`。

```bash
# 冷却（墙钟时间）过后状态显示为 expired，仍可确认
curl -s http://localhost:8000/devices/dev-1/alarms | python3 -m json.tool

curl -s -X POST http://localhost:8000/devices/dev-1/alarms/<alarm_id>/ack \
  -H 'Content-Type: application/json' \
  -d '{"expected_status": "expired", "expected_rule_version": 1}'
```

### 存储与迁移

规则、告警实例与命中进度分别持久化在 SQLite 的 `alarm_rules`、`alarms`、`alarm_rule_progress` 表。`alarm_rules` 除到达高水位 `created_seq` 外还保存每个版本的事件时间时代边界（`era_start_*` / `era_end_*`，即创建版本时设备的 `max(occurred_at, event_id)` 前沿）。升级到本版本时服务启动以 `CREATE TABLE IF NOT EXISTS` / 幂等 `ALTER TABLE ADD COLUMN` **自动、幂等**建表补列，并依据各版本的 `created_seq` 与现存事件**重建时代边界**（旧库无需手工脚本，Docker Compose 启动方式、端口与既有接口完全不变）；`alarms` 上有 `UNIQUE (rule_version_id, trigger_key)` 约束与 `(device_id, fired_at)` 索引，`alarm_rules` 上有 `UNIQUE (device_id, rule_id, version)`。并发模型沿用 WAL + `BEGIN IMMEDIATE` + `busy_timeout`：事件写入与规则评估同事务提交，所有版本的重放与乱序重算都不会产生重复告警。

## 设备状态机事件投影

每台设备可注册若干**不可变、只能追加**的状态机版本（首版本必须为 `1`，之后每次只能注册"当前最新版本 + 1"）。一个版本定义包含：

| 字段 | 说明 |
|---|---|
| `initial_state` | 初始状态，必须属于 `states` |
| `states` | 非空、不重复的状态名称集合 |
| `transitions` | 转移数组；每条含 `from_state`、`event_type`、`to_state` 与可选 `conditions` |

每条转移在机器处于 `from_state` 时参与匹配：事件的 `event_type` 必须精确相等，且 `conditions`（最多 5 个 AND 条件，语法与 `POST /events/search` 完全一致，RFC 6901 JSON Pointer + `exists`/`eq`/`lt`/`lte`/`gt`/`gte`）全部命中；`conditions` 留空表示匹配该类型的任意载荷事件。无载荷事件（不带 `event_type`/`schema_version`/`payload` 三元组）永不匹配。

### 启用与到达序投影

设备通过 `POST /devices/{device_id}/state-machine/enable` 启用**一个**已注册版本，当前状态初始化为该版本的 `initial_state`，并把到达高水位钉在启用时刻设备的 `MAX(events.seq)`——**启用前的历史事件绝不回填**。启用的版本不可更换：重复启用同一版本是 `200` 幂等回放，启用其它版本返回 `409`。

启用之后，每次 `POST /events` 都与事件插入在**同一个写事务（`BEGIN IMMEDIATE`）**内完成投影，严格按**到达序**（持久、单调、不复用的 `events.seq`，而非 `occurred_at`）执行：

- **唯一匹配**：恰有一条转移命中时，机器迁移到 `to_state`，并向追加式转移日志写入一行；响应的 `state_transition` 给出本次转移（`from_state` / `to_state` / `transition_index` 等）。
- **非法事件（零匹配）**：当前状态下没有任何转移命中 → `409`（`reason: "no_matching_transition"`），**事件不落库、状态不前进、日志不追加**，整个事务回滚（同一事务内的告警重放也不会发生）。
- **歧义事件（多匹配）**：当前状态下有多于一条转移命中 → `409`（`reason: "ambiguous_transition"`，响应列出命中的转移下标），同样**完全不落库**。
- **重复 `event_id`**：幂等判定先于投影——与已存在事件完全相同的重复提交返回 `200`（`replay: true`），回放首次投影得到的那条转移，即使该事件在"当前状态"下已不合法也不会改判；同一 `event_id` 但内容不同仍是原有语义的 `409`。
- 未启用状态机的设备行为与旧版完全一致，`state_transition` 为 `null`。

### 注册版本 `PUT /devices/{device_id}/state-machine/versions/{version}`

- `201 Created`：版本注册成功（首版本为 1，之后必须连续递增）。
- `200 OK`：相同定义的重复 PUT（幂等回放），无副作用。
- `409 Conflict`：版本号不是"最新 + 1"（跳号），或已注册版本的定义不同——版本不可改写。
- `422`：路径版本与请求体 `version` 不一致；`states` 为空或重复、`initial_state` 不在状态集合中、转移引用未知状态、完全重复的转移定义、指针/操作符/条件值非法。`errors` 中每项带 JSON Pointer（如 `/initial_state`、`/transitions/0/to_state`、`/transitions/1/conditions/0/pointer`）。

```bash
curl -s -X PUT http://localhost:8000/devices/dev-1/state-machine/versions/1 \
  -H 'Content-Type: application/json' \
  -d '{
    "version": 1,
    "initial_state": "locked",
    "states": ["locked", "unlocked"],
    "transitions": [
      {
        "from_state": "locked", "event_type": "command", "to_state": "unlocked",
        "conditions": [{"pointer": "/action", "operator": "eq", "value": "unlock"}]
      },
      {
        "from_state": "unlocked", "event_type": "command", "to_state": "locked",
        "conditions": [{"pointer": "/action", "operator": "eq", "value": "lock"}]
      }
    ]
  }'
# 201 {"device_id":"dev-1","version":1,"initial_state":"locked", ...}

# 相同定义再次 PUT -> 200 幂等回放；跳号注册 v3 -> 409
```

### 查询版本

- `GET /devices/{device_id}/state-machine/versions`：列出该设备全部已注册版本（按版本号升序）。
- `GET /devices/{device_id}/state-machine/versions/{version}`：取单个版本，未知返回 `404`。

### 启用版本 `POST /devices/{device_id}/state-machine/enable`

请求体 `{"version": 1}`：

- `201 Created`：启用成功，返回状态机当前状态（初始为 `initial_state`，`last_event_seq` 为启用时刻高水位）。
- `200 OK`：重复启用同一版本的幂等回放。
- `409 Conflict`：设备已启用**另一个**版本（启用版本不可更换）。
- `404 Not Found`：该版本未在该设备注册。

### 查询当前状态 `GET /devices/{device_id}/state-machine`

```bash
curl -s http://localhost:8000/devices/dev-1/state-machine
# {"device_id":"dev-1","version":1,"current_state":"locked",
#  "last_event_seq":0,"enabled_at":"…","updated_at":"…"}
```

当前状态在每次投影时原地持久化，服务重启后原样可查；设备从未启用时返回 `404`。

### 转移日志（游标分页）`GET /devices/{device_id}/state-machine/transitions`

返回追加式投影日志，严格按**到达序**（日志自增 id，即投影序）排列。查询参数：

| 参数 | 说明 |
|---|---|
| `limit` | 每页条数，`1..500`，默认 `50` |
| `cursor` | 上一页返回的 `next_cursor`；首屏不传 |

```bash
BASE=http://localhost:8000

curl -s -X POST $BASE/devices/dev-1/state-machine/enable \
  -H 'Content-Type: application/json' -d '{"version": 1}'

# locked -> unlocked（唯一匹配，201，state_transition.to_state = unlocked）
curl -s -X POST $BASE/events -H 'Content-Type: application/json' -d '{
  "device_id":"dev-1","event_id":"e1","occurred_at":"2026-09-22T10:00:00Z",
  "event_type":"command","schema_version":"1.0","payload":{"action":"unlock"}
}'

# unlocked 状态下再发 unlock：零匹配 -> 409，事件不落库、日志不增长
curl -s -o /dev/null -w '%{http_code}\n' -X POST $BASE/events \
  -H 'Content-Type: application/json' -d '{
  "device_id":"dev-1","event_id":"e2","occurred_at":"2026-09-22T10:00:01Z",
  "event_type":"command","schema_version":"1.0","payload":{"action":"unlock"}
}'   # => 409

# lock 合法 -> 回到 locked
curl -s -X POST $BASE/events -H 'Content-Type: application/json' -d '{
  "device_id":"dev-1","event_id":"e3","occurred_at":"2026-09-22T10:01:00Z",
  "event_type":"command","schema_version":"1.0","payload":{"action":"lock"}
}'

# 重复 event_id（即使此刻在 locked 下 unlock 仍合法，也只回放首次结果）-> 200
curl -s -o /dev/null -w '%{http_code}\n' -X POST $BASE/events \
  -H 'Content-Type: application/json' -d '{
  "device_id":"dev-1","event_id":"e1","occurred_at":"2026-09-22T10:00:00Z",
  "event_type":"command","schema_version":"1.0","payload":{"action":"unlock"}
}'   # => 200

curl -s "$BASE/devices/dev-1/state-machine/transitions?limit=1"
# {"device_id":"dev-1","version":1,
#  "transitions":[{"event_seq":1,"event_id":"e1","event_type":"command",
#                  "occurred_at":"…","from_state":"locked","to_state":"unlocked",
#                  "transition_index":0,"projected_at":"…"}],
#  "next_cursor":"smc1:…","limit":1}
```

末页 `next_cursor` 为 `null`；设备未启用状态机时返回 `404`。cursor 为不透明的 HMAC 签名凭证（前缀 `smc1:`），格式非法、截断、伪造或签名不符一律 `422`；cursor 内嵌页大小，沿用游标继续翻页时以 cursor 中的 `limit` 为准。

### 存储、迁移与并发

版本定义、启用状态与转移日志分别持久化在 SQLite 的 `state_machine_versions`、`device_state_machines`、`state_transitions_log` 表：版本的完整定义以规范化 JSON 存于 `definition_json`（支撑幂等回放与不可变冲突判定）；启用行保存 `version`、`current_state` 与到达高水位 `last_event_seq`；日志以 `INTEGER PRIMARY KEY AUTOINCREMENT` 固定投影（到达）次序，并以 `UNIQUE (device_id, event_id)` 保证重复 `event_id` 绝不产生第二条日志。升级到本版本时服务启动以 `CREATE TABLE IF NOT EXISTS` **自动、幂等**建表（旧库无需手工脚本，Docker Compose 启动方式、端口与既有接口完全不变）。

并发模型沿用 WAL + `BEGIN IMMEDIATE` + `busy_timeout`：事件插入、唯一转移判定、状态推进、日志追加与告警重放在同一把单写者锁下串行化提交——并发事件按拿到写锁的顺序依次投影，先到者迁移状态，后到者在锁内看到的是迁移后的状态：合法则继续迁移，非法/歧义则 `409` 且回滚不留痕；同一 `event_id` 的并发提交恰有一个 `201`，其余为 `200` 幂等回放。状态推进的 UPDATE 还带 `last_event_seq < event_seq` 原子条件，任何乱序/陈旧投影都不可能回退当前状态。

## 采集完整性

设备侧的事件可能因弱网、重试、缓冲等原因**乱序、迟到或补传**。采集流为每台设备提供一条独立的、可检测缺口的有序采集通道：设备为每个事件分配流内序号 `source_seq`（**从 1 开始**），服务端维护"序号 → 事件"的**序号账本**并在事件落库的同一事务内绑定，从而可以随时回答"最高收到哪里、连续到哪里、中间缺了哪些序号"。

序号账本与检查点独立于 `events` 数据行：保留清理可以删除事件本身，但账本（哪个序号到过、绑定哪个 `event_id`）与已保存的检查点始终保留，因此删除事件后完整性仍可查询、检查点摘要仍可复算。

### 注册采集流 `PUT /devices/{device_id}/streams/{stream_id}`

请求体可省略（流的身份即定义，无可变字段）。流标识为 `(device_id, stream_id)`，`stream_id` 与 `command_id` 同形（非空、不含 `/`）：

- `201 Created`：注册成功，返回该流当前的完整性状态（此时计数与水位均为 0）。
- `200 OK`：重复注册同一流的幂等回放，返回首次注册的 `created_at` 与当前状态，不产生副作用。

```bash
curl -s -X PUT http://localhost:8000/devices/dev-1/streams/main
# 201 {"device_id":"dev-1","stream_id":"main","created_at":"…",
#      "event_count":0,"highest_seen":0,"contiguous_through":0,"missing_ranges":[]}
```

### 提交流事件 `PUT /devices/{device_id}/streams/{stream_id}/events`

请求体在 `POST /events` 的原有事件字段（`device_id`、`event_id`、`occurred_at`，以及可选的 `event_type` / `schema_version` / `payload` 契约三元组）之外增加 `source_seq`（≥ 1）。路径中的 `device_id` 必须与请求体一致，否则 `422`。事件本身**完全走现有摄取链路**：契约校验、`events` 落库、状态机投影、告警重算与序号账本绑定在**同一个 `BEGIN IMMEDIATE` 事务**内一并提交，任一步失败（含 `422` 契约失败、`409` 非法/歧义状态机事件）整体回滚，账本不留痕。原 `POST /events` 接口不变。

绑定规则（`(device_id, stream_id)` 命名空间内）：

- `(device_id, stream_id, source_seq)` **只能绑定一个 `event_id`**；
- 同一流内，一个 `event_id` 也只能占用一个 `source_seq`（同一事件可以在**不同流**中各出现一次）；
- **允许乱序补传**：可以先收 5、再补 1。

响应：

- `201 Created`：产生了新的序号绑定（事件可能是新插入，也可能是此前经 `POST /events` 或其它流已入库、本次为既有事件补绑序号）。
- `200 OK`（`replay: true`）：该 `(流, source_seq)` 已绑定**同一** `event_id`，且事件内容逐字段一致——幂等回放，不新增绑定。
- `409 Conflict`：
  - `reason: "source_seq_conflict"`：该序号已绑定**另一个** `event_id`（响应给出 `bound_event_id`）；
  - `reason: "event_id_conflict"`：该 `event_id` 已绑定同流的**另一个序号**（响应给出 `bound_source_seq`）；
  - 序号相同、`event_id` 相同但事件其它内容（`occurred_at`/契约三元组）不同：沿用既有事件内容冲突语义，同样 `409`，不改任何状态。
- `404 Not Found`：流未注册。`422`：契约不存在/载荷不合法，或 `source_seq < 1`、路径与体的 `device_id` 不一致。

```bash
BASE=http://localhost:8000

# 乱序到达：先 2，再 5，后补 1、3、4
curl -s -X PUT $BASE/devices/dev-1/streams/main/events \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"dev-1","source_seq":2,"event_id":"e2",
       "occurred_at":"2026-09-20T10:02:00Z"}'     # => 201

# 完全相同的重放 -> 200，replay=true
curl -s -X PUT $BASE/devices/dev-1/streams/main/events \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"dev-1","source_seq":2,"event_id":"e2",
       "occurred_at":"2026-09-20T10:02:00Z"}'     # => 200 {"replay":true,...}

# 序号 2 改绑别的 event_id -> 409 source_seq_conflict
curl -s -X PUT $BASE/devices/dev-1/streams/main/events \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"dev-1","source_seq":2,"event_id":"other",
       "occurred_at":"2026-09-20T10:09:00Z"}'     # => 409

# 同一 event_id 占用另一个序号 -> 409 event_id_conflict
curl -s -X PUT $BASE/devices/dev-1/streams/main/events \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"dev-1","source_seq":9,"event_id":"e2",
       "occurred_at":"2026-09-20T10:02:00Z"}'     # => 409
```

载荷事件（带契约三元组）的写法与 `POST /events` 完全一致，只是额外带上 `source_seq`；流事件同样会驱动已启用的状态机投影（响应含 `state_transition`）。

### 流状态 `GET /devices/{device_id}/streams/{stream_id}`（等价于 `.../status`）

返回流的完整性视图；流不存在返回 `404`：

| 字段 | 含义 |
|---|---|
| `event_count` | 已接受的序号绑定数量 |
| `highest_seen` | 已接受的最大 `source_seq`（账本为空时为 0） |
| `contiguous_through` | 从 1 起连续无缺口的最大序号（序号 1 缺失时为 0） |
| `missing_ranges` | 把 `1..highest_seen` 内的缺口**压缩**成若干闭区间 `[start, end]`；无缺口时为 `[]` |

`GET /devices/{device_id}/streams` 列出该设备所有流（按 `stream_id` 排序）及其状态。

```bash
# 已收 1、4、5、8
curl -s $BASE/devices/dev-1/streams/main
# {"stream_id":"main","event_count":4,"highest_seen":8,
#  "contiguous_through":1,"missing_ranges":[[2,3],[6,7]], ...}
```

### 检查点 `PUT /devices/{device_id}/streams/{stream_id}/checkpoints/{through_seq}`

为账本的无缺口前缀 `1..through_seq` 保存一个 SHA-256 检查点：

- 仅当 `1..through_seq` **每个序号都已到齐**才保存（`201`）。摘要按 **`source_seq` 升序**对 `(source_seq, event_id)` 序列、连同流坐标计算；与到达顺序无关（先乱后补的结果一致）。
- 前缀内存在缺口（或 `through_seq` 超过当前高水位）时返回 `409`（`reason: "checkpoint_gap"`），响应给出 `highest_seen`、`contiguous_through` 以及与该前缀相交的压缩缺口 `missing_ranges`，**不落库任何检查点**。
- **重复创建返回原结果**：对同一 `through_seq` 再次 PUT 返回 `200` 与首次完全一致的 `checksum_sha256` / `created_at`，不重新计算。
- `through_seq` 必须 ≥ 1（否则 `422`）；流未注册返回 `404`。

摘要材料为域分隔前缀 `device-event-stream-checkpoint:1\n` 后接规范化 JSON：

```json
{"device_id":"dev-1","stream_id":"main","entries":[[1,"e1"],[2,"e2"]]}
```

```bash
# 1..3 已齐
curl -s -X PUT $BASE/devices/dev-1/streams/main/checkpoints/3
# 201 {"through_seq":3,"checksum_sha256":"…","event_count":3,"created_at":"…"}

# 重复创建 -> 200，返回同一份结果
curl -s -X PUT $BASE/devices/dev-1/streams/main/checkpoints/3     # => 200

# 序号 4 缺失时想存到 5 -> 409
curl -s -X PUT $BASE/devices/dev-1/streams/main/checkpoints/5
# 409 {"detail":{"reason":"checkpoint_gap","through_seq":5,
#               "highest_seen":5,"contiguous_through":3,
#               "missing_ranges":[[4,4]]}}
```

`GET /devices/{device_id}/streams/{stream_id}/checkpoints` 按 `through_seq` 升序列出检查点；`GET .../checkpoints/{through_seq}` 取单个检查点（未知流/检查点 `404`）。

### 与保留清理的关系、存储与迁移

流注册、序号账本与检查点分别持久化在 SQLite 的 `collection_streams`、`collection_stream_events`、`collection_stream_checkpoints` 表。账本上有两条唯一约束：`PRIMARY KEY (device_id, stream_id, source_seq)`（一个序号只绑一个 `event_id`）与 `UNIQUE (device_id, stream_id, event_id)`（流内一个 `event_id` 只占一个序号）。账本表**不**对 `events(event_id)` 建外键，检查点摘要只依赖账本中的 `(source_seq, event_id)` 序列：因此经[可确认的保留清理](#可确认的保留清理)删除事件后，账本与检查点**原样保留**——状态接口照常返回、旧检查点幂等回放、甚至还能仅凭账本计算新检查点；删除事件也不会让某个序号变成可改绑（原绑定依旧冲突，原 `event_id` 的重放仍幂等）。

升级到本版本时服务启动以 `CREATE TABLE IF NOT EXISTS` **自动、幂等**建表（旧库无需手工脚本，Docker Compose 启动方式、端口与既有接口——包括原 `POST /events`——完全不变）。并发模型沿用 WAL + `BEGIN IMMEDIATE` + `busy_timeout`：账本判定与事件落库、状态机投影、告警重算在同一把单写者锁下串行化提交。

## 设备时钟校准

设备时钟会漂移、跳变甚至被重置。设备在能同时读到本机时钟与可信参考时钟（如对时成功的瞬间）时上报**校准锚点**，服务用锚点把事件携带的设备时间归一化到统一的参考时间轴。

每个锚点包含：

| 字段 | 含义 |
|---|---|
| `anchor_id` | 客户端选择的锚点标识；`(device_id, anchor_id)` 幂等 |
| `observed_seq` | 设备侧单调观测序号；必须**严格大于**该设备已接受的最大序号 |
| `device_time` | 观测瞬间的设备时钟读数（ISO-8601，朴素时间按 UTC） |
| `reference_time` | 同一瞬间的参考时钟读数；必须随锚点**严格递增**（含跨 epoch） |

校验与 epoch 规则：

- **幂等**：同一 `(device_id, anchor_id)` 且 `observed_seq` / `device_time` / `reference_time` 内容完全一致的重复 PUT 返回 `200`，无副作用；同 id 内容不同返回 `409`（`reason: anchor_content_conflict`）。
- **序号/参考时间**：`observed_seq` 不严格递增返回 `409`（`observed_seq_conflict`）；`reference_time` 不严格递增返回 `409`（`reference_time_not_monotonic`），均不落库。
- **时钟重置**：`device_time` 不前进（小于或等于上一锚点）视为时钟回退/重置，**开启新 epoch**（epoch 号 +1），不做跨重置的漂移比较，插值/外推直线也绝不跨越 epoch。
- **漂移闸门**：同一 epoch 内，按相邻两锚点计算速率偏差 `|reference_span - device_span| / reference_span × 10^6` ppm；超过 `CLOCK_DRIFT_PPM` 返回 `409`（`drift_ppm_exceeded`，响应带 `measured_drift_ppm`）且不落库。

### 上报锚点 `PUT /devices/{device_id}/clock/anchors/{anchor_id}`

```bash
# 首个锚点：epoch 1，drift_ppm 为 null
curl -s -X PUT http://localhost:8000/devices/dev-1/clock/anchors/a1 \
  -H 'Content-Type: application/json' \
  -d '{"anchor_id":"a1","observed_seq":1,
       "device_time":"2026-09-20T10:00:00Z",
       "reference_time":"2026-09-20T10:00:00Z"}'
# 201 {"epoch":1,"drift_ppm":null,...}

# 第二个锚点：设备表走 100s，参考表走 101s（约 9901 ppm，默认上限内）
curl -s -X PUT http://localhost:8000/devices/dev-1/clock/anchors/a2 \
  -H 'Content-Type: application/json' \
  -d '{"anchor_id":"a2","observed_seq":2,
       "device_time":"2026-09-20T10:01:40Z",
       "reference_time":"2026-09-20T10:01:41Z"}'
# 201 {"epoch":1,"drift_ppm":9900.99,...}

# 完全相同的重放 -> 200；序号不增 / reference_time 回退 / 漂移超限 -> 409 且不落库
```

### 查询锚点 `GET /devices/{device_id}/clock/anchors`

按 `observed_seq` 升序返回设备的全部锚点（含每个锚点的 `epoch` 与 `drift_ppm`）。

### 单事件归一化 `POST /devices/{device_id}/clock/normalize`

请求体给出一个 `device_time`，可选 `observed_seq`（选择“最后一个 `observed_seq` 不超过它的锚点”所属 epoch）或显式 `epoch`（二者互斥；都不给则用最新 epoch）。返回：

- `corrected_at`：参考时间映射结果；
- `epoch`：使用的时钟 epoch；
- `method`：计算方式——`anchor`（恰为锚点位置，精确）、`interpolate`（同一 epoch 相邻锚点之间的线性插值）、`extrapolate`（支持区间之外的线性外推，斜率取最近一对锚点；单锚点 epoch 假设速率 1:1）；
- `uncertainty_seconds` 与 `uncertainty.{lower,upper}`：对称的绝对不确定区间半宽与上下界（插值时按漂移上限从较远的括号锚点累积；外推时还要叠加锚点对实测斜率残差）；
- `bracket_anchor_ids`：括号锚点 `[left, right]`，外推时外侧为 `null`。

落在锚点支持区间之外时，只有偏离边缘不超过 `CLOCK_MAX_EXTRAPOLATION_SECONDS` 才允许外推；超出返回 `409`（`outside_extrapolation_range`）。设备尚无锚点、显式 epoch 不存在、或 `observed_seq` 早于首个锚点，同样返回 `409`（`no_calibration_anchors` / `observed_seq_before_first_anchor`）。

```bash
# 两锚点之间：设备 t=50s，斜率 101/100 -> corrected_at=50.5s
curl -s -X POST http://localhost:8000/devices/dev-1/clock/normalize \
  -H 'Content-Type: application/json' \
  -d '{"device_time":"2026-09-20T10:00:50Z"}'
# {
#   "corrected_at":"2026-09-20T10:00:50.500000+00:00",
#   "epoch":1,"method":"interpolate","uncertainty_seconds":2.5,
#   "uncertainty":{"lower":"...50T10:00:48+00:00","upper":"...50T10:00:53+00:00"},
#   "bracket_anchor_ids":["a1","a2"]
# }

# 精确命中锚点 -> method=anchor, 不确定度 0
# 超出外推预算 -> 409 {"reason":"outside_extrapolation_range", ...}
# 按观测序号选 epoch：{"device_time":"...","observed_seq":12}
```

### 跨设备时间线 `POST /clock/timeline`

把所有已存储事件的 `occurred_at` 视为各设备的设备时钟读数，用该设备**最新锚点链**归一化后，按 `(corrected_at, device_id, event_id)` 全序排序返回。没有锚点的设备、以及超出外推预算而无法映射的事件不出现在时间线中（不会让整页失败）。请求体可带 `device_ids`（只看指定设备）、`limit`（1..500，默认 50）与 `cursor`。

**冻结高水位**：首屏（无 cursor 的请求）把两个标记冻结进不透明 HMAC 游标——全局事件高水位 `events.seq` 与**各设备**的锚点高水位 `clock_anchors.seq`（响应同时以 `frozen_anchor_high_waters` 回显）。于是翻页过程中：

- 新写入的事件不会进入本次遍历（首屏之后不可见）；
- 新写入/重置的锚点即使会重排历史 `corrected_at`，也只影响**重新开始**的新遍历，进行中的遍历始终用冻结锚点链重算，键集分页因此既不重复也不漏项。

cursor 与规范化的设备过滤条件绑定：换过滤条件、伪造或截断签名一律 `422`；页大小内嵌在 cursor 中（与事件检索一致）。

```bash
curl -s -X POST http://localhost:8000/clock/timeline \
  -H 'Content-Type: application/json' -d '{"limit":50}'
# {
#   "events":[{"corrected_at":"...","epoch":1,"method":"interpolate",
#              "uncertainty_seconds":2.5,"device_id":"dev-1","event_id":"e7",
#              "occurred_at":"...", ...}, ...],
#   "next_cursor":"ctl1:...","limit":50,
#   "frozen_anchor_high_waters":{"dev-1":2,"dev-2":1}
# }

# 后续页：{"cursor":"ctl1:..."}；只看部分设备：{"device_ids":["dev-1"]}
```

### 存储与迁移

锚点持久化在 SQLite 的 `clock_anchors` 表（`(device_id, anchor_id)` 唯一、`observed_seq` 与时间列、`epoch`、实测 `drift_ppm`，以及与 `events` 同序列语义的持久、不复用 `seq`），并维护 `(device_id, observed_seq)` 与 `(device_id, seq)` 两个索引。升级到本版本时服务启动以 `CREATE TABLE IF NOT EXISTS` **自动、幂等**建表，旧库无需手工脚本，Docker Compose 启动方式、端口与既有接口保持不变。锚点的幂等/冲突判定、链校验与插入同在一把 `BEGIN IMMEDIATE` 单写者锁内完成，并发上报按设备锚点链串行校验。

## 事件检索 `POST /events/search`

按标量列与**载荷条件**检索事件，结果按 `(occurred_at, event_id)` 升序返回，并以不透明 cursor 做键集分页。请求体所有字段均可选（空请求体即检索全部事件）：

| 字段 | 说明 |
|---|---|
| `device_id` | 精确匹配设备 |
| `event_type` | 精确匹配事件类型；无载荷事件不匹配 |
| `schema_version` | 精确匹配契约版本；无载荷事件不匹配 |
| `occurred_at` | 半开区间对象 `{"start": ..., "end": ...}`，两端均可省略：`start` 含、`end` 不含；朴素时间按 UTC 处理，同一时刻的不同时区写法等价。`start >= end` 返回 `422` |
| `conditions` | 最多 **5** 个 AND 条件（见下），全部满足才命中 |
| `limit` | 每页条数，`1..500`，默认 `50` |
| `cursor` | 上一页返回的 `next_cursor`；首屏不传 |

### 载荷条件（RFC 6901 JSON Pointer）

每个条件形如 `{"pointer": "/reading/value", "operator": "gte", "value": 0}`：

- `pointer` 是指向**事件 payload 内部**字段的 [RFC 6901 JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901)（空串 `""` 指向载荷根）；`~0`/`~1` 转义、数组下标（`/tags/0`）均按规范处理，`-` 与前导零下标不命中。指针语法错误返回 `422`。
- 操作符：

| 操作符 | 语义 |
|---|---|
| `exists` | `value` 必须是布尔；字段**存在**（含显式 `null`）即为 `true`，字段缺失即为 `false` |
| `eq` | 严格按 **JSON 类型**相等：`1` 与 `"1"`、`true`、`null` 互不相等；`1` 与 `1.0` 相等（同为数字）；对象按键归一化后比较；**显式 `null` 与字段缺失严格区分** |
| `lt` / `lte` / `gt` / `gte` | **仅数值比较**：两侧都必须是 JSON 数字（布尔不是数字）。字符串、布尔、`null`、对象、数组以及缺失字段一律**不参与比较**（不报错，只是不命中）；请求中的 `value` 不是数字时返回 `422` |

- 各条件之间是 **AND**；无载荷事件（payload 列为 NULL）没有任何指针位置，只可能被 `exists: false` 命中。
- 条件里 `value` 键必须出现（即使是 `null`），缺失返回 `422`；超过 5 个条件返回 `422`。

```bash
curl -s -X POST http://localhost:8000/events/search \
  -H 'Content-Type: application/json' \
  -d '{
    "device_id": "dev-1",
    "occurred_at": {
      "start": "2026-09-01T00:00:00Z",
      "end":   "2026-10-01T00:00:00Z"
    },
    "conditions": [
      {"pointer": "/reading/value", "operator": "gte", "value": 20},
      {"pointer": "/tags", "operator": "exists", "value": true}
    ],
    "limit": 100
  }'
```

```json
{
  "events": [
    {"device_id": "dev-1", "event_id": "e1", "occurred_at": "…",
     "event_type": "telemetry", "schema_version": "1.0", "payload": { … }}
  ],
  "next_cursor": "evc2:…",
  "limit": 100
}
```

末页的 `next_cursor` 为 `null`；无任何命中时首屏即返回 `{"events": [], "next_cursor": null, ...}`。

### 快照与 cursor 限制

- **首屏锁定**：不带 `cursor` 的请求在响应时锁定当前最大**持久序列 `seq`**（高水位）并写入后续每页 cursor。`seq` 是 `events` 表上 `INTEGER PRIMARY KEY AUTOINCREMENT` 的单调序列，其高水位持久化在 SQLite 的 `sqlite_sequence` 中，**删除当前最大序列值的事件后该值也不会被复用**——因此即使清理确认恰好删掉了高水位对应的事件，之后再写入（哪怕 `occurred_at` 排序位置落在游标之后）的事件也必定拿到更大的 `seq`，不会混入本次检索的后续页。翻页期间**新写入的事件不会混入**结果；期间被删除的事件则直接**缺席**，不影响继续翻页。需要看到新数据时，请不带 cursor 重新发起首屏。
- **cursor 版本兼容**：当前 cursor 前缀为 **`evc2:`**，载荷内显式带版本号并做 HMAC 签名。旧版本（**`evc1:`**，以隐式 `rowid` 为高水位）存在 rowid 被复用的隐患，提交旧 cursor 一律返回 **`422`**（`detail.message` 提示 cursor 来自不兼容的旧版本），重新发起一次不带 cursor 的首屏即可继续。签名、指纹校验规则与旧版一致。
- **cursor 绑定规范化查询**：cursor 内含查询摘要与 HMAC 签名。带着 cursor 更换任何条件（`device_id` / `event_type` / `schema_version` / `occurred_at` 区间 / 条件列表，或同一时刻的等价时区写法除外）都会返回 **`422`**；cursor 过期、截断、伪造、签名不符、**版本过旧**同样 **`422`**，且 `detail` 说明原因。修改 `limit` 不视为换条件（可以用不同页长继续走，但仍按同一条排序游标前进）。
- cursor 是**不透明**的分页凭证：只能用**上一页**返回的 `next_cursor` 顺序前进，不能跳转、不能复用旧页 cursor 配合新条件；格式非法、签名不符或版本过旧一律 `422`，重新发起首屏即可。
- 删除 cursor 锚点行本身是安全的：键集条件是严格的 `(occurred_at > ? OR (occurred_at = ? AND event_id > ?))`，锚点缺席只会少一条，不会重复或跳过其他行。

## 可确认的保留清理

清理采用**两阶段、可确认**流程：先创建清理计划（预演，不删任何数据），确认时在事务内重新计算候选，只有结果与计划**完全一致**才整批删除；一旦发现漂移即在同一事务内把计划标记为终态 `invalidated` 并返回 `409`，此后该计划永远不会再删除事件（事件数据本身在失效时同样原样保留）。

候选选择规则（全部按 `occurred_at`）：

- 仅属于指定 `device_id`；
- 严格早于 `before`（`occurred_at < before`，截止时刻本身保留）；
- 不落在任何保护时间段内。保护段为半开区间 `[start, end)`：落在起点受保护，落在终点可删除；多段重叠时按并集处理。
- 时间戳均归一化为 UTC（朴素时间按 UTC 处理）。

### 创建清理计划（预演）`POST /retention/cleanup-plans`

请求体：

| 字段 | 说明 |
|---|---|
| `device_id` | 目标设备 |
| `before` | 排他截止时间（ISO-8601） |
| `protected_windows` | 保护段数组，每项含半开的 `start` / `end`；可为空 |

返回 `201`，**不删除任何事件**。`candidates` 为按 `(occurred_at, event_id)` 排序的候选清单，`count` 为数量；`plan_digest` 是对选择条件与有序候选 `event_id` 列表计算的 SHA-256（规范化 JSON，前缀 `device-event-retention-plan:1`，保护段在摘要中按起点排序，因此段的提交顺序不影响摘要）。

```bash
curl -s -X POST http://localhost:8000/retention/cleanup-plans \
  -H 'Content-Type: application/json' \
  -d '{
    "device_id": "dev-1",
    "before": "2026-09-10T00:00:00Z",
    "protected_windows": [
      {"start": "2026-09-01T08:15:00Z", "end": "2026-09-01T09:00:00Z"}
    ]
  }'
```

```json
{
  "plan_id": "9f3c…",
  "device_id": "dev-1",
  "before": "2026-09-10T00:00:00Z",
  "protected_windows": [
    {"start": "2026-09-01T08:15:00Z", "end": "2026-09-01T09:00:00Z"}
  ],
  "plan_digest": "8a1b…",
  "status": "pending",
  "candidates": [ {"event_id": "e1", "occurred_at": "2026-09-01T08:00:00Z", …} ],
  "count": 1,
  "created_at": "…",
  "confirmed_at": null
}
```

`start >= end` 的空保护段返回 `422`。可随时通过 `GET /retention/cleanup-plans/{plan_id}` 查看计划快照（包含当前 `status`：`pending` / `confirmed` / `invalidated`；未知计划 `404`）。

### 确认清理 `POST /retention/cleanup-plans/{plan_id}/confirm`

#### 计划状态语义

计划的 `status` 有三种取值，**终态不可逆**：

| 状态 | 含义 | 再次确认的结果 |
|---|---|---|
| `pending` | 预演后、尚未确认 | 按下方规则确认或失效 |
| `confirmed`（终态） | 候选与计划一致，批次已删除 | `200`，幂等回放**首次确认**的同一份结果（含相同的 `confirmed_at`），不会重复删除 |
| `invalidated`（终态） | 确认时检测到候选集合或摘要漂移 | 永远 `409`，**此后不可能再删除任何事件**，即使候选集合后来恢复成计划快照也一样 |

确认时在**单个写事务**（`BEGIN IMMEDIATE`）内按计划保存的条件（`device_id`、`before`、保护段）重新计算候选：

- 重新计算出的候选 `event_id` 列表与 `plan_digest` 均与计划**完全一致**：整批删除，计划置为 `confirmed`，返回 `200` 与计划内容（含 `confirmed_at`）。删除后会话接口按**剩余事件**实时重新归并。
- **首次**发现候选集合或摘要不一致（计划创建后有更早的事件入库、候选被外部删除等，即计划已**失效**）：在**同一写事务内**把计划持久化为 `invalidated` 并返回 `409`。事件一条都不会删除；此后对同一计划的任何确认都只会得到 `409`。这样即使候选集合后来“恢复”到与快照完全相同，旧计划也不会复活而误删事件。失效后请用相同条件重新 `POST` 一个新计划再确认。响应体同时给出计划/当前两套 `event_ids` 与摘要，便于比对后重新预演：

```json
{
  "detail": {
    "message": "cleanup plan is stale: the recomputed candidate set no longer matches the plan; the plan has been invalidated and can never delete events",
    "plan_id": "9f3c…",
    "planned_event_ids": ["e1"],
    "current_event_ids": ["e0", "e1"],
    "planned_digest": "8a1b…",
    "current_digest": "2d77…"
  }
}
```

对已经是 `invalidated` 的计划再次确认，同样返回 `409`，消息变为 `cleanup plan was previously invalidated … it can no longer be confirmed`，响应体仍附带当前候选集合与摘要。

- **幂等重放**：对已 `confirmed` 的计划再次确认（包括同一请求因网络重试而并发到达），返回完全相同的结果，不会重复删除。服务端先取得写锁再重新读取计划：并发的相同确认在锁上排队，后到者看到的已是 `confirmed`，统一回放首次确认结果而不是报 `409`。
- 未知计划返回 `404`；未知或已失效的计划都不可能删除任何事件。
- 候选为空的计划也可以正常确认（结果为删除 0 条）。

```bash
# 预演
curl -s -X POST http://localhost:8000/retention/cleanup-plans \
  -H 'Content-Type: application/json' \
  -d '{"device_id":"dev-1","before":"2026-09-10T00:00:00Z","protected_windows":[]}'
# => { "plan_id": "<id>", "count": 2, "candidates": [e1, e2], "status": "pending" }

# 确认（并发/重复提交同一计划都返回首次确认的同一结果）
for i in 1 2; do
  curl -s -X POST http://localhost:8000/retention/cleanup-plans/<id>/confirm &
done
wait
# => 两次都是 200 { "status": "confirmed", "count": 2,
#                 "confirmed_at": "…", "candidates": [e1, e2] }

# 若预演之后数据发生变化：
curl -s -X POST http://localhost:8000/retention/cleanup-plans/<id>/confirm
# => 409；同一写事务内计划已持久化为 "status": "invalidated"，事件原样保留
curl -s http://localhost:8000/retention/cleanup-plans/<id>
# => { "status": "invalidated", … }
# 此后无论候选集合如何变化，该计划的确认一律 409 且不删除事件；
# 需要继续清理时，用同一条件重新 POST 一个新计划再确认即可。
```

清理计划（条件、候选快照、摘要、状态与确认时间）持久化在 SQLite 的 `cleanup_plans` 表中。

## 透明事件回执

每次**首次成功写入**的事件（`POST /events` 与流事件 `PUT .../streams/{stream_id}/events` 走同一条摄取链路）都会在原 **`BEGIN IMMEDIATE`** 事务内、紧邻 `events` 行插入后追加一个 **Merkle 叶**。叶位置是密集的、从 1 开始且永不复用的日志序号；叶同时记录它绑定的 `events.seq`。**幂等重放（`200 replay`）与任何失败请求（`422` 契约失败、`409` 冲突/非法状态机事件）都不会新增叶**——叶与事件行在同一事务内提交或回滚，因此日志严格按到达序只增不改。

### 叶的规范化绑定与 RFC 6962 前缀

每个叶的输入是以下字段的**规范化 JSON**（键排序、无空白、UTF-8），字段固定为：

```json
{"device_id":"dev-1","event_id":"e1","event_type":null,"occurred_at":"2026-09-20T10:00:00+00:00","payload":null,"schema_version":null,"seq":1}
```

哈希严格遵循 [RFC 6962](https://www.rfc-editor.org/rfc/rfc6962#section-2.1) 的域分隔：

- 叶哈希：`SHA-256(0x00 || leaf_data)`
- 内部节点：`SHA-256(0x01 || left_hash || right_hash)`

无载荷事件的 `event_type` / `schema_version` / `payload` 为显式 JSON `null`；`occurred_at` 是归一化后的 UTC ISO-8601（等价时区写法产生同一叶）；`payload` 以规范化 JSON 持久化，对象键序不影响叶哈希。

### 当前日志 `GET /receipts/tree`

```bash
curl -s http://localhost:8000/receipts/tree
# {"tree_size":3,"root_hash":"…","checkpoint_count":1}
```

首个事件写入前 `tree_size` 为 `0`、`root_hash` 为 `null`。

### 创建不可变检查点 `PUT /receipts/checkpoints/{tree_size}`

冻结前 `tree_size` 个叶的 RFC 6962 根。检查点**不可变、不删除**：

- `201 Created`：冻结成功，返回 `tree_size`、`root_hash`、`created_at`。
- `200 OK`：对同一 `tree_size` 重复 PUT 的幂等回放——返回首次完全相同的 `root_hash` 与 `created_at`，不重新计算。
- `422 Unprocessable Entity`：`tree_size` 不在 `1..当前树大小`（含 0、未来大小、非整数），`detail.reason` 为 `invalid_tree_size` 并回显 `current_tree_size`；不写入任何检查点。

```bash
curl -s -X PUT http://localhost:8000/receipts/checkpoints/3
# 201 {"tree_size":3,"root_hash":"6f2a…","created_at":"…"}
curl -s -o /dev/null -w '%{http_code}\n' -X PUT \
  http://localhost:8000/receipts/checkpoints/3     # => 200（幂等回放）
curl -s -o /dev/null -w '%{http_code}\n' -X PUT \
  http://localhost:8000/receipts/checkpoints/99    # => 422（树还没这么大）
```

`GET /receipts/checkpoints` 按 `tree_size` 升序列出检查点；`GET /receipts/checkpoints/{tree_size}` 取单个（未知 `404`）。

### 成员证明 `GET /events/{event_id}/receipt`

按 `event_id` 返回该事件叶在指定树大小（可选 `?tree_size=`，默认当前大小）下的**包含证明（Merkle Audit Path）**。返回数据足以**仅凭自身离线复算**根：

| 字段 | 含义 |
|---|---|
| `leaf_index` | 叶的密集 1-based Merkle 位置 |
| `event_seq` | 绑定的、不复用的 `events.seq` |
| `tree_size` | 证明所基于的树大小 |
| `leaf_data` | 被哈希的完整规范化文档（验证方重新规范化即可） |
| `leaf_hash` | 十六进制 `SHA-256(0x00 || leaf_data)` |
| `audit_path` | 自底向上的 RFC 6962 审计路径（每项为 32 字节节点哈希的十六进制；单叶树为 `[]`） |
| `root_hash` | 按 `tree_size` 复算的根，可与可信检查点比对 |
| `checkpoint` | 当该大小存在检查点时，回显冻结的 `root_hash` / `created_at`；否则为 `null` |

- `404 Not Found`：该 `event_id` 没有回执叶。
- `422`：`tree_size` 非法（非正、超过当前树，`reason=invalid_tree_size`），或该叶不属于那么小的树（`reason=leaf_not_in_tree`）。

```bash
curl -s 'http://localhost:8000/events/e1/receipt?tree_size=3'
```

**离线验证示例**（不访问服务，仅用上面返回的 JSON 与一个可信检查点根）：

```python
import hashlib, json

def H(prefix, *parts):                 # RFC 6962 域分隔
    return hashlib.sha256(prefix + b"".join(parts)).digest()

def split(n):                          # 严格小于 n 的最大 2 的幂（n >= 2）
    return 1 << ((n - 1).bit_length() - 1)

def verify(proof, trusted_root_hex):
    data = json.dumps(proof["leaf_data"], sort_keys=True,
                      separators=(",", ":"), ensure_ascii=False).encode()
    leaf = H(b"\x00", data)            # 必须等于 proof["leaf_hash"]
    assert leaf.hex() == proof["leaf_hash"]
    nodes = [bytes.fromhex(x) for x in proof["audit_path"]]
    idx, size = proof["leaf_index"] - 1, proof["tree_size"]
    cursor = 0
    def take():
        nonlocal cursor
        node = nodes[cursor]; cursor += 1
        return node
    def walk(m, n):                    # RFC 6962 §2.1.1/§2.1.2 复算
        if n == 1:
            return leaf
        k = split(n)
        if m < k:                      # 兄弟是右侧子树根
            inner = walk(m, k)
            return H(b"\x01", inner, take())
        inner = walk(m - k, n - k)     # 兄弟在左侧
        return H(b"\x01", take(), inner)
    root = walk(idx, size)
    assert cursor == len(nodes)        # 路径必须恰好消费完
    assert root.hex() == trusted_root_hex
```

> 注意：非 2 的幂大小的树，审计路径长度是**变化的**（例如 3 叶树的第 3 个叶只有一个兄弟节点，而非两个）；上面按树分裂递归复算，天然处理这种情况，不依赖“树高固定”的逐位算法。

### 一致性证明 `GET /receipts/consistency/{first_size}/{second_size}`

返回两个**已冻结检查点**之间的**一致性证明（Merkle Consistency Proof）**：持有较早检查点可信根的客户端可仅凭返回数据离线复算出**两个根**，从而证明新树只是老树的**严格追加**——历史叶一个都没有被改写。

- `200 OK`：返回两个 `tree_size`、两边的 `root_hash` / `created_at` 与自底向上的 `consistency_path`（大小相等时为 `[]`）。
- `404 Not Found`：任一 `tree_size` 没有对应检查点。
- `422`：不满足 `1 <= first_size <= second_size <= 当前树大小`（`reason=invalid_tree_size`）。

```bash
curl -s http://localhost:8000/receipts/consistency/3/7
# {"first_tree_size":3,"second_tree_size":7,
#  "first_root_hash":"6f2a…","second_root_hash":"b910…",
#  "first_checkpoint_created_at":"…","second_checkpoint_created_at":"…",
#  "consistency_path":["…", "…"]}
```

离线复算遵循 RFC 6962 §2.1.4：从路径重建时，在顶层边界用**可信的旧根**作为左子树，路径节点按自底向上顺序消费；复算出的旧根必须等于可信旧根、新根必须等于响应中的 `second_root_hash`。

### 与保留清理的关系、存储与迁移

- **清理只删事件正文**：`event_receipt_leaves` 对 `events` **不建外键**，且叶内存有完整规范化文档副本。[可确认的保留清理](#可确认的保留清理)删除事件后，叶、按 `event_id` 的成员证明、旧检查点以及检查点间一致性证明**原样可验证**（被删事件仍可凭其叶与旧检查点离线证明曾被收录）；之后新事件继续追加日志，跨越清理点的一致性证明照常成立。
- 叶与检查点分别持久化在 SQLite 的 `event_receipt_leaves`（密集 Merkle 位置 `seq`、绑定的 `event_seq`、`event_id`、规范化 `leaf_data`、`leaf_hash`、`created_at`）与 `receipt_checkpoints`（`tree_size` 主键、`root_hash`、`created_at`）表，二者只追加、不更新、不删除。
- **旧库一次性回填**：升级后服务启动在单个事务内为回填前已存在的事件按 `events.seq` 顺序补叶；只用存储中已归一化的列，因此**结果逐字节可复现**（同一批事件在任意旧库回填出相同的根）。叶位置按到达序密集编号——即使旧库在升级前已删除过事件、使现存 `events.seq` 有缺口，Merkle 位置仍连续无缺口（缺口只体现在叶绑定的 `event_seq` 上）。回填幂等：已有叶即为空操作；之后新事件继续在密集尾部追加。
- 并发模型沿用 WAL + `BEGIN IMMEDIATE` + `busy_timeout`：叶追加与事件插入、状态机投影、告警重放在同一把单写者锁下串行提交，任何一步失败整体回滚，绝不留下半个叶。

## 会话标识

`session_id` 是确定性的 SHA-256：对 `device_id` 与按 `(occurred_at, event_id)` 排序的成员列表（每个成员取 UTC ISO-8601 时间戳与 `event_id`）做哈希。成员集合不变，标识不变；迟到事件桥接两组后成员变化，标识随之更新。该算法不包含载荷字段，新旧事件的归并结果完全一致。

## 调用示例：迟到事件桥接两个会话

```bash
# 两个相隔较远的事件 -> 两个会话（间隔 480s > 默认阈值 300s）
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' -d '{"device_id":"d","event_id":"a","occurred_at":"2026-09-20T10:00:00Z"}'
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' -d '{"device_id":"d","event_id":"b","occurred_at":"2026-09-20T10:08:00Z"}'
curl -s localhost:8000/devices/d/sessions        # => 2 个会话

# 迟到事件落在中间（两侧间隔均为 240s <= 300s）-> 桥接为一个会话
curl -s -X POST localhost:8000/events -H 'Content-Type: application/json' -d '{"device_id":"d","event_id":"c","occurred_at":"2026-09-20T10:04:00Z"}'
curl -s localhost:8000/devices/d/sessions        # => 1 个会话，成员 [a, c, b]
```

## 持久化与并发模型

契约（文档、`supersedes`、注册时间）与事件的 `event_type` / `schema_version` / `payload` 均保存在 SQLite（`schemas` 表与扩展后的 `events` 表）。保留清理计划保存在 `cleanup_plans` 表（条件、候选快照、摘要、状态、确认时间）。下行命令队列保存在 `device_commands` 表（`seq`、命令标识与规范化 payload、租约/退避时间戳、`attempts` 与终态）。版本化告警规则、告警实例、命中进度与规则版本的事件时间时代边界保存在 `alarm_rules` / `alarms` / `alarm_rule_progress` 表。设备状态机的不可变版本定义、启用状态/当前状态/到达高水位与追加式转移日志保存在 `state_machine_versions` / `device_state_machines` / `state_transitions_log` 表。设备时钟校准锚点保存在 `clock_anchors` 表（观测序号、设备/参考时间、epoch、实测漂移与持久 `seq`）。采集流、序号账本与检查点保存在 `collection_streams` / `collection_stream_events` / `collection_stream_checkpoints` 表。会话演变快照保存在 `session_snapshots` / `session_snapshot_sessions` 表（冻结的高水位、gap 配置与会话成员的完整 JSON 副本，不引用 `events` 行）。透明回执的 Merkle 叶与不可变检查点保存在 `event_receipt_leaves` / `receipt_checkpoints` 表（叶是规范化事件文档的完整副本、不引用 `events` 行，因此清理删除事件正文后回执仍可验证）。服务启动时自动幂等建表、为旧版数据库补齐新列、按需建出队列表，历史数据无需迁移脚本。`events` 上维护 `(device_id, occurred_at, event_id)` 与 `(occurred_at, event_id)` 两个索引，后者供 `POST /events/search` 的跨设备键集扫描使用；`device_commands` 上维护 `(device_id, seq)` 索引以支持严格 FIFO 取队首；`alarms` 上维护 `(device_id, fired_at)` 索引以支持告警列表；`state_transitions_log` 上维护 `(device_id, id)` 索引以支持按到达序的游标分页；`clock_anchors` 上维护 `(device_id, observed_seq)` 与 `(device_id, seq)` 两个索引，分别支撑锚点链校验与时间线首屏的逐设备锚点高水位冻结。

### 自动迁移与单调序列 `seq`

`events` 表以 `seq INTEGER PRIMARY KEY AUTOINCREMENT` 作为**持久、单调、不复用**的事件序列：每个事件入库时在写事务内取得唯一递增值，`sqlite_sequence` 持久记录已分配的最大值，即使删除当前最大 `seq` 的事件（例如保留清理确认），后续写入也只会拿到更大的值。`POST /events/search` 首屏以 `MAX(seq)` 锁定快照高水位，后续页继续按 `(occurred_at, event_id)` 键集顺序返回——删除只造成缺席，首屏之后的任何写入都不会出现。

升级到本版本时，服务启动会**自动、幂等**地迁移旧版 SQLite 文件（在单个事务内完成，无需手工脚本，Docker Compose 启动方式不变）：

1. 旧 `events` 表缺少 `seq` 列时，先按需补齐 `event_type` / `schema_version` / `payload` 旧列；
2. 以新结构重建 `events` 表，历史行按原 `rowid` 顺序赋值 `seq`（保持到达次序），并重建两个索引、保留 `event_id` 的唯一约束；
3. AUTOINCREMENT 计数器自动初始化为迁移行的最大 `seq`，此后新事件继续递增、绝不复用；
4. 透明回执表幂等建出后，在单个事务内为回填前已存在的事件按 `events.seq` 顺序**一次性补 Merkle 叶**（只用已存储的归一化列，结果可复现；现存 `seq` 若因升级前清理而有缺口，叶位置仍密集连续）。已有叶时该步骤为空操作。

迁移期间无需停写之外的额外操作；重复启动也是安全的（已迁移的库检测到 `seq` 列即跳过，已回填回执的库检测到叶即跳过回填）。若旧库文件较大，首次启动会多花一次表重建与叶回填的时间。

### cursor 版本

cursor 前缀随游标格式升级：事件检索当前为 `evc2:`（seq 高水位）。旧前缀 `evc1:`（rowid 高水位）的 cursor 不再可用——提交即返回 **`422`**，重新发起不带 cursor 的首屏即可；客户端无需解析 cursor 内容。设备状态机日志游标前缀为 `smc1:`，跨设备时钟时间线游标前缀为 `ctl1:`（内嵌事件高水位与逐设备锚点高水位），伪造或格式不符一律 `422`。

数据库以 **WAL** 模式运行：多个读请求互不阻塞，写操作经 `BEGIN IMMEDIATE` 在单写者锁上串行化。每个 HTTP 请求通过 FastAPI 依赖取得一条**私有连接**并在请求结束时关闭；连接以 `check_same_thread=False` 建立，因为 FastAPI 会把同步端点及其依赖的进入/退出分派到 anyio 线程池中**不同的工作线程**（SQLite 默认的线程亲和校验在并发下会抛异常）。连接不会跨线程共享，同时设置了 `busy_timeout=10s`，使锁竞争表现为短暂等待而非立即失败。并发写入的 `seq` 分配同样在该单写者事务内完成，因此高并发提交拿到的 `seq` 仍然唯一且严格递增。

清理确认的并发安全要点：确认端点开写锁**之后**才重新读取计划行，因此并发确认在锁上排队——先到者负责删除并置 `confirmed`（或在漂移时置 `invalidated`），后到者只回放终态结果，不会因“候选刚被前一个请求删掉”而把幂等重放误报为 `409`。

租约结算（ack/nack）的并发安全要点与 claim 一致：请求先以 `BEGIN IMMEDIATE` 取得写锁，再做命令查找、token 与到期校验和状态更新；UPDATE 的 WHERE 同时要求 `state='pending'`、存储的 `lease_token` 等于请求 token、`expires_at` 晚于当前时刻，并以命中行数（必须为 1）判定胜负。于是同一 token 的并发结算（ack/ack、nack/nack、ack/nack）在锁上串行化后只有先到者成功；nack 作废、租约到期重领导致 token 轮换之后，持旧 token 到达的结算请求匹配 0 行，返回 `409` 且绝不改状态。

## 摄取链路架构（POST /events）

`POST /events` 的写入链路分为**薄路由**与**摄取服务**两层：

- **薄路由**（`app/main.py` 的 `submit_event`）：只负责 HTTP 边界——开启 `BEGIN IMMEDIATE` 写事务、调用摄取服务、成功则 `commit`、任一步失败则 `rollback` 整体回滚；随后把领域错误映射为 HTTP 响应（`ContractError` → `422`、`IngestionConflict` → `409`），并把服务返回的框架无关结果（`IngestionOutcome`）映射为响应模型 `EventAccepted`。
- **摄取服务**（`app/ingestion.py` 的 `EventIngestionService`）：编排一次事件写入的全部步骤——契约三元组校验、契约解析与载荷校验、幂等判定、事件插入、**透明回执 Merkle 叶追加**、状态机投影（唯一/零/多匹配判定）、告警重算，以及响应会话的选取。服务通过**显式参数**获取协作者：SQLite 连接按次传入（`ingest(conn, ...)`），契约解析器（`contract_resolver`）与会话构建器（`session_builder`）在构造时注入。服务**不持有模块级连接，也不持有应用级缓存**，因此可以脱离 FastAPI 直接实例化、独立测试（见 `tests/test_ingestion.py`）。

事务边界不变：幂等判定、事件插入、回执叶追加、状态机投影与告警重算仍由**调用方（路由）开启的同一次 `BEGIN IMMEDIATE`** 覆盖——服务自身不开启、不提交、不回滚事务；任一步失败（包括告警重算抛出异常）都使事件行、回执叶、转移日志行、状态推进与告警变更**整体回滚**，绝不留下半成品；幂等重放不追加叶。响应语义不变：`replay`、事件回显、会话与 `state_transition` 的取值与分层前完全一致（会话在提交前基于同一事务快照归并，并发下结果也是确定的）。

每个 `create_app` 实例拥有**各自独立**的连接工厂、解析后契约缓存（`app.state.schema_cache`）与归并间隔配置：契约解析器与会话构建器以本实例的闭包注入服务，缓存与配置归该实例所有——两个实例之间互不串用（`tests/test_ingestion.py` 中有对应的回归验证）。写入链路共用的序列化辅助函数也随分层收敛：`canonical_json` 移至 `app/ingestion.py`、`log_entry_to_dict` 移至 `app/statemachines.py`，`app.main` 直接复用。

## 本地开发与测试

```bash
pip install -r requirements-dev.txt
pytest
SESSION_GAP_SECONDS=300 CLOCK_DRIFT_PPM=50000 \
  CLOCK_MAX_EXTRAPOLATION_SECONDS=3600 \
  uvicorn app.main:app --port 8000
```
