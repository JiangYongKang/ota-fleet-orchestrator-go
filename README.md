# OTA Fleet Orchestrator（本地模拟版）

一套**本地可运行**的设备编队固件升级编排能力。设备端与云端交互全部由本地数据/内存结构与 JSON 文件模拟，**不依赖任何外部服务**，用于验证设备在线状态、期望配置（影子）与升级进度在**并发、乱序、重启**场景下仍然一致且可解释。

## 1. 目录结构

```
internal/
  clock/         时钟抽象：RealClock（墙钟）与 FakeClock（测试可控，可快进超时）
  version/       版本解析、确定序比较、升级合法性规则、既定升级路径（path.go）
  device/        设备注册、心跳、设备影子（desired / reported）、离线判定
  upgrade/       单设备升级任务生命周期、结果幂等去重、超时收敛、按发布统计在途数
  orchestrator/  按组分批灰度、并发上限、暂停/恢复、多步路径升级、回滚编排、重启恢复
  store/         JSON 文件原子持久化（写临时文件后 rename）
cmd/demo/        本地端到端演示
*_test.go        乱序心跳、重复上报、并发下发、超时回滚、重启恢复、限流补位、暂停恢复、路径升级等测试
```

## 2. 状态模型

### 2.1 设备影子（`internal/device`）

每台设备分离记录两段状态：

| 段 | 字段 | 含义 |
|---|---|---|
| desired | `DesiredVer` / `HasDesired` | 云端期望设备运行的版本；下发时写入 |
| reported | `ReportedVer` / `ReportedSeq` | 设备最近一次确认上报的版本与单调序号 |

- **收敛判定** `Converged()`：无 desired 视为已收敛；否则要求 `reported == desired`。
  - 成功：reported 追平 desired 后清除 desired；
  - 失败/回滚：撤回 desired，设备保留原版本，desired 与 reported 不再“悬空不一致”。
- **在线状态**：`ONLINE` / `OFFLINE`，变化原因可区分：
  - `REGISTERED`：首次注册即在线；
  - `HEARTBEAT_LOST`：超过心跳窗口（`heartbeatTimeout`）未收到更新心跳，由 `SweepOffline` 判定；
  - `HEARTBEAT_RESTORED`：离线期间收到**更新**心跳后恢复上线。
- **心跳乱序与幂等**（设备端 `Seq` 单调）：
  - `seq < 已应用seq` → `STALE`，拒绝，绝不回退 reported；
  - `seq == 已应用seq`（seq≠0 初始）→ `DUPLICATE`，幂等忽略；
  - `seq > 已应用seq` → `ACCEPTED`（若设备离线则为 `RESTORED_ONLINE`）；
  - 未注册设备 → `UNKNOWN_DEVICE`。
- **重复注册**：幂等返回既有记录（`created=false`），不覆盖组、版本与影子。

### 2.2 升级任务（`internal/upgrade`）

阶段机：

```
DISPATCHED ──进展──> DOWNLOADING ──进展──> INSTALLING
    │                   │                     │
    └───────────────────┴─────────────────────┴── 成功上报 ─> SUCCEEDED  (终态)
                                      └──────── 失败上报 ─> FAILED     (终态)
                                      └──────── 超时扫描 ─> TIMEOUT_FAILED (终态)
                                                              └─回滚完成─> ROLLED_BACK (终态)
```

- **在途互斥**：每台设备同一时刻最多一个在途任务（`activeDevice` 映射），重复 `Dispatch` 返回错误；单台阻塞不影响其它设备。
- **上报幂等**（`HandleReport`）：
  - 上报序号更小 → `STALE`；相同 → `DUPLICATE`；
  - 终态结果（成功/失败）以 `ResultReportedYet` 保证**只计数一次**，迟到的成功/失败不会翻案；
  - 每次有效进展刷新 `Deadline = 上报时间 + stepTimeout`。
- **超时收敛**（`SweepTimeouts`）：超过 `Deadline` 仍处中间状态 → `TIMEOUT_FAILED`，原因字符串包含“停留阶段 + 截止时间”，随后由编排层执行回滚。
- **部分成功可定位**：`JobsByUpgrade(upgradeID)` 按设备 ID 排序返回每台设备阶段；`Stats` 汇总 total/succeeded/failed/rolled_back/active。

## 3. 版本与兼容规则（`internal/version`）

- 版本固定为数字三元组 `major.minor.patch`，接受可选前导 `v`；比较严格按 `major → minor → patch` 数值序（确定语义，非字符串字典序，因此 `1.10.0 > 1.2.0`）。
- 升级合法性 `CheckUpgrade(from, to, rule)`：
  - 同版本重升、任何**降级**一律拒绝；
  - `SameMinorPatchAllowed`：仅允许同 minor 内的 patch 升级；
  - `NextMinorAllowed`：允许同 minor patch 升级或升级到**相邻**下一个 minor（禁止跨 minor、禁止跨 major）；
  - `AnyForwardAllowed`：任意严格向前升级允许。
- 灰度单在创建时对组内设备**逐台预校验**；非法设备会被逐台列出并整体拒单（不会出现“发到一半才发现非法跳变”）。

### 3.1 既定升级路径（多步跨版本升级）

机群中常有设备落后多个版本，`version.Path` 支持把跨版本升级拆成逐段进行：

- `NewPath([]Version)`：构造严格递增的路径（至少 2 个版本；重复或回降的路径本身即被拒绝）。
- `PlanSteps(from, to)`：计算 `from -> to` 沿路径的逐段步骤；**路径之外的版本组合明确拒绝**，`from` 在路径上位于 `to` 之后（降级）同样拒绝。
- `NextStep(cur, target)`：返回从当前版本走向目标的下一段目标版本；已到达、路径外、降级方向均返回 `ok=false`。
- 在发布策略中设置 `BatchPolicy.Path` 即启用路径模式（此时忽略 `Rule`，以路径为唯一合法依据）；编排层每拍为设备派发“当前版本 → 下一段”的任务，每一段成功落地后才推进下一段。

## 4. 灰度与回滚策略（`internal/orchestrator`）

- **分批**：`BatchSizes` 按设备 ID 排序后稳定切批（确定性，可重复）；批次 0 成功前不推进后续批次。
- **整体并发上限**：`BatchPolicy.MaxConcurrent` 限制**同一条发布内**同时处于在途状态的设备数（0 = 不限）。到达上限后其余设备排队等待；一旦有设备退出在途（成功、失败、超时回滚），下一拍 `Advance` 立即从队列补位。个别设备卡在中间状态时，其槽位由超时收敛机制回收，排队设备继续补位，整条任务不会被拖停；某条发布被暂停也不影响其它发布的推进节奏。
- **暂停 / 恢复**：`PauseRollout(id)` / `ResumeRollout(id)` 按发布粒度控制（均幂等，状态随快照持久化）：
  - 暂停后该发布不再派发新任务；已在途的设备按当前状态自然收尾（上报、超时、回滚照常受理）；
  - 恢复后从**各设备当前版本**继续推进：已到达目标版本的设备直接跳过、绝不重复下发，未处理的设备接着派发；
  - 暂停/恢复只作用于目标发布，其它分组的发布节奏完全不受影响。
- **多步路径升级**：`BatchPolicy.Path` 非空时沿既定路径逐段升级（见 3.1）。创单时逐台校验 `当前版本 -> 目标版本` 在路径上可达，路径外组合与降级整单拒绝；运行期每段成功后才派发下一段。路径中段失败时设备**停留在已升到的中间版本**（回滚也只回到本段的起点，绝不回降已成功的段），发布按既定熔断语义止步；修复后新建发布会从设备当前版本继续，不从头再来。进程重启同理：恢复后按持久化的当前版本续走下一段。
- **推进 `Advance`**（可重复调用、并发安全）：
  - 只处理当前批次；设备已到达目标版本 → 跳过（重启/恢复后**不重复执行**）；
  - 设备在本批已有失败/回滚终态任务 → 止步，不自动重试；
  - 设备离线 → 本拍 `defer`，恢复上线后的下一拍自动补发，**不阻塞同批其它设备**；
  - 设备在线且空闲且未达并发上限 → 建立 DISPATCHED 任务并写入 desired（路径模式下为下一段目标）。
- **批次裁决**（`recomputeBatches`）：以设备为单位判定（兼容多步路径升级）：
  - 设备当前版本已到达目标 → 完成；设备在本批有失败/超时/回滚终态任务 → 止步；其余为未决；
  - 批内全部设备完成 → `SUCCEEDED`，开放下一批；
  - 批内全部已决且存在止步设备 → `FAILED`，发布熔断结束，后续批次绝不下发。
- **定时驱动 `Tick`**：离线扫描 → 超时收敛 → 对超时任务撤回 desired 并调用注入的 `RollbackFunc`（模拟设备端回滚到 `fromVersion`）→ 登记 `ROLLED_BACK` → 持久化。每一步都返回人类可读的判定说明。
- **重启恢复**：每次状态变更后把注册表、任务、发布整体快照原子写入 JSON；启动时 `Restore()` 重建：
  - 在途任务保持在途（仍占用设备槽位与并发额度）；
  - 已到达目标版本的设备即使在“未完成批次”中也只会被跳过，不会二次下发；
  - 多步路径升级按持久化的设备当前版本续走下一段，已完成的段绝不重放；
  - 暂停标记随快照恢复，重启后暂停的发布不会意外恢复派发；
  - rollout 序号从快照恢复，新发布 ID 不复用。

## 5. 本地验证方法

要求 Go 1.25+（仅标准库，无第三方依赖）。

```bash
# 全部单测（-race 检测并发；-v 查看每个用例打印的输入与判定依据）
go test ./... -race -v

# 端到端演示（使用临时目录中的 state.json，结束自动清理）
go run ./cmd/demo
```

测试覆盖与关键场景：

| 测试 | 场景 |
|---|---|
| `version.Test*` | 解析非法输入、数值序比较、降级/跨版本跳变拒绝 |
| `version.TestNewPathValidation` / `TestPlanSteps*` / `TestNextStep` | 路径构造校验（重复/回降拒绝）、多段拆解、路径外与降级组合拒绝 |
| `device.TestHeartbeatOutOfOrderAndDuplicate` | 乱序旧心跳不回退、重复心跳幂等 |
| `device.TestOfflineDetectionAndRestore` | 超时离线、原因区分、旧心跳不能恢复、新心跳恢复上线 |
| `device.TestConcurrentRegisterAndHeartbeat` | 50 心跳 + 20 重复注册并发（-race） |
| `upgrade.TestProgressThenSuccessCountedOnce` | 进展去重、成功只计一次、迟到成功不翻案 |
| `upgrade.TestTimeoutConvergence` | 进展刷新截止时间、超时收敛、回滚登记、sweep 幂等 |
| `upgrade.TestPartialSuccessLocatable` | 成功/失败/在途混合，逐设备定位 |
| `orchestrator.TestOfflineDeviceDeferredNotBlocking` | 离线设备延后补发，不阻塞同批设备 |
| `orchestrator.TestTimeoutTriggersRollbackAndHaltsCanary` | 超时→回滚→批次失败→熔断后续批次 |
| `orchestrator.TestRestartRecoversAndDoesNotDuplicate` | 重启恢复在途任务，已完成设备不重复下发 |
| `orchestrator.TestConcurrentAdvance` | 8 个并发 Advance，每设备恰好一个任务 |
| `orchestrator.TestMaxConcurrentQueueAndRefill` | 并发上限排队、退出在途即时补位、卡住设备超时被回收不拖停整单 |
| `orchestrator.TestPauseResumeRollout` | 暂停不新发/在途自然收尾、恢复不重复下发、其它分组不受影响、暂停跨重启保持 |
| `orchestrator.TestPathRolloutMultiStep` | 沿路径逐段升级、路径外/降级创单即拒 |
| `orchestrator.TestPathRolloutRestartResumesMidPath` | 路径中段重启后从中间版本续走、不重放已完成段 |
| `orchestrator.TestPathRolloutFailureKeepsProgress` | 中段失败停留在中间版本不回降，新发布从当前版本继续 |
| `store.TestSaveLoadRoundTrip` | JSON 原子写：往返一致、无 .tmp 残留 |

所有测试使用固定基准时间（`2026-09-24T10:00:00Z`）与 `FakeClock`，不依赖墙钟与外部服务，可重复运行。

## 6. 兼容范围说明

- **新增字段全部可选，零值即旧行为**：`BatchPolicy.MaxConcurrent=0` 表示不限并发；`BatchPolicy.Path` 为空表示按 `Rule` 一次到位升级。未使用新能力的调用方与既有用例行为完全不变。
- **持久化快照向后兼容**：旧版本写出的 `state.json` 不含新字段，恢复后取零值（不限并发、未暂停、单步升级），语义不漂移；新快照新增字段（`max_concurrent`、`path`、`paused`）随 JSON 自然序列化。
- **路径模式与 Rule 互斥**：`Path` 非空时以既定路径为唯一合法依据并忽略 `Rule`；路径之外的版本组合（含降级）在创单时整单拒绝。单步模式仍走原有 `CheckUpgrade` 规则。
- **限流与暂停均按发布维度生效**：`MaxConcurrent` 只统计本发布在途任务；`PauseRollout`/`ResumeRollout` 只作用于目标发布，不同分组/发布之间的推进节奏互不影响。
- **熔断语义不变**：批内出现失败/回滚终态即判定批次 `FAILED` 并熔断后续批次；多步路径升级同样遵守。失败设备保留已升到的中间版本，修复后需新建发布从当前版本继续（不会自动重试，也不会从头再来）。

## 7. 设计取舍说明

- 心跳序号与升级上报序号是**两条独立序列**：心跳乱序由 `Registry` 保护；升级结果的一次性由 `Manager` 的 `ResultReportedYet` 保证，二者不互相干扰。
- 批次裁决从“任务终态计数”改为“**以设备当前版本为准**”：设备影子中的当前版本是唯一事实源，天然支持多步路径升级、重启续走与“已成功不重复下发”，无需为每台设备额外维护步骤进度表。
- 超时以“**每次进展刷新的单步截止时间**”而非“下发总时长”计算，更贴近真实下载/安装分阶段场景。
- 持久化采用每次变更后整体快照（模拟规模小、可读性高、易于验证）；生产化时可替换为 WAL/数据库，只需实现 `orchestrator.Persist` 接口。
