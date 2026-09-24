# OTA Fleet Orchestrator（本地模拟版）

一套**本地可运行**的设备编队固件升级编排能力。设备端与云端交互全部由本地数据/内存结构与 JSON 文件模拟，**不依赖任何外部服务**，用于验证设备在线状态、期望配置（影子）与升级进度在**并发、乱序、重启**场景下仍然一致且可解释。

## 1. 目录结构

```
internal/
  clock/         时钟抽象：RealClock（墙钟）与 FakeClock（测试可控，可快进超时）
  version/       版本解析、确定序比较、升级合法性规则
  device/        设备注册、心跳、设备影子（desired / reported）、离线判定
  upgrade/       单设备升级任务生命周期、结果幂等去重、超时收敛
  orchestrator/  按组分批灰度、推进、回滚编排、重启恢复
  store/         JSON 文件原子持久化（写临时文件后 rename）
cmd/demo/        本地端到端演示
*_test.go        乱序心跳、重复上报、并发下发、超时回滚、重启恢复等测试
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

## 4. 灰度与回滚策略（`internal/orchestrator`）

- **分批**：`BatchSizes` 按设备 ID 排序后稳定切批（确定性，可重复）；批次 0 成功前不推进后续批次。
- **推进 `Advance`**（可重复调用、并发安全）：
  - 只处理当前批次；设备已有终态任务 → 跳过（重启后**不重复执行**）；
  - 设备离线 → 本拍 `defer`，恢复上线后的下一拍自动补发，**不阻塞同批其它设备**；
  - 设备在线且空闲 → 建立 DISPATCHED 任务并写入 desired。
- **批次裁决**（`recomputeBatches`）：批内任务全部终态时：
  - 全部成功 → `SUCCEEDED`，开放下一批；
  - 存在失败/回滚 → `FAILED`，发布熔断结束，后续批次绝不下发。
- **定时驱动 `Tick`**：离线扫描 → 超时收敛 → 对超时任务撤回 desired 并调用注入的 `RollbackFunc`（模拟设备端回滚到 `fromVersion`）→ 登记 `ROLLED_BACK` → 持久化。每一步都返回人类可读的判定说明。
- **重启恢复**：每次状态变更后把注册表、任务、发布整体快照原子写入 JSON；启动时 `Restore()` 重建：
  - 在途任务保持在途（仍占用设备槽位）；
  - 已记录 `JobID` 的设备即使在“未完成批次”中也只会被跳过，不会二次下发；
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
| `store.TestSaveLoadRoundTrip` | JSON 原子写：往返一致、无 .tmp 残留 |

所有测试使用固定基准时间（`2026-09-24T10:00:00Z`）与 `FakeClock`，不依赖墙钟与外部服务，可重复运行。

## 6. 设计取舍说明

- 心跳序号与升级上报序号是**两条独立序列**：心跳乱序由 `Registry` 保护；升级结果的一次性由 `Manager` 的 `ResultReportedYet` 保证，二者不互相干扰。
- 超时以“**每次进展刷新的单步截止时间**”而非“下发总时长”计算，更贴近真实下载/安装分阶段场景。
- 持久化采用每次变更后整体快照（模拟规模小、可读性高、易于验证）；生产化时可替换为 WAL/数据库，只需实现 `orchestrator.Persist` 接口。
