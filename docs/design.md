# 设备编队固件升级编排 —— 设计说明

本项目在本地用数据模拟"设备端 ↔ 云端"交互，实现设备注册、心跳与影子维护、
按组分批灰度、结果收敛、超时回滚与进程重启恢复。除 Go 标准库外无外部依赖，
所有时间均可注入（测试用可控时钟），结果在并发、乱序、重启下确定且可解释。

## 1. 包结构

| 包 | 职责 |
| --- | --- |
| `internal/version` | 版本号解析、确定序比较、升级合法性判定 |
| `internal/device` | 设备档案、设备影子（期望/上报分离）、心跳状态机、离线巡检 |
| `internal/store` | 单文件 JSON 快照的原子持久化（临时文件 + fsync + rename） |
| `internal/rollout` | 灰度计划、批次推进、任务状态机、超时收敛、回滚、重启恢复 |
| `cmd/demo` | 本地可运行的端到端演示 |

## 2. 状态模型

### 2.1 设备影子

每台设备的影子把**云端期望（desired）**与**设备上报（reported）**分别记录，
互不覆盖：

- `Reported{FirmwareVersion, Seq, At}`：设备最近一次被接受的心跳；
- `Desired{FirmwareVersion, At}`：最近一次下发计划设置的目标版本；
- `Online ∈ {never-seen, online, offline}`；
- `LastReason` + `LastTransitionAt`：最近一次在线状态变化的可解释原因。

收敛判定（`Shadow.Converged`）：无期望即已收敛；无上报即未收敛；
否则要求 `reported.firmware >= desired.firmware`。

### 2.2 心跳与乱序/幂等

设备为每次心跳携带**单调递增序号 seq**，云端按 seq 判定：

| 情形 | 判定 | 行为 |
| --- | --- | --- |
| `seq > 当前 seq`（或首次） | 接受 | 更新上报版本与 `LastSeen`，必要时上线 |
| `seq == 当前 seq` | 重复，幂等接受 | **不**改版本/在线状态，仅刷新 `LastSeen`，原因 `duplicate-heartbeat-ignored` |
| `seq < 当前 seq` | 乱序旧心跳，拒绝 | 版本不回退、不上线、**不**刷新 `LastSeen`，原因 `stale-heartbeat-rejected` |

在线状态转换原因可区分：

- `first-heartbeat`：首次心跳，never-seen → online；
- `heartbeat-fresh`：持续在线的新心跳；
- `heartbeat-timeout`：巡检发现 `now - LastSeen > OfflineTimeout`，→ offline；
- `heartbeat-recovered`：offline 后收到更新心跳，→ online（与首次上线区分）；
- 离线后收到的重复/旧心跳不会让设备"假上线"。

离线巡检是幂等的：已经 offline 的设备重复巡检不会重复计数。

### 2.3 升级任务状态机

```
pending ──dispatch──▶ inprogress ──成功上报──▶ succeeded
  │                     │
  │ dispatch 失败       │ 失败上报 / 超过 JobTimeout
  ▼                     ▼
 failed（瞬态：回滚待执行/重试中）──回滚成功──▶ rolledback
```

- `succeeded`、`rolledback` 是**终态**；
- `failed` 是瞬态：表示升级已失败、等待回滚动作确认；
- 结果上报只在 `inprogress` 被接受：
  - 对终态任务的重复上报 → `duplicate report for terminal job`，忽略；
  - 对 `failed` 的迟到上报 → `late report ... rollback pending/done`，忽略；
  - 对 `pending`（从未派发）的上报 → 忽略；
  - 因此**重复或迟到上报不会重复计数，也不会把成功翻成失败或重复触发回滚**；
- 部分成功可逐设备定位：每台设备一个独立 Job，终态与 `LastError` 各自记录。

### 2.4 超时收敛

- 任务在派发时设置 `Deadline = now + JobTimeout`（派发会重置截止时间）；
- 每个 Tick 检查 `pending/inprogress` 任务，超过 Deadline 即收敛为 `failed`，
  `LastError` 写明 `timeout after <JobTimeout> in state=<原状态>`；
- 同一 Tick 紧接着对 `failed` 任务执行回滚。

## 3. 版本与兼容规则

版本号为严格的 `MAJOR.MINOR.PATCH`（三段非负整数，无前导 `v`）；比较按
MAJOR → MINOR → PATCH 字典序，结果确定。

合法升级必须**同时**满足，否则以确定错误拒绝：

1. 目标严格新于当前：拒绝降级（`ErrDowngrade`）、拒绝原地升级（`ErrSameVersion`）；
2. `MAJOR` 必须相同：跨大版本拒绝（`ErrMajorJump`）；
3. 同 MAJOR 下 `MINOR` 最多向前跨 **1**：`1.0.x → 1.2.x` 拒绝（`ErrMinorJump`）；
4. 同 MINOR 内 `PATCH` 允许任意前推：`1.2.0 → 1.2.9` 允许。

版本判定是**逐设备**的：某台设备因降级/跳变被拒绝，只跳过该设备，不影响
同组其它设备入队。

## 4. 灰度与回滚策略

### 4.1 按组分批

- `StartRollout(group, target)` 取组内设备（按 ID 排序，结果确定），
  按 `BatchSize` 切成批次：`batchIndex = orderIndex / BatchSize`；
- 每台设备**同一时刻至多一个在途升级**：已有非终态任务的设备在新计划中跳过；
- Tick 只派发"当前开放批次"（`Plan.CurrentBatch`）中的 `pending` 任务；
- 当前批不再有 `pending/inprogress` 任务时开放下一批。
  **注意：`failed` 是等待回滚的瞬态，不阻塞批次推进**——单台设备在途或
  阻塞（例如回滚目标设备暂时不可达）不会拖住其它设备的下发。

### 4.2 下发去重（"先落盘后动作"）

派发前先把任务置为 `Attempted=true / inprogress` 并 `fsync` 落盘，
然后才调用 Dispatcher；回滚同理使用 `RollbackAttempted`。

- 升级动作对每个任务至多发生一次；
- 回滚在失败时会**重试**（设备暂不可达时下个 Tick 再试），但语义上是
  同一个回滚动作的重试，恢复后恰好完成一次；
- `SimDispatcher` 记录每次调用，测试据此断言 `upgrade` 每设备恰 1 次。

### 4.3 回滚

- 触发条件：设备主动上报失败，或任务停留中间状态超过 `JobTimeout`；
- 动作：向设备下发 `ToVersion → FromVersion`（恢复到升级前版本）；
- 回滚成功后任务进入终态 `rolledback`；此后迟到的"成功上报"也被忽略，
  设备保持已回滚的确定状态。

## 5. 持久化与重启恢复

- 所有状态在单个 JSON 文件中做**全量原子覆盖**
  （写 `*.tmp` → `fsync` → `rename`），读端只会看到完整快照；
- 快照内容：设备档案、设备影子（在线状态、reported/desired、LastSeen、原因）、
  升级任务（状态、`Attempted`、截止时间、错误）；
- 进程重启后 `New()` 从快照重建：
  - `inprogress + Attempted=true` 的任务**恢复为在途**，等待结果或超时，
    **绝不重新下发升级**（避免设备被重复刷写）；
  - 崩溃时停在瞬态 `failed` 的任务继续等待/重试回滚；
  - `succeeded/rolledback` 终态原样恢复，Tick 不会对其产生任何下发；
  - 期望/上报版本随影子恢复，收敛判定不丢失。

## 6. 并发模型

- `device.Registry` 用 `sync.RWMutex` 保护档案与影子；
- `rollout.Orchestrator` 用单把互斥锁串行化所有任务状态变更与快照写入，
  Dispatcher 调用在锁外语义上"只发生一次"由先落盘的 `Attempted` 保证；
- `SimDispatcher` 自带锁保护调用记录，可在并发测试中断言；
- 仓库以 `go test -race ./...` 验证无数据竞争。

## 7. 本地验证方法

```bash
# 端到端演示（打印每个输入与判定依据，状态写入 ./demo-data/state.json）
go run ./cmd/demo

# 全部单元测试（重复运行结果确定，不依赖任何外部服务）
go test ./...

# 带竞态检测
go test -race ./...

# 查看详细判定日志（输入 + 判定依据均在 t.Logf 中）
go test -v ./internal/rollout/
```

测试覆盖：乱序/重复心跳、离线超时与恢复原因、影子收敛、版本降级/跳变拒绝、
分批灰度、单台阻塞不影响他人、重复/迟到结果上报、并发心跳与并发下发、
超时回滚、以及三类重启恢复（在途不重发 / 终态不重放 / failed 续回滚）。
