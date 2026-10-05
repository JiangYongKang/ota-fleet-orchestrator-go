package orchestrator

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/clock"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/device"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/upgrade"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

// BatchPolicy 灰度批次策略。
type BatchPolicy struct {
	Group         string                    `json:"group"`
	ToVersion     version.Version           `json:"to_version"`
	Rule          version.CompatibilityRule `json:"rule"`
	BatchSizes    []int                     `json:"batch_sizes"`    // 每批设备数；为 0 表示组内全部设备一批
	StepTimeout   time.Duration             `json:"step_timeout"`   // 单设备中间状态超时
	MaxConcurrent int                       `json:"max_concurrent"` // 本任务整体在途升级上限；0 表示不限制
	Path          *version.Path             `json:"path,omitempty"` // 非空时按既定升级路径逐跳升级，忽略 Rule
	MaxAttempts   int                       `json:"max_attempts"`   // 路径模式下每一跳最多尝试次数；0 用默认值 3
}

// DefaultMaxAttempts 路径模式单跳默认最大尝试次数。
const DefaultMaxAttempts = 3

func (p BatchPolicy) maxAttempts() int {
	if p.MaxAttempts > 0 {
		return p.MaxAttempts
	}
	return DefaultMaxAttempts
}

// BatchStatus 批次状态。
type BatchStatus string

const (
	BatchPending   BatchStatus = "PENDING"
	BatchRunning   BatchStatus = "RUNNING"
	BatchSucceeded BatchStatus = "SUCCEEDED"
	BatchFailed    BatchStatus = "FAILED"
)

// Batch 灰度中的一个批次。
type Batch struct {
	Index     int         `json:"index"`
	DeviceIDs []string    `json:"device_ids"`
	Status    BatchStatus `json:"status"`
	JobIDs    []string    `json:"job_ids"`
}

// Rollout 一次按组灰度发布。
type Rollout struct {
	ID         string      `json:"id"`
	Policy     BatchPolicy `json:"policy"`
	Batches    []Batch     `json:"batches"`
	CreatedAt  time.Time   `json:"created_at"`
	FinishedAt time.Time   `json:"finished_at"`
}

func (r *Rollout) terminal() bool { return !r.FinishedAt.IsZero() }

// Persist 抽象本地持久化（重启不丢、不重复执行）。
type Persist interface {
	Save(state []byte) error
	Load() ([]byte, error)
}

// RollbackFunc 回滚动作（模拟设备端回滚到 fromVersion）。
type RollbackFunc func(job *upgrade.Job, at time.Time) error

// Orchestrator 协调注册表、任务管理器与持久化，方法并发安全。
type Orchestrator struct {
	mu  sync.Mutex
	reg *device.Registry
	mgr *upgrade.Manager
	clk clock.Clock
	p   Persist
	rb  RollbackFunc

	rollouts     map[string]*Rollout
	pausedGroups map[string]bool
	seq          int
}

// persistedState 是写入磁盘的完整状态快照。
type persistedState struct {
	Registry     device.Snapshot  `json:"registry"`
	Manager      upgrade.Snapshot `json:"manager"`
	Rollouts     []Rollout        `json:"rollouts"`
	PausedGroups []string         `json:"paused_groups"`
	Seq          int              `json:"seq"`
}

// New 创建编排器。
func New(reg *device.Registry, mgr *upgrade.Manager, clk clock.Clock, p Persist, rb RollbackFunc) *Orchestrator {
	return &Orchestrator{
		reg: reg, mgr: mgr, clk: clk, p: p, rb: rb,
		rollouts:     make(map[string]*Rollout),
		pausedGroups: make(map[string]bool),
	}
}

// persistLocked 调用方需持有 mu。
func (o *Orchestrator) persistLocked() error {
	if o.p == nil {
		return nil
	}
	st := persistedState{
		Registry: o.reg.Snapshot(),
		Manager:  o.mgr.Snapshot(),
		Seq:      o.seq,
	}
	ids := make([]string, 0, len(o.rollouts))
	for id := range o.rollouts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		st.Rollouts = append(st.Rollouts, *o.rollouts[id])
	}
	for g := range o.pausedGroups {
		if o.pausedGroups[g] {
			st.PausedGroups = append(st.PausedGroups, g)
		}
	}
	sort.Strings(st.PausedGroups)
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	return o.p.Save(b)
}

// StartRollout 校验组内每台设备的升级合法性并按 BatchSizes 切批，持久化后返回。
// 不立即下发：下发由 Advance 驱动，从而重启后可由持久化状态继续而不重复执行。
func (o *Orchestrator) StartRollout(policy BatchPolicy) (*Rollout, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	devs := o.reg.ListByGroup(policy.Group)
	if len(devs) == 0 {
		return nil, fmt.Errorf("no devices in group %q", policy.Group)
	}
	if policy.MaxConcurrent < 0 {
		return nil, fmt.Errorf("invalid max_concurrent %d: must be >= 0 (0 means unlimited)", policy.MaxConcurrent)
	}
	if policy.MaxAttempts < 0 {
		return nil, fmt.Errorf("invalid max_attempts %d: must be >= 0 (0 uses default %d)",
			policy.MaxAttempts, DefaultMaxAttempts)
	}
	if policy.Path != nil {
		// 路径模式：最终目标必须是路径终点；起点必须在路径上且落后于终点（禁止降级、禁止路径外版本）。
		versions := policy.Path.Versions()
		final := versions[len(versions)-1]
		if policy.ToVersion.Compare(final) != 0 {
			return nil, fmt.Errorf("rollout rejected: target %s is not the final version of the given path (final is %s)",
				policy.ToVersion, final)
		}
		var violations []string
		for _, d := range devs {
			next, more, err := policy.Path.NextStep(d.CurrentVer)
			switch {
			case err != nil:
				violations = append(violations, fmt.Sprintf("%s(current=%s): off-path version", d.ID, d.CurrentVer))
			case !more:
				violations = append(violations, fmt.Sprintf("%s(current=%s): already at/over the final version %s",
					d.ID, d.CurrentVer, final))
			case next.Compare(d.CurrentVer) <= 0:
				violations = append(violations, fmt.Sprintf("%s(current=%s): next step %s would not move forward",
					d.ID, d.CurrentVer, next))
			}
		}
		if len(violations) > 0 {
			sort.Strings(violations)
			return nil, fmt.Errorf("rollout rejected, %d device(s) not eligible for path: %v", len(violations), violations)
		}
	} else {
		// 逐台做版本合法性校验：单台非法不影响对其它设备给出明确判定，这里整体拒绝并列出原因。
		var violations []string
		for _, d := range devs {
			if err := version.CheckUpgrade(d.CurrentVer, policy.ToVersion, policy.Rule); err != nil {
				violations = append(violations, fmt.Sprintf("%s(current=%s): %v", d.ID, d.CurrentVer, err))
			}
		}
		if len(violations) > 0 {
			sort.Strings(violations)
			return nil, fmt.Errorf("rollout rejected, %d device(s) not eligible: %v", len(violations), violations)
		}
	}

	sizes := policy.BatchSizes
	if len(sizes) == 0 {
		sizes = []int{len(devs)}
	}
	idList := make([]string, len(devs))
	for i, d := range devs {
		idList[i] = d.ID
	}
	sort.Strings(idList)

	var batches []Batch
	idx := 0
	for bi, size := range sizes {
		if size <= 0 {
			continue
		}
		end := idx + size
		if end > len(idList) {
			end = len(idList)
		}
		batches = append(batches, Batch{
			Index:     bi,
			DeviceIDs: append([]string(nil), idList[idx:end]...),
			Status:    BatchPending,
		})
		idx = end
		if idx >= len(idList) {
			break
		}
	}
	if idx < len(idList) {
		batches = append(batches, Batch{
			Index:     len(batches),
			DeviceIDs: append([]string(nil), idList[idx:]...),
			Status:    BatchPending,
		})
	}

	o.seq++
	rid := fmt.Sprintf("rollout-%d", o.seq)
	r := &Rollout{
		ID: rid, Policy: policy, Batches: batches, CreatedAt: o.clk.Now(),
	}
	o.rollouts[rid] = r
	if err := o.persistLocked(); err != nil {
		return nil, err
	}
	cp := *r
	return &cp, nil
}

// Advance 推进发布：找到当前批次，对其中尚无在途任务且在线的设备下发。
// 离线设备跳过等待，不阻塞同批其它设备；批次全部终态后推进下一批。
// 受 MaxConcurrent 整体在途上限约束：到上限后排队设备等待补位。
// 返回本拍每个动作的说明（可解释）。
func (o *Orchestrator) Advance(upgradeID string) ([]string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	r, ok := o.rollouts[upgradeID]
	if !ok {
		return nil, fmt.Errorf("unknown rollout %q", upgradeID)
	}
	if r.terminal() {
		return []string{fmt.Sprintf("rollout %s already finished at %s", upgradeID, r.FinishedAt.Format(time.RFC3339))}, nil
	}
	logs := o.advanceLocked(r)
	if err := o.persistLocked(); err != nil {
		return logs, err
	}
	return logs, nil
}

// nextStepForDevice 计算设备在本发布中的下一跳目标。
// 路径模式：按既定路径逐跳；普通模式：直接目标策略版本。
// 返回 (to, needDispatch, reason)：needDispatch=false 时 reason 说明跳过原因（空串表示已终态完成）。
// 调用方需持有 mu。
func (o *Orchestrator) nextStepForDevice(r *Rollout, devID string) (version.Version, bool, string) {
	dev, registered := o.reg.Get(devID)
	if !registered {
		return version.Version{}, false, "device not registered"
	}
	if r.Policy.Path != nil {
		// 该设备对当前跳已有任务（任意状态）则本拍不重复派发。
		if j, ok := o.mgr.ActiveJob(devID); ok && j.UpgradeID == r.ID {
			return version.Version{}, false, ""
		}
		// 已成功到达该跳（终态 SUCCEEDED 且 to==cur）时推进到路径下一跳；
		// 失败/回滚的跳允许重试同一跳（不跳过、不前进）。
		next, more, err := r.Policy.Path.NextStep(dev.CurrentVer)
		if err != nil {
			return version.Version{}, false, fmt.Sprintf("off-path current %s: %v", dev.CurrentVer, err)
		}
		if !more {
			return version.Version{}, false, ""
		}
		// 对同一跳已成功过则不重复（CurrentVer 已前进的情况下 NextStep 自然给出下一跳；
		// 此判断额外防御乱序/重启窗口）。
		if j := o.latestTerminalJob(r, devID); j != nil &&
			j.Phase == upgrade.PhaseSucceeded && j.ToVersion.Compare(next) == 0 {
			return version.Version{}, false, ""
		}
		// 统计该跳已消耗的尝试次数（每个 job 一次），超过上限则明确拒绝再派。
		attempts := o.hopAttempts(r, devID, dev.CurrentVer, next)
		if attempts >= r.Policy.maxAttempts() {
			return version.Version{}, false, fmt.Sprintf(
				"hop %s -> %s exhausted %d attempts; rollout halted for this device",
				dev.CurrentVer, next, attempts)
		}
		// 对同一跳已有失败/回滚终态：在尝试次数内重试该跳。
		return next, true, ""
	}
	// 普通模式：该设备在本发布已有任意任务（含终态）即不再派发，避免重启后重复执行。
	if j, ok := o.mgr.ActiveJob(devID); ok && j.UpgradeID == r.ID {
		return version.Version{}, false, fmt.Sprintf("already has in-flight job %s (%s)", j.ID, j.Phase)
	}
	if j := o.latestTerminalJob(r, devID); j != nil {
		return version.Version{}, false, fmt.Sprintf("already has terminal job %s (%s)", j.ID, j.Phase)
	}
	if dev.CurrentVer.Compare(r.Policy.ToVersion) == 0 {
		return version.Version{}, false, "already at target version"
	}
	if err := version.CheckUpgrade(dev.CurrentVer, r.Policy.ToVersion, r.Policy.Rule); err != nil {
		return version.Version{}, false, err.Error()
	}
	return r.Policy.ToVersion, true, ""
}

// latestTerminalJob 返回设备在本发布中最近一个终态任务（按派发时间），无则 nil。
func (o *Orchestrator) latestTerminalJob(r *Rollout, devID string) *upgrade.Job {
	var latest *upgrade.Job
	for _, j := range o.mgr.JobsByUpgrade(r.ID) {
		if j.DeviceID != devID || !j.Phase.Terminal() {
			continue
		}
		if latest == nil || j.DispatchedAt.After(latest.DispatchedAt) {
			jj := j
			latest = jj
		}
	}
	return latest
}

// hopAttempts 统计设备针对 from -> to 这一跳已经创建过的任务数（含在途与终态）。
func (o *Orchestrator) hopAttempts(r *Rollout, devID string, from, to version.Version) int {
	n := 0
	for _, j := range o.mgr.JobsByUpgrade(r.ID) {
		if j.DeviceID == devID && j.FromVersion.Compare(from) == 0 && j.ToVersion.Compare(to) == 0 {
			n++
		}
	}
	return n
}

// advanceLocked 执行一拍推进，调用方需持有 mu；不负责持久化（由调用方统一落盘）。
func (o *Orchestrator) advanceLocked(r *Rollout) []string {
	var logs []string
	if r.terminal() {
		return []string{fmt.Sprintf("rollout %s already finished at %s", r.ID, r.FinishedAt.Format(time.RFC3339))}
	}
	cur := currentBatch(r)
	if cur == nil {
		return logs
	}
	if cur.Status == BatchPending {
		cur.Status = BatchRunning
		logs = append(logs, fmt.Sprintf("batch %d -> RUNNING (%d devices)", cur.Index, len(cur.DeviceIDs)))
	}

	paused := o.pausedGroups[r.Policy.Group]
	if paused {
		logs = append(logs, fmt.Sprintf("group %s PAUSED: batch %d dispatch frozen; in-flight devices continue naturally",
			r.Policy.Group, cur.Index))
		o.recomputeBatchesLocked(r, &logs)
		return logs
	}

	active := o.mgr.ActiveCount(r.ID)
	capHint := "unlimited"
	if r.Policy.MaxConcurrent > 0 {
		capHint = fmt.Sprintf("%d", r.Policy.MaxConcurrent)
	}
	dispatchCount, waitCount, skipCount := 0, 0, 0
	for _, devID := range cur.DeviceIDs {
		// 在途任务（任何发布占用该设备槽位）直接跳过，槽位由其完成时自然释放。
		if j, busy := o.mgr.ActiveJob(devID); busy {
			if j.UpgradeID != r.ID {
				skipCount++
				logs = append(logs, fmt.Sprintf("skip %s: busy with another upgrade %s", devID, j.UpgradeID))
			} else {
				skipCount++
			}
			continue
		}
		to, need, reason := o.nextStepForDevice(r, devID)
		if !need {
			// 空原因表示该设备已自然完成（在途/已到目标），不计入异常跳过。
			if reason != "" {
				skipCount++
				logs = append(logs, fmt.Sprintf("skip %s: %s", devID, reason))
			}
			continue
		}
		if r.Policy.MaxConcurrent > 0 && active >= r.Policy.MaxConcurrent {
			// 到整体在途上限：该设备排队，继续扫描后面没有意义（顺序确定），
			// 但已有槽位释放的设备会在 Report/Tick 的补位拍中及时进入。
			waitCount++
			logs = append(logs, fmt.Sprintf("queue %s: rollout concurrency at cap (%d/%d); waiting for a slot",
				devID, active, r.Policy.MaxConcurrent))
			continue
		}
		dev, registered := o.reg.Get(devID)
		if !registered {
			skipCount++
			logs = append(logs, fmt.Sprintf("skip %s: device not registered", devID))
			continue
		}
		if dev.Status != device.StatusOnline {
			skipCount++
			logs = append(logs, fmt.Sprintf("defer %s: OFFLINE (%s); will dispatch later without blocking peers", devID, dev.LastChangeReason))
			continue
		}
		now := o.clk.Now()
		job := &upgrade.Job{
			UpgradeID:    r.ID,
			DeviceID:     devID,
			Group:        r.Policy.Group,
			FromVersion:  dev.CurrentVer,
			ToVersion:    to,
			Phase:        upgrade.PhaseDispatched,
			DispatchedAt: now,
		}
		if r.Policy.Path != nil {
			logs = append(logs, fmt.Sprintf("path step for %s: %s -> %s (target %s)",
				devID, job.FromVersion, job.ToVersion, r.Policy.ToVersion))
		}
		if err := o.mgr.Dispatch(job); err != nil {
			// 单台冲突不影响其它设备。
			skipCount++
			logs = append(logs, fmt.Sprintf("skip %s: %v", devID, err))
			continue
		}
		cur.JobIDs = append(cur.JobIDs, job.ID)
		// 写入期望影子：desired / reported 开始分离。
		if err := o.reg.SetDesired(devID, to, now); err != nil {
			logs = append(logs, fmt.Sprintf("WARN set desired %s: %v", devID, err))
		}
		active++
		dispatchCount++
		logs = append(logs, fmt.Sprintf("dispatch %s: job %s %s -> %s", devID, job.ID, job.FromVersion, job.ToVersion))
	}
	logs = append(logs, fmt.Sprintf("batch %d: dispatched=%d queued=%d skipped/deferred=%d active=%d cap=%s",
		cur.Index, dispatchCount, waitCount, skipCount, active, capHint))

	o.recomputeBatchesLocked(r, &logs)
	return logs
}

// currentBatch 返回第一个未成功完成的批次（失败批次也停住，等待人工/超时裁决）。
func currentBatch(r *Rollout) *Batch {
	for i := range r.Batches {
		if r.Batches[i].Status != BatchSucceeded {
			return &r.Batches[i]
		}
	}
	return nil
}

// deviceVerdict 描述设备在当前批次中的推进结论。
type deviceVerdict string

const (
	verdictDone    deviceVerdict = "DONE"    // 已完成（普通模式成功；路径模式已到达本发布最终目标）
	verdictActive  deviceVerdict = "ACTIVE"  // 有本发布的在途任务
	verdictWaiting deviceVerdict = "WAITING" // 尚未派发（排队/离线/未到拍）
	verdictRetry   deviceVerdict = "RETRY"   // 路径模式：当前跳失败/回滚，等待下一拍重试同一跳
	verdictFailed  deviceVerdict = "FAILED"  // 普通模式：终态失败，熔断本批
)

// deviceBatchVerdictLocked 调用方需持有 mu。
func (o *Orchestrator) deviceBatchVerdictLocked(r *Rollout, devID string) deviceVerdict {
	if r.Policy.Path != nil {
		dev, ok := o.reg.Get(devID)
		if !ok {
			return verdictWaiting
		}
		if dev.CurrentVer.Compare(r.Policy.ToVersion) == 0 {
			return verdictDone
		}
		if j, busy := o.mgr.ActiveJob(devID); busy && j.UpgradeID == r.ID {
			return verdictActive
		}
		if j := o.latestTerminalJob(r, devID); j != nil {
			switch j.Phase {
			case upgrade.PhaseFailed, upgrade.PhaseTimeout, upgrade.PhaseRolledBack:
				// 失败跳停在 FromVersion：在尝试次数内下一拍重试同一跳，
				// 进度不回退、已完成跳不重走；次数耗尽则熔断。
				next, more, err := r.Policy.Path.NextStep(dev.CurrentVer)
				if err != nil || !more {
					return verdictFailed
				}
				if o.hopAttempts(r, devID, dev.CurrentVer, next) >= r.Policy.maxAttempts() {
					return verdictFailed
				}
				return verdictRetry
			}
		}
		return verdictWaiting
	}
	if j, busy := o.mgr.ActiveJob(devID); busy && j.UpgradeID == r.ID {
		return verdictActive
	}
	if j := o.latestTerminalJob(r, devID); j != nil {
		if j.Phase == upgrade.PhaseSucceeded {
			return verdictDone
		}
		return verdictFailed
	}
	return verdictWaiting
}

// recomputeBatchesLocked 根据每台设备的推进结论刷新批次/发布状态。
func (o *Orchestrator) recomputeBatchesLocked(r *Rollout, logs *[]string) {
	for i := range r.Batches {
		b := &r.Batches[i]
		if b.Status == BatchSucceeded {
			continue
		}
		allDone := len(b.DeviceIDs) > 0
		hasActive, hasFailure, hasRetry := false, false, false
		for _, devID := range b.DeviceIDs {
			switch o.deviceBatchVerdictLocked(r, devID) {
			case verdictDone:
				// 该设备完成。
			case verdictActive:
				allDone = false
				hasActive = true
			case verdictRetry:
				allDone = false
				hasRetry = true
			case verdictFailed:
				allDone = false
				hasFailure = true
			case verdictWaiting:
				allDone = false
			}
		}
		switch {
		case allDone:
			if b.Status != BatchSucceeded {
				b.Status = BatchSucceeded
				*logs = append(*logs, fmt.Sprintf("batch %d -> SUCCEEDED", b.Index))
			}
		case hasFailure:
			// 普通模式任一终态失败即熔断；路径模式尝试次数耗尽同样熔断（逐设备仍可定位）。
			b.Status = BatchFailed
			*logs = append(*logs, fmt.Sprintf("batch %d -> FAILED (partial success locatable per device); rollout halted", b.Index))
		case b.Status == BatchPending:
			// 尚未开始，保持 PENDING。
		default:
			// 路径模式下失败跳可重试，批次保持 RUNNING，等待下一拍补派。
			if b.Status != BatchRunning {
				b.Status = BatchRunning
			}
			if hasRetry {
				*logs = append(*logs, fmt.Sprintf("batch %d RUNNING: failed path step will be retried from the same version (done steps not re-run)", b.Index))
			} else if hasActive {
				// 在途推进中，无额外说明。
			}
		}
	}
	if !r.terminal() {
		allDone := true
		allOK := true
		for i := range r.Batches {
			if r.Batches[i].Status != BatchSucceeded {
				allDone = false
				if r.Batches[i].Status == BatchFailed {
					allOK = false
				}
			}
		}
		if allDone || batchFailed(r) {
			r.FinishedAt = o.clk.Now()
			if allDone && allOK {
				*logs = append(*logs, fmt.Sprintf("rollout %s FINISHED: all batches succeeded", r.ID))
			} else {
				*logs = append(*logs, fmt.Sprintf("rollout %s FINISHED: failed batch stops the canary; later batches not dispatched", r.ID))
			}
		}
	}
}

func batchFailed(r *Rollout) bool {
	for i := range r.Batches {
		if r.Batches[i].Status == BatchFailed {
			return true
		}
	}
	return false
}

// Report 透传设备上报；成功终态时判定影子收敛并清除 desired，失败则撤回 desired。
func (o *Orchestrator) Report(rep upgrade.Report) upgrade.ReportResult {
	o.mu.Lock()
	defer o.mu.Unlock()
	res := o.mgr.HandleReport(rep)
	now := o.clk.Now()
	if res.Verdict == upgrade.ReportAccepted && res.Job != nil && res.Job.Phase.Terminal() {
		var logs []string
		rr, hasRollout := o.rollouts[res.Job.UpgradeID]
		switch res.Job.Phase {
		case upgrade.PhaseSucceeded:
			// 设备成功，reported 已在成功上报时由设备确认；同步影子并判定收敛。
			if err := o.reg.MarkReportedVersion(rep.DeviceID, res.Job.ToVersion, now); err != nil {
				logs = append(logs, fmt.Sprintf("WARN reported shadow: %v", err))
			}
			conv, _ := o.reg.Converged(rep.DeviceID)
			if conv {
				_ = o.reg.ClearDesired(rep.DeviceID, now)
			}
		case upgrade.PhaseFailed:
			// 普通模式失败：撤回期望，设备保留原版本。
			// 路径模式单跳失败仍会重试同一跳，期望（最终目标）保留；
			// 若该失败导致发布熔断（尝试耗尽），在裁决后再撤回期望。
			if !hasRollout || rr.Policy.Path == nil {
				_ = o.reg.ClearDesired(rep.DeviceID, now)
			}
		}
		if hasRollout {
			o.recomputeBatchesLocked(rr, &logs)
			if res.Job.Phase == upgrade.PhaseFailed && rr.Policy.Path != nil && rr.terminal() {
				_ = o.reg.ClearDesired(rep.DeviceID, now)
				logs = append(logs, fmt.Sprintf("rollout %s halted after failure; desired withdrawn for %s", rr.ID, rep.DeviceID))
			}
			// 设备退出在途后立即补位：排队设备/路径下一跳无需等待外部下一拍。
			// 暂停态与熔断批次在 advanceLocked 内自行拦截。
			if !rr.terminal() {
				logs = append(logs, o.advanceLocked(rr)...)
			}
		}
	}
	_ = o.persistLocked()
	return res
}

// Tick 驱动一次：离线扫描 -> 超时收敛与回滚 -> 批次状态 -> 持久化。
// 返回每一步的判定说明（可解释）。
func (o *Orchestrator) Tick() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := o.clk.Now()
	var logs []string

	for _, ev := range o.reg.SweepOffline(now) {
		logs = append(logs, fmt.Sprintf("device %s -> OFFLINE reason=%s at %s", ev.DeviceID, ev.Reason, ev.At.Format(time.RFC3339)))
	}

	var timedOutPathJobs []*upgrade.Job
	for _, j := range o.mgr.SweepTimeouts(now) {
		logs = append(logs, fmt.Sprintf("job %s device %s -> TIMEOUT_FAILED: %s; triggering rollback to %s",
			j.ID, j.DeviceID, j.FailReason, j.FromVersion))
		// 普通模式超时失败：撤回期望并执行回滚动作（模拟）。
		// 路径模式：回滚只回到本跳起点，最终期望保留以便同跳重试；
		// 若尝试耗尽导致熔断，在补位裁决后撤回。
		rr, hasRollout := o.rollouts[j.UpgradeID]
		pathMode := hasRollout && rr.Policy.Path != nil
		if !pathMode {
			_ = o.reg.ClearDesired(j.DeviceID, now)
		} else {
			timedOutPathJobs = append(timedOutPathJobs, j)
		}
		if o.rb != nil {
			if err := o.rb(j, now); err != nil {
				logs = append(logs, fmt.Sprintf("rollback job %s device %s ERROR: %v", j.ID, j.DeviceID, err))
				continue
			}
		}
		o.mgr.MarkRolledBack(j.ID, now)
		logs = append(logs, fmt.Sprintf("job %s device %s -> ROLLED_BACK to %s", j.ID, j.DeviceID, j.FromVersion))
	}

	// 超时收敛/回滚后，空出的在途槽位立即补位给排队设备（路径模式同跳重试也在此触发）。
	for _, r := range o.sortedRollouts() {
		if r.terminal() {
			continue
		}
		logs = append(logs, o.advanceLocked(r)...)
	}
	// 路径模式：补位裁决后若发布已熔断（尝试耗尽），再撤回悬空期望。
	for _, j := range timedOutPathJobs {
		if rr, ok := o.rollouts[j.UpgradeID]; ok && rr.terminal() {
			_ = o.reg.ClearDesired(j.DeviceID, now)
			logs = append(logs, fmt.Sprintf("rollout %s halted after rollback; desired withdrawn for %s", rr.ID, j.DeviceID))
		}
	}
	if err := o.persistLocked(); err != nil {
		logs = append(logs, fmt.Sprintf("persist ERROR: %v", err))
	}
	return logs
}

func (o *Orchestrator) sortedRollouts() []*Rollout {
	ids := make([]string, 0, len(o.rollouts))
	for id := range o.rollouts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]*Rollout, 0, len(ids))
	for _, id := range ids {
		out = append(out, o.rollouts[id])
	}
	return out
}

// GetRollout 查询发布副本。
func (o *Orchestrator) GetRollout(id string) (*Rollout, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	r, ok := o.rollouts[id]
	if !ok {
		return nil, false
	}
	cp := *r
	cp.Batches = append([]Batch(nil), r.Batches...)
	return &cp, true
}

// Restore 从持久化恢复注册表、任务与发布。
// 在途任务原样恢复，已生成 JobID 的设备不会被重复下发。
func (o *Orchestrator) Restore() error {
	if o.p == nil {
		return nil
	}
	b, err := o.p.Load()
	if err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	var st persistedState
	if err := json.Unmarshal(b, &st); err != nil {
		return fmt.Errorf("unmarshal state: %w", err)
	}
	o.reg.Restore(st.Registry)
	o.mgr.Restore(st.Manager)
	o.seq = st.Seq
	for _, g := range st.PausedGroups {
		o.pausedGroups[g] = true
	}
	for i := range st.Rollouts {
		r := st.Rollouts[i]
		o.rollouts[r.ID] = &r
	}
	return nil
}

// PauseGroup 暂停一个分组：该分组下所有未结束的发布不再派发新升级，
// 已在途的设备按当前状态自然收尾；其它分组不受影响。幂等。
// 返回受影响（未结束）的发布 ID 与判定说明。
func (o *Orchestrator) PauseGroup(group string) ([]string, []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	already := o.pausedGroups[group]
	o.pausedGroups[group] = true
	var logs []string
	if already {
		logs = append(logs, fmt.Sprintf("group %s already PAUSED; no change (idempotent)", group))
	} else {
		logs = append(logs, fmt.Sprintf("group %s -> PAUSED: no new dispatches; in-flight devices run to completion", group))
	}
	affected := o.rolloutsAffectingGroupLocked(group)
	for _, id := range affected {
		logs = append(logs, fmt.Sprintf("rollout %s: dispatch frozen while group %s paused", id, group))
	}
	_ = o.persistLocked()
	return affected, logs
}

// ResumeGroup 恢复一个分组：冻结解除，后续 Advance 从未完成的设备继续派发；
// 已成功的设备/已完成的跳不会重复下发。幂等。
func (o *Orchestrator) ResumeGroup(group string) ([]string, []string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.pausedGroups[group] {
		return o.rolloutsAffectingGroupLocked(group),
			[]string{fmt.Sprintf("group %s was not PAUSED; resume is a no-op (idempotent)", group)}
	}
	delete(o.pausedGroups, group)
	logs := []string{fmt.Sprintf("group %s -> RESUMED: continue with unfinished devices; succeeded devices/steps are never re-dispatched", group)}
	affected := o.rolloutsAffectingGroupLocked(group)
	// 立即补位：恢复后无需等待外部下一拍。
	for _, id := range affected {
		if r, ok := o.rollouts[id]; ok && !r.terminal() {
			logs = append(logs, o.advanceLocked(r)...)
		}
	}
	_ = o.persistLocked()
	return affected, logs
}

// IsGroupPaused 查询分组是否处于暂停态。
func (o *Orchestrator) IsGroupPaused(group string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.pausedGroups[group]
}

// rolloutsAffectingGroupLocked 调用方需持有 mu。
func (o *Orchestrator) rolloutsAffectingGroupLocked(group string) []string {
	var ids []string
	for id, r := range o.rollouts {
		if r.Policy.Group == group {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// HasActiveJobForTest 供测试跨实例确认设备在途状态。
func (o *Orchestrator) HasActiveJobForTest(upgradeID, deviceID string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	j, ok := o.mgr.ActiveJob(deviceID)
	return ok && j.UpgradeID == upgradeID
}
