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
	Group       string                    `json:"group"`
	ToVersion   version.Version           `json:"to_version"`
	Rule        version.CompatibilityRule `json:"rule"`
	BatchSizes  []int                     `json:"batch_sizes"`  // 每批设备数；为 0 表示组内全部设备一批
	StepTimeout time.Duration             `json:"step_timeout"` // 单设备中间状态超时
	// MaxConcurrent 本发布内允许同时处于在途状态的设备数上限；0 表示不限。
	// 到达上限后其余设备排队等待，有设备退出在途（成功/失败/超时回滚）后按序补位。
	MaxConcurrent int `json:"max_concurrent"`
	// Path 既定的跨版本升级路径（严格递增，至少 2 个版本）。
	// 为空时按 Rule 做一次到位升级；非空时忽略 Rule，沿路径逐段升级，
	// 路径之外的版本组合在创建发布时即被拒绝。
	Path []version.Version `json:"path,omitempty"`
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
	// Paused 暂停标记：暂停后不再派发新任务，已在途设备自然收尾；
	// 恢复后从各设备当前版本继续，不重复下发已成功的设备。
	Paused bool `json:"paused"`
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

	rollouts map[string]*Rollout
	seq      int
}

// persistedState 是写入磁盘的完整状态快照。
type persistedState struct {
	Registry device.Snapshot  `json:"registry"`
	Manager  upgrade.Snapshot `json:"manager"`
	Rollouts []Rollout        `json:"rollouts"`
	Seq      int              `json:"seq"`
}

// New 创建编排器。
func New(reg *device.Registry, mgr *upgrade.Manager, clk clock.Clock, p Persist, rb RollbackFunc) *Orchestrator {
	return &Orchestrator{
		reg: reg, mgr: mgr, clk: clk, p: p, rb: rb,
		rollouts: make(map[string]*Rollout),
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
	// 路径模式：policy.Path 非空时沿既定路径逐段升级，忽略 Rule。
	var path version.Path
	usePath := len(policy.Path) > 0
	if usePath {
		p, err := version.NewPath(policy.Path)
		if err != nil {
			return nil, fmt.Errorf("rollout rejected: %v", err)
		}
		path = p
	}
	// 逐台做版本合法性校验：单台非法不影响对其它设备给出明确判定，这里整体拒绝并列出原因。
	var violations []string
	for _, d := range devs {
		if usePath {
			if _, err := path.PlanSteps(d.CurrentVer, policy.ToVersion); err != nil {
				violations = append(violations, fmt.Sprintf("%s(current=%s): %v", d.ID, d.CurrentVer, err))
			}
			continue
		}
		if err := version.CheckUpgrade(d.CurrentVer, policy.ToVersion, policy.Rule); err != nil {
			violations = append(violations, fmt.Sprintf("%s(current=%s): %v", d.ID, d.CurrentVer, err))
		}
	}
	if len(violations) > 0 {
		sort.Strings(violations)
		return nil, fmt.Errorf("rollout rejected, %d device(s) not eligible: %v", len(violations), violations)
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
	var logs []string
	changed := false

	cur := currentBatch(r)
	if cur == nil {
		return logs, nil
	}
	if r.Paused {
		// 暂停：不派发新任务；在途设备的上报/超时仍由 Report/Tick 自然收尾。
		logs = append(logs, fmt.Sprintf("rollout %s is PAUSED: no new dispatches; %d job(s) still in flight finish naturally",
			r.ID, o.mgr.ActiveCountByUpgrade(r.ID)))
		o.recomputeBatchesLocked(r, &logs)
		return logs, nil
	}
	if cur.Status == BatchPending {
		cur.Status = BatchRunning
		changed = true
		logs = append(logs, fmt.Sprintf("batch %d -> RUNNING (%d devices)", cur.Index, len(cur.DeviceIDs)))
	}

	// 路径模式：沿既定路径逐段升级；否则一次到位。
	var path version.Path
	usePath := len(r.Policy.Path) > 0
	if usePath {
		// StartRollout 已校验路径合法，这里不会再失败。
		path, _ = version.NewPath(r.Policy.Path)
	}
	limit := r.Policy.MaxConcurrent
	inFlight := o.mgr.ActiveCountByUpgrade(r.ID)

	dispatchCount, skipCount, queuedCount := 0, 0, 0
	for _, devID := range cur.DeviceIDs {
		if o.mgr.HasActiveJob(devID) {
			skipCount++
			continue
		}
		dev, registered := o.reg.Get(devID)
		if !registered {
			logs = append(logs, fmt.Sprintf("skip %s: device not registered", devID))
			skipCount++
			continue
		}
		if dev.CurrentVer.Compare(r.Policy.ToVersion) == 0 {
			// 已到达目标版本（含暂停前已成功的设备）：恢复后绝不重复下发。
			logs = append(logs, fmt.Sprintf("skip %s: already at target %s; not re-dispatched", devID, r.Policy.ToVersion))
			skipCount++
			continue
		}
		if j, halted := o.haltedJobForDeviceInBatch(r, cur.Index, devID); halted {
			// 本批内已有失败/回滚终态任务：该设备止步，等待人工裁决，不自动重试。
			logs = append(logs, fmt.Sprintf("skip %s: halted by terminal job %s (%s)", devID, j.ID, j.Phase))
			skipCount++
			continue
		}
		if dev.Status != device.StatusOnline {
			logs = append(logs, fmt.Sprintf("defer %s: OFFLINE (%s); will dispatch later without blocking peers", devID, dev.LastChangeReason))
			skipCount++
			continue
		}
		if limit > 0 && inFlight >= limit {
			// 到达本发布并发上限：设备排队，等有设备退出在途后下一拍补位。
			queuedCount++
			continue
		}
		to := r.Policy.ToVersion
		if usePath {
			next, ok := path.NextStep(dev.CurrentVer, r.Policy.ToVersion)
			if !ok {
				logs = append(logs, fmt.Sprintf("skip %s: no path step from %s toward %s", devID, dev.CurrentVer, r.Policy.ToVersion))
				skipCount++
				continue
			}
			to = next
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
		if err := o.mgr.Dispatch(job); err != nil {
			// 单台冲突不影响其它设备。
			logs = append(logs, fmt.Sprintf("skip %s: %v", devID, err))
			skipCount++
			continue
		}
		cur.JobIDs = append(cur.JobIDs, job.ID)
		// 写入期望影子：desired / reported 开始分离。多步升级时期望始终为最终目标版本。
		if err := o.reg.SetDesired(devID, r.Policy.ToVersion, now); err != nil {
			logs = append(logs, fmt.Sprintf("WARN set desired %s: %v", devID, err))
		}
		inFlight++
		dispatchCount++
		changed = true
		if to.Compare(r.Policy.ToVersion) == 0 {
			logs = append(logs, fmt.Sprintf("dispatch %s: job %s %s -> %s", devID, job.ID, job.FromVersion, job.ToVersion))
		} else {
			logs = append(logs, fmt.Sprintf("dispatch %s: job %s %s -> %s (path step toward %s)",
				devID, job.ID, job.FromVersion, job.ToVersion, r.Policy.ToVersion))
		}
	}
	logs = append(logs, fmt.Sprintf("batch %d: dispatched=%d skipped/deferred=%d queued=%d in_flight=%d limit=%d",
		cur.Index, dispatchCount, skipCount, queuedCount, inFlight, limit))

	o.recomputeBatchesLocked(r, &logs)
	if changed {
		if err := o.persistLocked(); err != nil {
			return logs, err
		}
	}
	return logs, nil
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

// haltedJobForDeviceInBatch 返回设备在本批中已终态但未成功的任务（失败/超时/回滚）。
// 成功的终态任务不算止步：多步升级中设备可能已完成前一段、还需继续下一段。
func (o *Orchestrator) haltedJobForDeviceInBatch(r *Rollout, batchIdx int, devID string) (*upgrade.Job, bool) {
	for _, jid := range r.Batches[batchIdx].JobIDs {
		if j, ok := o.mgr.Get(jid); ok && j.DeviceID == devID && j.Phase.Terminal() && j.Phase != upgrade.PhaseSucceeded {
			return j, true
		}
	}
	return nil, false
}

// recomputeBatchesLocked 根据各设备当前状态刷新批次/发布状态。
// 判定以设备为单位（而非单任务），从而兼容多步路径升级：
//   - 设备当前版本已到达目标版本 -> 该设备完成（重启恢复后判定依旧成立）；
//   - 设备在本批存在失败/超时/回滚的终态任务 -> 该设备止步；
//   - 其余（在途、待派发、离线等待）-> 未决。
//
// 批内全部设备完成 -> SUCCEEDED；全部已决且存在止步设备 -> FAILED（熔断）。
func (o *Orchestrator) recomputeBatchesLocked(r *Rollout, logs *[]string) {
	for i := range r.Batches {
		b := &r.Batches[i]
		if b.Status == BatchSucceeded || b.Status == BatchFailed {
			continue
		}
		settled, doneCount, haltedCount := true, 0, 0
		for _, devID := range b.DeviceIDs {
			if dev, ok := o.reg.Get(devID); ok && dev.CurrentVer.Compare(r.Policy.ToVersion) == 0 {
				doneCount++
				continue
			}
			if _, halted := o.haltedJobForDeviceInBatch(r, b.Index, devID); halted {
				haltedCount++
				continue
			}
			settled = false
		}
		if !settled {
			continue
		}
		if haltedCount == 0 {
			b.Status = BatchSucceeded
			*logs = append(*logs, fmt.Sprintf("batch %d -> SUCCEEDED (%d/%d devices at %s)",
				b.Index, doneCount, len(b.DeviceIDs), r.Policy.ToVersion))
		} else {
			b.Status = BatchFailed
			*logs = append(*logs, fmt.Sprintf("batch %d -> FAILED (%d halted, %d done; partial success locatable per device); rollout halted",
				b.Index, haltedCount, doneCount))
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
			// 设备上报失败：撤回期望，设备保留原版本。
			_ = o.reg.ClearDesired(rep.DeviceID, now)
		}
		if r, ok := o.rollouts[res.Job.UpgradeID]; ok {
			o.recomputeBatchesLocked(r, &logs)
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

	for _, j := range o.mgr.SweepTimeouts(now) {
		logs = append(logs, fmt.Sprintf("job %s device %s -> TIMEOUT_FAILED: %s; triggering rollback to %s",
			j.ID, j.DeviceID, j.FailReason, j.FromVersion))
		// 超时失败：撤回期望并执行回滚动作（模拟）。
		_ = o.reg.ClearDesired(j.DeviceID, now)
		if o.rb != nil {
			if err := o.rb(j, now); err != nil {
				logs = append(logs, fmt.Sprintf("rollback job %s device %s ERROR: %v", j.ID, j.DeviceID, err))
				continue
			}
		}
		o.mgr.MarkRolledBack(j.ID, now)
		logs = append(logs, fmt.Sprintf("job %s device %s -> ROLLED_BACK to %s", j.ID, j.DeviceID, j.FromVersion))
	}

	for _, r := range o.sortedRollouts() {
		o.recomputeBatchesLocked(r, &logs)
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

// PauseRollout 暂停指定发布：不再派发新任务，已在途设备按当前状态自然收尾。
// 幂等：已暂停时重复调用直接成功。返回本次动作的说明（可解释）。
func (o *Orchestrator) PauseRollout(id string) (string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	r, ok := o.rollouts[id]
	if !ok {
		return "", fmt.Errorf("unknown rollout %q", id)
	}
	if r.terminal() {
		return fmt.Sprintf("rollout %s already finished; pause is a no-op", id), nil
	}
	if r.Paused {
		return fmt.Sprintf("rollout %s already paused; idempotent no-op", id), nil
	}
	r.Paused = true
	if err := o.persistLocked(); err != nil {
		return "", err
	}
	return fmt.Sprintf("rollout %s PAUSED: no new dispatches; in-flight devices finish naturally", id), nil
}

// ResumeRollout 恢复指定发布：从各设备当前版本继续推进，
// 已成功设备不会被重复下发。幂等。返回本次动作的说明（可解释）。
func (o *Orchestrator) ResumeRollout(id string) (string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	r, ok := o.rollouts[id]
	if !ok {
		return "", fmt.Errorf("unknown rollout %q", id)
	}
	if r.terminal() {
		return fmt.Sprintf("rollout %s already finished; resume is a no-op", id), nil
	}
	if !r.Paused {
		return fmt.Sprintf("rollout %s is not paused; idempotent no-op", id), nil
	}
	r.Paused = false
	if err := o.persistLocked(); err != nil {
		return "", err
	}
	return fmt.Sprintf("rollout %s RESUMED: dispatch continues from each device's current version", id), nil
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
	for i := range st.Rollouts {
		r := st.Rollouts[i]
		o.rollouts[r.ID] = &r
	}
	return nil
}
