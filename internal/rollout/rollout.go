// Package rollout 在设备注册表之上实现按组分批灰度下发、
// 结果收敛、超时回滚以及重启恢复。
package rollout

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/device"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/store"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

// Config 是编排器配置。
type Config struct {
	OfflineTimeout time.Duration // 心跳超时阈值
	JobTimeout     time.Duration // 单个升级任务在中间状态的最长停留时间
	BatchSize      int           // 每批并发下发的设备数
	TickInterval   time.Duration // 后台巡检间隔
	StorePath      string        // 持久化文件路径
}

// Orchestrator 是升级编排器。
type Orchestrator struct {
	cfg      Config
	registry *device.Registry
	st       *store.Store
	disp     Dispatcher
	now      func() time.Time
	logger   *log.Logger

	mu      sync.Mutex
	plans   map[string]*Plan // planID -> plan
	jobs    map[string]*Job  // deviceID -> 该设备最近一次任务
	planSeq int64
}

// New 创建编排器并从持久化文件恢复历史任务（重启恢复入口）。
func New(cfg Config, now func() time.Time) (*Orchestrator, error) {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 2
	}
	if cfg.JobTimeout <= 0 {
		cfg.JobTimeout = 60 * time.Second
	}
	if cfg.OfflineTimeout <= 0 {
		cfg.OfflineTimeout = 30 * time.Second
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = time.Second
	}
	if now == nil {
		now = time.Now
	}
	st, err := store.New(cfg.StorePath)
	if err != nil {
		return nil, err
	}
	snap, err := st.Load()
	if err != nil {
		return nil, err
	}
	o := &Orchestrator{
		cfg:      cfg,
		registry: device.NewRegistry(cfg.OfflineTimeout, now),
		st:       st,
		disp:     NewSimDispatcher(),
		now:      now,
		logger:   log.Default(),
		plans:    make(map[string]*Plan),
		jobs:     make(map[string]*Job),
	}
	if err := o.restore(snap); err != nil {
		return nil, err
	}
	return o, nil
}

// restore 从快照重建设备、计划与任务；
// 已 Attempted 的任务恢复为 inprogress 等待结果或超时，绝不重复下发。
func (o *Orchestrator) restore(snap store.Snapshot) error {
	for _, d := range snap.Devices {
		if err := o.registry.Register(d); err != nil {
			return err
		}
	}
	// 重建影子：期望/上报版本、在线状态与最近原因均来自持久化，
	// 使重启后 Converged() 判定与离线/恢复原因不丢失。
	entries := make([]device.ShadowEntry, 0, len(snap.Shadows))
	for _, r := range snap.Shadows {
		st := device.ShadowState{
			Online:     statusFromName(r.Online),
			LastSeen:   time.Unix(r.LastSeenUnix, 0),
			LastReason: device.TransitionReason(r.LastReason),
		}
		if r.ReportedVersion != nil {
			v := *r.ReportedVersion
			st.ReportedVersion = &v
			st.ReportedSeq = r.ReportedSeq
			st.ReportedAt = time.Unix(r.ReportedUnix, 0)
		}
		if r.DesiredVersion != nil {
			v := *r.DesiredVersion
			st.DesiredVersion = &v
			st.DesiredAt = time.Unix(r.DesiredUnix, 0)
		}
		entries = append(entries, device.ShadowEntry{DeviceID: r.DeviceID, State: st})
	}
	o.registry.RestoreShadows(entries)

	planByKey := make(map[string]*Plan)
	for _, rec := range snap.Jobs {
		key := rec.Group + "@" + rec.ToVersion.String()
		p := planByKey[key]
		if p == nil {
			p = &Plan{
				ID:        fmt.Sprintf("plan-%d", len(o.plans)+1),
				Group:     rec.Group,
				Target:    rec.ToVersion,
				BatchSize: o.cfg.BatchSize,
			}
			planByKey[key] = p
			o.plans[p.ID] = p
		}
		state := rec.State
		// 重启恢复：崩溃时停在瞬态 failed 的任务继续保留 failed 等待回滚；
		// 已真正下发但没结果的 inprogress 保持在途，不会重新下发。
		if state == "" {
			state = StatePending
		}
		deadline := time.Unix(rec.DeadlineUnix, 0)
		updated := time.Unix(rec.UpdatedUnix, 0)
		j := &Job{
			DeviceID:          rec.DeviceID,
			Group:             rec.Group,
			PlanID:            p.ID,
			FromVersion:       rec.FromVersion,
			ToVersion:         rec.ToVersion,
			BatchIndex:        rec.BatchIndex,
			State:             state,
			Attempted:         rec.Attempted,
			RollbackAttempted: false,
			Deadline:          deadline,
			UpdatedAt:         updated,
			LastError:         rec.LastError,
		}
		o.jobs[j.DeviceID] = j
		if rec.BatchIndex > p.CurrentBatch {
			p.CurrentBatch = rec.BatchIndex
		}
	}
	o.logger.Printf("[restore] devices=%d jobs=%d: attempted in-flight jobs are resumed, never re-dispatched", len(snap.Devices), len(snap.Jobs))
	return nil
}

// persistLocked 把当前内存状态全量落盘；调用方必须持有 o.mu。
func (o *Orchestrator) persistLocked(stage string) {
	snap := store.Snapshot{Devices: o.registry.List(), Jobs: make([]store.JobRecord, 0, len(o.jobs))}
	for _, sh := range o.registry.SnapshotShadows() {
		rec := store.ShadowRecord{
			DeviceID: sh.DeviceID, Online: statusName(sh.Online),
			LastSeenUnix: sh.LastSeen.Unix(), LastReason: string(sh.LastReason),
		}
		if sh.ReportedVersion != nil {
			v := *sh.ReportedVersion
			rec.ReportedVersion = &v
			rec.ReportedSeq = sh.ReportedSeq
			rec.ReportedUnix = sh.ReportedAt.Unix()
		}
		if sh.DesiredVersion != nil {
			v := *sh.DesiredVersion
			rec.DesiredVersion = &v
			rec.DesiredUnix = sh.DesiredAt.Unix()
		}
		snap.Shadows = append(snap.Shadows, rec)
	}
	for _, j := range o.jobs {
		snap.Jobs = append(snap.Jobs, store.JobRecord{
			DeviceID:     j.DeviceID,
			Group:        j.Group,
			FromVersion:  j.FromVersion,
			ToVersion:    j.ToVersion,
			BatchIndex:   j.BatchIndex,
			State:        j.State,
			Attempted:    j.Attempted,
			DeadlineUnix: j.Deadline.Unix(),
			UpdatedUnix:  j.UpdatedAt.Unix(),
			LastError:    j.LastError,
		})
	}
	sort.Slice(snap.Jobs, func(i, k int) bool { return snap.Jobs[i].DeviceID < snap.Jobs[k].DeviceID })
	if err := o.st.Save(snap); err != nil {
		o.logger.Printf("[persist:%s] ERROR %v", stage, err)
	}
}

// statusName 在不形成 device->store 依赖循环的前提下由编排器映射状态名。
func statusName(s device.OnlineStatus) string {
	switch s {
	case device.StatusOnline:
		return "online"
	case device.StatusOffline:
		return "offline"
	default:
		return "never-seen"
	}
}

func statusFromName(name string) device.OnlineStatus {
	switch name {
	case "online":
		return device.StatusOnline
	case "offline":
		return device.StatusOffline
	default:
		return device.StatusNeverSeen
	}
}

// activeJobLocked 报告设备是否存在非终态任务。调用方持锁。
func (o *Orchestrator) activeJobLocked(deviceID string) (*Job, bool) {
	j, ok := o.jobs[deviceID]
	if !ok || isTerminal(j.State) {
		return nil, false
	}
	return j, true
}

// RegisterDevice 注册设备并持久化；重复注册由注册表保证幂等。
func (o *Orchestrator) RegisterDevice(d device.Device) error {
	if err := o.registry.Register(d); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.persistLocked("register")
	o.logger.Printf("[register] id=%s group=%s version=%s", d.ID, d.Group, d.CurrentVersion)
	return nil
}

// Heartbeat 处理设备心跳（委托给注册表）。
func (o *Orchestrator) Heartbeat(id string, fw version.Version, seq int64) (device.HeartbeatResult, error) {
	res, err := o.registry.Heartbeat(id, fw, seq)
	if err != nil {
		return res, err
	}
	o.logger.Printf("[heartbeat] id=%s seq=%d fw=%s accepted=%t appliedSeq=%d status=%s reason=%s",
		id, seq, fw, res.Accepted, res.AppliedSeq, res.Online, res.Reason)
	return res, nil
}

// StartItem 描述每台设备在计划创建时的判定依据。
type StartItem struct {
	DeviceID   string
	Accepted   bool
	BatchIndex int
	Reason     string
}

// StartResult 是 StartRollout 的逐项结果。
type StartResult struct {
	PlanID string
	Items  []StartItem
}

// StartRollout 为设备组创建分批灰度计划并持久化。
//
// 每台设备同一时刻至多一个在途升级：已有非终态任务的设备被跳过；
// 版本合法性按 version.IsUpgradeAllowed 逐设备判定，非法设备被跳过，
// 不影响同组其它设备入队。
func (o *Orchestrator) StartRollout(group string, target version.Version) (StartResult, error) {
	ids := o.registry.ListByGroup(group)
	o.mu.Lock()
	defer o.mu.Unlock()

	o.planSeq++
	plan := &Plan{
		ID:        fmt.Sprintf("plan-%d", o.planSeq),
		Group:     group,
		Target:    target,
		BatchSize: o.cfg.BatchSize,
		CreatedAt: o.now(),
	}
	o.plans[plan.ID] = plan

	res := StartResult{PlanID: plan.ID}
	for idx, id := range ids {
		d, _ := o.registry.Get(id)
		item := StartItem{DeviceID: id, BatchIndex: batchOf(idx, plan.BatchSize)}
		if j, active := o.activeJobLocked(id); active {
			item.Accepted = false
			item.Reason = fmt.Sprintf("skipped: device already has in-flight job %s->%s state=%s",
				j.FromVersion, j.ToVersion, j.State)
			o.logger.Printf("[rollout-start] id=%s %s", id, item.Reason)
			res.Items = append(res.Items, item)
			continue
		}
		if err := version.IsUpgradeAllowed(d.CurrentVersion, target); err != nil {
			item.Accepted = false
			item.Reason = "skipped: " + err.Error()
			o.logger.Printf("[rollout-start] id=%s %s", id, item.Reason)
			res.Items = append(res.Items, item)
			continue
		}
		now := o.now()
		j := &Job{
			DeviceID:    id,
			Group:       group,
			PlanID:      plan.ID,
			FromVersion: d.CurrentVersion,
			ToVersion:   target,
			BatchIndex:  item.BatchIndex,
			State:       StatePending,
			Deadline:    now.Add(o.cfg.JobTimeout),
			UpdatedAt:   now,
		}
		o.jobs[id] = j
		if err := o.registry.SetDesired(id, target); err != nil {
			return res, err
		}
		item.Accepted = true
		item.Reason = fmt.Sprintf("queued in batch %d (%s->%s)", item.BatchIndex, d.CurrentVersion, target)
		o.logger.Printf("[rollout-start] id=%s %s", id, item.Reason)
		res.Items = append(res.Items, item)
	}
	o.persistLocked("rollout-start")
	return res, nil
}

// ReportOutcome 描述结果上报的处理判定（重复/迟到上报 Accepted=false）。
type ReportOutcome struct {
	DeviceID string
	Accepted bool
	State    string
	Reason   string
}

// ReportResult 处理设备上报的升级结果。
//
// 只有 inprogress 任务接受结果；对终态任务的重复上报、对 pending/failed
// 任务的迟到上报一律幂等忽略，绝不重复计数、不重复触发回滚。
func (o *Orchestrator) ReportResult(deviceID string, success bool, errMsg string) (ReportOutcome, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	j, ok := o.jobs[deviceID]
	out := ReportOutcome{DeviceID: deviceID}
	if !ok {
		out.State = "none"
		out.Reason = "ignored: no job for device"
		o.logger.Printf("[result] id=%s success=%t %s", deviceID, success, out.Reason)
		return out, device.ErrDeviceNotFound
	}
	out.State = j.State
	switch {
	case j.State == StateSucceeded, j.State == StateRolledBack:
		out.Accepted = false
		out.Reason = "ignored: duplicate report for terminal job"
	case j.State == StateFailed:
		out.Accepted = false
		out.Reason = "ignored: late report, job already failed and rollback is pending/done"
	case j.State == StatePending:
		out.Accepted = false
		out.Reason = "ignored: late report for job never dispatched"
	default: // inprogress
		out.Accepted = true
		j.UpdatedAt = o.now()
		if success {
			j.State = StateSucceeded
			j.LastError = ""
			out.State = j.State
			out.Reason = "job succeeded"
		} else {
			j.State = StateFailed
			j.LastError = errMsg
			out.State = j.State
			out.Reason = "job failed by device report; rollback will be driven by tick"
		}
	}
	o.persistLocked("result")
	o.logger.Printf("[result] id=%s success=%t accepted=%t state=%s %s err=%q",
		deviceID, success, out.Accepted, out.State, out.Reason, errMsg)
	return out, nil
}

// Tick 推进一次编排：
//  1. 心跳超时巡检（离线判定，原因 heartbeat-timeout）；
//  2. 把已完成批次推进到下一批；
//  3. 派发就绪批次中未下发的升级（单台失败/阻塞不影响其它设备）；
//  4. 收敛停留中间状态过久的任务为 failed；
//  5. 对 failed 任务触发且仅触发一次回滚。
func (o *Orchestrator) Tick(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	offline := o.registry.ReapOffline(ctx)
	for _, id := range offline {
		o.logger.Printf("[reaper] id=%s status=offline reason=%s", id, device.ReasonTimeout)
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	// 2. 批次推进：当前批不再有 pending/inprogress 任务（派发与结果都已
	// 落定）时开放下一批。failed 是等待回滚的瞬态，不阻塞其它设备推进。
	for _, p := range o.plans {
		done := true
		maxBatch := 0
		for _, j := range o.jobs {
			if j.PlanID != p.ID {
				continue
			}
			if j.BatchIndex > maxBatch {
				maxBatch = j.BatchIndex
			}
			if j.BatchIndex == p.CurrentBatch && (j.State == StatePending || j.State == StateInProgress) {
				done = false
			}
		}
		if done && p.CurrentBatch < maxBatch {
			p.CurrentBatch++
			o.logger.Printf("[batch] plan=%s advanced to batch %d", p.ID, p.CurrentBatch)
		}
	}

	now := o.now()

	// 3. 升级派发：仅当前开放批次的 pending 任务；已 Attempted 的任务不重复下发。
	for _, j := range o.sortedJobs() {
		if j.State != StatePending || j.BatchIndex != o.plans[j.PlanID].CurrentBatch {
			continue
		}
		// 先置 Attempted 并落盘，再执行真实下发：崩溃后也不会重复升级。
		j.Attempted = true
		j.State = StateInProgress
		j.Deadline = now.Add(o.cfg.JobTimeout)
		j.UpdatedAt = now
		o.persistLocked("dispatch-pre")
		if err := o.disp.Dispatch(j.DeviceID, j.FromVersion, j.ToVersion, DirectionUpgrade); err != nil {
			o.logger.Printf("[dispatch] id=%s %s->%s ERROR %v", j.DeviceID, j.FromVersion, j.ToVersion, err)
			j.State = StateFailed
			j.LastError = "dispatch failed: " + err.Error()
			j.UpdatedAt = now
		} else {
			o.logger.Printf("[dispatch] id=%s %s->%s batch=%d in-progress", j.DeviceID, j.FromVersion, j.ToVersion, j.BatchIndex)
		}
		o.persistLocked("dispatch-post")
	}

	// 4. 超时收敛：pending（未及时派发）或 inprogress 超过截止时间即失败。
	for _, j := range o.sortedJobs() {
		if j.State == StatePending || j.State == StateInProgress {
			if now.After(j.Deadline) {
				old := j.State
				j.State = StateFailed
				j.LastError = fmt.Sprintf("timeout after %s in state=%s", o.cfg.JobTimeout, old)
				j.UpdatedAt = now
				o.logger.Printf("[timeout] id=%s %s->failed reason=%s", j.DeviceID, old, j.LastError)
			}
		}
	}

	// 5. 回滚：每个 failed 任务至多回滚一次；回滚失败则下一个 tick 重试，
	//    但因 RollbackAttempted 已先落盘，语义上仍是"同一个回滚动作"的重试。
	for _, j := range o.sortedJobs() {
		if j.State != StateFailed {
			continue
		}
		if !j.RollbackAttempted {
			j.RollbackAttempted = true
			j.UpdatedAt = now
			o.persistLocked("rollback-pre")
		}
		err := o.disp.Dispatch(j.DeviceID, j.ToVersion, j.FromVersion, DirectionRollback)
		if err != nil {
			o.logger.Printf("[rollback] id=%s %s->%s ERROR %v (will retry next tick)", j.DeviceID, j.ToVersion, j.FromVersion, err)
			continue
		}
		j.State = StateRolledBack
		j.UpdatedAt = now
		o.logger.Printf("[rollback] id=%s %s->%s rolled-back (device restored)", j.DeviceID, j.ToVersion, j.FromVersion)
		o.persistLocked("rollback-post")
	}
	return nil
}

// sortedJobs 返回按设备 ID 排序的任务列表，保证处理与日志顺序确定。
func (o *Orchestrator) sortedJobs() []*Job {
	out := make([]*Job, 0, len(o.jobs))
	for _, j := range o.jobs {
		out = append(out, j)
	}
	sort.Slice(out, func(i, k int) bool { return out[i].DeviceID < out[k].DeviceID })
	return out
}

// Run 周期性执行 Tick 直到 ctx 取消。
func (o *Orchestrator) Run(ctx context.Context) {
	t := time.NewTicker(o.cfg.TickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := o.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
				o.logger.Printf("[tick] ERROR %v", err)
			}
		}
	}
}

// SetDispatcher 替换下发器（测试用）。
func (o *Orchestrator) SetDispatcher(d Dispatcher) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.disp = d
}

// JobView 是任务状态的只读视图。
type JobView struct {
	DeviceID    string
	PlanID      string
	Group       string
	FromVersion version.Version
	ToVersion   version.Version
	BatchIndex  int
	State       string
	Attempted   bool
	LastError   string
}

// Jobs 返回全部任务的只读视图（按设备 ID 排序）。
func (o *Orchestrator) Jobs() []JobView {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]JobView, 0, len(o.jobs))
	for _, j := range o.sortedJobs() {
		out = append(out, JobView{
			DeviceID: j.DeviceID, PlanID: j.PlanID, Group: j.Group,
			FromVersion: j.FromVersion, ToVersion: j.ToVersion,
			BatchIndex: j.BatchIndex, State: j.State,
			Attempted: j.Attempted, LastError: j.LastError,
		})
	}
	return out
}

// Job 查询单台设备最近任务的只读视图。
func (o *Orchestrator) Job(deviceID string) (JobView, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	j, ok := o.jobs[deviceID]
	if !ok {
		return JobView{}, false
	}
	return JobView{
		DeviceID: j.DeviceID, PlanID: j.PlanID, Group: j.Group,
		FromVersion: j.FromVersion, ToVersion: j.ToVersion,
		BatchIndex: j.BatchIndex, State: j.State,
		Attempted: j.Attempted, LastError: j.LastError,
	}, true
}
