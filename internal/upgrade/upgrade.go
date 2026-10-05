package upgrade

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

// Phase 升级任务生命周期阶段。
type Phase string

const (
	PhaseDispatched  Phase = "DISPATCHED"     // 已下发，等待设备 ACK
	PhaseDownloading Phase = "DOWNLOADING"    // 设备已确认，正在下载
	PhaseInstalling  Phase = "INSTALLING"     // 正在安装
	PhaseSucceeded   Phase = "SUCCEEDED"      // 成功（终态）
	PhaseFailed      Phase = "FAILED"         // 设备上报失败（终态）
	PhaseTimeout     Phase = "TIMEOUT_FAILED" // 超时收敛为失败（终态）
	PhaseRolledBack  Phase = "ROLLED_BACK"    // 已回滚（终态）
)

// Terminal 判断是否终态。
func (p Phase) Terminal() bool {
	switch p {
	case PhaseSucceeded, PhaseFailed, PhaseTimeout, PhaseRolledBack:
		return true
	}
	return false
}

// Job 单台设备的一次升级。
type Job struct {
	ID                string          `json:"id"`
	UpgradeID         string          `json:"upgrade_id"`
	DeviceID          string          `json:"device_id"`
	Group             string          `json:"group"`
	FromVersion       version.Version `json:"from_version"`
	ToVersion         version.Version `json:"to_version"`
	Phase             Phase           `json:"phase"`
	ProgressPct       int             `json:"progress_pct"`
	DispatchedAt      time.Time       `json:"dispatched_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
	Deadline          time.Time       `json:"deadline"`
	LastReportSeq     int64           `json:"last_report_seq"`
	ResultReportedYet bool            `json:"result_reported_yet"`
	Attempts          int             `json:"attempts"`
	FailReason        string          `json:"fail_reason,omitempty"`
}

func (j *Job) clone() Job {
	cp := *j
	return cp
}

// Report 设备上报的升级进展/结果。
type Report struct {
	DeviceID  string    `json:"device_id"`
	UpgradeID string    `json:"upgrade_id"`
	Phase     Phase     `json:"phase"`
	Progress  int       `json:"progress"`
	Success   bool      `json:"success"`
	Message   string    `json:"message"`
	Seq       int64     `json:"seq"`
	At        time.Time `json:"at"`
}

// ReportVerdict 上报处理结论。
type ReportVerdict string

const (
	ReportNoActiveJob ReportVerdict = "NO_ACTIVE_JOB" // 该设备无在途升级或发布不匹配
	ReportStale       ReportVerdict = "STALE"         // 旧序号，拒绝
	ReportDuplicate   ReportVerdict = "DUPLICATE"     // 重复结果/重复序号，不重复计数
	ReportAccepted    ReportVerdict = "ACCEPTED"      // 接受
)

// ReportResult 上报处理结果。
type ReportResult struct {
	Verdict ReportVerdict
	Reason  string
	Job     *Job
}

// Manager 管理单设备升级任务与结果幂等，方法并发安全。
type Manager struct {
	mu           sync.Mutex
	jobs         map[string]*Job
	activeDevice map[string]string // deviceID -> 在途 jobID
	stepTimeout  time.Duration
	seq          int
}

// NewManager 创建任务管理器。
// stepTimeout: 每个中间状态（DISPATCHED/DOWNLOADING/INSTALLING）最长停留时间，
// 每次新进展上报会刷新截止时间。
func NewManager(stepTimeout time.Duration) *Manager {
	return &Manager{
		jobs:         make(map[string]*Job),
		activeDevice: make(map[string]string),
		stepTimeout:  stepTimeout,
	}
}

// Dispatch 为设备创建在途升级。
// 同一设备同一时刻只能有一个在途升级，冲突时返回错误（不影响其它设备）。
func (m *Manager) Dispatch(j *Job) error {
	if j == nil || j.DeviceID == "" || j.UpgradeID == "" {
		return fmt.Errorf("dispatch: device id and upgrade id are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if id, busy := m.activeDevice[j.DeviceID]; busy {
		ex := m.jobs[id]
		return fmt.Errorf("dispatch rejected: device %s already has in-flight job %s (upgrade %s, phase %s)",
			j.DeviceID, id, ex.UpgradeID, ex.Phase)
	}
	m.seq++
	if j.ID == "" {
		j.ID = fmt.Sprintf("job-%d", m.seq)
	}
	if _, exists := m.jobs[j.ID]; exists {
		return fmt.Errorf("dispatch rejected: duplicate job id %s", j.ID)
	}
	now := j.DispatchedAt
	if now.IsZero() {
		now = time.Now()
	}
	if j.Phase == "" {
		j.Phase = PhaseDispatched
	}
	j.DispatchedAt = now
	j.UpdatedAt = now
	j.Deadline = now.Add(m.stepTimeout)
	j.Attempts++
	m.jobs[j.ID] = j
	m.activeDevice[j.DeviceID] = j.ID
	return nil
}

// HandleReport 处理设备上报：乱序旧报拒绝、重复结果不重复计数。
func (m *Manager) HandleReport(r Report) ReportResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.activeDevice[r.DeviceID]
	if !ok {
		return ReportResult{Verdict: ReportNoActiveJob,
			Reason: fmt.Sprintf("device %s has no in-flight job", r.DeviceID)}
	}
	j := m.jobs[id]
	if r.UpgradeID != "" && j.UpgradeID != r.UpgradeID {
		return ReportResult{
			Verdict: ReportNoActiveJob,
			Reason:  fmt.Sprintf("report upgrade %s does not match in-flight upgrade %s", r.UpgradeID, j.UpgradeID),
			Job:     ptrJob(j.clone()),
		}
	}
	// 序号判定：相同 seq 幂等，更旧 seq 拒绝。
	// 序号判定：相同 seq 幂等，更旧 seq 拒绝。
	if r.Seq < j.LastReportSeq {
		return ReportResult{Verdict: ReportStale,
			Reason: fmt.Sprintf("report seq=%d older than applied seq=%d", r.Seq, j.LastReportSeq),
			Job:    ptrJob(j.clone())}
	}
	if r.Seq == j.LastReportSeq {
		return ReportResult{Verdict: ReportDuplicate,
			Reason: fmt.Sprintf("report seq=%d already applied; not counted again", r.Seq),
			Job:    ptrJob(j.clone())}
	}
	isResult := r.Success || r.Phase == PhaseFailed || r.Phase == PhaseSucceeded
	if isResult {
		// 结果只能生效一次：迟到的成功/失败一律不重复计数。
		if j.ResultReportedYet {
			return ReportResult{Verdict: ReportDuplicate,
				Reason: fmt.Sprintf("terminal result already recorded for job %s; late report ignored", j.ID),
				Job:    ptrJob(j.clone())}
		}
		j.ResultReportedYet = true
		j.LastReportSeq = r.Seq
		delete(m.activeDevice, j.DeviceID)
		if r.Success || r.Phase == PhaseSucceeded {
			j.Phase = PhaseSucceeded
			j.ProgressPct = 100
		} else {
			j.Phase = PhaseFailed
			j.FailReason = r.Message
		}
		j.UpdatedAt = r.At
		return ReportResult{Verdict: ReportAccepted,
			Reason: fmt.Sprintf("terminal result recorded once: %s", j.Phase),
			Job:    ptrJob(j.clone())}
	}
	// 进展上报：推进阶段并刷新超时窗口。
	j.LastReportSeq = r.Seq
	switch r.Phase {
	case PhaseDownloading, PhaseInstalling:
		j.Phase = r.Phase
	}
	if r.Progress > j.ProgressPct {
		j.ProgressPct = r.Progress
	}
	j.UpdatedAt = r.At
	j.Deadline = r.At.Add(m.stepTimeout)
	return ReportResult{Verdict: ReportAccepted,
		Reason: fmt.Sprintf("progress accepted: phase=%s progress=%d%%, deadline reset", j.Phase, j.ProgressPct),
		Job:    ptrJob(j.clone())}
}

func ptrJob(j Job) *Job { return &j }

// SweepTimeouts 把超过 Deadline 仍停留中间状态的在途任务收敛为 TIMEOUT_FAILED，
// 返回这些任务（调用方负责触发回滚）。每个任务只被收敛一次。
func (m *Manager) SweepTimeouts(now time.Time) []*Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	var timed []*Job
	for _, id := range sortedKeys(m.jobs) {
		j := m.jobs[id]
		if !j.Phase.Terminal() && !now.Before(j.Deadline) {
			stuck := j.Phase
			j.FailReason = fmt.Sprintf("stuck in %s since %s past deadline %s",
				stuck, j.UpdatedAt.Format(time.RFC3339), j.Deadline.Format(time.RFC3339))
			j.Phase = PhaseTimeout
			j.ResultReportedYet = true // 超时收敛视为唯一失败结论，后续迟到结果按重复处理
			j.UpdatedAt = now
			delete(m.activeDevice, j.DeviceID)
			timed = append(timed, ptrJob(j.clone()))
		}
	}
	return timed
}

// MarkRolledBack 回滚执行完毕后登记终态。
func (m *Manager) MarkRolledBack(jobID string, at time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok {
		return false
	}
	j.Phase = PhaseRolledBack
	j.UpdatedAt = at
	return true
}

// HasActiveJob 判断设备是否有在途升级。
func (m *Manager) HasActiveJob(deviceID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.activeDevice[deviceID]
	return ok
}

// ActiveCountByUpgrade 统计某次发布下当前处于在途状态的任务数。
// 用于按发布维度限流：只数在途，不含已终态任务。
func (m *Manager) ActiveCountByUpgrade(upgradeID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, jobID := range m.activeDevice {
		if j, ok := m.jobs[jobID]; ok && j.UpgradeID == upgradeID {
			n++
		}
	}
	return n
}

// ActiveJob 取设备当前在途任务副本。
func (m *Manager) ActiveJob(deviceID string) (*Job, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id, ok := m.activeDevice[deviceID]
	if !ok {
		return nil, false
	}
	return ptrJob(m.jobs[id].clone()), true
}

// Get 按任务 ID 查询副本。
func (m *Manager) Get(jobID string) (*Job, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[jobID]
	if !ok {
		return nil, false
	}
	return ptrJob(j.clone()), true
}

// JobsByUpgrade 返回某次发布下全部任务副本（按设备 ID 排序，便于逐设备定位）。
func (m *Manager) JobsByUpgrade(upgradeID string) []*Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*Job
	for _, j := range m.jobs {
		if j.UpgradeID == upgradeID {
			out = append(out, ptrJob(j.clone()))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DeviceID < out[j].DeviceID })
	return out
}

// Stats 按发布汇总计数。
type Stats struct {
	Total      int `json:"total"`
	Succeeded  int `json:"succeeded"`
	Failed     int `json:"failed"`
	RolledBack int `json:"rolled_back"`
	Active     int `json:"active"`
}

func (m *Manager) Stats(upgradeID string) Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	var s Stats
	for _, j := range m.jobs {
		if j.UpgradeID != upgradeID {
			continue
		}
		s.Total++
		switch j.Phase {
		case PhaseSucceeded:
			s.Succeeded++
		case PhaseFailed, PhaseTimeout:
			s.Failed++
		case PhaseRolledBack:
			s.RolledBack++
		default:
			s.Active++
		}
	}
	return s
}

// Snapshot / Restore 用于持久化与重启恢复。
type Snapshot struct {
	StepTimeout time.Duration `json:"step_timeout"`
	Seq         int           `json:"seq"`
	Jobs        []Job         `json:"jobs"`
}

func (m *Manager) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	jobs := make([]Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		jobs = append(jobs, j.clone())
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })
	return Snapshot{StepTimeout: m.stepTimeout, Seq: m.seq, Jobs: jobs}
}

func (m *Manager) Restore(s Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s.StepTimeout > 0 {
		m.stepTimeout = s.StepTimeout
	}
	if s.Seq > m.seq {
		m.seq = s.Seq
	}
	for _, j := range s.Jobs {
		jc := j
		m.jobs[jc.ID] = &jc
		if !jc.Phase.Terminal() {
			m.activeDevice[jc.DeviceID] = jc.ID
		}
	}
}

func sortedKeys(m map[string]*Job) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
