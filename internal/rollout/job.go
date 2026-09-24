package rollout

import (
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

// 任务状态机：
//
//	pending ──dispatch──> inprogress ──success report──> succeeded
//	   │                     │
//	   │ dispatch 失败/超时   │ 失败上报/超时
//	   ▼                     ▼
//	failed(瞬时，等待回滚) ──rollback 下发成功──> rolledback
//
// succeeded / rolledback 为终态；重复与迟到的结果上报对终态任务不再计数。
const (
	StatePending    = "pending"
	StateInProgress = "inprogress"
	StateSucceeded  = "succeeded"
	StateFailed     = "failed" // 已失败、回滚尚未确认（瞬态）
	StateRolledBack = "rolledback"
)

// isTerminal 报告状态是否终态。
func isTerminal(state string) bool {
	return state == StateSucceeded || state == StateRolledBack
}

// Job 是内存中的单设备升级任务。
type Job struct {
	DeviceID          string
	Group             string
	PlanID            string
	FromVersion       version.Version
	ToVersion         version.Version
	BatchIndex        int
	State             string
	Attempted         bool // 升级动作是否已真正下发（重启后据此避免重复下发）
	RollbackAttempted bool // 回滚动作是否已真正下发
	Deadline          time.Time
	UpdatedAt         time.Time
	LastError         string
}

// clone 返回任务副本。
func (j *Job) clone() Job {
	return *j
}

// Plan 是一次"按组灰度"的下发计划。
type Plan struct {
	ID           string
	Group        string
	Target       version.Version
	BatchSize    int
	CurrentBatch int // 已开放下发的最大批次（0 基），每 Tick 按完成度推进
	CreatedAt    time.Time
}

// batchOf 按设备在组内的确定顺序与 BatchSize 计算其批次号。
func batchOf(orderIndex, batchSize int) int {
	if batchSize <= 1 {
		return orderIndex
	}
	return orderIndex / batchSize
}
