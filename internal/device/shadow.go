package device

import (
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

// OnlineStatus 表示设备在线状态。
type OnlineStatus int

const (
	StatusNeverSeen OnlineStatus = iota
	StatusOnline
	StatusOffline
)

// String 返回状态的可解释名称。
func (s OnlineStatus) String() string {
	switch s {
	case StatusOnline:
		return "online"
	case StatusOffline:
		return "offline"
	default:
		return "never-seen"
	}
}

// ReportedState 是设备最近一次上报的状态。
type ReportedState struct {
	FirmwareVersion version.Version
	Seq             int64 // 设备侧单调递增的心跳序号，用于乱序判定
	At              time.Time
}

// DesiredState 是云端期望设备达到的状态。
type DesiredState struct {
	FirmwareVersion version.Version
	At              time.Time
}

// TransitionReason 描述在线状态变化的可解释原因。
type TransitionReason string

const (
	ReasonFirstHeartbeat TransitionReason = "first-heartbeat"
	ReasonHeartbeatFresh TransitionReason = "heartbeat-fresh"
	ReasonTimeout        TransitionReason = "heartbeat-timeout"
	ReasonRecovered      TransitionReason = "heartbeat-recovered"
	ReasonStaleRejected  TransitionReason = "stale-heartbeat-rejected"
	ReasonDuplicate      TransitionReason = "duplicate-heartbeat-ignored"
	ReasonNone           TransitionReason = ""
)

// Shadow 是单个设备的影子：期望状态与上报状态分别记录。
type Shadow struct {
	DeviceID string
	Online   OnlineStatus
	Reported *ReportedState
	Desired  *DesiredState
	LastSeen time.Time
	// LastReason / LastTransitionAt 记录最近一次在线状态变化的可解释原因。
	LastReason       TransitionReason
	LastTransitionAt time.Time
}

// Converged 判定上报版本是否已达到期望版本：
// 无期望时无需收敛；无上报时尚未收敛；否则比较版本。
func (sh *Shadow) Converged() bool {
	if sh.Desired == nil {
		return true
	}
	if sh.Reported == nil {
		return false
	}
	return sh.Reported.FirmwareVersion.Compare(sh.Desired.FirmwareVersion) >= 0
}
