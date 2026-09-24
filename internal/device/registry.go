// Device 注册表与心跳状态机的实现。
package device

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

// 注册表对外暴露的确定错误。
var (
	// ErrDeviceNotFound 设备未注册。
	ErrDeviceNotFound = errors.New("device not found")
)

// HeartbeatResult 描述一次心跳处理的判定结果，便于日志解释。
type HeartbeatResult struct {
	DeviceID    string
	Accepted    bool // 心跳是否被接受（乱序旧心跳为 false）
	Online      OnlineStatus
	Reason      TransitionReason
	IncomingSeq int64
	AppliedSeq  int64 // 接受后影子中生效的 seq；被拒/重复时保持为已有 seq
}

// Registry 管理设备档案与影子，所有方法并发安全。
type Registry struct {
	mu             sync.RWMutex
	devices        map[string]Device
	shadows        map[string]*Shadow
	offlineTimeout time.Duration
	now            func() time.Time
}

// NewRegistry 创建注册表；offlineTimeout 为心跳超时阈值，
// now 可为 nil 表示使用墙上时钟（测试时可注入）。
func NewRegistry(offlineTimeout time.Duration, now func() time.Time) *Registry {
	if offlineTimeout <= 0 {
		offlineTimeout = 30 * time.Second
	}
	if now == nil {
		now = time.Now
	}
	return &Registry{
		devices:        make(map[string]Device),
		shadows:        make(map[string]*Shadow),
		offlineTimeout: offlineTimeout,
		now:            now,
	}
}

// OfflineDetector 抽象超时巡检能力，便于上层按周期调用。
type OfflineDetector interface {
	ReapOffline(ctx context.Context) []string
}

// Register 注册设备；重复注册幂等（同 ID 再次注册不报错、不覆盖既有档案）。
func (r *Registry) Register(d Device) error {
	if d.ID == "" {
		return errors.New("device id is empty")
	}
	if d.RegisteredAt.IsZero() {
		d.RegisteredAt = r.now()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.devices[d.ID]; exists {
		return nil // 幂等：忽略重复注册，不覆盖
	}
	r.devices[d.ID] = d
	r.shadows[d.ID] = &Shadow{
		DeviceID: d.ID,
		Online:   StatusNeverSeen,
		LastSeen: d.RegisteredAt,
	}
	return nil
}

// Get 查询设备档案。
func (r *Registry) Get(id string) (Device, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.devices[id]
	return d, ok
}

// cloneShadow 返回影子的深拷贝（指针字段另起新值），避免调用方持锁外修改。
func cloneShadow(sh Shadow) Shadow {
	out := sh
	if sh.Reported != nil {
		rep := *sh.Reported
		out.Reported = &rep
	}
	if sh.Desired != nil {
		des := *sh.Desired
		out.Desired = &des
	}
	return out
}

// GetShadow 查询设备影子副本。
func (r *Registry) GetShadow(id string) (Shadow, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	sh, ok := r.shadows[id]
	if !ok {
		return Shadow{}, false
	}
	return cloneShadow(*sh), true
}

// ListByGroup 列出组内全部设备 ID（按 ID 排序，保证灰度批次确定）。
func (r *Registry) ListByGroup(group string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0)
	for id, d := range r.devices {
		if d.Group == group {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// List 列出全部设备档案（按 ID 排序）。
func (r *Registry) List() []Device {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Device, 0, len(r.devices))
	for _, d := range r.devices {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// SetDesired 设置设备影子的期望版本（下发计划时调用）。
func (r *Registry) SetDesired(id string, target version.Version) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	sh, ok := r.shadows[id]
	if !ok {
		return ErrDeviceNotFound
	}
	sh.Desired = &DesiredState{FirmwareVersion: target, At: r.now()}
	return nil
}

// ShadowState 是设备影子的可持久化投影。
type ShadowState struct {
	DeviceID         string
	Online           OnlineStatus
	ReportedVersion  *version.Version
	ReportedSeq      int64
	ReportedAt       time.Time
	DesiredVersion   *version.Version
	DesiredAt        time.Time
	LastSeen         time.Time
	LastReason       TransitionReason
	LastTransitionAt time.Time
}

// SnapshotShadows 导出全部影子（按设备 ID 排序）。
func (r *Registry) SnapshotShadows() []ShadowState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.shadows))
	for id := range r.shadows {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]ShadowState, 0, len(ids))
	for _, id := range ids {
		sh := r.shadows[id]
		st := ShadowState{
			DeviceID: id, Online: sh.Online, LastSeen: sh.LastSeen,
			LastReason: sh.LastReason, LastTransitionAt: sh.LastTransitionAt,
		}
		if sh.Reported != nil {
			v := sh.Reported.FirmwareVersion
			st.ReportedVersion = &v
			st.ReportedSeq = sh.Reported.Seq
			st.ReportedAt = sh.Reported.At
		}
		if sh.Desired != nil {
			v := sh.Desired.FirmwareVersion
			st.DesiredVersion = &v
			st.DesiredAt = sh.Desired.At
		}
		out = append(out, st)
	}
	return out
}

// ShadowEntry 是 RestoreShadows 的单条输入（ID 与状态投影）。
type ShadowEntry struct {
	DeviceID string
	State    ShadowState
}

// RestoreShadows 在设备已注册后用持久化数据重建影子。
func (r *Registry) RestoreShadows(entries []ShadowEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range entries {
		sh, ok := r.shadows[e.DeviceID]
		if !ok {
			continue
		}
		s := e.State
		sh.Online = s.Online
		sh.LastSeen = s.LastSeen
		sh.LastReason = s.LastReason
		sh.LastTransitionAt = s.LastTransitionAt
		if s.ReportedVersion != nil {
			sh.Reported = &ReportedState{FirmwareVersion: *s.ReportedVersion, Seq: s.ReportedSeq, At: s.ReportedAt}
		}
		if s.DesiredVersion != nil {
			sh.Desired = &DesiredState{FirmwareVersion: *s.DesiredVersion, At: s.DesiredAt}
		}
	}
}

// Heartbeat 处理一次设备心跳。
//
// 乱序与幂等规则（依据设备单调递增的 seq）：
//   - seq < 已生效 seq：判定为乱序旧心跳，拒绝（Accepted=false），
//     不回退上报版本、不上线、不刷新 LastSeen；
//   - seq == 已生效 seq：判定为重复心跳，幂等接受但不更新任何状态，
//     仅刷新 LastSeen；
//   - seq > 已生效 seq（或首次心跳）：接受并更新上报版本与 LastSeen。
//
// 在线状态转换给出可区分原因：首次上线 first-heartbeat；超时后恢复
// heartbeat-recovered；持续在线 heartbeat-fresh；超时离线 heartbeat-timeout
// （由 ReapOffline 给出）。
func (r *Registry) Heartbeat(id string, fw version.Version, seq int64) (HeartbeatResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sh, ok := r.shadows[id]
	if !ok {
		return HeartbeatResult{}, ErrDeviceNotFound
	}
	now := r.now()
	res := HeartbeatResult{DeviceID: id, IncomingSeq: seq, Online: sh.Online}

	// 乱序旧心跳：拒绝且绝不回退任何状态。
	if sh.Reported != nil && seq < sh.Reported.Seq {
		res.Accepted = false
		res.AppliedSeq = sh.Reported.Seq
		res.Reason = ReasonStaleRejected
		sh.LastReason = ReasonStaleRejected
		return res, nil
	}

	// 重复心跳：幂等，仅刷新 LastSeen，不更新版本与状态。
	if sh.Reported != nil && seq == sh.Reported.Seq {
		res.Accepted = true
		res.AppliedSeq = seq
		res.Reason = ReasonDuplicate
		sh.LastSeen = now
		res.Online = sh.Online
		return res, nil
	}

	// 接受新心跳。
	prevStatus := sh.Online
	sh.Reported = &ReportedState{FirmwareVersion: fw, Seq: seq, At: now}
	sh.LastSeen = now
	if prevStatus == StatusNeverSeen {
		sh.Online = StatusOnline
		sh.LastReason = ReasonFirstHeartbeat
	} else if prevStatus == StatusOffline {
		sh.Online = StatusOnline
		sh.LastReason = ReasonRecovered
	} else {
		sh.LastReason = ReasonHeartbeatFresh
	}
	if sh.Online != prevStatus {
		sh.LastTransitionAt = now
	}
	res.Accepted = true
	res.AppliedSeq = seq
	res.Online = sh.Online
	res.Reason = sh.LastReason
	return res, nil
}

// ReapOffline 把"最近心跳距今超过 offlineTimeout"的在线/从未见设备
// 标记为离线，返回发生 online/never-seen -> offline 变化的设备 ID。
// 已经离线的设备重复巡检不重复计数。
func (r *Registry) ReapOffline(_ context.Context) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	changed := make([]string, 0)
	for id, sh := range r.shadows {
		if sh.Online == StatusOffline {
			continue
		}
		if now.Sub(sh.LastSeen) > r.offlineTimeout {
			sh.Online = StatusOffline
			sh.LastReason = ReasonTimeout
			sh.LastTransitionAt = now
			changed = append(changed, id)
		}
	}
	sort.Strings(changed)
	return changed
}
