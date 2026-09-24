package device

import (
	"fmt"
	"sync"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

// Status 表示设备连接状态。
type Status string

const (
	StatusOnline  Status = "ONLINE"
	StatusOffline Status = "OFFLINE"
)

// OfflineReason 离线/上线的可区分原因。
type OfflineReason string

const (
	ReasonNone          OfflineReason = ""                   // 仍在线，无状态变化
	ReasonRegistered    OfflineReason = "REGISTERED"         // 首次注册
	ReasonHeartbeatLost OfflineReason = "HEARTBEAT_LOST"     // 心跳超时判定离线
	ReasonRestored      OfflineReason = "HEARTBEAT_RESTORED" // 收到更新心跳，从离线恢复
)

// Device 表示注册设备及其影子（desired / reported 分离记录）。
type Device struct {
	ID         string
	Group      string
	CurrentVer version.Version
	Status     Status
	// 影子 desired 段：云端期望设备运行的版本。
	DesiredVer version.Version
	HasDesired bool
	// 影子 reported 段：设备最近一次确认上报的版本。
	ReportedVer       version.Version
	ReportedVersionOK bool
	ReportedSeq       int64
	LastHeartbeat     time.Time
	LastChangeReason  OfflineReason
	UpdatedAt         time.Time
}

func (d *Device) clone() Device { return *d }

// Heartbeat 设备心跳/状态上报。Seq 为设备端单调序号。
type Heartbeat struct {
	DeviceID string
	Seq      int64
	Reported version.Version
	HasVer   bool
	At       time.Time
}

// RegisterRequest 注册请求。
type RegisterRequest struct {
	DeviceID string
	Group    string
	Version  version.Version
}

// HBVerdict 心跳处理结论（可解释）。
type HBVerdict string

const (
	HBUnknownDevice HBVerdict = "UNKNOWN_DEVICE"
	HBDuplicate     HBVerdict = "DUPLICATE"       // 相同 seq 重投，幂等忽略
	HBStale         HBVerdict = "STALE"           // 更旧 seq，拒绝回退
	HBAccepted      HBVerdict = "ACCEPTED"        // 接受并更新影子
	HBRestored      HBVerdict = "RESTORED_ONLINE" // 接受心跳同时从离线恢复
)

// HBResult 心跳处理结果。
type HBResult struct {
	Verdict HBVerdict
	Reason  string
	Device  *Device
}

// OfflineEvent 一次离线状态变化，供编排层解释/持久化。
type OfflineEvent struct {
	DeviceID string
	Status   Status
	Reason   OfflineReason
	At       time.Time
}

// Registry 管理设备注册与设备影子，所有方法并发安全。
type Registry struct {
	mu      sync.Mutex
	devices map[string]*Device
	timeout time.Duration
}

// NewRegistry 创建空注册表。timeout 为心跳超时判定窗口。
func NewRegistry(timeout time.Duration) *Registry {
	return &Registry{devices: make(map[string]*Device), timeout: timeout}
}

// Register 注册设备。重复注册幂等：已存在时原样返回既有记录，
// created=false，绝不覆盖影子状态。
func (r *Registry) Register(req RegisterRequest, at time.Time) (*Device, bool, error) {
	if req.DeviceID == "" {
		return nil, false, fmt.Errorf("device id is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if d, ok := r.devices[req.DeviceID]; ok {
		cp := d.clone()
		return &cp, false, nil
	}
	d := &Device{
		ID:                req.DeviceID,
		Group:             req.Group,
		CurrentVer:        req.Version,
		Status:            StatusOnline,
		ReportedVer:       req.Version,
		ReportedVersionOK: true,
		LastHeartbeat:     at,
		LastChangeReason:  ReasonRegistered,
		UpdatedAt:         at,
	}
	r.devices[req.DeviceID] = d
	cp := d.clone()
	return &cp, true, nil
}

// Heartbeat 处理心跳：旧序号拒绝回退，相同序号幂等忽略，
// 新序号更新 reported 影子并在离线时恢复上线。
func (r *Registry) Heartbeat(hb Heartbeat) HBResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[hb.DeviceID]
	if !ok {
		return HBResult{Verdict: HBUnknownDevice, Reason: fmt.Sprintf("device %s is not registered", hb.DeviceID)}
	}
	switch {
	case hb.Seq < d.ReportedSeq:
		return HBResult{
			Verdict: HBStale,
			Reason: fmt.Sprintf("heartbeat seq=%d is older than applied seq=%d; rejected to avoid rollback",
				hb.Seq, d.ReportedSeq),
			Device: ptr(d.clone()),
		}
	case hb.Seq == d.ReportedSeq && d.ReportedSeq != 0:
		return HBResult{
			Verdict: HBDuplicate,
			Reason:  fmt.Sprintf("heartbeat seq=%d already applied; idempotently ignored", hb.Seq),
			Device:  ptr(d.clone()),
		}
	default:
		wasOffline := d.Status == StatusOffline
		d.ReportedSeq = hb.Seq
		if hb.HasVer {
			d.ReportedVer = hb.Reported
			d.ReportedVersionOK = true
		}
		d.LastHeartbeat = hb.At
		d.UpdatedAt = hb.At
		if wasOffline {
			d.Status = StatusOnline
			d.LastChangeReason = ReasonRestored
			cp := d.clone()
			return HBResult{
				Verdict: HBRestored,
				Reason:  fmt.Sprintf("newer heartbeat seq=%d restored device ONLINE (was OFFLINE since %s)", hb.Seq, d.LastHeartbeat),
				Device:  &cp,
			}
		}
		d.LastChangeReason = ReasonNone
		cp := d.clone()
		return HBResult{
			Verdict: HBAccepted,
			Reason:  fmt.Sprintf("heartbeat seq=%d applied; reported=%s", hb.Seq, displayVer(hb.HasVer, hb.Reported, d)),
			Device:  &cp,
		}
	}
}

func displayVer(hasVer bool, v version.Version, d *Device) string {
	if hasVer {
		return v.String()
	}
	return d.ReportedVer.String()
}

func ptr(d Device) *Device { return &d }

// SetDesired 写入期望版本（影子 desired 段）。
func (r *Registry) SetDesired(deviceID string, v version.Version, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[deviceID]
	if !ok {
		return fmt.Errorf("device %s is not registered", deviceID)
	}
	d.DesiredVer = v
	d.HasDesired = true
	d.UpdatedAt = at
	return nil
}

// ClearDesired 撤回/清除期望标记（升级失败回滚或收敛后调用）。
func (r *Registry) ClearDesired(deviceID string, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[deviceID]
	if !ok {
		return fmt.Errorf("device %s is not registered", deviceID)
	}
	d.HasDesired = false
	d.DesiredVer = version.Version{}
	d.UpdatedAt = at
	return nil
}

// Converged 判断 desired 与 reported 是否已收敛：
// 无期望时视为已收敛；否则 reported 版本必须等于 desired 版本。
func (r *Registry) Converged(deviceID string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[deviceID]
	if !ok {
		return false, fmt.Errorf("device %s is not registered", deviceID)
	}
	if !d.HasDesired {
		return true, nil
	}
	return d.ReportedVersionOK && d.ReportedVer.Compare(d.DesiredVer) == 0, nil
}

// SweepOffline 扫描全部设备：在线但超过 timeout 未收到更新心跳者判离线，
// 仅对本次发生变化的设备返回事件（可解释原因）。
func (r *Registry) SweepOffline(now time.Time) []OfflineEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	var events []OfflineEvent
	for _, d := range r.devices {
		if d.Status == StatusOnline && now.Sub(d.LastHeartbeat) > r.timeout {
			d.Status = StatusOffline
			d.LastChangeReason = ReasonHeartbeatLost
			d.UpdatedAt = now
			events = append(events, OfflineEvent{
				DeviceID: d.ID, Status: StatusOffline, Reason: ReasonHeartbeatLost, At: now,
			})
		}
	}
	return events
}

// Get 查询设备（返回副本）。
func (r *Registry) Get(deviceID string) (*Device, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[deviceID]
	if !ok {
		return nil, false
	}
	cp := d.clone()
	return &cp, true
}

// ListByGroup 返回组内设备副本（按 ID 排序，保证可重复）。
func (r *Registry) ListByGroup(group string) []*Device {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*Device
	for _, d := range r.devices {
		if d.Group == group {
			cp := d.clone()
			out = append(out, &cp)
		}
	}
	sortDevices(out)
	return out
}

// Snapshot / Restore 用于持久化与重启恢复（深拷贝）。
type Snapshot struct {
	Timeout time.Duration     `json:"timeout"`
	Devices map[string]Device `json:"devices"`
}

func (r *Registry) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := make(map[string]Device, len(r.devices))
	for id, d := range r.devices {
		m[id] = d.clone()
	}
	return Snapshot{Timeout: r.timeout, Devices: m}
}

func (r *Registry) Restore(s Snapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s.Timeout > 0 {
		r.timeout = s.Timeout
	}
	r.devices = make(map[string]*Device, len(s.Devices))
	for id, d := range s.Devices {
		dc := d
		r.devices[id] = &dc
	}
}

// MarkReportedVersion 记录设备在升级结果中确认上报的版本（升级结果序列与
// 心跳序号相互独立，故不经过心跳序号保护，仅由升级层自身的幂等保证一次生效）。
// 设备确认运行新版本后，其当前版本一并更新。
func (r *Registry) MarkReportedVersion(deviceID string, v version.Version, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.devices[deviceID]
	if !ok {
		return fmt.Errorf("device %s is not registered", deviceID)
	}
	d.ReportedVer = v
	d.ReportedVersionOK = true
	d.CurrentVer = v
	d.UpdatedAt = at
	return nil
}
