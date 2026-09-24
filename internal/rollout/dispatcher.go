package rollout

import (
	"fmt"
	"sync"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

// DispatchDirection 区分一次下发是升级还是回滚。
type DispatchDirection string

const (
	DirectionUpgrade  DispatchDirection = "upgrade"
	DirectionRollback DispatchDirection = "rollback"
)

// DispatchCall 记录一次真实下发动作，供测试断言"每个升级/回滚只执行一次"。
type DispatchCall struct {
	DeviceID  string
	From      version.Version
	To        version.Version
	Direction DispatchDirection
}

// Dispatcher 抽象设备侧下发动作。升级与回滚走同一接口，由 direction 区分：
// 编排器保证对同一任务的每个方向至多调用一次。
type Dispatcher interface {
	Dispatch(deviceID string, from, to version.Version, direction DispatchDirection) error
}

// SimDispatcher 是本地模拟器：默认全部成功；可按设备 ID 注入持续失败，
// 并以锁保护调用计数，支持并发场景下的断言。
type SimDispatcher struct {
	mu          sync.Mutex
	calls       []DispatchCall
	failDevices map[string]bool
}

// NewSimDispatcher 创建模拟器。
func NewSimDispatcher() *SimDispatcher {
	return &SimDispatcher{failDevices: make(map[string]bool)}
}

// FailDevice 让某台设备的后续下发持续返回错误（模拟设备端故障）。
func (d *SimDispatcher) FailDevice(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failDevices[id] = true
}

// RecoverDevice 解除 FailDevice 的故障注入（模拟设备恢复）。
func (d *SimDispatcher) RecoverDevice(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.failDevices, id)
}

// Dispatch 执行模拟下发并记录调用。
func (d *SimDispatcher) Dispatch(deviceID string, from, to version.Version, direction DispatchDirection) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, DispatchCall{DeviceID: deviceID, From: from, To: to, Direction: direction})
	if d.failDevices[deviceID] {
		return fmt.Errorf("simulated device-side failure: %s", deviceID)
	}
	return nil
}

// Calls 返回下发记录的副本。
func (d *SimDispatcher) Calls() []DispatchCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]DispatchCall, len(d.calls))
	copy(out, d.calls)
	return out
}

// Count 统计某设备某方向的下发次数（应为 0 或 1）。
func (d *SimDispatcher) Count(deviceID string, direction DispatchDirection) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, c := range d.calls {
		if c.DeviceID == deviceID && c.Direction == direction {
			n++
		}
	}
	return n
}
