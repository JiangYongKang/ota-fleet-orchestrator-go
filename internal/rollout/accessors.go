package rollout

import "github.com/highcumontoa/ota-fleet-orchestrator-go/internal/device"

// Shadow 返回设备影子副本（透传注册表）。
func (o *Orchestrator) Shadow(id string) (device.Shadow, bool) {
	return o.registry.GetShadow(id)
}

// SimDispatcher 尝试把当前下发器还原为本地模拟器（demo 故障注入用）。
func (o *Orchestrator) SimDispatcher() (*SimDispatcher, bool) {
	d, ok := o.disp.(*SimDispatcher)
	return d, ok
}

// SimCalls 在当前下发器为本地模拟器时返回其调用记录副本。
func (o *Orchestrator) SimCalls() []DispatchCall {
	if d, ok := o.disp.(*SimDispatcher); ok {
		return d.Calls()
	}
	return nil
}
