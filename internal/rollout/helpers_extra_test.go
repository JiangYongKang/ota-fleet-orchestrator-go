package rollout

import "github.com/highcumontoa/ota-fleet-orchestrator-go/internal/device"

func mustDev(id, group, fw string) device.Device {
	return device.Device{ID: id, Group: group, CurrentVersion: mustVer(fw)}
}

func jobState(o *Orchestrator, id string) string {
	j, ok := o.Job(id)
	if !ok {
		return "<no-job>"
	}
	return j.State
}
