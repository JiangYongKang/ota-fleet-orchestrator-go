// Package device 维护设备档案、设备影子以及心跳/在线状态。
package device

import (
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

// Device 是设备的静态档案。
type Device struct {
	ID             string
	Group          string
	CurrentVersion version.Version // 注册时声明的当前固件版本
	RegisteredAt   time.Time
}
