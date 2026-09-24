// Package store 提供设备档案与升级任务的本地文件持久化，
// 使进程重启后未完成的升级不丢失、不重复执行。
//
// 持久化采用"写临时文件 + fsync + 原子 rename"的单 JSON 文件方案：
// 读端要么看到上一个完整快照，要么看到新完整快照，不会读到半截内容。
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/device"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

// JobRecord 是一个在途升级任务的持久化快照。
type JobRecord struct {
	DeviceID     string          `json:"device_id"`
	Group        string          `json:"group"`
	FromVersion  version.Version `json:"from_version"`
	ToVersion    version.Version `json:"to_version"`
	BatchIndex   int             `json:"batch_index"`
	State        string          `json:"state"` // pending / inprogress / succeeded / failed / rolledback
	Attempted    bool            `json:"attempted"`
	DeadlineUnix int64           `json:"deadline_unix"`
	UpdatedUnix  int64           `json:"updated_unix"`
	LastError    string          `json:"last_error,omitempty"`
}

// ShadowRecord 持久化设备影子中的期望/上报版本，使重启后收敛判定不丢失。
type ShadowRecord struct {
	DeviceID        string           `json:"device_id"`
	Online          string           `json:"online"`
	ReportedVersion *version.Version `json:"reported_version,omitempty"`
	ReportedSeq     int64            `json:"reported_seq,omitempty"`
	ReportedUnix    int64            `json:"reported_unix,omitempty"`
	DesiredVersion  *version.Version `json:"desired_version,omitempty"`
	DesiredUnix     int64            `json:"desired_unix,omitempty"`
	LastSeenUnix    int64            `json:"last_seen_unix,omitempty"`
	LastReason      string           `json:"last_reason,omitempty"`
}

// Snapshot 是持久化文件的完整内容。
type Snapshot struct {
	Devices []device.Device `json:"devices"`
	Jobs    []JobRecord     `json:"jobs"`
	Shadows []ShadowRecord  `json:"shadows,omitempty"`
}

// Store 把编排状态原子地写入单个本地 JSON 文件。
type Store struct {
	path string
}

// New 创建/打开持久化文件；文件不存在时延迟到首次 Save 时创建。
func New(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("store path is empty")
	}
	return &Store{path: path}, nil
}

// Load 读取最近一次快照；文件不存在时返回零值快照（首次启动）。
func (s *Store) Load() (Snapshot, error) {
	var snap Snapshot
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{Devices: []device.Device{}, Jobs: []JobRecord{}}, nil
	}
	if err != nil {
		return Snapshot{}, fmt.Errorf("load snapshot: %w", err)
	}
	if len(data) == 0 {
		return Snapshot{Devices: []device.Device{}, Jobs: []JobRecord{}}, nil
	}
	if err := json.Unmarshal(data, &snap); err != nil {
		return Snapshot{}, fmt.Errorf("decode snapshot %s: %w", s.path, err)
	}
	if snap.Devices == nil {
		snap.Devices = []device.Device{}
	}
	if snap.Jobs == nil {
		snap.Jobs = []JobRecord{}
	}
	return snap, nil
}

// Save 全量原子覆盖写入快照。
func (s *Store) Save(snap Snapshot) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return fmt.Errorf("create snapshot dir: %w", err)
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open temp snapshot: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write temp snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync temp snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp snapshot: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename snapshot: %w", err)
	}
	return nil
}
