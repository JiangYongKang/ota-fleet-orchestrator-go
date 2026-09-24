package store

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/device"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

func TestSaveLoadRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	st, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	from, _ := version.Parse("1.0.0")
	to, _ := version.Parse("1.1.0")
	snap := Snapshot{
		Devices: []device.Device{{ID: "d1", Group: "g", CurrentVersion: from, RegisteredAt: time.Unix(100, 0)}},
		Jobs: []JobRecord{{
			DeviceID: "d1", Group: "g", FromVersion: from, ToVersion: to,
			BatchIndex: 0, State: "inprogress", Attempted: true,
			DeadlineUnix: 200, UpdatedUnix: 150,
		}},
	}
	t.Logf("输入快照: devices=%d jobs=%d (job state=inprogress attempted=true)", len(snap.Devices), len(snap.Jobs))
	if err := st.Save(snap); err != nil {
		t.Fatal(err)
	}
	got, err := st.Load()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("读回: devices=%d jobs=%d; job state=%s attempted=%t (依据: 原子写 JSON 全量快照)",
		len(got.Devices), len(got.Jobs), got.Jobs[0].State, got.Jobs[0].Attempted)
	if len(got.Devices) != 1 || got.Devices[0].ID != "d1" {
		t.Fatalf("设备往返不一致: %+v", got.Devices)
	}
	if got.Jobs[0].State != "inprogress" || !got.Jobs[0].Attempted ||
		got.Jobs[0].ToVersion.Compare(to) != 0 {
		t.Fatalf("任务往返不一致: %+v", got.Jobs[0])
	}

	// 全量覆盖：第二次保存应整体替换
	snap2 := Snapshot{Devices: []device.Device{}, Jobs: []JobRecord{}}
	if err := st.Save(snap2); err != nil {
		t.Fatal(err)
	}
	got2, _ := st.Load()
	t.Logf("二次保存空快照后读回 devices=%d jobs=%d (依据: Save 为全量覆盖)", len(got2.Devices), len(got2.Jobs))
	if len(got2.Devices) != 0 || len(got2.Jobs) != 0 {
		t.Fatal("全量覆盖语义被破坏")
	}
}

func TestLoadMissingFile(t *testing.T) {
	st, _ := New(filepath.Join(t.TempDir(), "absent.json"))
	snap, err := st.Load()
	t.Logf("输入: 不存在的快照文件; err=%v devices=%d jobs=%d (依据: 首次启动视为空快照)",
		err, len(snap.Devices), len(snap.Jobs))
	if err != nil || len(snap.Devices) != 0 || len(snap.Jobs) != 0 {
		t.Fatalf("缺失文件应返回空快照无错误, got %v %+v", err, snap)
	}
}

func TestNewEmptyPath(t *testing.T) {
	if _, err := New(""); err == nil {
		t.Fatal("空路径应报错")
	}
}
