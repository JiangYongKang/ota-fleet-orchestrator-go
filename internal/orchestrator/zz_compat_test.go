package orchestrator

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/clock"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/device"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/store"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/upgrade"
)

func TestRestoreOldObjectStyleSnapshot(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "state.json")
	// 旧格式：Version 为 {Major,Minor,Patch} 对象；无 paused_groups/max_concurrent 字段。
	old := `{
  "registry": {"timeout":60000000000,"devices":{"d1":{"ID":"d1","Group":"g1","CurrentVer":{"Major":1,"Minor":0,"Patch":0},"Status":"ONLINE","DesiredVer":{"Major":0,"Minor":0,"Patch":0},"HasDesired":false,"ReportedVer":{"Major":1,"Minor":0,"Patch":0},"ReportedVersionOK":true,"ReportedSeq":0,"LastHeartbeat":"2026-09-24T10:00:00Z","LastChangeReason":"REGISTERED","UpdatedAt":"2026-09-24T10:00:00Z"}}},
  "manager": {"step_timeout":10000000000,"seq":0,"jobs":[]},
  "rollouts": [],
  "seq": 0
}`
	if err := os.WriteFile(p, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := store.NewJSONStore(p)
	if err != nil {
		t.Fatal(err)
	}
	reg := device.NewRegistry(time.Minute)
	mgr := upgrade.NewManager(10 * time.Second)
	o := New(reg, mgr, clock.NewFakeClock(t0), st, nil)
	if err := o.Restore(); err != nil {
		t.Fatalf("old snapshot must restore: %v", err)
	}
	d, ok := reg.Get("d1")
	if !ok || d.CurrentVer.String() != "1.0.0" {
		t.Fatalf("device version from object-style snapshot wrong: %+v ok=%v", d, ok)
	}
	// 旧格式无暂停信息：默认不暂停；旧策略字段零值保持旧行为。
	if o.IsGroupPaused("g1") {
		t.Fatal("default paused state must be false for old snapshots")
	}
	t.Logf("INPUT 旧格式快照（Version 对象、无新字段）=> 恢复成功 d1=%s paused=false（依据：双格式反序列化 + 零值默认）", d.CurrentVer)
}
