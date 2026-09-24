package device

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

// t0 为固定基准时间，保证本地可重复运行。
var t0 = time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)

func v(s string) version.Version {
	ver, err := version.Parse(s)
	if err != nil {
		panic(err)
	}
	return ver
}

// logDecision 打印输入与判定依据，满足“单测日志中需打印输入与判定依据”。
func logDecision(t *testing.T, input string, res HBResult) {
	t.Helper()
	t.Logf("INPUT {%s} => VERDICT=%s | REASON=%q", input, res.Verdict, res.Reason)
}

func TestRegisterIdempotent(t *testing.T) {
	r := NewRegistry(time.Minute)
	d1, created1, err := r.Register(RegisterRequest{DeviceID: "dev-1", Group: "g1", Version: v("1.0.0")}, t0)
	if err != nil || !created1 {
		t.Fatalf("first register: created=%v err=%v", created1, err)
	}
	t.Logf("INPUT register dev-1@1.0.0 => device status=%s reason=%s", d1.Status, d1.LastChangeReason)

	// 重复注册：幂等，不覆盖任何影子字段。
	_, created2, _ := r.Register(RegisterRequest{DeviceID: "dev-1", Group: "OTHER", Version: v("9.9.9")}, t0.Add(time.Second))
	if created2 {
		t.Fatal("duplicate registration must be idempotent (created=false)")
	}
	got, _ := r.Get("dev-1")
	if got.Group != "g1" || got.CurrentVer.Compare(v("1.0.0")) != 0 {
		t.Fatalf("duplicate registration overwrote state: %+v", got)
	}
	t.Logf("INPUT duplicate register dev-1@9.9.9(group=OTHER) => kept group=%s version=%s", got.Group, got.CurrentVer)
}

func TestHeartbeatOutOfOrderAndDuplicate(t *testing.T) {
	r := NewRegistry(time.Minute)
	_, _, _ = r.Register(RegisterRequest{DeviceID: "dev-1", Group: "g1", Version: v("1.0.0")}, t0)

	// 正常接受 seq=10。
	res10 := r.Heartbeat(Heartbeat{DeviceID: "dev-1", Seq: 10, Reported: v("1.0.1"), HasVer: true, At: t0.Add(2 * time.Second)})
	logDecision(t, "heartbeat dev-1 seq=10 reported=1.0.1", res10)
	if res10.Verdict != HBAccepted {
		t.Fatalf("seq=10: want ACCEPTED got %s", res10.Verdict)
	}
	// 乱序旧心跳 seq=9：不得把 reported 回退。
	res9 := r.Heartbeat(Heartbeat{DeviceID: "dev-1", Seq: 9, Reported: v("1.0.0"), HasVer: true, At: t0.Add(3 * time.Second)})
	logDecision(t, "OUT-OF-ORDER heartbeat dev-1 seq=9 reported=1.0.0", res9)
	if res9.Verdict != HBStale {
		t.Fatalf("seq=9: want STALE got %s", res9.Verdict)
	}
	got, _ := r.Get("dev-1")
	if got.ReportedVer.Compare(v("1.0.1")) != 0 {
		t.Fatalf("stale heartbeat rolled reported back to %s", got.ReportedVer)
	}
	// 重复心跳 seq=10：幂等。
	resDup := r.Heartbeat(Heartbeat{DeviceID: "dev-1", Seq: 10, Reported: v("1.0.1"), HasVer: true, At: t0.Add(4 * time.Second)})
	logDecision(t, "DUPLICATE heartbeat dev-1 seq=10", resDup)
	if resDup.Verdict != HBDuplicate {
		t.Fatalf("dup seq=10: want DUPLICATE got %s", resDup.Verdict)
	}
	// 更新心跳 seq=11 接受。
	res11 := r.Heartbeat(Heartbeat{DeviceID: "dev-1", Seq: 11, Reported: v("1.0.2"), HasVer: true, At: t0.Add(5 * time.Second)})
	logDecision(t, "heartbeat dev-1 seq=11 reported=1.0.2", res11)
	if res11.Verdict != HBAccepted {
		t.Fatalf("seq=11: want ACCEPTED got %s", res11.Verdict)
	}
	// 未知设备。
	resX := r.Heartbeat(Heartbeat{DeviceID: "ghost", Seq: 1, At: t0})
	logDecision(t, "heartbeat unknown device ghost", resX)
	if resX.Verdict != HBUnknownDevice {
		t.Fatalf("want UNKNOWN_DEVICE got %s", resX.Verdict)
	}
}

func TestOfflineDetectionAndRestore(t *testing.T) {
	timeout := 30 * time.Second
	r := NewRegistry(timeout)
	_, _, _ = r.Register(RegisterRequest{DeviceID: "dev-1", Version: v("1.0.0")}, t0)

	// 29s：仍在线。
	if evs := r.SweepOffline(t0.Add(29 * time.Second)); len(evs) != 0 {
		t.Fatalf("29s should be online, got events %v", evs)
	}
	// 31s：超时，判离线，原因 HEARTBEAT_LOST。
	evs := r.SweepOffline(t0.Add(31 * time.Second))
	if len(evs) != 1 || evs[0].Reason != ReasonHeartbeatLost {
		t.Fatalf("want 1 HEARTBEAT_LOST event, got %v", evs)
	}
	got, _ := r.Get("dev-1")
	t.Logf("INPUT sweep at +31s (last hb %s, timeout %s) => %s reason=%s",
		t0.Format(time.RFC3339), timeout, got.Status, got.LastChangeReason)
	if got.Status != StatusOffline {
		t.Fatal("device must be OFFLINE")
	}
	// 再扫一次不重复产生事件。
	if evs := r.SweepOffline(t0.Add(32 * time.Second)); len(evs) != 0 {
		t.Fatalf("offline transition must be emitted once, got %v", evs)
	}
	// 旧心跳不能让离线设备“恢复”。
	stale := r.Heartbeat(Heartbeat{DeviceID: "dev-1", Seq: -1, At: t0.Add(33 * time.Second)})
	logDecision(t, "stale heartbeat while offline seq=-1", stale)
	if stale.Verdict != HBStale {
		t.Fatalf("want STALE got %s", stale.Verdict)
	}
	got, _ = r.Get("dev-1")
	if got.Status != StatusOffline {
		t.Fatal("stale heartbeat must not restore device online")
	}
	// 更新心跳到达：恢复上线，原因可区分。
	res := r.Heartbeat(Heartbeat{DeviceID: "dev-1", Seq: 1, At: t0.Add(34 * time.Second)})
	logDecision(t, "fresh heartbeat dev-1 seq=1 while offline", res)
	if res.Verdict != HBRestored {
		t.Fatalf("want RESTORED_ONLINE got %s", res.Verdict)
	}
	got, _ = r.Get("dev-1")
	if got.Status != StatusOnline || got.LastChangeReason != ReasonRestored {
		t.Fatalf("want ONLINE/HEARTBEAT_RESTORED, got %s/%s", got.Status, got.LastChangeReason)
	}
}

func TestShadowConvergence(t *testing.T) {
	r := NewRegistry(time.Minute)
	_, _, _ = r.Register(RegisterRequest{DeviceID: "dev-1", Version: v("1.0.0")}, t0)
	// 无 desired：天然收敛。
	conv, _ := r.Converged("dev-1")
	if !conv {
		t.Fatal("device without desired should be converged")
	}
	if err := r.SetDesired("dev-1", v("1.1.0"), t0); err != nil {
		t.Fatal(err)
	}
	t.Log("INPUT SetDesired 1.1.0 while reported=1.0.0 => expected NOT converged")
	conv, _ = r.Converged("dev-1")
	if conv {
		t.Fatal("desired != reported must not be converged")
	}
	// reported 追平 desired。
	if err := r.MarkReportedVersion("dev-1", v("1.1.0"), t0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	conv, _ = r.Converged("dev-1")
	t.Log("INPUT reported=1.1.0 desired=1.1.0 => expected converged")
	if !conv {
		t.Fatal("equal desired/reported must be converged")
	}
	_ = r.ClearDesired("dev-1", t0.Add(2*time.Second))
	conv, _ = r.Converged("dev-1")
	if !conv {
		t.Fatal("after ClearDesired should be converged")
	}
}

func TestConcurrentRegisterAndHeartbeat(t *testing.T) {
	// 乱序并发到达：每个 seq 恰好生效一次，最终 reported 必须是最高 seq 携带的版本。
	r := NewRegistry(time.Minute)
	_, _, _ = r.Register(RegisterRequest{DeviceID: "dev-1", Version: v("1.0.0")}, t0)
	var wg sync.WaitGroup
	verdicts := make(map[HBVerdict]int)
	var mu sync.Mutex
	for seq := int64(1); seq <= 50; seq++ {
		wg.Add(1)
		go func(seq int64) {
			defer wg.Done()
			ver := version.Version{Major: 1, Minor: 0, Patch: int(seq)}
			res := r.Heartbeat(Heartbeat{DeviceID: "dev-1", Seq: seq, Reported: ver, HasVer: true, At: t0.Add(time.Duration(seq) * time.Second)})
			mu.Lock()
			verdicts[res.Verdict]++
			mu.Unlock()
		}(seq)
	}
	wg.Wait()
	got, _ := r.Get("dev-1")
	want := version.Version{Major: 1, Minor: 0, Patch: 50}
	t.Logf("INPUT 50 concurrent heartbeats seq=1..50 => verdicts=%v final reported=%s (want %s)",
		verdicts, got.ReportedVer, want)
	if got.ReportedVer.Compare(want) != 0 || got.ReportedSeq != 50 {
		t.Fatalf("final shadow must be seq=50/1.0.50, got seq=%d/%s", got.ReportedSeq, got.ReportedVer)
	}
	// 乱序并发：每个心跳恰好有一次确定判定（接受或因更旧而拒绝），总数必须守恒。
	if verdicts[HBAccepted]+verdicts[HBStale] != 50 {
		t.Fatalf("every heartbeat must be decided exactly once, got %v", verdicts)
	}
	if verdicts[HBAccepted] < 1 {
		t.Fatal("at least the highest/newest heartbeat must be accepted")
	}

	// 并发重复注册不得产生重复设备或污染状态。
	var wg2 sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			_, created, err := r.Register(RegisterRequest{DeviceID: "dev-1", Group: fmt.Sprintf("g%d", i), Version: v("9.9.9")}, t0)
			if err != nil || created {
				t.Errorf("duplicate concurrent register: created=%v err=%v", created, err)
			}
		}(i)
	}
	wg2.Wait()
	d, _ := r.Get("dev-1")
	if d.CurrentVer.Compare(v("1.0.0")) != 0 {
		t.Fatalf("concurrent duplicate registers polluted state: %s", d.CurrentVer)
	}
}
