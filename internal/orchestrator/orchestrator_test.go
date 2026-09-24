package orchestrator

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/clock"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/device"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/store"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/upgrade"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

var t0 = time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)

func vv(s string) version.Version {
	ver, _ := version.Parse(s)
	return ver
}

type rbRecorder struct {
	mu    sync.Mutex
	calls []string // jobID:deviceID:fromVer
}

func (r *rbRecorder) fn(j *upgrade.Job, at time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, fmt.Sprintf("%s:%s:%s", j.ID, j.DeviceID, j.FromVersion))
	return nil
}

func (r *rbRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

type env struct {
	reg *device.Registry
	mgr *upgrade.Manager
	clk *clock.FakeClock
	rb  *rbRecorder
	st  *store.JSONStore
	o   *Orchestrator
}

func newEnv(t *testing.T, stepTimeout, hbTimeout time.Duration) *env {
	t.Helper()
	st, err := store.NewJSONStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	reg := device.NewRegistry(hbTimeout)
	mgr := upgrade.NewManager(stepTimeout)
	clk := clock.NewFakeClock(t0)
	rb := &rbRecorder{}
	o := New(reg, mgr, clk, st, rb.fn)
	return &env{reg: reg, mgr: mgr, clk: clk, rb: rb, st: st, o: o}
}

func (e *env) register(id, group, ver string, at time.Time) {
	if _, _, err := e.reg.Register(device.RegisterRequest{DeviceID: id, Group: group, Version: vv(ver)}, at); err != nil {
		panic(err)
	}
}

func policy(group, to string, sizes []int, step time.Duration) BatchPolicy {
	return BatchPolicy{
		Group: group, ToVersion: vv(to),
		Rule: version.AnyForwardAllowed, BatchSizes: sizes, StepTimeout: step,
	}
}

func TestRolloutBatchingProgressAndConvergence(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	for _, id := range []string{"d1", "d2", "d3", "d4", "d5"} {
		e.register(id, "g1", "1.0.0", t0)
	}
	r, err := e.o.StartRollout(policy("g1", "1.1.0", []int{2, 3}, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT StartRollout g1 1.0.0->1.1.0 batches=[2,3] => id=%s batches=%d", r.ID, len(r.Batches))

	logs, _ := e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance1] %s", l)
	}
	st := e.mgr.Stats(r.ID)
	if st.Active != 2 || st.Total != 2 {
		t.Fatalf("first advance: want 2 active jobs, got %+v", st)
	}
	// 第二批此时不得提前下发。
	// 第一批两台成功上报（不同 seq）。
	for i, id := range []string{"d1", "d2"} {
		res := e.o.Report(upgrade.Report{DeviceID: id, UpgradeID: r.ID, Seq: int64(i + 1), Success: true, At: e.clk.Now()})
		t.Logf("INPUT success report %s seq=%d => %s | %s", id, i+1, res.Verdict, res.Reason)
		conv, _ := e.reg.Converged(id)
		if !conv {
			t.Fatalf("%s should be converged after success, shadow desired must be cleared", id)
		}
		d, _ := e.reg.Get(id)
		if d.CurrentVer.Compare(vv("1.1.0")) != 0 {
			t.Fatalf("%s current version should be 1.1.0, got %s", id, d.CurrentVer)
		}
	}
	logs, _ = e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance2] %s", l)
	}
	st = e.mgr.Stats(r.ID)
	if st.Active != 3 {
		t.Fatalf("second batch should dispatch 3, stats=%+v", st)
	}
	for i, id := range []string{"d3", "d4", "d5"} {
		e.o.Report(upgrade.Report{DeviceID: id, UpgradeID: r.ID, Seq: int64(i + 1), Success: true, At: e.clk.Now()})
	}
	rr, ok := e.o.GetRollout(r.ID)
	if !ok || rr.FinishedAt.IsZero() {
		t.Fatal("rollout should be finished after all batches succeeded")
	}
	t.Logf("INPUT all 5 success => rollout finished at %s", rr.FinishedAt.Format(time.RFC3339))
	if got := e.mgr.Stats(r.ID); got.Succeeded != 5 {
		t.Fatalf("want 5 succeeded, got %+v", got)
	}
}

func TestOfflineDeviceDeferredNotBlocking(t *testing.T) {
	e := newEnv(t, 10*time.Second, 30*time.Second)
	for _, id := range []string{"d1", "d2", "d3"} {
		e.register(id, "g1", "1.0.0", t0)
	}
	// 仅 d2 心跳超时离线：d1/d3 在 t0+31s 仍有更新心跳。
	e.clk.Advance(31 * time.Second)
	e.reg.Heartbeat(device.Heartbeat{DeviceID: "d1", Seq: 1, At: e.clk.Now()})
	e.reg.Heartbeat(device.Heartbeat{DeviceID: "d3", Seq: 1, At: e.clk.Now()})
	e.reg.SweepOffline(e.clk.Now())
	d2, _ := e.reg.Get("d2")
	t.Logf("INPUT at %s: d2 last hb=%s => status=%s reason=%s; d1/d3 kept fresh",
		e.clk.Now(), t0, d2.Status, d2.LastChangeReason)

	r, _ := e.o.StartRollout(policy("g1", "1.1.0", []int{3}, 10*time.Second))
	logs, _ := e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance] %s", l)
	}
	if e.mgr.HasActiveJob("d2") {
		t.Fatal("offline d2 must not be dispatched")
	}
	if !e.mgr.HasActiveJob("d1") || !e.mgr.HasActiveJob("d3") {
		t.Fatal("online peers must still be dispatched")
	}
	// d2 恢复上线后，下一拍即可补发。
	e.reg.Heartbeat(device.Heartbeat{DeviceID: "d2", Seq: 1, At: e.clk.Now()})
	logs, _ = e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-restored] %s", l)
	}
	if !e.mgr.HasActiveJob("d2") {
		t.Fatal("restored d2 should be dispatched on next advance without a new rollout")
	}
}

func TestIllegalUpgradeRejected(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	e.register("d1", "g1", "1.0.0", t0)
	e.register("d2", "g1", "1.0.0", t0)
	// 严格 patch 规则下，1.0.0 -> 1.2.0 属于跨 minor 跳变，整单拒绝并逐设备说明。
	p := BatchPolicy{Group: "g1", ToVersion: vv("1.2.0"), Rule: version.SameMinorPatchAllowed,
		BatchSizes: []int{2}, StepTimeout: 10 * time.Second}
	_, err := e.o.StartRollout(p)
	if err == nil {
		t.Fatal("cross-minor jump must be rejected under SameMinorPatchAllowed")
	}
	t.Logf("INPUT rollout 1.0.0->1.2.0 rule=SameMinor => REJECTED: %v", err)
}

func TestTimeoutTriggersRollbackAndHaltsCanary(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	for _, id := range []string{"d1", "d2", "d3", "d4"} {
		e.register(id, "g1", "1.0.0", t0)
	}
	r, _ := e.o.StartRollout(policy("g1", "1.1.0", []int{2, 2}, 10*time.Second))
	logs, _ := e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance] %s", l)
	}
	// d1 卡在 DOWNLOADING，d2 正常成功。
	e.o.Report(upgrade.Report{DeviceID: "d1", UpgradeID: r.ID, Seq: 1, Phase: upgrade.PhaseDownloading, Progress: 20, At: e.clk.Now()})
	e.o.Report(upgrade.Report{DeviceID: "d2", UpgradeID: r.ID, Seq: 1, Success: true, At: e.clk.Now()})
	// 超过单步超时，Tick 收敛 d1 并回滚。
	e.clk.Advance(11 * time.Second)
	tickLogs := e.o.Tick()
	for _, l := range tickLogs {
		t.Logf("[tick] %s", l)
	}
	if e.rb.count() != 1 {
		t.Fatalf("exactly one rollback expected, got %d (%v)", e.rb.count(), e.rb.calls)
	}
	jobs := e.mgr.JobsByUpgrade(r.ID)
	phase := map[string]upgrade.Phase{}
	for _, j := range jobs {
		phase[j.DeviceID] = j.Phase
	}
	if phase["d1"] != upgrade.PhaseRolledBack {
		t.Fatalf("d1 must be ROLLED_BACK, got %s", phase["d1"])
	}
	t.Logf("INPUT d1 stuck +11s => per-device phases=%v rollbacks=%v", phase, e.rb.calls)
	if d1, _ := e.reg.Get("d1"); d1.HasDesired {
		t.Fatal("failed/rolled-back device must have desired withdrawn")
	}
	// 批次失败，第二批绝不能下发。
	logs, _ = e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-after-fail] %s", l)
	}
	for _, id := range []string{"d3", "d4"} {
		if e.mgr.HasActiveJob(id) {
			t.Fatalf("%s in later batch must not be dispatched after failed batch", id)
		}
	}
	rr, _ := e.o.GetRollout(r.ID)
	if rr.FinishedAt.IsZero() {
		t.Fatal("rollout should be finished (halted) after a failed batch")
	}
}

func TestRestartRecoversAndDoesNotDuplicate(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	for _, id := range []string{"d1", "d2", "d3", "d4"} {
		e.register(id, "g1", "1.0.0", t0)
	}
	r, _ := e.o.StartRollout(policy("g1", "1.1.0", []int{2, 2}, 10*time.Second))
	e.o.Advance(r.ID)
	// d1 成功；d2 仍在途。模拟进程重启。
	e.o.Report(upgrade.Report{DeviceID: "d1", UpgradeID: r.ID, Seq: 1, Success: true, At: e.clk.Now()})

	reg2 := device.NewRegistry(time.Minute)
	mgr2 := upgrade.NewManager(10 * time.Second)
	rb2 := &rbRecorder{}
	o2 := New(reg2, mgr2, e.clk, e.st, rb2.fn)
	if err := o2.Restore(); err != nil {
		t.Fatal(err)
	}
	rr, ok := o2.GetRollout(r.ID)
	if !ok || len(rr.Batches) != 2 {
		t.Fatalf("rollout must survive restart: ok=%v rollout=%+v", ok, rr)
	}
	jobs := mgr2.JobsByUpgrade(r.ID)
	t.Logf("INPUT after restart: restored %d jobs; phases=%v", len(jobs), phases(jobs))
	if len(jobs) != 2 || !mgr2.HasActiveJob("d2") {
		t.Fatal("in-flight d2 job must be recovered after restart")
	}
	// 再推进：d1 不得重复下发，d2 继续，且只能在第一批全部成功后才发第二批。
	logs, _ := o2.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-restarted] %s", l)
	}
	if mgr2.HasActiveJob("d1") {
		t.Fatal("d1 must not be dispatched again after restart (no duplicate execution)")
	}
	for _, id := range []string{"d3", "d4"} {
		if mgr2.HasActiveJob(id) {
			t.Fatalf("%s must wait until batch 0 fully succeeds", id)
		}
	}
	// d2 完成 -> 第一批成功 -> 第二批下发一次。
	o2.Report(upgrade.Report{DeviceID: "d2", UpgradeID: r.ID, Seq: 1, Success: true, At: e.clk.Now()})
	logs, _ = o2.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-batch1] %s", l)
	}
	if !mgr2.HasActiveJob("d3") || !mgr2.HasActiveJob("d4") {
		t.Fatal("batch 1 devices must be dispatched exactly once after batch 0 succeeds")
	}
	st := mgr2.Stats(r.ID)
	if st.Total != 4 {
		t.Fatalf("exactly 4 jobs must exist (no duplicates), got %+v", st)
	}
	t.Logf("INPUT complete after restart => stats=%+v", st)
}

func phases(jobs []*upgrade.Job) map[string]upgrade.Phase {
	m := map[string]upgrade.Phase{}
	for _, j := range jobs {
		m[j.DeviceID] = j.Phase
	}
	return m
}

func TestConcurrentAdvance(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	const n = 20
	for i := 0; i < n; i++ {
		e.register(fmt.Sprintf("d%02d", i), "g1", "1.0.0", t0)
	}
	r, _ := e.o.StartRollout(policy("g1", "1.1.0", []int{n}, 10*time.Second))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = e.o.Advance(r.ID)
		}()
	}
	wg.Wait()
	st := e.mgr.Stats(r.ID)
	t.Logf("INPUT 8 concurrent Advance calls over %d devices => stats=%+v", n, st)
	if st.Total != n || st.Active != n {
		t.Fatalf("each device must receive exactly one job, got total=%d active=%d", st.Total, st.Active)
	}
}
