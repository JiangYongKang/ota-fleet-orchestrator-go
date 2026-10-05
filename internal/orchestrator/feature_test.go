package orchestrator

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/device"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/upgrade"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

func pad2(i int) string { return fmt.Sprintf("%02d", i) }

func succeed(e *env, rid, dev string) upgrade.ReportResult {
	return e.o.Report(upgrade.Report{DeviceID: dev, UpgradeID: rid, Seq: 1, Success: true, At: e.clk.Now()})
}

// 场景：同一条升级任务整体并发上限 cap=2，到上限排队；设备完成立即补位。
func TestMaxConcurrentCapQueueAndRefill(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	for _, id := range []string{"d1", "d2", "d3", "d4", "d5"} {
		e.register(id, "g1", "1.0.0", t0)
	}
	p := policy("g1", "1.1.0", []int{5}, 10*time.Second)
	p.MaxConcurrent = 2
	r, err := e.o.StartRollout(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT 5 台设备一批，MaxConcurrent=2 => rollout=%s（依据：active 达到 2 后其余必须排队）", r.ID)

	logs, _ := e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-1] %s", l)
	}
	if st := e.mgr.Stats(r.ID); st.Active != 2 || st.Total != 2 {
		t.Fatalf("cap=2: want exactly 2 dispatched, got %+v", st)
	}
	if !e.mgr.HasActiveJob("d1") || !e.mgr.HasActiveJob("d2") {
		t.Fatal("d1/d2 must hold the two slots")
	}

	// d1 完成：空出的槽位必须在同一拍补入 d3，无需外部再调用 Advance。
	res := succeed(e, r.ID, "d1")
	t.Logf("INPUT d1 success => %s（补位由 Report 内部立即触发）", res.Reason)
	if !e.mgr.HasActiveJob("d3") {
		t.Fatal("queued d3 must be dispatched immediately after d1 frees a slot")
	}
	if st := e.mgr.Stats(r.ID); st.Active != 2 {
		t.Fatalf("active must stay at cap=2 after refill, got %+v", st)
	}
	succeed(e, r.ID, "d2")
	if !e.mgr.HasActiveJob("d4") {
		t.Fatal("d4 must refill after d2 completes")
	}
	succeed(e, r.ID, "d3")
	if !e.mgr.HasActiveJob("d5") {
		t.Fatal("d5 must refill after d3 completes")
	}
	// 队列耗尽，active 随完成自然下降，不再超过上限。
	succeed(e, r.ID, "d4")
	succeed(e, r.ID, "d5")
	rr, _ := e.o.GetRollout(r.ID)
	if rr.FinishedAt.IsZero() {
		t.Fatal("rollout must finish after queued devices refill and complete")
	}
	st := e.mgr.Stats(r.ID)
	t.Logf("INPUT 全部完成 => total=%d succeeded=%d active=%d（依据：每台设备一个 job，补位不超 cap）",
		st.Total, st.Succeeded, st.Active)
	if st.Succeeded != 5 || st.Total != 5 {
		t.Fatalf("want 5 jobs/5 succeeded, got %+v", st)
	}
}

// 场景：d1 长时间卡住占一个槽位，其它设备仍能用剩余槽位持续推进，不被堵死。
func TestStuckDeviceDoesNotStallOthers(t *testing.T) {
	e := newEnv(t, 30*time.Second, time.Minute)
	for _, id := range []string{"d1", "d2", "d3", "d4", "d5"} {
		e.register(id, "g1", "1.0.0", t0)
	}
	p := policy("g1", "1.1.0", []int{5}, 30*time.Second)
	p.MaxConcurrent = 2
	r, _ := e.o.StartRollout(p)
	e.o.Advance(r.ID)
	// d1 报一次进展但不再上报（故意卡住，尚未超时）；d2 成功。
	e.o.Report(upgrade.Report{DeviceID: "d1", UpgradeID: r.ID, Seq: 1,
		Phase: upgrade.PhaseDownloading, Progress: 10, At: e.clk.Now()})
	succeed(e, r.ID, "d2")
	succeed(e, r.ID, "d3")
	succeed(e, r.ID, "d4")
	succeed(e, r.ID, "d5")
	st := e.mgr.Stats(r.ID)
	t.Logf("INPUT d1 卡住，d2..d5 逐个完成 => active=%d succeeded=%d（依据：卡住只占 1 个槽，另一槽持续补位）",
		st.Active, st.Succeeded)
	if !e.mgr.HasActiveJob("d1") {
		t.Fatal("d1 stays in-flight (stuck, not timed out yet)")
	}
	if st.Succeeded != 4 || st.Active != 1 {
		t.Fatalf("d2..d5 must all succeed while d1 is stuck, got %+v", st)
	}
}

// 场景：一条任务暂停/恢复 g1 不影响另一条 g2 任务的节奏。
func TestPauseOneGroupDoesNotBlockOtherGroup(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	e.register("a1", "g1", "1.0.0", t0)
	e.register("a2", "g1", "1.0.0", t0)
	e.register("b1", "g2", "1.0.0", t0)
	e.register("b2", "g2", "1.0.0", t0)
	r1, _ := e.o.StartRollout(policy("g1", "1.1.0", []int{2}, 10*time.Second))
	r2, _ := e.o.StartRollout(policy("g2", "1.1.0", []int{2}, 10*time.Second))

	affected, plogs := e.o.PauseGroup("g1")
	for _, l := range plogs {
		t.Logf("[pause] %s", l)
	}
	if len(affected) != 1 || affected[0] != r1.ID {
		t.Fatalf("only g1 rollout affected, got %v", affected)
	}
	if !e.o.IsGroupPaused("g1") || e.o.IsGroupPaused("g2") {
		t.Fatal("pause state must be group-scoped")
	}

	l1, _ := e.o.Advance(r1.ID)
	for _, l := range l1 {
		t.Logf("[advance-g1-paused] %s", l)
	}
	l2, _ := e.o.Advance(r2.ID)
	for _, l := range l2 {
		t.Logf("[advance-g2] %s", l)
	}
	if e.mgr.HasActiveJob("a1") || e.mgr.HasActiveJob("a2") {
		t.Fatal("paused g1 must not dispatch anything")
	}
	if !e.mgr.HasActiveJob("b1") || !e.mgr.HasActiveJob("b2") {
		t.Fatal("g2 must keep normal cadence while g1 paused")
	}
	succeed(e, r2.ID, "b1")
	succeed(e, r2.ID, "b2")
	rr2, _ := e.o.GetRollout(r2.ID)
	if rr2.FinishedAt.IsZero() {
		t.Fatal("g2 rollout must finish unaffected by g1 pause")
	}

	// 恢复 g1：立即补派，无需外部 Advance。
	_, rlogs := e.o.ResumeGroup("g1")
	for _, l := range rlogs {
		t.Logf("[resume] %s", l)
	}
	if !e.mgr.HasActiveJob("a1") || !e.mgr.HasActiveJob("a2") {
		t.Fatal("resume must dispatch unfinished g1 devices immediately")
	}
}

// 场景：暂停时已在途设备自然收尾，恢复后已成功设备不重复下发，继续未完成部分。
func TestPauseResumeInFlightCompletesAndNoDuplicate(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	for _, id := range []string{"d1", "d2", "d3"} {
		e.register(id, "g1", "1.0.0", t0)
	}
	p := policy("g1", "1.1.0", []int{3}, 10*time.Second)
	p.MaxConcurrent = 3
	r, _ := e.o.StartRollout(p)
	e.o.Advance(r.ID)
	// 三台都在途时暂停；d1 成功收尾，d2/d3 仍在途。
	e.o.PauseGroup("g1")
	succeed(e, r.ID, "d1")
	logs, _ := e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-paused] %s", l)
	}
	if st := e.mgr.Stats(r.ID); st.Total != 3 {
		t.Fatalf("no new dispatch while paused, got %+v", st)
	}
	// d2 也在暂停期间自然完成。
	succeed(e, r.ID, "d2")

	e.o.ResumeGroup("g1")
	// 再拍：d1/d2 已成功不得重派，只继续 d3。
	logs, _ = e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-resumed] %s", l)
	}
	for _, id := range []string{"d1", "d2"} {
		jobs := e.mgr.JobsByUpgrade(r.ID)
		n := 0
		for _, j := range jobs {
			if j.DeviceID == id {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("%s must keep exactly one job after resume, got %d", id, n)
		}
	}
	succeed(e, r.ID, "d3")
	rr, _ := e.o.GetRollout(r.ID)
	if rr.FinishedAt.IsZero() {
		t.Fatal("rollout should finish after resume continues unfinished devices")
	}
	if st := e.mgr.Stats(r.ID); st.Total != 3 || st.Succeeded != 3 {
		t.Fatalf("want 3 jobs / 3 succeeded, got %+v", st)
	}
	t.Logf("INPUT 恢复后继续 => stats=%+v（依据：成功设备 job 唯一，不从头再走）", e.mgr.Stats(r.ID))
}

// 场景：暂停状态跨重启保留。
func TestPauseStatePersistsAcrossRestart(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	e.register("d1", "g1", "1.0.0", t0)
	r, _ := e.o.StartRollout(policy("g1", "1.1.0", []int{1}, 10*time.Second))
	e.o.PauseGroup("g1")
	e.o.Advance(r.ID)
	if e.mgr.HasActiveJob("d1") {
		t.Fatal("paused before any dispatch")
	}

	o2 := New(device.NewRegistry(time.Minute), upgrade.NewManager(10*time.Second), e.clk, e.st, (&rbRecorder{}).fn)
	if err := o2.Restore(); err != nil {
		t.Fatal(err)
	}
	if !o2.IsGroupPaused("g1") {
		t.Fatal("paused state must survive restart")
	}
	logs, _ := o2.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-restarted-paused] %s", l)
	}
	if o2.HasActiveJobForTest(r.ID, "d1") {
		t.Fatal("restored orchestrator must keep g1 frozen")
	}
	o2.ResumeGroup("g1")
	if !o2.HasActiveJobForTest(r.ID, "d1") {
		t.Fatal("resume after restart must continue the rollout")
	}
}

// 路径模式：落后不同版本的设备沿路径逐跳升级，全部收敛到终点。
func TestPathRolloutStepByStep(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	e.register("d1", "g1", "1.0.0", t0) // 落后两跳
	e.register("d2", "g1", "1.1.0", t0) // 落后一跳
	path, err := version.ParsePath([]string{"1.0.0", "1.1.0", "1.2.0"})
	if err != nil {
		t.Fatal(err)
	}
	p := BatchPolicy{Group: "g1", ToVersion: vv("1.2.0"), Path: path,
		BatchSizes: []int{2}, StepTimeout: 10 * time.Second, MaxConcurrent: 2}
	r, err := e.o.StartRollout(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT 路径 %v -> 1.2.0：d1@1.0.0 d2@1.1.0 => rollout=%s", path.Versions(), r.ID)

	logs, _ := e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-1] %s", l)
	}
	assertJob := func(dev, from, to string) {
		t.Helper()
		j, ok := e.mgr.ActiveJob(dev)
		if !ok {
			t.Fatalf("%s expected active job %s->%s", dev, from, to)
		}
		if j.FromVersion.Compare(vv(from)) != 0 || j.ToVersion.Compare(vv(to)) != 0 {
			t.Fatalf("%s job hop mismatch: want %s->%s got %s->%s",
				dev, from, to, j.FromVersion, j.ToVersion)
		}
	}
	assertJob("d1", "1.0.0", "1.1.0")
	assertJob("d2", "1.1.0", "1.2.0")

	// d1 第一跳成功后应立即自动派发第二跳，不能停在 1.1.0。
	succeed(e, r.ID, "d1")
	d, _ := e.reg.Get("d1")
	t.Logf("INPUT d1 第一跳成功 => current=%s（依据：影子随成功上报前进，desired 已切到下一跳）", d.CurrentVer)
	assertJob("d1", "1.1.0", "1.2.0")
	succeed(e, r.ID, "d2")
	succeed(e, r.ID, "d1")

	rr, _ := e.o.GetRollout(r.ID)
	if rr.FinishedAt.IsZero() {
		t.Fatal("rollout finishes when every device reaches the path final version")
	}
	jobs := e.mgr.JobsByUpgrade(r.ID)
	byDev := map[string]int{}
	for _, j := range jobs {
		byDev[j.DeviceID]++
	}
	t.Logf("INPUT 全部收敛 => 每设备 job 数=%v（依据：1.0.0 两跳两个 job，1.1.0 一跳一个）", byDev)
	if byDev["d1"] != 2 || byDev["d2"] != 1 {
		t.Fatalf("d1 needs 2 hops, d2 needs 1, got %v", byDev)
	}
	for _, id := range []string{"d1", "d2"} {
		d, _ := e.reg.Get(id)
		if d.CurrentVer.Compare(vv("1.2.0")) != 0 {
			t.Fatalf("%s must end at 1.2.0, got %s", id, d.CurrentVer)
		}
	}
}

// 路径外版本 / 目标非终点：创建时整单拒绝。
func TestPathRolloutRejectsOffPath(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	e.register("d1", "g1", "1.5.0", t0) // 不在路径上
	path, _ := version.ParsePath([]string{"1.0.0", "1.1.0", "1.2.0"})
	p := BatchPolicy{Group: "g1", ToVersion: vv("1.2.0"), Path: path,
		BatchSizes: []int{1}, StepTimeout: 10 * time.Second}
	if _, err := e.o.StartRollout(p); err == nil {
		t.Fatal("off-path current version must be rejected with per-device reason")
	} else {
		t.Logf("INPUT d1@1.5.0 不在路径 => REJECTED: %v", err)
	}

	// 目标版本必须等于路径终点（禁止路径中间版本当终点之外的目标错配）。
	e2 := newEnv(t, 10*time.Second, time.Minute)
	e2.register("x1", "g1", "1.0.0", t0)
	p2 := BatchPolicy{Group: "g1", ToVersion: vv("1.1.0"), Path: path,
		BatchSizes: []int{1}, StepTimeout: 10 * time.Second}
	if _, err := e2.o.StartRollout(p2); err == nil {
		t.Fatal("target that is not the path final must be rejected")
	} else {
		t.Logf("INPUT target=1.1.0 非路径终点 => REJECTED: %v", err)
	}
}

// 路径中途失败：重试同一跳，已完成跳不重走、版本不回退。
func TestPathHopFailureRetriesSameHop(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	e.register("d1", "g1", "1.0.0", t0)
	path, _ := version.ParsePath([]string{"1.0.0", "1.1.0", "1.2.0"})
	p := BatchPolicy{Group: "g1", ToVersion: vv("1.2.0"), Path: path,
		BatchSizes: []int{1}, StepTimeout: 10 * time.Second, MaxAttempts: 3}
	r, _ := e.o.StartRollout(p)
	e.o.Advance(r.ID)
	// 第一跳设备上报失败。
	fail := e.o.Report(upgrade.Report{DeviceID: "d1", UpgradeID: r.ID, Seq: 1,
		Phase: upgrade.PhaseFailed, Success: false, Message: "install verify failed", At: e.clk.Now()})
	t.Logf("INPUT 第一跳失败上报 => %s %s（依据：终态失败只计一次，current 保留 1.0.0）", fail.Verdict, fail.Reason)
	if d, _ := e.reg.Get("d1"); d.CurrentVer.Compare(vv("1.0.0")) != 0 {
		t.Fatalf("failed hop must keep 1.0.0, got %s", d.CurrentVer)
	}
	if d, _ := e.reg.Get("d1"); !d.HasDesired {
		t.Fatal("path mode keeps desired (final target) while the failed hop will be retried")
	}
	// 下一拍重试同一跳：仍是 1.0.0 -> 1.1.0，而非跳到第二跳。
	logs, _ := e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-retry] %s", l)
	}
	j, ok := e.mgr.ActiveJob("d1")
	if !ok || j.FromVersion.Compare(vv("1.0.0")) != 0 || j.ToVersion.Compare(vv("1.1.0")) != 0 {
		t.Fatalf("retry must repeat the same hop 1.0.0->1.1.0, got %+v", j)
	}
	succeed(e, r.ID, "d1") // 重试成功
	j2, ok := e.mgr.ActiveJob("d1")
	if !ok || j2.ToVersion.Compare(vv("1.2.0")) != 0 {
		t.Fatalf("after recovered hop, next hop 1.1.0->1.2.0 must dispatch, got %+v", j2)
	}
	t.Logf("INPUT 重试成功 => 自动进入下一跳 %s（依据：已完成跳不重走）", j2.ToVersion)
	succeed(e, r.ID, "d1")
	if st := e.mgr.Stats(r.ID); st.Succeeded != 2 {
		t.Fatalf("two successful hops expected, got %+v", st)
	}
}

// 尝试次数耗尽：批次熔断，不再无限重派。
func TestPathHopExhaustsAttemptsHalts(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	e.register("d1", "g1", "1.0.0", t0)
	path, _ := version.ParsePath([]string{"1.0.0", "1.1.0"})
	p := BatchPolicy{Group: "g1", ToVersion: vv("1.1.0"), Path: path,
		BatchSizes: []int{1}, StepTimeout: 10 * time.Second, MaxAttempts: 1}
	r, _ := e.o.StartRollout(p)
	e.o.Advance(r.ID)
	e.o.Report(upgrade.Report{DeviceID: "d1", UpgradeID: r.ID, Seq: 1,
		Phase: upgrade.PhaseFailed, Success: false, Message: "boom", At: e.clk.Now()})
	logs, _ := e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-exhausted] %s", l)
	}
	if e.mgr.HasActiveJob("d1") {
		t.Fatal("no further attempt allowed after attempts exhausted")
	}
	rr, _ := e.o.GetRollout(r.ID)
	if rr.FinishedAt.IsZero() || rr.Batches[0].Status != BatchFailed {
		t.Fatalf("batch must fail and rollout halt, batch=%s finished=%v", rr.Batches[0].Status, !rr.FinishedAt.IsZero())
	}
	t.Logf("INPUT MaxAttempts=1 且失败 => 批次 %s、发布结束（依据：逐设备可定位，禁止无限重派）", rr.Batches[0].Status)
}

// 路径走到一半重启：接着进度继续，不重走已完成跳，不降级。
func TestPathRestartContinuesMidPath(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	e.register("d1", "g1", "1.0.0", t0)
	path, _ := version.ParsePath([]string{"1.0.0", "1.1.0", "1.2.0"})
	p := BatchPolicy{Group: "g1", ToVersion: vv("1.2.0"), Path: path,
		BatchSizes: []int{1}, StepTimeout: 10 * time.Second}
	r, _ := e.o.StartRollout(p)
	e.o.Advance(r.ID)
	succeed(e, r.ID, "d1") // 已升到 1.1.0，第二跳尚未派发时重启
	if d, _ := e.reg.Get("d1"); d.CurrentVer.Compare(vv("1.1.0")) != 0 {
		t.Fatalf("precondition: d1 at 1.1.0, got %s", d.CurrentVer)
	}

	reg2 := device.NewRegistry(time.Minute)
	mgr2 := upgrade.NewManager(10 * time.Second)
	o2 := New(reg2, mgr2, e.clk, e.st, (&rbRecorder{}).fn)
	if err := o2.Restore(); err != nil {
		t.Fatal(err)
	}
	logs, _ := o2.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-restarted] %s", l)
	}
	j, ok := mgr2.ActiveJob("d1")
	if !ok {
		t.Fatal("second hop must dispatch after restart")
	}
	if j.FromVersion.Compare(vv("1.1.0")) != 0 || j.ToVersion.Compare(vv("1.2.0")) != 0 {
		t.Fatalf("must continue 1.1.0->1.2.0, never redo or downgrade, got %s->%s", j.FromVersion, j.ToVersion)
	}
	// 已完成的第一跳不得再出现任务。
	for _, xj := range mgr2.JobsByUpgrade(r.ID) {
		if xj.ToVersion.Compare(vv("1.1.0")) == 0 && xj.DeviceID == "d1" && xj.Phase == upgrade.PhaseSucceeded {
			t.Logf("INPUT 重启后保留已完成跳 job=%s %s->%s %s（不重复下发）",
				xj.ID, xj.FromVersion, xj.ToVersion, xj.Phase)
		}
	}
	o2.Report(upgrade.Report{DeviceID: "d1", UpgradeID: r.ID, Seq: 1, Success: true, At: e.clk.Now()})
	d, _ := reg2.Get("d1")
	if d.CurrentVer.Compare(vv("1.2.0")) != 0 {
		t.Fatalf("d1 must finish at 1.2.0 after resume, got %s", d.CurrentVer)
	}
}

// 路径中途超时：回滚到本跳起点，之后重试同一跳，已完成版本不丢。
func TestPathTimeoutRollsBackHopAndRetries(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	e.register("d1", "g1", "1.0.0", t0)
	path, _ := version.ParsePath([]string{"1.0.0", "1.1.0", "1.2.0"})
	p := BatchPolicy{Group: "g1", ToVersion: vv("1.2.0"), Path: path,
		BatchSizes: []int{1}, StepTimeout: 10 * time.Second}
	r, _ := e.o.StartRollout(p)
	e.o.Advance(r.ID)
	succeed(e, r.ID, "d1") // 第一跳完成，自动派发第二跳 1.1.0->1.2.0
	j, _ := e.mgr.ActiveJob("d1")
	if j.ToVersion.Compare(vv("1.2.0")) != 0 {
		t.Fatalf("precondition second hop, got %s->%s", j.FromVersion, j.ToVersion)
	}
	e.o.Report(upgrade.Report{DeviceID: "d1", UpgradeID: r.ID, Seq: 1,
		Phase: upgrade.PhaseInstalling, Progress: 80, At: e.clk.Now()})
	e.clk.Advance(11 * time.Second)
	logs := e.o.Tick()
	for _, l := range logs {
		t.Logf("[tick] %s", l)
	}
	if e.rb.count() != 1 {
		t.Fatalf("one rollback expected for the timed-out hop, got %d", e.rb.count())
	}
	d, _ := e.reg.Get("d1")
	if d.CurrentVer.Compare(vv("1.1.0")) != 0 {
		t.Fatalf("rollback returns device to hop start 1.1.0, got %s", d.CurrentVer)
	}
	j2, ok := e.mgr.ActiveJob("d1")
	if !ok || j2.FromVersion.Compare(vv("1.1.0")) != 0 || j2.ToVersion.Compare(vv("1.2.0")) != 0 {
		t.Fatalf("must retry same hop 1.1.0->1.2.0 after rollback, got %+v ok=%v", j2, ok)
	}
	t.Logf("INPUT 第二跳超时 => 回滚到 %s 并重试同跳（依据：已完成的 1.0.0->1.1.0 不降级）", d.CurrentVer)
}

// 路径模式 + 并发推进/暂停恢复调用：在途互斥仍保证每跳每设备恰好一个 job。
func TestPathConcurrentAdvanceAndPauseRace(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	const n = 12
	for i := 0; i < n; i++ {
		e.register("d"+pad2(i), "g1", "1.0.0", t0)
	}
	path, _ := version.ParsePath([]string{"1.0.0", "1.1.0", "1.2.0"})
	p := BatchPolicy{Group: "g1", ToVersion: vv("1.2.0"), Path: path,
		BatchSizes: []int{n}, StepTimeout: 10 * time.Second, MaxConcurrent: 4}
	r, _ := e.o.StartRollout(p)

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			_, _ = e.o.Advance(r.ID)
			if k%2 == 0 {
				e.o.PauseGroup("g1")
				e.o.ResumeGroup("g1")
			}
		}(i)
	}
	wg.Wait()
	st := e.mgr.Stats(r.ID)
	t.Logf("INPUT 6 个并发 goroutine（Advance/Pause/Resume 混合） => stats=%+v（依据：activeDevice 互斥 + cap=4）", st)
	if st.Active > 4 || st.Total > n {
		t.Fatalf("concurrency cap/uniqueness violated: %+v", st)
	}
	// 每个在途设备恰好一个任务。
	seen := map[string]int{}
	for _, j := range e.mgr.JobsByUpgrade(r.ID) {
		if !j.Phase.Terminal() {
			seen[j.DeviceID]++
			if seen[j.DeviceID] > 1 {
				t.Fatalf("device %s has multiple in-flight jobs", j.DeviceID)
			}
		}
	}
}

// cap 与暂停叠加：暂停期间槽位被释放的设备不会越组补位到别的任务，
// 恢复后严格按本任务队列继续。
func TestCapWithPauseDrainsAndResumesQueue(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	for _, id := range []string{"d1", "d2", "d3", "d4"} {
		e.register(id, "g1", "1.0.0", t0)
	}
	p := policy("g1", "1.1.0", []int{4}, 10*time.Second)
	p.MaxConcurrent = 2
	r, _ := e.o.StartRollout(p)
	e.o.Advance(r.ID) // d1,d2 占用；d3,d4 排队
	e.o.PauseGroup("g1")
	succeed(e, r.ID, "d1") // 暂停期间 d1 完成，但不补位
	succeed(e, r.ID, "d2")
	if e.mgr.HasActiveJob("d3") || e.mgr.HasActiveJob("d4") {
		t.Fatal("paused rollout must not refill even when slots free up")
	}
	st := e.mgr.Stats(r.ID)
	t.Logf("INPUT 暂停期间两槽放空 => active=%d succeeded=%d（依据：暂停冻结新派发）", st.Active, st.Succeeded)
	_, logs := e.o.ResumeGroup("g1")
	for _, l := range logs {
		t.Logf("[resume] %s", l)
	}
	if !e.mgr.HasActiveJob("d3") || !e.mgr.HasActiveJob("d4") {
		t.Fatal("resume must refill queued devices up to cap")
	}
	succeed(e, r.ID, "d3")
	succeed(e, r.ID, "d4")
	if st := e.mgr.Stats(r.ID); st.Succeeded != 4 || st.Total != 4 {
		t.Fatalf("all 4 devices exactly one successful job, got %+v", st)
	}
}

// 路径模式第一跳成功后：desired 必须保持为最终目标（未收敛），直到最后一跳成功才清除。
func TestPathDesiredStaysUntilFinalHop(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	e.register("d1", "g1", "1.0.0", t0)
	path, _ := version.ParsePath([]string{"1.0.0", "1.1.0", "1.2.0"})
	p := BatchPolicy{Group: "g1", ToVersion: vv("1.2.0"), Path: path,
		BatchSizes: []int{1}, StepTimeout: 10 * time.Second}
	r, _ := e.o.StartRollout(p)
	e.o.Advance(r.ID)
	succeed(e, r.ID, "d1")
	d, _ := e.reg.Get("d1")
	if !d.HasDesired || d.DesiredVer.Compare(vv("1.2.0")) != 0 {
		t.Fatalf("desired must remain final 1.2.0 between hops, got has=%v ver=%s", d.HasDesired, d.DesiredVer)
	}
	conv, _ := e.reg.Converged("d1")
	if conv {
		t.Fatal("device at 1.1.0 must not be considered converged toward 1.2.0")
	}
	succeed(e, r.ID, "d1")
	d, _ = e.reg.Get("d1")
	if d.HasDesired || d.CurrentVer.Compare(vv("1.2.0")) != 0 {
		t.Fatalf("after final hop desired must clear at 1.2.0, got has=%v cur=%s", d.HasDesired, d.CurrentVer)
	}
}
