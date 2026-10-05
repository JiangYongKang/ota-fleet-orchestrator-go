package orchestrator

import (
	"testing"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/device"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/upgrade"
)

// TestMaxConcurrentQueueAndRefill 验证按发布配置的整体并发上限：
// 到达上限后设备排队；有设备退出在途（成功/超时回滚）后排队设备及时补位；
// 个别设备卡住不会拖停整条任务。
func TestMaxConcurrentQueueAndRefill(t *testing.T) {
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
	t.Logf("INPUT StartRollout 5 devices MaxConcurrent=2 => id=%s", r.ID)

	logs, _ := e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance1] %s", l)
	}
	st := e.mgr.Stats(r.ID)
	if st.Active != 2 || st.Total != 2 {
		t.Fatalf("concurrency cap: want exactly 2 in-flight, got %+v", st)
	}
	// 无人退出在途时再推进：不得超发，其余 3 台保持排队。
	logs, _ = e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance2-no-slot] %s", l)
	}
	if st := e.mgr.Stats(r.ID); st.Active != 2 || st.Total != 2 {
		t.Fatalf("no slot freed: must not exceed cap, got %+v", st)
	}

	// d1 成功退出在途 -> 下一拍立刻补位一台（总量 3，在途仍 2）。
	res := e.o.Report(upgrade.Report{DeviceID: "d1", UpgradeID: r.ID, Seq: 1, Success: true, At: e.clk.Now()})
	t.Logf("INPUT d1 success => %s | %s", res.Verdict, res.Reason)
	logs, _ = e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance3-refill] %s", l)
	}
	st = e.mgr.Stats(r.ID)
	if st.Active != 2 || st.Total != 3 || st.Succeeded != 1 {
		t.Fatalf("slot must be refilled after d1 exits: got %+v", st)
	}

	// d2 卡在 DOWNLOADING；d3 保持有进展（刷新其超时窗口），
	// 使 +11s 时只有 d2 超时：验证“个别设备卡住”只释放自己的槽位。
	e.o.Report(upgrade.Report{DeviceID: "d2", UpgradeID: r.ID, Seq: 1, Phase: upgrade.PhaseDownloading, Progress: 30, At: e.clk.Now()})
	e.clk.Advance(5 * time.Second)
	e.o.Report(upgrade.Report{DeviceID: "d3", UpgradeID: r.ID, Seq: 1, Phase: upgrade.PhaseDownloading, Progress: 10, At: e.clk.Now()})
	e.clk.Advance(6 * time.Second) // t0+11s：d2 已过 t0+10s 截止，d3 截止在 t0+15s
	for _, l := range e.o.Tick() {
		t.Logf("[tick-timeout] %s", l)
	}
	st = e.mgr.Stats(r.ID)
	t.Logf("INPUT d2 stuck until %s => stats=%+v", e.clk.Now(), st)
	if st.RolledBack != 1 || st.Active != 1 {
		t.Fatalf("only stuck d2 must be rolled back (d3 refreshed its deadline), got %+v", st)
	}
	// d2 的槽位释放 -> 排队的 d4 补位；d3 仍在途不受影响。
	logs, _ = e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance4-after-timeout] %s", l)
	}
	st = e.mgr.Stats(r.ID)
	if st.Active != 2 || st.Total != 4 || !e.mgr.HasActiveJob("d4") {
		t.Fatalf("freed slot must be refilled by queued d4, got %+v", st)
	}
	// d2 已回滚止步，绝不自动重试。
	if e.mgr.HasActiveJob("d2") {
		t.Fatal("rolled-back d2 must not be retried automatically")
	}
	// d3 成功 -> 队尾 d5 补位，整条任务持续推进。
	e.o.Report(upgrade.Report{DeviceID: "d3", UpgradeID: r.ID, Seq: 2, Success: true, At: e.clk.Now()})
	logs, _ = e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance5-refill-d5] %s", l)
	}
	if !e.mgr.HasActiveJob("d5") {
		t.Fatal("queued d5 must be dispatched after d3 succeeded")
	}
	t.Logf("INPUT final stats => %+v", e.mgr.Stats(r.ID))
}

// TestPauseResumeRollout 验证单条发布的暂停/恢复：
// 暂停后不再派发新任务、在途设备自然收尾；恢复后已成功设备不重复下发；
// 暂停期间其它分组的发布节奏不受影响；暂停状态跨重启保持。
func TestPauseResumeRollout(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	for _, id := range []string{"a1", "a2", "a3"} {
		e.register(id, "ga", "1.0.0", t0)
	}
	for _, id := range []string{"b1", "b2"} {
		e.register(id, "gb", "1.0.0", t0)
	}
	ra, err := e.o.StartRollout(policy("ga", "1.1.0", []int{1, 2}, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	rb, err := e.o.StartRollout(policy("gb", "1.1.0", []int{2}, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	logs, _ := e.o.Advance(ra.ID)
	for _, l := range logs {
		t.Logf("[advance-ga-batch0] %s", l)
	}
	if !e.mgr.HasActiveJob("a1") || e.mgr.HasActiveJob("a2") {
		t.Fatal("batch 0 of ga must dispatch only a1")
	}

	msg, err := e.o.PauseRollout(ra.ID)
	t.Logf("INPUT PauseRollout(%s) => %s (err=%v)", ra.ID, msg, err)
	if err != nil {
		t.Fatal(err)
	}
	// 暂停幂等；未知发布报错。
	if _, err := e.o.PauseRollout(ra.ID); err != nil {
		t.Fatalf("pause must be idempotent: %v", err)
	}
	if _, err := e.o.PauseRollout("nope"); err == nil {
		t.Fatal("unknown rollout must error")
	}
	// 暂停期间：ga 不再派发新任务。
	logs, _ = e.o.Advance(ra.ID)
	for _, l := range logs {
		t.Logf("[advance-paused] %s", l)
	}
	if st := e.mgr.Stats(ra.ID); st.Total != 1 {
		t.Fatalf("paused rollout must not dispatch new jobs, got %+v", st)
	}
	// 在途 a1 自然收尾：上报成功被正常受理。
	res := e.o.Report(upgrade.Report{DeviceID: "a1", UpgradeID: ra.ID, Seq: 1, Success: true, At: e.clk.Now()})
	t.Logf("INPUT a1 success while paused => %s | %s", res.Verdict, res.Reason)
	if res.Verdict != upgrade.ReportAccepted {
		t.Fatalf("in-flight device must finish naturally while paused, got %s", res.Verdict)
	}
	// 其它分组节奏不受影响：gb 正常派发并成功收尾。
	logs, _ = e.o.Advance(rb.ID)
	for _, l := range logs {
		t.Logf("[advance-gb] %s", l)
	}
	if !e.mgr.HasActiveJob("b1") || !e.mgr.HasActiveJob("b2") {
		t.Fatal("other group rollout must proceed while ga is paused")
	}
	e.o.Report(upgrade.Report{DeviceID: "b1", UpgradeID: rb.ID, Seq: 1, Success: true, At: e.clk.Now()})
	e.o.Report(upgrade.Report{DeviceID: "b2", UpgradeID: rb.ID, Seq: 1, Success: true, At: e.clk.Now()})
	if rr, _ := e.o.GetRollout(rb.ID); rr.FinishedAt.IsZero() {
		t.Fatal("gb rollout must finish normally while ga paused")
	}

	// 模拟进程重启：暂停标记必须随快照恢复。
	reg2 := device.NewRegistry(time.Minute)
	mgr2 := upgrade.NewManager(10 * time.Second)
	rb2 := &rbRecorder{}
	o2 := New(reg2, mgr2, e.clk, e.st, rb2.fn)
	if err := o2.Restore(); err != nil {
		t.Fatal(err)
	}
	rr, ok := o2.GetRollout(ra.ID)
	if !ok || !rr.Paused {
		t.Fatalf("paused flag must survive restart, got %+v ok=%v", rr, ok)
	}
	logs, _ = o2.Advance(ra.ID)
	for _, l := range logs {
		t.Logf("[advance-restarted-still-paused] %s", l)
	}
	if st := mgr2.Stats(ra.ID); st.Total != 1 {
		t.Fatalf("restarted orchestrator must still honor pause, got %+v", st)
	}

	// 恢复：a1 已成功不得重复下发；a2/a3 接着派发。
	msg, err = o2.ResumeRollout(ra.ID)
	t.Logf("INPUT ResumeRollout(%s) => %s (err=%v)", ra.ID, msg, err)
	if err != nil {
		t.Fatal(err)
	}
	logs, _ = o2.Advance(ra.ID)
	for _, l := range logs {
		t.Logf("[advance-resumed] %s", l)
	}
	st := mgr2.Stats(ra.ID)
	if st.Total != 3 {
		t.Fatalf("resume must dispatch exactly the 2 remaining devices (a1 not re-dispatched), got %+v", st)
	}
	for _, j := range mgr2.JobsByUpgrade(ra.ID) {
		if j.DeviceID == "a1" && j.Phase != upgrade.PhaseSucceeded {
			t.Fatalf("a1 must keep its single succeeded job, got %+v", j)
		}
	}
	// 恢复后完成剩余设备，发布正常收尾。
	o2.Report(upgrade.Report{DeviceID: "a2", UpgradeID: ra.ID, Seq: 1, Success: true, At: e.clk.Now()})
	o2.Report(upgrade.Report{DeviceID: "a3", UpgradeID: ra.ID, Seq: 1, Success: true, At: e.clk.Now()})
	o2.Advance(ra.ID)
	if rr, _ := o2.GetRollout(ra.ID); rr.FinishedAt.IsZero() {
		t.Fatal("ga rollout must finish after resume completes remaining devices")
	}
	t.Logf("INPUT final ga stats => %+v", mgr2.Stats(ra.ID))
}

// pathPolicy 构造沿既定路径升级的发布策略。
func pathPolicy(group, to string, path []string, sizes []int, step time.Duration) BatchPolicy {
	p := policy(group, to, sizes, step)
	for _, s := range path {
		p.Path = append(p.Path, vv(s))
	}
	return p
}

// TestPathRolloutMultiStep 验证跨版本沿既定路径逐段升级：
// 每段成功后才推进下一段；路径外组合与降级在创建时即被拒绝。
func TestPathRolloutMultiStep(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	e.register("d1", "g1", "1.0.0", t0)
	e.register("d2", "g1", "1.0.0", t0)

	// 路径外目标版本：整单拒绝。
	if _, err := e.o.StartRollout(pathPolicy("g1", "1.5.0", []string{"1.0.0", "1.1.0", "1.2.0"}, []int{2}, 10*time.Second)); err == nil {
		t.Fatal("off-path target must be rejected at StartRollout")
	} else {
		t.Logf("INPUT StartRollout to=1.5.0 path=[1.0.0 1.1.0 1.2.0] => REJECTED: %v", err)
	}
	// 降级目标：整单拒绝。
	if _, err := e.o.StartRollout(pathPolicy("g1", "0.9.0", []string{"0.9.0", "1.0.0"}, []int{2}, 10*time.Second)); err == nil {
		t.Fatal("downgrade must be rejected at StartRollout")
	} else {
		t.Logf("INPUT StartRollout to=0.9.0 (current=1.0.0) => REJECTED: %v", err)
	}

	r, err := e.o.StartRollout(pathPolicy("g1", "1.2.0", []string{"1.0.0", "1.1.0", "1.2.0"}, []int{2}, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT StartRollout 1.0.0->1.2.0 via path [1.0.0 1.1.0 1.2.0] => id=%s", r.ID)

	// 第一拍：只走第一段 1.0.0 -> 1.1.0。
	logs, _ := e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-step1] %s", l)
	}
	for _, id := range []string{"d1", "d2"} {
		j, ok := e.mgr.ActiveJob(id)
		if !ok || j.ToVersion.Compare(vv("1.1.0")) != 0 {
			t.Fatalf("%s first step must target 1.1.0, got %+v", id, j)
		}
	}
	// 第一段完成后再推进：第二段 1.1.0 -> 1.2.0。
	for i, id := range []string{"d1", "d2"} {
		e.o.Report(upgrade.Report{DeviceID: id, UpgradeID: r.ID, Seq: int64(i + 1), Success: true, At: e.clk.Now()})
	}
	logs, _ = e.o.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-step2] %s", l)
	}
	for _, id := range []string{"d1", "d2"} {
		j, ok := e.mgr.ActiveJob(id)
		if !ok || j.FromVersion.Compare(vv("1.1.0")) != 0 || j.ToVersion.Compare(vv("1.2.0")) != 0 {
			t.Fatalf("%s second step must be 1.1.0->1.2.0, got %+v", id, j)
		}
	}
	// 全部完成：发布收敛，设备到达最终目标。
	for i, id := range []string{"d1", "d2"} {
		e.o.Report(upgrade.Report{DeviceID: id, UpgradeID: r.ID, Seq: int64(i + 10), Success: true, At: e.clk.Now()})
	}
	e.o.Advance(r.ID)
	rr, _ := e.o.GetRollout(r.ID)
	if rr.FinishedAt.IsZero() {
		t.Fatal("path rollout must finish after both steps succeed")
	}
	for _, id := range []string{"d1", "d2"} {
		d, _ := e.reg.Get(id)
		if d.CurrentVer.Compare(vv("1.2.0")) != 0 {
			t.Fatalf("%s must land on 1.2.0, got %s", id, d.CurrentVer)
		}
	}
	t.Logf("INPUT both steps succeeded => stats=%+v rollout finished", e.mgr.Stats(r.ID))
}

// TestPathRolloutRestartResumesMidPath 路径走到一半进程重启：
// 恢复后从中间版本继续下一段，不从头再来、不重复已完成的段。
func TestPathRolloutRestartResumesMidPath(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	e.register("d1", "g1", "1.0.0", t0)
	r, err := e.o.StartRollout(pathPolicy("g1", "1.2.0", []string{"1.0.0", "1.1.0", "1.2.0"}, []int{1}, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	e.o.Advance(r.ID)
	e.o.Report(upgrade.Report{DeviceID: "d1", UpgradeID: r.ID, Seq: 1, Success: true, At: e.clk.Now()})
	if d, _ := e.reg.Get("d1"); d.CurrentVer.Compare(vv("1.1.0")) != 0 {
		t.Fatalf("d1 must be at intermediate 1.1.0 before restart, got %s", d.CurrentVer)
	}
	t.Logf("INPUT d1 finished step 1 (now 1.1.0); simulating process restart")

	reg2 := device.NewRegistry(time.Minute)
	mgr2 := upgrade.NewManager(10 * time.Second)
	o2 := New(reg2, mgr2, e.clk, e.st, (&rbRecorder{}).fn)
	if err := o2.Restore(); err != nil {
		t.Fatal(err)
	}
	logs, _ := o2.Advance(r.ID)
	for _, l := range logs {
		t.Logf("[advance-after-restart] %s", l)
	}
	j, ok := mgr2.ActiveJob("d1")
	if !ok {
		t.Fatal("d1 must be dispatched for the next step after restart")
	}
	if j.FromVersion.Compare(vv("1.1.0")) != 0 || j.ToVersion.Compare(vv("1.2.0")) != 0 {
		t.Fatalf("restart must resume at 1.1.0->1.2.0, got %s -> %s", j.FromVersion, j.ToVersion)
	}
	// 已完成的 1.0.0->1.1.0 段不得重复：d1 全程只有 2 个任务。
	if n := len(mgr2.JobsByUpgrade(r.ID)); n != 2 {
		t.Fatalf("completed step must not be re-executed after restart, want 2 jobs got %d", n)
	}
	t.Logf("INPUT restart resume => job %s %s -> %s (no replay of 1.0.0->1.1.0)", j.ID, j.FromVersion, j.ToVersion)
}

// TestPathRolloutFailureKeepsProgress 路径中段失败：
// 设备停留在已升到的中间版本（绝不回降）；修复后新发布从当前版本继续，
// 不从头再来。
func TestPathRolloutFailureKeepsProgress(t *testing.T) {
	e := newEnv(t, 10*time.Second, time.Minute)
	e.register("d1", "g1", "1.0.0", t0)
	r, err := e.o.StartRollout(pathPolicy("g1", "1.2.0", []string{"1.0.0", "1.1.0", "1.2.0"}, []int{1}, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	e.o.Advance(r.ID)
	e.o.Report(upgrade.Report{DeviceID: "d1", UpgradeID: r.ID, Seq: 1, Success: true, At: e.clk.Now()})
	e.o.Advance(r.ID) // 第二段 1.1.0 -> 1.2.0 在途
	// 第二段失败：设备保留在中间版本 1.1.0。
	res := e.o.Report(upgrade.Report{DeviceID: "d1", UpgradeID: r.ID, Seq: 2, Success: false, Phase: upgrade.PhaseFailed, Message: "flash verify error", At: e.clk.Now()})
	t.Logf("INPUT step2 (1.1.0->1.2.0) failed => %s | %s", res.Verdict, res.Reason)
	d, _ := e.reg.Get("d1")
	if d.CurrentVer.Compare(vv("1.1.0")) != 0 {
		t.Fatalf("failed step must not downgrade device: want 1.1.0, got %s", d.CurrentVer)
	}
	if d.HasDesired {
		t.Fatal("desired must be withdrawn on failure")
	}
	e.o.Advance(r.ID)
	if rr, _ := e.o.GetRollout(r.ID); rr.FinishedAt.IsZero() {
		t.Fatal("rollout must halt (fuse) after failed step")
	}

	// 修复后发起新发布：从 1.1.0 继续，而不是从 1.0.0 重来。
	r2, err := e.o.StartRollout(pathPolicy("g1", "1.2.0", []string{"1.0.0", "1.1.0", "1.2.0"}, []int{1}, 10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	logs, _ := e.o.Advance(r2.ID)
	for _, l := range logs {
		t.Logf("[advance-new-rollout] %s", l)
	}
	j, ok := e.mgr.ActiveJob("d1")
	if !ok {
		t.Fatal("new rollout must dispatch d1 from its current version")
	}
	if j.FromVersion.Compare(vv("1.1.0")) != 0 || j.ToVersion.Compare(vv("1.2.0")) != 0 {
		t.Fatalf("must continue from 1.1.0 (not restart at 1.0.0), got %s -> %s", j.FromVersion, j.ToVersion)
	}
	t.Logf("INPUT new rollout after mid-path failure => continues %s -> %s", j.FromVersion, j.ToVersion)
}
