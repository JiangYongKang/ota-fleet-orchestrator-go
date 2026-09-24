package upgrade

import (
	"testing"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

var t0 = time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)

func v(s string) version.Version {
	ver, err := version.Parse(s)
	if err != nil {
		panic(err)
	}
	return ver
}

func newTestManager() *Manager { return NewManager(10 * time.Second) }

func dispatchOne(t *testing.T, m *Manager, devID string) *Job {
	t.Helper()
	j := &Job{UpgradeID: "rollout-1", DeviceID: devID, Group: "g1",
		FromVersion: v("1.0.0"), ToVersion: v("1.0.1"), DispatchedAt: t0}
	if err := m.Dispatch(j); err != nil {
		t.Fatalf("dispatch %s: %v", devID, err)
	}
	return j
}

func logReport(t *testing.T, r Report, res ReportResult) {
	t.Helper()
	t.Logf("INPUT report device=%s upgrade=%s seq=%d success=%v phase=%s => VERDICT=%s jobPhase=%s | %q",
		r.DeviceID, r.UpgradeID, r.Seq, r.Success, r.Phase, res.Verdict, phaseOf(res), res.Reason)
}

func phaseOf(res ReportResult) Phase {
	if res.Job == nil {
		return "-"
	}
	return res.Job.Phase
}

func TestDispatchMutualExclusion(t *testing.T) {
	m := newTestManager()
	j1 := dispatchOne(t, m, "dev-1")
	t.Logf("INPUT dispatch dev-1 => job %s phase=%s deadline=%s", j1.ID, j1.Phase, j1.Deadline.Format(time.RFC3339))
	if !m.HasActiveJob("dev-1") {
		t.Fatal("device must have in-flight job after dispatch")
	}
	// 同一设备第二个在途升级必须被拒绝。
	err := m.Dispatch(&Job{UpgradeID: "rollout-2", DeviceID: "dev-1",
		FromVersion: v("1.0.1"), ToVersion: v("1.0.2"), DispatchedAt: t0})
	if err == nil {
		t.Fatal("second in-flight upgrade for the same device must be rejected")
	}
	t.Logf("INPUT second dispatch dev-1 => REJECTED: %v", err)

	// 另一台设备不受影响，照常下发（单台在途不阻塞他人）。
	j2 := dispatchOne(t, m, "dev-2")
	t.Logf("INPUT dispatch dev-2 while dev-1 in-flight => job %s accepted", j2.ID)
}

func TestProgressThenSuccessCountedOnce(t *testing.T) {
	m := newTestManager()
	dispatchOne(t, m, "dev-1")

	r1 := Report{DeviceID: "dev-1", UpgradeID: "rollout-1", Seq: 1, Phase: PhaseDownloading, Progress: 30, At: t0.Add(time.Second)}
	logReport(t, r1, m.HandleReport(r1))
	if res := m.HandleReport(r1); res.Verdict != ReportDuplicate {
		t.Fatalf("same seq re-send must be DUPLICATE, got %s", res.Verdict)
	}
	// 乱序旧进展 seq=0 拒绝。
	old := Report{DeviceID: "dev-1", UpgradeID: "rollout-1", Seq: 0, Phase: PhaseDispatched, At: t0.Add(2 * time.Second)}
	if res := m.HandleReport(old); res.Verdict != ReportStale {
		t.Fatalf("older seq must be STALE, got %s (%s)", res.Verdict, res.Reason)
	}
	r2 := Report{DeviceID: "dev-1", UpgradeID: "rollout-1", Seq: 2, Phase: PhaseInstalling, Progress: 80, At: t0.Add(3 * time.Second)}
	logReport(t, r2, m.HandleReport(r2))

	// 成功结果：计数一次。
	ok := Report{DeviceID: "dev-1", UpgradeID: "rollout-1", Seq: 3, Success: true, At: t0.Add(4 * time.Second)}
	resOK := m.HandleReport(ok)
	logReport(t, ok, resOK)
	if resOK.Verdict != ReportAccepted || resOK.Job.Phase != PhaseSucceeded {
		t.Fatalf("success report: %s %s", resOK.Verdict, phaseOf(resOK))
	}
	if m.HasActiveJob("dev-1") {
		t.Fatal("device must be free after terminal result")
	}
	// 重复成功（相同 seq 与无 seq 重投都不能重复计数）。
	for i, dup := range []Report{
		{DeviceID: "dev-1", UpgradeID: "rollout-1", Seq: 3, Success: true, At: t0.Add(5 * time.Second)},
		{DeviceID: "dev-1", UpgradeID: "rollout-1", Success: true, At: t0.Add(6 * time.Second)},
		{DeviceID: "dev-1", UpgradeID: "rollout-1", Seq: 99, Success: true, At: t0.Add(7 * time.Second)},
	} {
		res := m.HandleReport(dup)
		logReport(t, dup, res)
		if res.Verdict != ReportDuplicate && res.Verdict != ReportNoActiveJob {
			t.Fatalf("late/duplicate success #%d must not be counted: got %s", i, res.Verdict)
		}
	}
	st := m.Stats("rollout-1")
	if st.Succeeded != 1 || st.Total != 1 {
		t.Fatalf("success must be counted exactly once, stats=%+v", st)
	}
	t.Logf("INPUT 3 duplicate/late success reports => stats=%+v (succeeded must stay 1)", st)

	// 终态后新升级可以下发（上一段升级已释放）。
	j2 := &Job{UpgradeID: "rollout-9", DeviceID: "dev-1",
		FromVersion: v("1.0.1"), ToVersion: v("1.0.2"), DispatchedAt: t0.Add(8 * time.Second)}
	if err := m.Dispatch(j2); err != nil {
		t.Fatalf("dispatch after terminal job must succeed: %v", err)
	}
	t.Logf("INPUT new dispatch after terminal => job %s accepted", j2.ID)
}

func TestFailureAndWrongUpgrade(t *testing.T) {
	m := newTestManager()
	dispatchOne(t, m, "dev-1")
	wrong := Report{DeviceID: "dev-1", UpgradeID: "rollout-X", Seq: 1, Success: true, At: t0}
	if res := m.HandleReport(wrong); res.Verdict != ReportNoActiveJob {
		t.Fatalf("mismatched upgrade id must be NO_ACTIVE_JOB, got %s", res.Verdict)
	}
	fail := Report{DeviceID: "dev-1", UpgradeID: "rollout-1", Seq: 1, Phase: PhaseFailed, Message: "checksum mismatch", At: t0}
	res := m.HandleReport(fail)
	logReport(t, fail, res)
	if res.Job.Phase != PhaseFailed || res.Job.FailReason != "checksum mismatch" {
		t.Fatalf("want FAILED with reason, got %s/%q", res.Job.Phase, res.Job.FailReason)
	}
	late := Report{DeviceID: "dev-1", UpgradeID: "rollout-1", Seq: 2, Success: true, At: t0.Add(time.Second)}
	if res := m.HandleReport(late); res.Verdict != ReportNoActiveJob {
		t.Fatalf("late success after failure must not reopen job: %s", res.Verdict)
	}
}

func TestTimeoutConvergence(t *testing.T) {
	m := newTestManager() // stepTimeout = 10s
	j := dispatchOne(t, m, "dev-1")
	// 5s 有进展：刷新 deadline。
	m.HandleReport(Report{DeviceID: "dev-1", Seq: 1, Phase: PhaseDownloading, Progress: 40, At: t0.Add(5 * time.Second)})
	// +10s 即 t0+15s 之前不应超时。
	if timed := m.SweepTimeouts(t0.Add(14 * time.Second)); len(timed) != 0 {
		t.Fatalf("within refreshed deadline must not time out, got %d", len(timed))
	}
	t.Log("INPUT sweep at +14s (deadline was reset at +5s => +15s) => no timeout")
	// t0+16s：停留在 DOWNLOADING 超过刷新后的截止时间 -> TIMEOUT_FAILED。
	timed := m.SweepTimeouts(t0.Add(16 * time.Second))
	if len(timed) != 1 || timed[0].Phase != PhaseTimeout {
		t.Fatalf("want 1 TIMEOUT_FAILED, got %v", timed)
	}
	got, _ := m.Get(j.ID)
	t.Logf("INPUT sweep at +16s stuck=DOWNLOADING => phase=%s reason=%q", got.Phase, got.FailReason)
	if m.HasActiveJob("dev-1") {
		t.Fatal("timed out job must release the device slot")
	}
	// 迟到的成功上报不得翻案（结果唯一）。
	res := m.HandleReport(Report{DeviceID: "dev-1", UpgradeID: "rollout-1", Seq: 2, Success: true, At: t0.Add(17 * time.Second)})
	logReport(t, Report{DeviceID: "dev-1", UpgradeID: "rollout-1", Seq: 2, Success: true}, res)
	if res.Verdict == ReportAccepted {
		t.Fatal("late success after timeout must not be accepted")
	}
	// 回滚登记。
	if !m.MarkRolledBack(j.ID, t0.Add(18*time.Second)) {
		t.Fatal("MarkRolledBack should find the job")
	}
	got, _ = m.Get(j.ID)
	if got.Phase != PhaseRolledBack {
		t.Fatalf("want ROLLED_BACK got %s", got.Phase)
	}
	// Sweep 幂等：不会再次收敛同一任务。
	if again := m.SweepTimeouts(t0.Add(30 * time.Second)); len(again) != 0 {
		t.Fatalf("timeout sweep must be idempotent, got %v", again)
	}
}

func TestPartialSuccessLocatable(t *testing.T) {
	m := newTestManager()
	for _, id := range []string{"dev-1", "dev-2", "dev-3"} {
		dispatchOne(t, m, id)
	}
	// dev-1 成功，dev-2 失败，dev-3 仍在途。
	m.HandleReport(Report{DeviceID: "dev-1", UpgradeID: "rollout-1", Seq: 1, Success: true, At: t0})
	m.HandleReport(Report{DeviceID: "dev-2", UpgradeID: "rollout-1", Seq: 1, Phase: PhaseFailed, Message: "boom", At: t0})
	jobs := m.JobsByUpgrade("rollout-1")
	phases := map[string]Phase{}
	for _, j := range jobs {
		phases[j.DeviceID] = j.Phase
	}
	t.Logf("INPUT mixed results dev-1=ok dev-2=fail dev-3=pending => per-device phases=%v", phases)
	if phases["dev-1"] != PhaseSucceeded || phases["dev-2"] != PhaseFailed || phases["dev-3"] != PhaseDispatched {
		t.Fatalf("per-device phases wrong: %v", phases)
	}
	st := m.Stats("rollout-1")
	want := Stats{Total: 3, Succeeded: 1, Failed: 1, Active: 1}
	if st != want {
		t.Fatalf("stats=%+v want %+v", st, want)
	}
}

func TestSnapshotRestoreActiveJobs(t *testing.T) {
	m := newTestManager()
	j1 := dispatchOne(t, m, "dev-1")
	m.HandleReport(Report{DeviceID: "dev-1", Seq: 1, Phase: PhaseInstalling, Progress: 60, At: t0.Add(time.Second)})
	snap := m.Snapshot()

	m2 := NewManager(time.Minute)
	m2.Restore(snap)
	got, ok := m2.Get(j1.ID)
	if !ok || got.Phase != PhaseInstalling || got.ProgressPct != 60 {
		t.Fatalf("restore lost in-flight state: %+v ok=%v", got, ok)
	}
	if !m2.HasActiveJob("dev-1") {
		t.Fatal("in-flight job must remain active after restore")
	}
	if err := m2.Dispatch(&Job{UpgradeID: "x", DeviceID: "dev-1", DispatchedAt: t0}); err == nil {
		t.Fatal("restored in-flight job must still block duplicate dispatch after restart")
	}
	t.Logf("INPUT restore snapshot => job %s phase=%s active=true, duplicate dispatch rejected",
		j1.ID, got.Phase)
}
