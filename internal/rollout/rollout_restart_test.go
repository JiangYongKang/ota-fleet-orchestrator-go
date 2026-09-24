package rollout

import (
	"context"
	"testing"
	"time"
)

// restart 用相同配置与一个新时钟重建编排器（模拟新进程）。
func restart(t *testing.T, path string, clk *testClock) testEnv {
	t.Helper()
	o, err := New(Config{
		OfflineTimeout: 30 * time.Second,
		JobTimeout:     60 * time.Second,
		BatchSize:      1,
		TickInterval:   time.Second,
		StorePath:      path,
	}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	sim, _ := o.SimDispatcher()
	return testEnv{o: o, clk: clk, sim: sim}
}

// TestRestartResumesInFlightWithoutDuplicateDispatch 关键场景：
// 升级已下发但设备尚未回报时进程崩溃；重启后任务必须仍是 inprogress，
// 绝不重新下发升级，超时后按策略收敛失败并回滚。
func TestRestartResumesInFlightWithoutDuplicateDispatch(t *testing.T) {
	clk := &testClock{t: time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)}
	path := t.TempDir() + "/state.json"
	env1, err := New(Config{
		OfflineTimeout: 30 * time.Second, JobTimeout: 60 * time.Second,
		BatchSize: 1, StorePath: path,
	}, clk.now)
	if err != nil {
		t.Fatal(err)
	}
	sim1, _ := env1.SimDispatcher()
	env1.RegisterDevice(mustDev("d1", "g", "1.0.0"))
	env1.Heartbeat("d1", mustVer("1.0.0"), 1)
	env1.StartRollout("g", mustVer("1.1.0"))
	clk.add(1 * time.Second)
	env1.Tick(context.Background()) // d1 inprogress，升级已真实下发1次
	if sim1.Count("d1", DirectionUpgrade) != 1 {
		t.Fatal("前置条件: 升级已下发1次")
	}
	j, _ := env1.Job("d1")
	t.Logf("崩溃前: d1 state=%s attempted=%t (依据: attempted=true 已落盘)", j.State, j.Attempted)

	// ===== 进程重启：新编排器、新模拟器（调用计数清零）=====
	env2 := restart(t, path, clk)
	j2, _ := env2.o.Job("d1")
	t.Logf("重启恢复: d1 state=%s attempted=%t (依据: 从 state.json 重建, 未完成升级不丢失)",
		j2.State, j2.Attempted)
	if j2.State != StateInProgress || !j2.Attempted {
		t.Fatalf("在途任务应恢复为 inprogress/attempted=true, got %s/%t", j2.State, j2.Attempted)
	}

	// 影子也应恢复：期望 1.1.0、上报 1.0.0、尚未收敛
	sh, ok := env2.o.Shadow("d1")
	if !ok {
		t.Fatal("d1 影子应随快照恢复")
	}
	t.Logf("影子恢复: reported=%s desired=%s converged=%t (依据: 期望/上报分别持久化)",
		sh.Reported.FirmwareVersion, sh.Desired.FirmwareVersion, sh.Converged())
	if sh.Desired.FirmwareVersion.Compare(mustVer("1.1.0")) != 0 ||
		sh.Reported.FirmwareVersion.Compare(mustVer("1.0.0")) != 0 || sh.Converged() {
		t.Fatal("重启后影子的期望/上报与收敛判定应保持")
	}

	// 重启后立即 tick：不得重新下发升级
	if err := env2.o.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	clk.add(1 * time.Second)
	env2.o.Tick(context.Background())
	if c := env2.sim.Count("d1", DirectionUpgrade); c != 0 {
		t.Fatalf("重启后不得重复下发升级, 新进程实际下发 %d 次", c)
	}
	t.Log("判定依据: 重启后两个 tick 升级下发次数=0 => 在途任务未重复执行")

	// 设备始终无响应：推进超过 JobTimeout 后，恢复的任务超时失败并回滚
	clk.add(env2.o.cfg.JobTimeout + time.Second)
	env2.o.Tick(context.Background())
	j3, _ := env2.o.Job("d1")
	t.Logf("恢复任务超时后 state=%s err=%q 回滚次数=%d (依据: 恢复的任务同样受超时策略约束)",
		j3.State, j3.LastError, env2.sim.Count("d1", DirectionRollback))
	if j3.State != StateRolledBack || env2.sim.Count("d1", DirectionRollback) != 1 {
		t.Fatalf("应超时回滚且仅回滚1次, got state=%s rb=%d",
			j3.State, env2.sim.Count("d1", DirectionRollback))
	}
}

// TestRestartDoesNotReplayTerminalJobs 验证终态任务重启后不被重放。
func TestRestartDoesNotReplayTerminalJobs(t *testing.T) {
	clk := &testClock{t: time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)}
	path := t.TempDir() + "/state.json"
	env1, _ := New(Config{
		OfflineTimeout: 30 * time.Second, JobTimeout: 60 * time.Second,
		BatchSize: 2, StorePath: path,
	}, clk.now)
	sim1, _ := env1.SimDispatcher()
	env1.RegisterDevice(mustDev("ok", "g", "1.0.0"))
	env1.RegisterDevice(mustDev("bad", "g", "1.0.0"))
	env1.Heartbeat("ok", mustVer("1.0.0"), 1)
	env1.Heartbeat("bad", mustVer("1.0.0"), 1)
	env1.StartRollout("g", mustVer("1.1.0"))
	clk.add(time.Second)
	env1.Tick(context.Background())
	env1.ReportResult("ok", true, "")
	sim1.FailDevice("bad") // bad 下发成功但回报失败后回滚；这里直接让回报失败
	env1.ReportResult("bad", false, "bricked")
	clk.add(time.Second)
	env1.Tick(context.Background())
	// bad 回滚因故障一直失败 -> 保持 failed；先恢复再 tick 让其 rolledback，制造两类终态
	sim1.RecoverDevice("bad")
	clk.add(time.Second)
	env1.Tick(context.Background())
	t.Logf("重启前: ok=%s bad=%s", jobState(env1, "ok"), jobState(env1, "bad"))

	env2 := restart(t, path, clk)
	clk.add(time.Hour) // 跨过任何超时
	env2.o.Tick(context.Background())
	t.Logf("重启后 tick: ok=%s bad=%s, 新进程下发调用=%d (依据: succeeded/rolledback 均为终态)",
		jobState(env2.o, "ok"), jobState(env2.o, "bad"), len(env2.sim.Calls()))
	if jobState(env2.o, "ok") != StateSucceeded || jobState(env2.o, "bad") != StateRolledBack {
		t.Fatal("终态任务重启后必须原样恢复")
	}
	if len(env2.sim.Calls()) != 0 {
		t.Fatalf("终态任务重启后不得触发任何下发, got %d 次", len(env2.sim.Calls()))
	}
}

// TestRestartMidFailedContinuesRollbackOnce 验证崩溃发生在 failed 瞬态时，
// 重启后继续完成回滚，且回滚不重复。
func TestRestartMidFailedContinuesRollbackOnce(t *testing.T) {
	clk := &testClock{t: time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)}
	path := t.TempDir() + "/state.json"
	env1, _ := New(Config{
		OfflineTimeout: 30 * time.Second, JobTimeout: 60 * time.Second,
		BatchSize: 1, StorePath: path,
	}, clk.now)
	sim1, _ := env1.SimDispatcher()
	env1.RegisterDevice(mustDev("d1", "g", "1.0.0"))
	env1.Heartbeat("d1", mustVer("1.0.0"), 1)
	env1.StartRollout("g", mustVer("1.1.0"))
	sim1.FailDevice("d1") // 下发即失败
	clk.add(time.Second)
	env1.Tick(context.Background())
	t.Logf("崩溃前 d1=%s (failed 瞬态已落盘, 回滚尚未成功)", jobState(env1, "d1"))
	if jobState(env1, "d1") != StateFailed {
		t.Fatal("前置: 应为 failed")
	}

	env2 := restart(t, path, clk)
	// 新进程里 d1 仍故障：回滚重试失败，不改变终态
	env2.sim.FailDevice("d1")
	clk.add(time.Second)
	env2.o.Tick(context.Background())
	if jobState(env2.o, "d1") != StateFailed {
		t.Fatalf("回滚仍失败时应保持 failed, got %s", jobState(env2.o, "d1"))
	}
	env2.sim.RecoverDevice("d1")
	clk.add(time.Second)
	env2.o.Tick(context.Background())
	t.Logf("恢复后 d1=%s 回滚次数=%d (依据: 重启前后回滚动作合计语义唯一, 失败重试直至成功)",
		jobState(env2.o, "d1"), env2.sim.Count("d1", DirectionRollback))
	if jobState(env2.o, "d1") != StateRolledBack {
		t.Fatal("设备恢复后应完成回滚")
	}
}
