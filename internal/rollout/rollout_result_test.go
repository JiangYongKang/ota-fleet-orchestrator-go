package rollout

import (
	"context"
	"testing"
	"time"
)

// TestDuplicateAndLateReportsNotCounted 验证重复/迟到上报不重复计数。
func TestDuplicateAndLateReportsNotCounted(t *testing.T) {
	env := newEnv(t, 1)
	env.register("d1", "g", "1.0.0")
	env.beat("d1", "1.0.0", 1)
	env.o.StartRollout("g", mustVer("1.1.0"))
	env.clk.add(time.Second)
	env.o.Tick(context.Background()) // d1 -> inprogress

	// 首次成功上报被接受
	out1, _ := env.o.ReportResult("d1", true, "")
	t.Logf("第1次上报 success=true: accepted=%t state=%s 依据=%s", out1.Accepted, out1.State, out1.Reason)
	if !out1.Accepted || env.state("d1") != StateSucceeded {
		t.Fatal("首次成功上报应被接受并置 succeeded")
	}

	// 重复成功上报：忽略
	out2, _ := env.o.ReportResult("d1", true, "")
	// 迟到的相反结果（设备晚到的失败）：必须忽略，不得把成功改成失败/回滚
	out3, _ := env.o.ReportResult("d1", false, "late failure after reboot")
	t.Logf("重复上报 accepted=%t; 迟到失败上报 accepted=%t state 保持=%s (依据: 终态任务结果不再计数)",
		out2.Accepted, out3.Accepted, env.state("d1"))
	if out2.Accepted || out3.Accepted || env.state("d1") != StateSucceeded {
		t.Fatal("重复与迟到上报必须幂等忽略")
	}
	// 终态后不得触发任何回滚下发
	if n := env.sim.Count("d1", DirectionRollback); n != 0 {
		t.Fatalf("成功任务不应有回滚调用, got %d", n)
	}

	// 未知设备上报
	_, err := env.o.ReportResult("ghost", true, "")
	t.Logf("未知设备上报 err=%v (依据: 无任务可匹配)", err)
	if err == nil {
		t.Fatal("未知设备应返回错误")
	}
}

// TestPendingTimeoutBeforeBatchOpens 验证任务在批次开放前等待过久：
// 批次开放的同一 tick 先派发（截止时间重置），下一 tick 仍无结果即超时收敛。
// 这里用 batchSize=1 + 两台设备制造"第二台任务在 pending 等待"的情形。
func TestPendingTimeoutBeforeBatchOpens(t *testing.T) {
	env := newEnv(t, 1)
	env.register("a", "g", "1.0.0")
	env.register("b", "g", "1.0.0")
	env.beat("a", "1.0.0", 1)
	env.beat("b", "1.0.0", 1)
	env.o.StartRollout("g", mustVer("1.1.0"))

	// 批次0(a) 派发；b 在批次1 保持 pending。让 a 成功以开放批次1。
	env.clk.add(time.Second)
	env.o.Tick(context.Background())
	env.o.ReportResult("a", true, "")

	// 时间直接跨过 JobTimeout 后再 tick：b 会在本 tick 先派发(inprogress)，
	// 由于派发重置了截止时间，b 此时不会超时；再跨过 JobTimeout 才超时回滚。
	env.clk.add(env.o.cfg.JobTimeout + time.Second)
	env.o.Tick(context.Background())
	t.Logf("跨超时后首个 tick: a=%s b=%s (依据: 派发会重置任务截止时间)",
		env.state("a"), env.state("b"))
	if env.state("b") != StateInProgress {
		t.Fatalf("b 在派发当 tick 不应立即超时, got %s", env.state("b"))
	}
	env.clk.add(env.o.cfg.JobTimeout + time.Second)
	env.o.Tick(context.Background())
	j, _ := env.o.Job("b")
	t.Logf("再跨 %v 后 b state=%s err=%q (依据: inprogress 停留过久收敛失败并回滚)",
		env.o.cfg.JobTimeout, j.State, j.LastError)
	if j.State != StateRolledBack {
		t.Fatalf("b 应超时回滚, got %s", j.State)
	}
}

// TestInProgressTimeoutRollback 验证已下发(inprogress)任务超时路径。
func TestInProgressTimeoutRollback(t *testing.T) {
	env := newEnv(t, 1)
	env.register("d1", "g", "1.0.0")
	env.beat("d1", "1.0.0", 1)
	env.o.StartRollout("g", mustVer("1.1.0"))
	env.clk.add(time.Second)
	env.o.Tick(context.Background()) // inprogress
	if env.state("d1") != StateInProgress {
		t.Fatalf("期望 inprogress, got %s", env.state("d1"))
	}
	env.clk.add(env.o.cfg.JobTimeout + time.Second)
	env.o.Tick(context.Background())
	j, _ := env.o.Job("d1")
	t.Logf("inprogress 超时后 state=%s lastError=%q (依据: 停留中间状态超 JobTimeout 即失败回滚)",
		j.State, j.LastError)
	if j.State != StateRolledBack || j.LastError == "" {
		t.Fatalf("应 rolledback 且记录超时原因, got %s %q", j.State, j.LastError)
	}
	if env.sim.Count("d1", DirectionUpgrade) != 1 || env.sim.Count("d1", DirectionRollback) != 1 {
		t.Fatalf("升级/回滚应各1次, got up=%d rb=%d",
			env.sim.Count("d1", DirectionUpgrade), env.sim.Count("d1", DirectionRollback))
	}
}

// TestFailureReportTriggersExactlyOneRollback 验证设备主动报失败时回滚一次。
func TestFailureReportTriggersExactlyOneRollback(t *testing.T) {
	env := newEnv(t, 1)
	env.register("d1", "g", "1.0.0")
	env.beat("d1", "1.0.0", 1)
	env.o.StartRollout("g", mustVer("1.1.0"))
	env.clk.add(time.Second)
	env.o.Tick(context.Background())
	env.o.ReportResult("d1", false, "power lost")
	if env.state("d1") != StateFailed {
		t.Fatalf("失败上报后应为 failed(待回滚), got %s", env.state("d1"))
	}
	// 再重复失败上报：不能重复计数
	out, _ := env.o.ReportResult("d1", false, "power lost again")
	if out.Accepted {
		t.Fatal("对 failed 任务的重复/迟到上报必须忽略")
	}
	env.clk.add(time.Second)
	env.o.Tick(context.Background())
	t.Logf("回滚后 state=%s, rollback 次数=%d", env.state("d1"), env.sim.Count("d1", DirectionRollback))
	if env.state("d1") != StateRolledBack || env.sim.Count("d1", DirectionRollback) != 1 {
		t.Fatal("失败任务应回滚且仅回滚一次")
	}
}
