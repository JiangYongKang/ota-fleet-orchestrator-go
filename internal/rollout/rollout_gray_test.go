package rollout

import (
	"context"
	"testing"
	"time"
)

func TestBatchedGrayRollout(t *testing.T) {
	env := newEnv(t, 2)
	ctx := context.Background()
	for _, id := range []string{"dev-1", "dev-2", "dev-3", "dev-4"} {
		env.register(id, "g1", "1.0.0")
		env.beat(id, "1.0.0", 1)
	}

	res, err := env.o.StartRollout("g1", mustVer("1.1.0"))
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range res.Items {
		t.Logf("输入设备=%s: accepted=%t 批次=%d 依据=%s", it.DeviceID, it.Accepted, it.BatchIndex, it.Reason)
	}

	// tick1：仅批次0（dev-1/dev-2）被派发，批次1 必须 pending
	env.clk.add(time.Second)
	if err := env.o.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	wantAfterTick1 := map[string]string{
		"dev-1": StateInProgress, "dev-2": StateInProgress,
		"dev-3": StatePending, "dev-4": StatePending,
	}
	for id, want := range wantAfterTick1 {
		t.Logf("tick1 后 %s state=%s (期望 %s; 依据: 仅当前开放批次可派发)", id, env.state(id), want)
		if env.state(id) != want {
			t.Fatalf("%s state=%s want %s", id, env.state(id), want)
		}
	}
	if env.sim.Count("dev-3", DirectionUpgrade) != 0 {
		t.Fatal("批次1 的任务在批次0完成前不应被派发")
	}

	// dev-1 成功；dev-2 失败上报 -> failed，回滚在下个 tick 完成
	env.o.ReportResult("dev-1", true, "")
	env.o.ReportResult("dev-2", false, "checksum mismatch")

	// tick2：dev-2 回滚至 rolledback；批次0 落定 -> 批次1 开放派发
	env.clk.add(time.Second)
	if err := env.o.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("tick2 后: dev-1=%s dev-2=%s dev-3=%s dev-4=%s (依据: failed 不阻塞批次推进)",
		env.state("dev-1"), env.state("dev-2"), env.state("dev-3"), env.state("dev-4"))
	if env.state("dev-2") != StateRolledBack {
		t.Fatalf("dev-2 失败后应回滚完成, got %s", env.state("dev-2"))
	}
	if env.state("dev-3") != StateInProgress || env.state("dev-4") != StateInProgress {
		t.Fatal("批次0落定后应开放并派发批次1")
	}

	// 部分成功可逐设备定位
	env.o.ReportResult("dev-3", true, "")
	env.o.ReportResult("dev-4", true, "")
	env.clk.add(time.Second)
	env.o.Tick(ctx)
	terminal := map[string]string{}
	for _, j := range env.o.Jobs() {
		terminal[j.DeviceID] = j.State
	}
	t.Logf("最终逐设备结果=%v (依据: 每台设备独立状态机)", terminal)
	if terminal["dev-1"] != StateSucceeded || terminal["dev-2"] != StateRolledBack ||
		terminal["dev-3"] != StateSucceeded || terminal["dev-4"] != StateSucceeded {
		t.Fatalf("部分成功定位错误: %v", terminal)
	}
}

func TestVersionRejectionIsPerDevice(t *testing.T) {
	env := newEnv(t, 4)
	// 目标版本 1.2.0：逐设备按"档案当前版本 -> 目标"判定，合法与否互不影响。
	env.register("ok-minor", "g", "1.1.0") // 1.1.0 -> 1.2.0 跨一个 minor：合法
	env.register("ok-patch", "g", "1.2.0") // 档案同版本，改用 1.2.1 目标验证 patch，见下
	env.register("jump", "g", "1.0.0")     // 1.0.0 -> 1.2.0 跨两个 minor：拒绝跳变
	env.register("newer", "g", "1.3.0")    // 1.3.0 -> 1.2.0 降级：拒绝
	for _, id := range []string{"ok-minor", "ok-patch", "jump", "newer"} {
		env.beat(id, "1.0.0", 1)
	}

	// 计划 A：目标 1.2.0
	res, err := env.o.StartRollout("g", mustVer("1.2.0"))
	if err != nil {
		t.Fatal(err)
	}
	accepted := map[string]bool{}
	for _, it := range res.Items {
		t.Logf("[目标1.2.0] 设备=%s accepted=%t 依据=%s", it.DeviceID, it.Accepted, it.Reason)
		accepted[it.DeviceID] = it.Accepted
	}
	if !accepted["ok-minor"] {
		t.Fatal("1.1.0->1.2.0 应允许")
	}
	if accepted["jump"] || accepted["newer"] {
		t.Fatal("跨版本跳变与降级必须被拒绝")
	}
	if accepted["ok-patch"] {
		t.Fatal("1.2.0->1.2.0 为原地升级，应拒绝")
	}

	// 计划 B：目标 1.2.1，验证同 minor patch 直发；同时验证 ok-minor 已有在途任务被跳过
	env.clk.add(time.Second)
	env.o.Tick(context.Background()) // ok-minor 进入 inprogress
	res2, err := env.o.StartRollout("g", mustVer("1.2.1"))
	if err != nil {
		t.Fatal(err)
	}
	acc2 := map[string]bool{}
	for _, it := range res2.Items {
		t.Logf("[目标1.2.1] 设备=%s accepted=%t 依据=%s", it.DeviceID, it.Accepted, it.Reason)
		acc2[it.DeviceID] = it.Accepted
	}
	if !acc2["ok-patch"] {
		t.Fatal("1.2.0->1.2.1 同 minor patch 前推应允许")
	}
	if acc2["ok-minor"] {
		t.Fatal("ok-minor 已有在途升级，新计划必须跳过它")
	}
}

func TestSingleDeviceBlockDoesNotStopOthers(t *testing.T) {
	env := newEnv(t, 1) // 每批一台，批次号即设备序号
	env.register("a", "g", "1.0.0")
	env.register("b", "g", "1.0.0")
	env.beat("a", "1.0.0", 1)
	env.beat("b", "1.0.0", 1)
	env.o.StartRollout("g", mustVer("1.1.0"))

	// a 设备故障：升级下发失败 -> failed；回滚也持续失败，停留在 failed
	env.sim.FailDevice("a")
	env.clk.add(time.Second)
	env.o.Tick(context.Background())
	t.Logf("a 故障后 state=%s (依据: dispatch 失败立即 failed, 回滚将重试)", env.state("a"))
	if env.state("a") != StateFailed {
		t.Fatalf("a 应为 failed, got %s", env.state("a"))
	}

	// 关键断言：a 卡在 failed（回滚未完成）不得阻塞 b 的下发
	env.clk.add(time.Second)
	env.o.Tick(context.Background())
	t.Logf("再次 tick: a=%s, b=%s (依据: 批次只被 pending/inprogress 阻塞, failed 瞬态不阻塞)",
		env.state("a"), env.state("b"))
	if env.state("b") != StateInProgress {
		t.Fatalf("a 阻塞不应影响 b, b got %s", env.state("b"))
	}

	// a 恢复后下一次 tick 回滚成功
	env.sim.RecoverDevice("a")
	env.clk.add(time.Second)
	env.o.Tick(context.Background())
	if env.state("a") != StateRolledBack {
		t.Fatalf("a 恢复后应 rolledback, got %s", env.state("a"))
	}
	if env.sim.Count("a", DirectionUpgrade) != 1 || env.sim.Count("a", DirectionRollback) < 1 {
		t.Fatalf("a 的升级只应下发1次(实际%d)，回滚可重试但有记录(实际%d)",
			env.sim.Count("a", DirectionUpgrade), env.sim.Count("a", DirectionRollback))
	}
	t.Logf("a: upgrade 调用=%d 次(必须1), rollback 调用=%d 次(故障期重试, 恢复后成功)",
		env.sim.Count("a", DirectionUpgrade), env.sim.Count("a", DirectionRollback))
}

func TestOneInFlightPerDevice(t *testing.T) {
	env := newEnv(t, 4)
	env.register("d1", "g", "1.0.0")
	env.beat("d1", "1.0.0", 1)
	r1, _ := env.o.StartRollout("g", mustVer("1.1.0"))
	t.Logf("首次计划: accepted=%v", r1.Items[0].Accepted)
	env.clk.add(time.Second)
	env.o.Tick(context.Background()) // d1 -> inprogress

	// d1 在途时再发起一次计划：必须跳过该设备
	r2, err := env.o.StartRollout("g", mustVer("1.1.0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("在途时再次计划 d1: accepted=%t 依据=%s", r2.Items[0].Accepted, r2.Items[0].Reason)
	if r2.Items[0].Accepted {
		t.Fatal("同一设备同一时刻只能有一个在途升级")
	}
}
