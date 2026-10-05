// Command demo 在本地用模拟数据演示设备编队固件升级编排：
// 设备注册/心跳、乱序与重复、分批灰度、部分成功、超时回滚与重启恢复。
// 不依赖任何外部服务：状态写入临时目录下的 JSON 文件。
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/clock"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/device"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/orchestrator"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/store"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/upgrade"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

const (
	hbTimeout   = 30 * time.Second
	stepTimeout = 10 * time.Second
)

func main() {
	start := time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)
	clk := clock.NewFakeClock(start)
	dir, err := os.MkdirTemp("", "ota-demo-")
	must(err)
	defer os.RemoveAll(dir)
	st, err := store.NewJSONStore(filepath.Join(dir, "state.json"))
	must(err)

	reg := device.NewRegistry(hbTimeout)
	mgr := upgrade.NewManager(stepTimeout)
	rb := func(j *upgrade.Job, at time.Time) error {
		fmt.Printf("    [rollback] device=%s 回滚 %s -> 保持 %s @ %s\n",
			j.DeviceID, j.ToVersion, j.FromVersion, at.Format(time.RFC3339))
		return nil
	}
	o := orchestrator.New(reg, mgr, clk, st, rb)

	fmt.Println("== 1) 注册 6 台设备（组 g1，当前版本 1.2.0）==")
	ids := []string{"dev-A", "dev-B", "dev-C", "dev-D", "dev-E", "dev-F"}
	for _, id := range ids {
		_, created, err := reg.Register(device.RegisterRequest{
			DeviceID: id, Group: "g1", Version: version.Version{Major: 1, Minor: 2, Patch: 0},
		}, clk.Now())
		must(err)
		fmt.Printf("    注册 %s created=%v\n", id, created)
	}
	// 重复注册幂等。
	_, created, _ := reg.Register(device.RegisterRequest{DeviceID: "dev-A", Group: "g1"}, clk.Now())
	fmt.Printf("    重复注册 dev-A created=%v（幂等，状态不变）\n", created)

	fmt.Println("== 2) 心跳：正常 / 乱序旧心跳 / 重复心跳 ==")
	heartbeat := func(id string, seq int64, has bool, ver version.Version) {
		res := reg.Heartbeat(device.Heartbeat{DeviceID: id, Seq: seq, Reported: ver, HasVer: has, At: clk.Now()})
		fmt.Printf("    心跳 %s seq=%-3d => %-16s %s\n", id, seq, res.Verdict, res.Reason)
	}
	heartbeat("dev-A", 10, false, version.Version{})
	heartbeat("dev-A", 9, true, version.Version{Major: 1, Minor: 1, Patch: 0})  // 乱序旧
	heartbeat("dev-A", 10, false, version.Version{})                            // 重复
	heartbeat("dev-A", 11, true, version.Version{Major: 1, Minor: 2, Patch: 0}) // 新

	fmt.Println("== 3) 创建灰度发布 1.2.0 -> 1.3.0，批次 [2,2,2]，规则=相邻 minor ==")
	pol := orchestrator.BatchPolicy{
		Group:       "g1",
		ToVersion:   version.Version{Major: 1, Minor: 3, Patch: 0},
		Rule:        version.NextMinorAllowed,
		BatchSizes:  []int{2, 2, 2},
		StepTimeout: stepTimeout,
	}
	rollout, err := o.StartRollout(pol)
	must(err)
	fmt.Printf("    rollout=%s\n", rollout.ID)
	for _, b := range rollout.Batches {
		fmt.Printf("    batch %d: %v\n", b.Index, b.DeviceIDs)
	}

	fmt.Println("== 4) 推进第 1 批（dev-A/dev-B 下发）==")
	lgs, err := o.Advance(rollout.ID)
	printLogs(lgs, err)

	fmt.Println("== 5) dev-A 成功；dev-B 先报进展后卡住 ==")
	resA := o.Report(upgrade.Report{DeviceID: "dev-A", UpgradeID: rollout.ID, Seq: 1, Phase: upgrade.PhaseDownloading, Progress: 50, At: clk.Now()})
	fmt.Printf("    dev-A 进展: %s\n", resA.Reason)
	resA = o.Report(upgrade.Report{DeviceID: "dev-A", UpgradeID: rollout.ID, Seq: 2, Success: true, At: clk.Now()})
	fmt.Printf("    dev-A 结果: %s -> %s\n", resA.Verdict, resA.Job.Phase)
	resA2 := o.Report(upgrade.Report{DeviceID: "dev-A", UpgradeID: rollout.ID, Seq: 2, Success: true, At: clk.Now()})
	fmt.Printf("    dev-A 重复结果: %s（不重复计数）\n", resA2.Verdict)

	o.Report(upgrade.Report{DeviceID: "dev-B", UpgradeID: rollout.ID, Seq: 1, Phase: upgrade.PhaseDownloading, Progress: 10, At: clk.Now()})
	fmt.Println("    dev-B 停留在 DOWNLOADING，时间向前推进 12s 超过单步超时…")
	clk.Advance(12 * time.Second)
	fmt.Println("== 6) Tick：离线扫描 + dev-B 超时收敛 + 回滚 ==")
	printLogs(o.Tick(), nil)

	fmt.Println("== 7) 再推进：批次失败，后续批次被熔断，不下发 dev-C..F ==")
	lgs, err = o.Advance(rollout.ID)
	printLogs(lgs, err)
	printStats(mgr, rollout.ID)

	fmt.Println("== 8) 模拟进程重启：从 JSON 恢复，在途/终态任务不丢不重 ==")
	reg2 := device.NewRegistry(hbTimeout)
	mgr2 := upgrade.NewManager(stepTimeout)
	o2 := orchestrator.New(reg2, mgr2, clk, st, rb)
	must(o2.Restore())
	rr, ok := o2.GetRollout(rollout.ID)
	fmt.Printf("    恢复 rollout=%s ok=%v finished=%v batches=%d\n",
		rollout.ID, ok, !rr.FinishedAt.IsZero(), len(rr.Batches))
	lgs2, err2 := o2.Advance(rollout.ID)
	printLogs(lgs2, err2)
	printStats(mgr2, rollout.ID)
	if d, ok := reg2.Get("dev-A"); ok {
		fmt.Printf("    恢复后 dev-A 当前版本=%s 期望存在=%v（成功后 desired 已清除）\n", d.CurrentVer, d.HasDesired)
	}
	if d, ok := reg2.Get("dev-B"); ok {
		fmt.Printf("    恢复后 dev-B 当前版本=%s 期望存在=%v（失败后 desired 已撤回）\n", d.CurrentVer, d.HasDesired)
	}
	fmt.Printf("    持久化文件：%s\n", filepath.Join(dir, "state.json"))

	// ---- 新能力演示：整体并发上限 / 分组暂停恢复 / 路径逐跳 ----
	demoConcurrencyAndPause(clk)
	demoUpgradePath(clk)
	fmt.Println("演示完成。")
}

// demoConcurrencyAndPause 演示同一条任务整体并发上限与分组暂停/恢复。
func demoConcurrencyAndPause(clk *clock.FakeClock) {
	fmt.Println("== 9) 新能力：整体并发上限 cap=2 + 暂停/恢复（独立存储，互不影响）==")
	dir, err := os.MkdirTemp("", "ota-cap-")
	must(err)
	defer os.RemoveAll(dir)
	st, err := store.NewJSONStore(filepath.Join(dir, "state.json"))
	must(err)
	reg := device.NewRegistry(hbTimeout)
	mgr := upgrade.NewManager(stepTimeout)
	rb := func(j *upgrade.Job, at time.Time) error { return nil }
	o := orchestrator.New(reg, mgr, clk, st, rb)
	for _, id := range []string{"c1", "c2", "c3", "c4"} {
		_, _, err := reg.Register(device.RegisterRequest{DeviceID: id, Group: "g-cap", Version: version.Version{Major: 2, Minor: 0, Patch: 0}}, clk.Now())
		must(err)
	}
	pol := orchestrator.BatchPolicy{
		Group: "g-cap", ToVersion: version.Version{Major: 2, Minor: 1, Patch: 0},
		Rule: version.NextMinorAllowed, BatchSizes: []int{4},
		StepTimeout: stepTimeout, MaxConcurrent: 2,
	}
	rollout, err := o.StartRollout(pol)
	must(err)
	fmt.Println("    -- 首拍：只有 c1/c2 在途，c3/c4 排队 --")
	lgs2, err2 := o.Advance(rollout.ID)
	printLogs(lgs2, err2)

	fmt.Println("    -- 暂停 g-cap：c1/c2 在途自然收尾，空槽不补位 --")
	_, pls := o.PauseGroup("g-cap")
	for _, l := range pls {
		fmt.Println("   ", l)
	}
	for _, id := range []string{"c1", "c2"} {
		res := o.Report(upgrade.Report{DeviceID: id, UpgradeID: rollout.ID, Seq: 1, Success: true, At: clk.Now()})
		fmt.Printf("    %s 成功（暂停期间）=> %s\n", id, res.Job.Phase)
	}
	lgs2, err2 = o.Advance(rollout.ID)
	printLogs(lgs2, err2)

	fmt.Println("    -- 恢复：c1/c2 不重派，c3/c4 立即补位 --")
	_, rls := o.ResumeGroup("g-cap")
	for _, l := range rls {
		fmt.Println("   ", l)
	}
	for _, id := range []string{"c3", "c4"} {
		o.Report(upgrade.Report{DeviceID: id, UpgradeID: rollout.ID, Seq: 1, Success: true, At: clk.Now()})
	}
	printStats(mgr, rollout.ID)
}

// demoUpgradePath 演示跨版本设备按既定路径逐跳升级。
func demoUpgradePath(clk *clock.FakeClock) {
	fmt.Println("== 10) 新能力：升级路径 1.0.0 -> 1.1.0 -> 1.2.0（落后两跳自动拆步）==")
	dir, err := os.MkdirTemp("", "ota-path-")
	must(err)
	defer os.RemoveAll(dir)
	st, err := store.NewJSONStore(filepath.Join(dir, "state.json"))
	must(err)
	reg := device.NewRegistry(hbTimeout)
	mgr := upgrade.NewManager(stepTimeout)
	o := orchestrator.New(reg, mgr, clk, st, func(j *upgrade.Job, at time.Time) error { return nil })
	_, _, err = reg.Register(device.RegisterRequest{DeviceID: "p1", Group: "g-path", Version: version.Version{Major: 1}}, clk.Now())
	must(err)

	path, err := version.ParsePath([]string{"1.0.0", "1.1.0", "1.2.0"})
	must(err)
	rollout, err := o.StartRollout(orchestrator.BatchPolicy{
		Group: "g-path", ToVersion: version.Version{Major: 1, Minor: 2, Patch: 0},
		Path: path, BatchSizes: []int{1}, StepTimeout: stepTimeout,
	})
	must(err)
	for hop := 0; hop < 2; hop++ {
		lgs2, err2 := o.Advance(rollout.ID)
		printLogs(lgs2, err2)
		res := o.Report(upgrade.Report{DeviceID: "p1", UpgradeID: rollout.ID, Seq: int64(hop + 1), Success: true, At: clk.Now()})
		d, _ := reg.Get("p1")
		fmt.Printf("    第 %d 跳完成 => job 终态=%s 设备当前=%s\n", hop+1, res.Job.Phase, d.CurrentVer)
	}
	printStats(mgr, rollout.ID)

	// 路径之外的版本组合必须被明确拒绝。
	if _, err := o.StartRollout(orchestrator.BatchPolicy{
		Group: "g-path", ToVersion: version.Version{Major: 1, Minor: 1, Patch: 0},
		Path: path, BatchSizes: []int{1}, StepTimeout: stepTimeout,
	}); err != nil {
		fmt.Printf("    目标非路径终点被拒绝：%v\n", err)
	}
}

func printLogs(logs []string, err error) {
	must(err)
	for _, l := range logs {
		fmt.Println("   ", l)
	}
}

func printStats(mgr *upgrade.Manager, id string) {
	s := mgr.Stats(id)
	fmt.Printf("    统计 total=%d succeeded=%d failed=%d rolledBack=%d active=%d\n",
		s.Total, s.Succeeded, s.Failed, s.RolledBack, s.Active)
	for _, j := range mgr.JobsByUpgrade(id) {
		fmt.Printf("      - %-6s %-12s reason=%q\n", j.DeviceID, j.Phase, j.FailReason)
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
