// Command demo 用本地数据模拟设备端与云端的完整升级编排流程：
// 注册 -> 乱序心跳 -> 分组灰度 -> 重复/迟到上报 -> 超时回滚 -> 进程重启恢复。
//
// 运行：go run ./cmd/demo
// 所有状态保存在 ./demo-data/state.json，无任何外部服务依赖。
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/device"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/rollout"
	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time                    { return c.t }
func (c *clock) advance(d time.Duration) time.Time { c.t = c.t.Add(d); return c.t }

func mustVersion(s string) version.Version {
	v, err := version.Parse(s)
	if err != nil {
		panic(err)
	}
	return v
}

func printJobs(o *rollout.Orchestrator, title string) {
	fmt.Println("--- " + title + " ---")
	for _, j := range o.Jobs() {
		fmt.Printf("  device=%s %s->%s batch=%d state=%s attempted=%t err=%q\n",
			j.DeviceID, j.FromVersion, j.ToVersion, j.BatchIndex, j.State, j.Attempted, j.LastError)
	}
}

func main() {
	const statePath = "demo-data/state.json"
	_ = os.RemoveAll("demo-data") // 演示每次从干净状态开始

	clk := &clock{t: time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)}
	cfg := rollout.Config{
		OfflineTimeout: 30 * time.Second,
		JobTimeout:     60 * time.Second,
		BatchSize:      2, // 4 台设备 -> 批次0(dev-1,dev-2)，批次1(dev-3,dev-4)
		TickInterval:   10 * time.Second,
		StorePath:      statePath,
	}

	// ===== 第一段：启动编排器，注册 4 台设备 =====
	o, err := rollout.New(cfg, clk.now)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	devices := []device.Device{
		{ID: "dev-1", Group: "g1", CurrentVersion: mustVersion("1.0.0")},
		{ID: "dev-2", Group: "g1", CurrentVersion: mustVersion("1.0.5")},
		{ID: "dev-3", Group: "g1", CurrentVersion: mustVersion("1.0.9")},
		{ID: "dev-4", Group: "g1", CurrentVersion: mustVersion("1.0.0")}, // 将模拟设备端故障
	}
	for _, d := range devices {
		if err := o.RegisterDevice(d); err != nil {
			panic(err)
		}
	}
	if err := o.RegisterDevice(devices[0]); err != nil { // 重复注册幂等
		panic(err)
	}

	// ===== 心跳：乱序旧心跳不得回退，重复心跳幂等 =====
	o.Heartbeat("dev-1", mustVersion("1.0.0"), 10)
	o.Heartbeat("dev-1", mustVersion("1.0.1"), 12)
	o.Heartbeat("dev-1", mustVersion("1.0.0"), 11) // 乱序旧心跳：拒绝
	o.Heartbeat("dev-1", mustVersion("1.0.1"), 12) // 重复心跳：忽略
	for _, id := range []string{"dev-2", "dev-3", "dev-4"} {
		o.Heartbeat(id, mustVersion("1.0.0"), 1)
	}
	if sh, ok := o.Shadow("dev-1"); ok {
		fmt.Printf("[check] dev-1 reported fw=%s seq=%d after stale heartbeat (expect 1.0.1/12, no rollback)\n",
			sh.Reported.FirmwareVersion, sh.Reported.Seq)
	}

	// ===== 对 g1 灰度升级到 1.1.0（同 major 跨一个 minor，合法）=====
	res, err := o.StartRollout("g1", mustVersion("1.1.0"))
	if err != nil {
		panic(err)
	}
	for _, it := range res.Items {
		fmt.Printf("[plan] dev=%s accepted=%t batch=%d reason=%s\n", it.DeviceID, it.Accepted, it.BatchIndex, it.Reason)
	}

	// dev-4 设备端持续故障（升级下发即失败 -> failed -> 等待回滚）
	sim, _ := o.SimDispatcher()
	sim.FailDevice("dev-4")

	// tick 1：派发首批 dev-1/dev-2
	clk.advance(time.Second)
	if err := o.Tick(ctx); err != nil {
		panic(err)
	}
	printJobs(o, "after tick 1 (batch 0 dispatched)")

	// dev-1/dev-2 上报成功；随后的迟到相反结果与重复上报必须被忽略
	o.ReportResult("dev-1", true, "")
	o.ReportResult("dev-2", true, "")
	r2, _ := o.ReportResult("dev-2", false, "late failure")
	fmt.Printf("[check] dev-2 late report accepted=%t reason=%s (expect false)\n", r2.Accepted, r2.Reason)
	r3, _ := o.ReportResult("dev-2", true, "")
	fmt.Printf("[check] dev-2 duplicate report accepted=%t reason=%s (expect false)\n", r3.Accepted, r3.Reason)

	// tick 2：首批全部终态（dev-1/2 succeeded），开放批次1：dev-3 下发，dev-4 下发失败
	clk.advance(time.Second)
	if err := o.Tick(ctx); err != nil {
		panic(err)
	}
	printJobs(o, "after tick 2 (batch 1 opens; dev-4 dispatch fails)")

	// dev-4 设备恢复，下一次 tick 回滚重试成功
	sim.RecoverDevice("dev-4")
	clk.advance(time.Second)
	if err := o.Tick(ctx); err != nil {
		panic(err)
	}
	printJobs(o, "after tick 3 (dev-4 rollback succeeded)")

	// dev-3 长时间停留在 inprogress：超时收敛为 failed 并回滚
	clk.advance(cfg.JobTimeout + time.Second)
	if err := o.Tick(ctx); err != nil {
		panic(err)
	}
	printJobs(o, "after timeout tick (dev-3 timed out -> failed -> rolledback)")

	// 设备重新心跳上报真实版本，展示期望/上报收敛判定
	o.Heartbeat("dev-1", mustVersion("1.1.0"), 13)
	o.Heartbeat("dev-3", mustVersion("1.0.9"), 2)
	for _, id := range []string{"dev-1", "dev-3"} {
		sh, _ := o.Shadow(id)
		fmt.Printf("[convergence] %s online=%s reported=%s desired=%s converged=%t reason=%s\n",
			id, sh.Online, sh.Reported.FirmwareVersion, sh.Desired.FirmwareVersion, sh.Converged(), sh.LastReason)
	}

	// 离线判定：dev-2 心跳超时
	clk.advance(cfg.OfflineTimeout + time.Second)
	o.Tick(ctx)
	sh2, _ := o.Shadow("dev-2")
	fmt.Printf("[offline] dev-2 status=%s reason=%s\n", sh2.Online, sh2.LastReason)
	// 恢复上线：原因与首次上线可区分
	o.Heartbeat("dev-2", mustVersion("1.1.0"), 2)
	sh2, _ = o.Shadow("dev-2")
	fmt.Printf("[recover] dev-2 status=%s reason=%s\n", sh2.Online, sh2.LastReason)

	// ===== 模拟进程重启：从 state.json 恢复，终态任务不重复执行 =====
	fmt.Println("=== simulating process restart ===")
	o2, err := rollout.New(cfg, clk.now)
	if err != nil {
		panic(err)
	}
	printJobs(o2, "jobs restored after restart (state identical)")
	before := len(o2.SimCalls())
	if err := o2.Tick(ctx); err != nil {
		panic(err)
	}
	fmt.Printf("[check] post-restart new dispatch calls=%d (terminal jobs not re-dispatched)\n",
		len(o2.SimCalls())-before)

	fmt.Println("demo finished; snapshot at", statePath)
}
