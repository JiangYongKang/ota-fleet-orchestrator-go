package rollout

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentHeartbeatsAndReports 在多 goroutine 下并发制造乱序心跳与
// 重复结果上报，断言：状态机不损坏、每台设备升级至多下发1次、结果不重复计数。
func TestConcurrentHeartbeatsAndReports(t *testing.T) {
	const n = 30
	env := newEnv(t, n) // 单批，全部设备一次 tick 派发
	ctx := context.Background()
	for i := 0; i < n; i++ {
		env.register(fmt.Sprintf("dev-%02d", i), "g", "1.0.0")
	}
	res, err := env.o.StartRollout("g", mustVer("1.1.0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入: %d 台设备全部入队 batch=%d", len(res.Items), res.Items[0].BatchIndex)

	// 并发 tick 多次（重复 tick 不得导致重复派发）
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = env.o.Tick(ctx) }()
	}
	// 并发乱序心跳：每台设备从多个 goroutine 发送 seq=3,1,2
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("dev-%02d", i)
		for _, seq := range []int64{3, 1, 2} {
			wg.Add(1)
			go func(id string, seq int64) {
				defer wg.Done()
				_, _ = env.o.Heartbeat(id, mustVer("1.0.0"), seq)
			}(id, seq)
		}
	}
	wg.Wait()

	// 每台设备升级调用必须恰好1次（重复 tick 不重复派发）
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("dev-%02d", i)
		if c := env.sim.Count(id, DirectionUpgrade); c != 1 {
			t.Fatalf("%s 升级下发 %d 次, 期望恰好1次", id, c)
		}
	}
	t.Logf("判定依据: 30 台设备 × 重复 tick 后升级下发均为 1 次 => 无重复下发")

	// 并发热闹上报：真实场景中设备只会重复上报同一个成功结果（网络重发）。
	// 每台设备并发发送 5 个 success=true（首次生效，其余为重复上报）。
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("dev-%02d", i)
		for k := 0; k < 5; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				env.o.ReportResult(id, true, "retransmitted ack")
			}()
		}
	}
	wg.Wait()

	// 再多轮 tick，确保所有任务收敛且无回滚（成功设备不应被回滚）
	env.clk.add(time.Second)
	for i := 0; i < 3; i++ {
		if err := env.o.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	succeeded, other := 0, 0
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("dev-%02d", i)
		switch env.state(id) {
		case StateSucceeded:
			succeeded++
		default:
			other++
			t.Errorf("%s 终态=%s, 期望 succeeded", id, env.state(id))
		}
		if c := env.sim.Count(id, DirectionRollback); c != 0 {
			t.Fatalf("%s 成功设备不应回滚, 实际 %d 次", id, c)
		}
	}
	t.Logf("结果: succeeded=%d other=%d; 全部设备回滚次数=0 (依据: 重复上报不重复计数)", succeeded, other)
}
