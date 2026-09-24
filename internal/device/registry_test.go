package device

import (
	"context"
	"testing"
	"time"

	"github.com/highcumontoa/ota-fleet-orchestrator-go/internal/version"
)

func v(s string) version.Version {
	ver, err := version.Parse(s)
	if err != nil {
		panic(err)
	}
	return ver
}

// fakeClock 是可控时钟。
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time                { return c.t }
func (c *fakeClock) add(d time.Duration) time.Time { c.t = c.t.Add(d); return c.t }

func newTestRegistry(t *testing.T) (*Registry, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Date(2026, 9, 24, 0, 0, 0, 0, time.UTC)}
	r := NewRegistry(10*time.Second, clk.now)
	return r, clk
}

func TestRegisterIdempotent(t *testing.T) {
	r, _ := newTestRegistry(t)
	d := Device{ID: "d1", Group: "g", CurrentVersion: v("1.0.0")}
	if err := r.Register(d); err != nil {
		t.Fatal(err)
	}
	// 重复注册：幂等、不覆盖档案（用不同 group/版本验证保持原样）
	d2 := Device{ID: "d1", Group: "HACKED", CurrentVersion: v("9.9.9")}
	if err := r.Register(d2); err != nil {
		t.Fatal(err)
	}
	got, _ := r.Get("d1")
	t.Logf("输入: 先注册 group=g/1.0.0 再重复注册 group=HACKED/9.9.9; 实际档案=%+v (依据: 重复注册幂等不覆盖)", got)
	if got.Group != "g" || got.CurrentVersion.Compare(v("1.0.0")) != 0 {
		t.Fatalf("重复注册覆盖了既有档案: %+v", got)
	}
	if _, ok := r.GetShadow("d1"); !ok {
		t.Fatal("注册后应存在影子")
	}
}

func TestHeartbeatOutOfOrderAndDuplicate(t *testing.T) {
	r, clk := newTestRegistry(t)
	r.Register(Device{ID: "d1", Group: "g", CurrentVersion: v("1.0.0")})

	r1, _ := r.Heartbeat("d1", v("1.0.0"), 5)
	t.Logf("心跳1 seq=5 fw=1.0.0 => accepted=%t status=%s reason=%s", r1.Accepted, r1.Online, r1.Reason)
	if r1.Online != StatusOnline || r1.Reason != ReasonFirstHeartbeat {
		t.Fatalf("首次心跳应 first-heartbeat/online, got %s/%s", r1.Online, r1.Reason)
	}

	r2, _ := r.Heartbeat("d1", v("1.0.2"), 8)
	t.Logf("心跳2 seq=8 fw=1.0.2 => accepted=%t reason=%s (更新心跳)", r2.Accepted, r2.Reason)

	// 乱序旧心跳 seq=6：必须拒绝，版本不回退、不刷新 LastSeen
	shBefore, _ := r.GetShadow("d1")
	clk.add(5 * time.Second)
	r3, _ := r.Heartbeat("d1", v("1.0.0"), 6)
	shAfter, _ := r.GetShadow("d1")
	t.Logf("乱序心跳 seq=6 fw=1.0.0 => accepted=%t appliedSeq=%d reason=%s; 影子 fw=%s seq=%d lastSeenUnchanged=%t",
		r3.Accepted, r3.AppliedSeq, r3.Reason, shAfter.Reported.FirmwareVersion, shAfter.Reported.Seq,
		shAfter.LastSeen.Equal(shBefore.LastSeen))
	if r3.Accepted || r3.Reason != ReasonStaleRejected {
		t.Fatal("乱序旧心跳应被拒绝并给出 stale-heartbeat-rejected")
	}
	if shAfter.Reported.FirmwareVersion.Compare(v("1.0.2")) != 0 || shAfter.Reported.Seq != 8 {
		t.Fatal("乱序旧心跳把状态回退了")
	}
	if !shAfter.LastSeen.Equal(shBefore.LastSeen) {
		t.Fatal("乱序旧心跳不得刷新 LastSeen，否则会错误延长在线判定")
	}

	// 重复心跳 seq=8：幂等接受但不改版本；只刷新 LastSeen
	clk.add(1 * time.Second)
	r4, _ := r.Heartbeat("d1", v("1.0.2"), 8)
	shDup, _ := r.GetShadow("d1")
	t.Logf("重复心跳 seq=8 => accepted=%t reason=%s; fw 保持=%s (依据: 同 seq 幂等)", r4.Accepted, r4.Reason, shDup.Reported.FirmwareVersion)
	if !r4.Accepted || r4.Reason != ReasonDuplicate {
		t.Fatal("重复心跳应幂等接受并标记 duplicate-heartbeat-ignored")
	}

	// 更新心跳 seq=9 恢复正常
	r5, _ := r.Heartbeat("d1", v("1.0.3"), 9)
	if !r5.Accepted || r5.Reason != ReasonHeartbeatFresh {
		t.Fatalf("新心跳应 accepted/fresh, got %t/%s", r5.Accepted, r5.Reason)
	}
}

func TestOfflineTimeoutAndRecovery(t *testing.T) {
	r, clk := newTestRegistry(t)
	r.Register(Device{ID: "d1", Group: "g", CurrentVersion: v("1.0.0")})
	r.Heartbeat("d1", v("1.0.0"), 1)

	clk.add(10*time.Second + time.Nanosecond) // 超过 offlineTimeout
	gone := r.ReapOffline(context.Background())
	sh, _ := r.GetShadow("d1")
	t.Logf("推进 %v 后巡检 => 离线设备=%v, d1 status=%s reason=%s (依据: now-LastSeen>timeout)",
		10*time.Second, gone, sh.Online, sh.LastReason)
	if len(gone) != 1 || gone[0] != "d1" || sh.Online != StatusOffline || sh.LastReason != ReasonTimeout {
		t.Fatal("超时设备应被标记 offline/heartbeat-timeout")
	}

	// 重复巡检不重复计数
	if again := r.ReapOffline(context.Background()); len(again) != 0 {
		t.Fatalf("已离线设备重复巡检不应再次计数, got %v", again)
	}

	// 重复心跳(seq 相同)不能让离线设备"复活"：幂等接受但状态保持 offline
	dup, _ := r.Heartbeat("d1", v("1.0.0"), 1)
	sh2, _ := r.GetShadow("d1")
	t.Logf("离线后重复心跳 seq=1 => accepted=%t reason=%s, 仍 status=%s (依据: 重复心跳不更新在线状态)",
		dup.Accepted, dup.Reason, sh2.Online)
	if dup.Reason != ReasonDuplicate || sh2.Online != StatusOffline {
		t.Fatal("重复心跳不得使离线设备恢复上线")
	}

	// 新心跳恢复：原因必须可与首次上线区分
	rec, _ := r.Heartbeat("d1", v("1.0.1"), 2)
	sh3, _ := r.GetShadow("d1")
	t.Logf("新心跳 seq=2 => status=%s reason=%s (依据: offline->online 标记 heartbeat-recovered)", sh3.Online, sh3.LastReason)
	if rec.Reason != ReasonRecovered || sh3.Online != StatusOnline {
		t.Fatalf("恢复上线应标记 heartbeat-recovered, got %s", rec.Reason)
	}
}

func TestShadowConvergence(t *testing.T) {
	r, _ := newTestRegistry(t)
	r.Register(Device{ID: "d1", Group: "g", CurrentVersion: v("1.0.0")})

	sh0, _ := r.GetShadow("d1")
	t.Logf("无期望状态时 converged=%t (依据: 无期望即无需收敛)", sh0.Converged())
	if !sh0.Converged() {
		t.Fatal("无期望应视为已收敛")
	}

	r.SetDesired("d1", v("1.1.0"))
	r.Heartbeat("d1", v("1.0.0"), 1)
	sh1, _ := r.GetShadow("d1")
	t.Logf("期望=%s 上报=%s converged=%t (依据: 上报版本需 >= 期望)", sh1.Desired.FirmwareVersion, sh1.Reported.FirmwareVersion, sh1.Converged())
	if sh1.Converged() {
		t.Fatal("1.0.0 未达到期望 1.1.0，不应收敛")
	}

	r.Heartbeat("d1", v("1.1.0"), 2)
	sh2, _ := r.GetShadow("d1")
	t.Logf("期望=%s 上报=%s converged=%t", sh2.Desired.FirmwareVersion, sh2.Reported.FirmwareVersion, sh2.Converged())
	if !sh2.Converged() {
		t.Fatal("上报 1.1.0 应已收敛到期望")
	}
}

func TestHeartbeatUnknownDevice(t *testing.T) {
	r, _ := newTestRegistry(t)
	if _, err := r.Heartbeat("ghost", v("1.0.0"), 1); err != ErrDeviceNotFound {
		t.Fatalf("未注册设备心跳应返回 ErrDeviceNotFound, got %v", err)
	}
}
