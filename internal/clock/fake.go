package clock

import (
	"sync"
	"time"
)

// FakeClock 是测试用可控时钟：Now 可手动推进，After/Sleep 即时返回，
// 使超时相关测试可重复、快速运行。
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Advance 把时钟向前拨 d。
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Set 直接设置当前时间。
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// After 立即到期，返回当前时间。
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	ch <- c.Now()
	return ch
}

func (c *FakeClock) Sleep(d time.Duration) { c.Advance(d) }
