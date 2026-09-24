package clock

import "time"

// Clock 抽象时间源，便于测试中驱动超时。
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
}
