package clock

import "time"

// RealClock 使用系统墙钟。
type RealClock struct{}

func NewRealClock() *RealClock { return &RealClock{} }

func (c *RealClock) Now() time.Time                         { return time.Now() }
func (c *RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (c *RealClock) Sleep(d time.Duration)                  { time.Sleep(d) }
