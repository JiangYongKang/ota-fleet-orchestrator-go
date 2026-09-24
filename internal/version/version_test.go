package version

import (
	"errors"
	"testing"
)

func TestParseAndCompare(t *testing.T) {
	cases := []struct {
		in   string
		want Version
		str  string
	}{
		{"1.2.3", Version{1, 2, 3}, "1.2.3"},
		{"0.0.0", Version{0, 0, 0}, "0.0.0"},
		{"10.20.30", Version{10, 20, 30}, "10.20.30"},
	}
	for _, c := range cases {
		v, err := Parse(c.in)
		t.Logf("input=%q parsed=%v err=%v (依据: MAJOR.MINOR.PATCH 三段非负整数)", c.in, v, err)
		if err != nil || v != c.want || v.String() != c.str {
			t.Fatalf("Parse(%q)=%v,%v want %v,%q", c.in, v, err, c.want, c.str)
		}
	}
	bad := []string{"", "1.2", "1.2.3.4", "v1.2.3", "1.-2.3", "a.b.c", "1..3"}
	for _, in := range bad {
		v, err := Parse(in)
		t.Logf("非法输入=%q -> err=%v (依据: 必须严格为三段整数)", in, err)
		if err == nil {
			t.Fatalf("Parse(%q) 应失败, got %v", in, v)
		}
	}

	cmpCases := []struct {
		a, b Version
		want int
	}{
		{Version{1, 0, 0}, Version{1, 0, 1}, -1},
		{Version{1, 0, 9}, Version{1, 1, 0}, -1},
		{Version{1, 1, 0}, Version{2, 0, 0}, -1},
		{Version{1, 2, 3}, Version{1, 2, 3}, 0},
		{Version{2, 0, 0}, Version{1, 9, 9}, 1},
	}
	for _, c := range cmpCases {
		got := c.a.Compare(c.b)
		t.Logf("比较 %s vs %s => %d (依据: MAJOR→MINOR→PATCH 字典序)", c.a, c.b, got)
		if got != c.want {
			t.Fatalf("Compare(%s,%s)=%d want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestIsUpgradeAllowed(t *testing.T) {
	cases := []struct {
		from, to string
		wantErr  error // nil 表示允许
		reason   string
	}{
		{"1.0.0", "1.0.1", nil, "同 minor 内 patch 前推：允许"},
		{"1.0.5", "1.1.0", nil, "同 major 跨一个 minor：允许"},
		{"1.0.0", "1.2.0", ErrMinorJump, "跨两个 minor：拒绝跳变"},
		{"1.1.0", "2.0.0", ErrMajorJump, "跨 major：拒绝"},
		{"2.0.0", "1.9.9", ErrDowngrade, "目标更旧：拒绝降级"},
		{"1.1.1", "1.1.1", ErrSameVersion, "版本相同：拒绝原地升级"},
		{"1.0.9", "1.1.0", nil, "边界：minor+1 允许"},
		{"1.0.9", "1.2.0", ErrMinorJump, "边界：minor+2 拒绝"},
	}
	for _, c := range cases {
		from, _ := Parse(c.from)
		to, _ := Parse(c.to)
		err := IsUpgradeAllowed(from, to)
		t.Logf("输入 %s->%s err=%v 判定依据: %s", c.from, c.to, err, c.reason)
		if c.wantErr == nil && err != nil {
			t.Fatalf("%s->%s 应允许, got %v", c.from, c.to, err)
		}
		if c.wantErr != nil && !errors.Is(err, c.wantErr) {
			t.Fatalf("%s->%s err=%v, want %v (%s)", c.from, c.to, err, c.wantErr, c.reason)
		}
	}
}
