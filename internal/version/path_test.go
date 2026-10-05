package version

import (
	"testing"
)

func pv(s string) Version {
	v, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return v
}

func TestNewPathValidation(t *testing.T) {
	cases := []struct {
		name    string
		input   []string
		wantErr bool
	}{
		{"valid increasing", []string{"1.0.0", "1.1.0", "1.2.0", "2.0.0"}, false},
		{"too short", []string{"1.0.0"}, true},
		{"empty", nil, true},
		{"duplicate rejected", []string{"1.0.0", "1.1.0", "1.1.0"}, true},
		{"downgrade in path rejected", []string{"1.0.0", "1.2.0", "1.1.0"}, true},
	}
	for _, tc := range cases {
		var vs []Version
		for _, s := range tc.input {
			vs = append(vs, pv(s))
		}
		_, err := NewPath(vs)
		t.Logf("INPUT NewPath(%v) => err=%v", tc.input, err)
		if (err != nil) != tc.wantErr {
			t.Fatalf("%s: wantErr=%v got err=%v", tc.name, tc.wantErr, err)
		}
	}
}

func TestPlanStepsMultiHop(t *testing.T) {
	p, err := NewPath([]Version{pv("1.0.0"), pv("1.1.0"), pv("1.2.0"), pv("2.0.0")})
	if err != nil {
		t.Fatal(err)
	}
	steps, err := p.PlanSteps(pv("1.0.0"), pv("2.0.0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT PlanSteps(1.0.0 -> 2.0.0) path=%v => %d steps", p.Versions, len(steps))
	if len(steps) != 3 {
		t.Fatalf("want 3 steps, got %d: %v", len(steps), steps)
	}
	want := [][2]string{{"1.0.0", "1.1.0"}, {"1.1.0", "1.2.0"}, {"1.2.0", "2.0.0"}}
	for i, w := range want {
		if steps[i][0].String() != w[0] || steps[i][1].String() != w[1] {
			t.Fatalf("step %d: want %s->%s got %s->%s", i, w[0], w[1], steps[i][0], steps[i][1])
		}
		t.Logf("  step %d: %s -> %s", i, steps[i][0], steps[i][1])
	}
	// 相邻版本：单步。
	steps, err = p.PlanSteps(pv("1.1.0"), pv("1.2.0"))
	if err != nil || len(steps) != 1 {
		t.Fatalf("adjacent hop should be 1 step, got %v err=%v", steps, err)
	}
}

func TestPlanStepsRejectsOffPathAndDowngrade(t *testing.T) {
	p, _ := NewPath([]Version{pv("1.0.0"), pv("1.1.0"), pv("2.0.0")})
	cases := []struct {
		name       string
		from, to   string
		wantErrSub string
	}{
		{"from off path", "1.0.5", "2.0.0", "not on the upgrade path"},
		{"to off path", "1.0.0", "1.5.0", "off-path"},
		{"same version", "1.1.0", "1.1.0", "nothing to plan"},
		{"downgrade along path", "2.0.0", "1.0.0", "downgrade"},
	}
	for _, tc := range cases {
		_, err := p.PlanSteps(pv(tc.from), pv(tc.to))
		t.Logf("INPUT PlanSteps(%s -> %s) => err=%v", tc.from, tc.to, err)
		if err == nil {
			t.Fatalf("%s: must be rejected", tc.name)
		}
	}
}

func TestNextStep(t *testing.T) {
	p, _ := NewPath([]Version{pv("1.0.0"), pv("1.1.0"), pv("1.2.0")})
	next, ok := p.NextStep(pv("1.0.0"), pv("1.2.0"))
	t.Logf("INPUT NextStep(cur=1.0.0 target=1.2.0) => next=%s ok=%v", next, ok)
	if !ok || next.String() != "1.1.0" {
		t.Fatalf("want next=1.1.0 ok=true, got %s ok=%v", next, ok)
	}
	// 已到达目标：无下一步。
	if _, ok := p.NextStep(pv("1.2.0"), pv("1.2.0")); ok {
		t.Fatal("at target: NextStep must be false")
	}
	// 降级方向：拒绝。
	if _, ok := p.NextStep(pv("1.2.0"), pv("1.0.0")); ok {
		t.Fatal("downgrade direction: NextStep must be false")
	}
	// 路径外当前版本：拒绝。
	if _, ok := p.NextStep(pv("9.9.9"), pv("1.2.0")); ok {
		t.Fatal("off-path current: NextStep must be false")
	}
	t.Logf("INPUT NextStep at-target/downgrade/off-path => all rejected as expected")
}
