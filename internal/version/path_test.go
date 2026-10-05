package version

import "testing"

func TestPathConstructionAndValidation(t *testing.T) {
	// 输入：合法的严格向前路径。
	p, err := ParsePath([]string{"1.0.0", "1.1.0", "1.2.0", "2.0.0"})
	if err != nil {
		t.Fatalf("valid path rejected: %v", err)
	}
	t.Logf("INPUT ParsePath [1.0.0 1.1.0 1.2.0 2.0.0] => %v", p.Versions())

	cases := []struct {
		name    string
		input   []string
		wantErr bool
	}{
		{"too short", []string{"1.0.0"}, true},
		{"equal adjacent", []string{"1.0.0", "1.0.0"}, true},
		{"downgrade inside", []string{"1.2.0", "1.1.0"}, true},
		{"bad version string", []string{"1.0", "1.1.0"}, true},
		{"strictly forward", []string{"1.0.0", "1.10.0", "2.0.0"}, false},
	}
	for _, c := range cases {
		_, err := ParsePath(c.input)
		t.Logf("INPUT %-18s %v => err=%v", c.name, c.input, err)
		if (err != nil) != c.wantErr {
			t.Fatalf("%s: wantErr=%v got %v", c.name, c.wantErr, err)
		}
	}
}

func TestPathNextStepAndCheckStep(t *testing.T) {
	p, _ := ParsePath([]string{"1.0.0", "1.1.0", "1.2.0"})

	// 逐跳查询。
	next, more, err := p.NextStep(mustVer(t, "1.0.0"))
	if err != nil || !more || next.String() != "1.1.0" {
		t.Fatalf("next of 1.0.0 should be 1.1.0, got %s more=%v err=%v", next, more, err)
	}
	t.Logf("INPUT NextStep(1.0.0) => next=%s more=%v（依据：路径下标 i+1）", next, more)

	_, more, _ = p.NextStep(mustVer(t, "1.2.0"))
	if more {
		t.Fatal("final version must report more=false")
	}
	t.Logf("INPUT NextStep(1.2.0=终点) => more=false（依据：已是路径末尾）")

	if _, _, err := p.NextStep(mustVer(t, "9.9.9")); err == nil {
		t.Fatal("off-path version must be rejected")
	}
	t.Logf("INPUT NextStep(9.9.9 路径外) => err=%v（依据：Contains=false 明确拒绝）", err)

	checks := []struct {
		from, to string
		wantErr  bool
		why      string
	}{
		{"1.0.0", "1.1.0", false, "相邻一跳"},
		{"1.1.0", "1.2.0", false, "相邻一跳"},
		{"1.0.0", "1.2.0", true, "跨版本跳级，必须拆步"},
		{"1.1.0", "1.0.0", true, "降级，路径只有向前方向"},
		{"1.2.0", "1.3.0", true, "终点之外无路径"},
		{"1.0.0", "1.0.0", true, "同版本重升"},
	}
	for _, c := range checks {
		err := p.CheckStep(mustVer(t, c.from), mustVer(t, c.to))
		t.Logf("INPUT CheckStep %s -> %s（%s） => err=%v", c.from, c.to, c.why, err)
		if (err != nil) != c.wantErr {
			t.Fatalf("CheckStep %s->%s: wantErr=%v got %v", c.from, c.to, c.wantErr, err)
		}
	}
}

func TestPathJSONRoundTrip(t *testing.T) {
	p, _ := ParsePath([]string{"1.0.0", "1.1.0"})
	b, err := p.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT marshal path => %s", string(b))
	var p2 Path
	if err := p2.UnmarshalJSON(b); err != nil {
		t.Fatal(err)
	}
	if p2.Versions()[1].String() != "1.1.0" {
		t.Fatalf("round trip mismatch: %v", p2.Versions())
	}
	// 落盘内容若回退则重新校验。
	if err := p2.UnmarshalJSON([]byte(`["2.0.0","1.0.0"]`)); err == nil {
		t.Fatal("unmarshalling a downgrade path must fail validation")
	}
}

func mustVer(t *testing.T, s string) Version {
	t.Helper()
	v, err := Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
