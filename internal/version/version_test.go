package version

import "testing"

func TestParseAndCompare(t *testing.T) {
	cases := []struct {
		in   string
		want Version
	}{
		{"1.2.3", Version{1, 2, 3}},
		{"v0.0.1", Version{0, 0, 1}},
		{" 10.20.30 ", Version{10, 20, 30}},
	}
	for _, c := range cases {
		got, err := Parse(c.in)
		if err != nil {
			t.Fatalf("Parse(%q) unexpected error: %v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("Parse(%q)=%v want %v", c.in, got, c.want)
		}
		if got.String() != trimSpace(c.in) && got.String() != c.want.String() {
			t.Fatalf("String()=%s", got.String())
		}
	}
	// 比较的确定语义：major > minor > patch，非字典序。
	if (Version{1, 10, 0}).Compare(Version{1, 2, 0}) <= 0 {
		t.Fatal("1.10.0 must be greater than 1.2.0 numerically")
	}
	if (Version{1, 0, 0}).Compare(Version{1, 0, 0}) != 0 {
		t.Fatal("equal versions must compare to 0")
	}
	if (Version{1, 0, 0}).Compare(Version{2, 0, 0}) >= 0 {
		t.Fatal("1.0.0 must be less than 2.0.0")
	}
}

func trimSpace(s string) string {
	out := []byte{}
	for i := 0; i < len(s); i++ {
		if s[i] != ' ' {
			out = append(out, s[i])
		}
	}
	return string(out)
}

func TestParseInvalid(t *testing.T) {
	for _, in := range []string{"", "1.2", "1.2.x", "1.-2.3", "v1", "1.2.3.4"} {
		if _, err := Parse(in); err == nil {
			t.Fatalf("Parse(%q) expected error, got nil", in)
		}
	}
}

func TestCheckUpgrade(t *testing.T) {
	ok := func(err error) bool { return err == nil }
	// SameMinorPatchAllowed
	if err := CheckUpgrade(must("1.2.0"), must("1.2.1"), SameMinorPatchAllowed); !ok(err) {
		t.Fatalf("patch upgrade should be allowed: %v", err)
	}
	if err := CheckUpgrade(must("1.2.0"), must("1.3.0"), SameMinorPatchAllowed); err == nil {
		t.Fatal("minor jump must be rejected under SameMinorPatchAllowed")
	}
	// NextMinorAllowed
	if err := CheckUpgrade(must("1.2.9"), must("1.3.0"), NextMinorAllowed); err != nil {
		t.Fatalf("adjacent minor should be allowed: %v", err)
	}
	if err := CheckUpgrade(must("1.2.0"), must("1.4.0"), NextMinorAllowed); err == nil {
		t.Fatal("skipping a minor must be rejected")
	}
	if err := CheckUpgrade(must("1.2.0"), must("2.0.0"), NextMinorAllowed); err == nil {
		t.Fatal("major change must be rejected under NextMinorAllowed")
	}
	// AnyForwardAllowed
	if err := CheckUpgrade(must("1.0.0"), must("2.5.9"), AnyForwardAllowed); err != nil {
		t.Fatalf("any forward should be allowed: %v", err)
	}
	// 通用拒绝：降级、同版本。
	if err := CheckUpgrade(must("2.0.0"), must("1.9.9"), AnyForwardAllowed); err == nil {
		t.Fatal("downgrade must be rejected")
	}
	if err := CheckUpgrade(must("1.0.0"), must("1.0.0"), AnyForwardAllowed); err == nil {
		t.Fatal("same-version reinstall must be rejected")
	}
}

func must(s string) Version {
	v, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return v
}
