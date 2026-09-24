package version

import (
	"fmt"
	"strconv"
	"strings"
)

// Version 表示语义化版本的数字三元组（major.minor.patch）。
type Version struct {
	Major int
	Minor int
	Patch int
}

// Parse 把 "1.2.3"（允许前导 'v'/'V'）形式的字符串解析为 Version。
// 段数必须恰好为 3，且每段为非负整数，不接受前导零以外的非法字符。
func Parse(s string) (Version, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Version{}, fmt.Errorf("empty version string")
	}
	if s[0] == 'v' || s[0] == 'V' {
		s = s[1:]
	}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return Version{}, fmt.Errorf("invalid version %q: want major.minor.patch", s)
	}
	nums := [3]int{}
	for i, p := range parts {
		if p == "" {
			return Version{}, fmt.Errorf("invalid version %q: empty segment", s)
		}
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return Version{}, fmt.Errorf("invalid version %q: segment %q is not a non-negative integer", s, p)
		}
		nums[i] = n
	}
	return Version{Major: nums[0], Minor: nums[1], Patch: nums[2]}, nil
}

// String 返回规范字符串。
func (v Version) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// Compare 返回 -1/0/1，分别表示 v < o、v == o、v > o。
// 比较顺序固定为 major -> minor -> patch，语义确定，不依赖字符串字典序。
func (v Version) Compare(o Version) int {
	switch {
	case v.Major != o.Major:
		if v.Major < o.Major {
			return -1
		}
		return 1
	case v.Minor != o.Minor:
		if v.Minor < o.Minor {
			return -1
		}
		return 1
	case v.Patch != o.Patch:
		if v.Patch < o.Patch {
			return -1
		}
		return 1
	default:
		return 0
	}
}

// CompatibilityRule 描述允许的升级跨度规则。
type CompatibilityRule int

const (
	// SameMinorPatchAllowed 仅允许同一 minor 内的 patch 级升级。
	SameMinorPatchAllowed CompatibilityRule = iota
	// NextMinorAllowed 允许同 minor 内 patch 升级，或升级到相邻的下一个
	// minor（不得跨 minor 跳变）；major 必须相同。
	NextMinorAllowed
	// AnyForwardAllowed 任意向前升级均允许（仍拒绝降级与同版本重升）。
	AnyForwardAllowed
)

// CheckUpgrade 判定 from -> to 是否为合法升级。
// 拒绝：同版本重升、任何降级、违反规则的跨版本跳变。
func CheckUpgrade(from, to Version, rule CompatibilityRule) error {
	c := to.Compare(from)
	if c == 0 {
		return fmt.Errorf("illegal upgrade: target %s equals current %s", to, from)
	}
	if c < 0 {
		return fmt.Errorf("illegal downgrade: %s -> %s", from, to)
	}
	switch rule {
	case SameMinorPatchAllowed:
		if to.Major != from.Major || to.Minor != from.Minor {
			return fmt.Errorf("illegal jump: %s -> %s only patch-level upgrade within the same minor is allowed", from, to)
		}
	case NextMinorAllowed:
		if to.Major != from.Major {
			return fmt.Errorf("illegal jump: %s -> %s major version change is not allowed", from, to)
		}
		switch {
		case to.Minor == from.Minor:
			// 同 minor 内 patch 升级，Compare 已保证 patch 向前。
		case to.Minor == from.Minor+1:
			// 相邻 minor，patch 任意非负。
		default:
			return fmt.Errorf("illegal jump: %s -> %s crosses more than one minor version", from, to)
		}
	case AnyForwardAllowed:
		// Compare 已保证严格向前。
	default:
		return fmt.Errorf("unknown compatibility rule %d", rule)
	}
	return nil
}
