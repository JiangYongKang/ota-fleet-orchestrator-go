// Package version 定义固件版本号及其比较、兼容判定语义。
//
// 版本语义（本项目的确定规则）：
//   - 版本号形式为 MAJOR.MINOR.PATCH，三段均为非负整数，不接受前导 v。
//   - 比较按 MAJOR -> MINOR -> PATCH 字典序，结果确定且与字符串无关。
//   - 合法升级必须同时满足：
//     1. 目标版本严格新于当前版本（拒绝降级、拒绝原地升级）；
//     2. MAJOR 相同（跨大版本需显式迁移，编排器一律拒绝）；
//     3. 同 MAJOR 下 MINOR 最多向前跨 1 个次版本（防跨版本跳变）；
//     4. 同 MINOR 内 PATCH 允许任意向前推进（补丁直发）。
package version

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Version 表示语义化固件版本号 major.minor.patch。
type Version struct {
	Major int
	Minor int
	Patch int
}

// ErrEmptyVersion 表示空版本字符串。
var ErrEmptyVersion = errors.New("empty version string")

// Parse 把形如 "1.2.3" 的字符串解析为 Version。
func Parse(s string) (Version, error) {
	if s == "" {
		return Version{}, ErrEmptyVersion
	}
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return Version{}, fmt.Errorf("version %q: expect MAJOR.MINOR.PATCH", s)
	}
	nums := make([]int, 3)
	for i, p := range parts {
		if p == "" {
			return Version{}, fmt.Errorf("version %q: empty component", s)
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return Version{}, fmt.Errorf("version %q: component %q is not an integer", s, p)
		}
		if n < 0 {
			return Version{}, fmt.Errorf("version %q: negative component", s)
		}
		nums[i] = n
	}
	return Version{Major: nums[0], Minor: nums[1], Patch: nums[2]}, nil
}

// String 返回 "major.minor.patch" 形式。
func (v Version) String() string {
	return strconv.Itoa(v.Major) + "." + strconv.Itoa(v.Minor) + "." + strconv.Itoa(v.Patch)
}

// Compare 返回 -1/0/1，分别表示 v 旧于/等于/新于 other。
func (v Version) Compare(other Version) int {
	switch {
	case v.Major != other.Major:
		if v.Major < other.Major {
			return -1
		}
		return 1
	case v.Minor != other.Minor:
		if v.Minor < other.Minor {
			return -1
		}
		return 1
	case v.Patch != other.Patch:
		if v.Patch < other.Patch {
			return -1
		}
		return 1
	default:
		return 0
	}
}

// 升级规则违例的确定原因，调用方可据此打印判定依据。
var (
	// ErrDowngrade 目标版本低于当前版本。
	ErrDowngrade = errors.New("upgrade rejected: target is older than current (downgrade)")
	// ErrSameVersion 目标版本等于当前版本，无需升级。
	ErrSameVersion = errors.New("upgrade rejected: target equals current version")
	// ErrMajorJump 跨大版本升级被拒绝。
	ErrMajorJump = errors.New("upgrade rejected: cross-major upgrade is not allowed")
	// ErrMinorJump 跨多个次版本的跳变被拒绝。
	ErrMinorJump = errors.New("upgrade rejected: cannot skip more than one MINOR version")
)

// IsUpgradeAllowed 判定从 from 升级到 to 是否合法。
func IsUpgradeAllowed(from, to Version) error {
	cmp := to.Compare(from)
	if cmp < 0 {
		return ErrDowngrade
	}
	if cmp == 0 {
		return ErrSameVersion
	}
	if to.Major != from.Major {
		return ErrMajorJump
	}
	if to.Minor-from.Minor > 1 {
		return ErrMinorJump
	}
	return nil
}
