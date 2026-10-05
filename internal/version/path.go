package version

import "fmt"

// Path 表示一条既定的升级路径：严格递增的版本序列。
// 跨版本升级必须沿路径逐段进行；路径之外的版本组合一律拒绝。
type Path struct {
	// Versions 为有序版本序列（要求严格递增，构造时校验）。
	Versions []Version
}

// NewPath 由有序版本列表构造升级路径。
// 要求：至少 2 个版本、无重复、严格递增（路径本身绝不允许隐含降级）。
func NewPath(versions []Version) (Path, error) {
	if len(versions) < 2 {
		return Path{}, fmt.Errorf("invalid upgrade path: need at least 2 versions, got %d", len(versions))
	}
	for i := 1; i < len(versions); i++ {
		if c := versions[i].Compare(versions[i-1]); c == 0 {
			return Path{}, fmt.Errorf("invalid upgrade path: duplicate version %s at index %d", versions[i], i)
		} else if c < 0 {
			return Path{}, fmt.Errorf("invalid upgrade path: %s -> %s is a downgrade; path must be strictly increasing",
				versions[i-1], versions[i])
		}
	}
	cp := append([]Version(nil), versions...)
	return Path{Versions: cp}, nil
}

// index 返回版本在路径中的下标，不在路径上返回 -1。
func (p Path) index(v Version) int {
	for i, pv := range p.Versions {
		if pv.Compare(v) == 0 {
			return i
		}
	}
	return -1
}

// Contains 判断版本是否在路径上。
func (p Path) Contains(v Version) bool { return p.index(v) >= 0 }

// PlanSteps 计算 from -> to 沿路径的逐段升级步骤。
// 返回的每个元素为一段 {from, to}，依次衔接。
// 拒绝：from/to 不在路径上、from==to、from 在路径上位于 to 之后（即降级）。
func (p Path) PlanSteps(from, to Version) ([][2]Version, error) {
	fi, ti := p.index(from), p.index(to)
	if fi < 0 {
		return nil, fmt.Errorf("version %s is not on the upgrade path; cannot plan", from)
	}
	if ti < 0 {
		return nil, fmt.Errorf("target %s is not on the upgrade path; off-path combinations are rejected", to)
	}
	if fi == ti {
		return nil, fmt.Errorf("already at target %s; nothing to plan", from)
	}
	if fi > ti {
		return nil, fmt.Errorf("illegal downgrade along path: %s is after %s on the path", from, to)
	}
	steps := make([][2]Version, 0, ti-fi)
	for i := fi; i < ti; i++ {
		steps = append(steps, [2]Version{p.Versions[i], p.Versions[i+1]})
	}
	return steps, nil
}

// NextStep 返回从 cur 出发沿路径走向 target 的下一段目标版本。
// ok=false 表示 cur 已到达 target，或 cur/target 不在路径上、或会构成降级。
func (p Path) NextStep(cur, target Version) (Version, bool) {
	ci, ti := p.index(cur), p.index(target)
	if ci < 0 || ti < 0 || ci >= ti {
		return Version{}, false
	}
	return p.Versions[ci+1], true
}
