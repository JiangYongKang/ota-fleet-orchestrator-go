package version

import (
	"encoding/json"
	"fmt"
)

// Path 是一条预先定义的严格升级路径，版本必须沿 Versions 顺序逐跳升级：
// Versions[i] -> Versions[i+1]，路径之外的组合一律拒绝，且不允许回退。
type Path struct {
	versions []Version
}

// NewPath 校验并构造升级路径：至少两个版本、版本严格单调向前。
func NewPath(versions []Version) (*Path, error) {
	if len(versions) < 2 {
		return nil, fmt.Errorf("upgrade path must contain at least two versions, got %d", len(versions))
	}
	cp := append([]Version(nil), versions...)
	for i := 1; i < len(cp); i++ {
		c := cp[i].Compare(cp[i-1])
		if c == 0 {
			return nil, fmt.Errorf("upgrade path invalid: %s appears adjacent to an equal version", cp[i])
		}
		if c < 0 {
			return nil, fmt.Errorf("upgrade path invalid: %s -> %s is a downgrade", cp[i-1], cp[i])
		}
	}
	return &Path{versions: cp}, nil
}

// ParsePath 从 "1.0.0","1.1.0","1.2.0" 这样的版本字符串构造路径。
func ParsePath(versions []string) (*Path, error) {
	vs := make([]Version, 0, len(versions))
	for _, s := range versions {
		v, err := Parse(s)
		if err != nil {
			return nil, fmt.Errorf("upgrade path: %w", err)
		}
		vs = append(vs, v)
	}
	return NewPath(vs)
}

// Versions 返回路径上的版本副本。
func (p *Path) Versions() []Version {
	return append([]Version(nil), p.versions...)
}

// Contains 判断版本是否在路径上。
func (p *Path) Contains(v Version) bool {
	for _, pv := range p.versions {
		if pv.Compare(v) == 0 {
			return true
		}
	}
	return false
}

// NextStep 返回 from 之后的下一跳：from 在路径上且不是终点时返回 (next,true)；
// from 已是终点时返回 (zero,false) 表示无需再升；from 不在路径上时返回错误。
func (p *Path) NextStep(from Version) (Version, bool, error) {
	for i, pv := range p.versions {
		if pv.Compare(from) == 0 {
			if i == len(p.versions)-1 {
				return Version{}, false, nil
			}
			return p.versions[i+1], true, nil
		}
	}
	return Version{}, false, fmt.Errorf("version %s is not on the upgrade path %v", from, p.versions)
}

// CheckStep 判定 from -> to 是否为路径上相邻且向前的一跳。
// 路径之外的组合、跳级、降级/同版本重升一律拒绝。
func (p *Path) CheckStep(from, to Version) error {
	next, ok, err := p.NextStep(from)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("illegal upgrade: %s is already the final version on path, cannot go to %s", from, to)
	}
	if to.Compare(next) != 0 {
		return fmt.Errorf("illegal jump: %s -> %s is not the next step on path (next is %s)", from, to, next)
	}
	return nil
}

// MarshalJSON 以 ["1.0.0","1.1.0",...] 形式持久化路径。
func (p *Path) MarshalJSON() ([]byte, error) {
	if p == nil {
		return []byte("null"), nil
	}
	ss := make([]string, len(p.versions))
	for i, v := range p.versions {
		ss[i] = v.String()
	}
	return json.Marshal(ss)
}

// UnmarshalJSON 从版本字符串数组恢复路径并重新校验（严格单调向前）。
func (p *Path) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		p.versions = nil
		return nil
	}
	var ss []string
	if err := json.Unmarshal(b, &ss); err != nil {
		return fmt.Errorf("upgrade path json must be an array of version strings: %w", err)
	}
	np, err := ParsePath(ss)
	if err != nil {
		return err
	}
	p.versions = np.versions
	return nil
}
