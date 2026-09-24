package store

import (
	"path/filepath"
	"testing"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := NewJSONStore(filepath.Join(dir, "nested", "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	// 首次启动：文件不存在，Load 返回空且无错误。
	b, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if b != nil {
		t.Fatalf("first Load should return nil, got %q", b)
	}
	payload := []byte(`{"rollouts":[]}`)
	if err := s.Save(payload); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load()
	if err != nil || string(got) != string(payload) {
		t.Fatalf("round trip failed: got %q err=%v", got, err)
	}
	// 覆盖写入应完整替换。
	payload2 := []byte(`{"rollouts":["rollout-1"]}`)
	if err := s.Save(payload2); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Load()
	if string(got) != string(payload2) {
		t.Fatalf("overwrite failed: got %q", got)
	}
	// 临时文件不得残留。
	if matches, _ := filepath.Glob(filepath.Join(dir, "nested", "*.tmp")); len(matches) != 0 {
		t.Fatalf("temporary files left behind: %v", matches)
	}
	t.Logf("INPUT save %s => loaded identical; no .tmp remains", payload2)
}
