package store

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// JSONStore 把状态以 JSON 原子写入单个文件，实现本地持久化。
// Save 先写临时文件再 rename，保证进程崩溃不会留下半截状态文件。
type JSONStore struct {
	path string
}

// NewJSONStore 创建存储；父目录不存在则创建。
func NewJSONStore(path string) (*JSONStore, error) {
	if path == "" {
		return nil, errors.New("store: empty path")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return &JSONStore{path: path}, nil
}

// Save 原子覆盖写入。
func (s *JSONStore) Save(state []byte) error {
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, state, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Load 读取状态；文件不存在时返回 nil, nil（首次启动）。
func (s *JSONStore) Load() ([]byte, error) {
	b, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return b, err
}
