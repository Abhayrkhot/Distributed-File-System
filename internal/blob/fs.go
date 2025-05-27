package blob

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// FS is a directory-backed Store. It stands in for S3 in tests and in
// single-host setups without MinIO; several processes may share one directory.
type FS struct {
	root string
}

func NewFS(root string) (*FS, error) {
	if err := os.MkdirAll(filepath.Join(root, ".tmp"), 0o755); err != nil {
		return nil, err
	}
	return &FS{root: root}, nil
}

func (f *FS) path(key string) (string, error) {
	clean := filepath.Clean("/" + key)
	if clean == "/" || strings.Contains(key, "..") {
		return "", errors.New("blob: invalid key")
	}
	return filepath.Join(f.root, filepath.FromSlash(clean)), nil
}

func (f *FS) Put(ctx context.Context, key string, data []byte) error {
	p, err := f.path(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Join(f.root, ".tmp"), "put-*")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), p)
}

func (f *FS) Get(ctx context.Context, key string) ([]byte, error) {
	p, err := f.path(key)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotFound
	}
	return b, err
}

func (f *FS) Exists(ctx context.Context, key string) (bool, error) {
	p, err := f.path(key)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (f *FS) List(ctx context.Context, prefix string, fn func(string) error) error {
	var keys []string
	err := filepath.WalkDir(f.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".tmp" {
				return filepath.SkipDir
			}
			return nil
		}
		rel, _ := filepath.Rel(f.root, p)
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(k); err != nil {
			return err
		}
	}
	return nil
}

func (f *FS) Delete(ctx context.Context, key string) error {
	p, err := f.path(key)
	if err != nil {
		return err
	}
	err = os.Remove(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}
