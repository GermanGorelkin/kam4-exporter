package storage

import (
	"fmt"
	"path/filepath"
)

type FS struct {
	path string
	host string
}

type FSConfig struct {
	Path string
	Host string
}

func NewFS(cfg FSConfig) (*FS, error) {
	if cfg.Host == "" {
		return nil, fmt.Errorf("The Link must be set")
	}
	if cfg.Path == "" {
		return nil, fmt.Errorf("The Host must be set")
	}

	return &FS{
		path: cfg.Path,
		host: cfg.Host,
	}, nil
}

func (fs *FS) GetFileLink(fileName string) (string, error) {
	return fmt.Sprintf("%s/%s", fs.host, fileName), nil
}

func (fs *FS) GetFilePath(fileName string) string {
	return filepath.Join(fs.path, fileName)
}
