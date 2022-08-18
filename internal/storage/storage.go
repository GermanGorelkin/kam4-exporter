package storage

import (
	"fmt"
	"time"
)

type Storage interface {
	GetFileLink(string) (string, error)
	GetFilePath(string) string
}

func UniqueFileName(extension string) string {
	return fmt.Sprintf("%d.%s", time.Now().UnixNano(), extension)
}
