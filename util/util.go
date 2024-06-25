package util

import (
	"fmt"
	"os"
	"path"
	"time"
)

func ConfigPath() string {
	dir := path.Join(BaseConfigDir(), "internal.db")
	return dir
}

func LogPath() string {
	return path.Join(BaseLogDir(), "engine.jsonl")
}

func IndexPath(name string) string {
	return path.Join(BaseConfigDir(), name)
}

func BaseConfigDir() string {
	dir, _ := createDir("_store_")
	return dir
}

func BaseLogDir() string {
	dir, _ := createDir("_logs_")
	return dir
}

func createDir(name string) (string, error) {
	_, err := os.Stat(name)
	if err == nil {
		return name, nil
	}
	err = os.MkdirAll(name, os.ModePerm)
	if err == nil {
		return name, nil
	}
	return path.Join(os.TempDir(), name), err
}

func Elapsed(start time.Time) string {
	return fmt.Sprintf("%v", time.Since(start))
}
