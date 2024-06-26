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

func CanalLogPath() string {
	return path.Join(BaseLogDir(), "canal.jsonl")
}

func IndexPath(name string) string {
	return path.Join(BaseConfigDir(), name)
}

func TempKvExists(loc, name string) bool {
	fileLoc := path.Join("_store_", loc, name)
	stat, err := os.Stat(fileLoc)
	if err != nil {
		return false
	}
	return !stat.IsDir()
}

func TempKvPath(loc string, name string) (string, error) {
	if !TempKvExists(loc, name) {
		dir, _ := createDir(path.Join("_store_", loc))
		kvPath := path.Join(dir, name)
		_, err := os.Create(kvPath)
		if err != nil {
			return "", err
		}
		return kvPath, nil
	}
	return path.Join("_store_", loc, name), nil
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

func Map[T, V any](ts []T, fn func(T) V) []V {
	result := make([]V, len(ts))
	for i, t := range ts {
		result[i] = fn(t)
	}
	return result
}
