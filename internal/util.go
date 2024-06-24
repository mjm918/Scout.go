package internal

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
	dir, err := createDir(path.Join(BaseConfigDir(), name))
	if err != nil {
		fmt.Println(fmt.Errorf("%v", err))
	}
	return dir
}

func BaseConfigDir() string {
	dir, _ := createDir(".db")
	return dir
}

func BaseLogDir() string {
	dir, _ := createDir(".logs")
	return dir
}

func createDir(name string) (string, error) {
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		return name, nil
	}
	err := os.Mkdir(name, 0750)
	if err != nil && !os.IsExist(err) {
		return name, nil
	}
	return path.Join(os.TempDir(), name), err
}

func Elapsed(start time.Time) string {
	return fmt.Sprintf("%v", time.Since(start))
}
