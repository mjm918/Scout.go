package internal

import (
	"os"
	"path"
)

func ConfigPath() string {
	dir := path.Join(baseDir(), "internal.db")
	return dir
}

func LogPath() string {
	dir, _ := createDir(".logs")
	return path.Join(dir, "engine.log")
}

func IndexPath(name string) string {
	dir, _ := createDir(path.Join(ConfigPath(), name))
	return dir
}

func baseDir() string {
	dir, _ := createDir(".db")
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
