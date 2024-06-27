package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"
)

var (
	Endian = binary.BigEndian
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

func ToString(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case int:
		return strconv.Itoa(v), nil
	case int8:
		return strconv.Itoa(int(v)), nil
	case int16:
		return strconv.Itoa(int(v)), nil
	case int32:
		return strconv.Itoa(int(v)), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(v), nil
	default:
		return "", fmt.Errorf("unsupported type: %T", v)
	}
}

func MakeUniqueById(arr []map[string]interface{}) []map[string]interface{} {
	uniqueMap := make(map[string]map[string]interface{})
	for _, item := range arr {
		if id, ok := item["id"].(string); ok {
			uniqueMap[id] = item
		}
	}

	uniqueArr := make([]map[string]interface{}, 0, len(uniqueMap))
	for _, v := range uniqueMap {
		uniqueArr = append(uniqueArr, v)
	}

	return uniqueArr
}

func GetUnixTimePrefixForCurrentMonth() string {
	now := time.Now()
	startOfMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	return fmt.Sprintf("%d", startOfMonth.Unix())
}

func ByteMarshal(pointerToData interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, Endian, pointerToData)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func ByteUnmarshal(pointerToData interface{}, bs []byte) error {
	buffer := bytes.NewBuffer(bs)
	err := binary.Read(buffer, Endian, pointerToData)
	if err != nil {
		return err
	}
	return nil
}
