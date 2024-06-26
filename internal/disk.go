package internal

import (
	"Scout.go/log"
	"Scout.go/util"
	"fmt"
	"github.com/goccy/go-json"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
	"reflect"
	"strings"
	"time"
)

const (
	DbConfigStore    = "_db_config_"
	IndexConfigStore = "_index_config_"
	defaultBucket    = "_default_"
)

type TempDisk struct {
	store *bbolt.DB
	dir   string
}

var DB *TempDisk

func NewDiskStorage() {
	DB = openTempDisk()
	err := DB.store.Update(func(tx *bbolt.Tx) error {
		if !tx.Writable() {
			log.L.Error("tx is not writable")
			return nil
		}
		_, err := tx.CreateBucketIfNotExists([]byte(defaultBucket))
		if err != nil {
			log.L.Error("create bucket error ", zap.String("bucket", defaultBucket), zap.Error(err))
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(IndexConfigStore))
		if err != nil {
			log.L.Error("create bucket error ", zap.String("bucket", IndexConfigStore), zap.Error(err))
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(DbConfigStore))
		if err != nil {
			log.L.Error("create bucket error ", zap.String("bucket", DbConfigStore), zap.Error(err))
			return err
		}
		return nil
	})
	if err != nil {
		log.L.Error("error creating temp disk storage", zap.Error(err))
		return
	}
}

func openTempDisk() *TempDisk {
	const diskPath = "internal"
	const diskStore = "storage.scout"

	dir, err := util.TempKvPath(diskPath, diskStore)
	if err != nil {
		log.L.Error(err.Error(), zap.String("path", dir), zap.String("NewTempDisk", "Path"))
	}

	d, err := bbolt.Open(dir, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.L.Error(err.Error(), zap.String("path", dir), zap.String("NewTempDisk", "Open"))
	}
	return &TempDisk{store: d, dir: dir}
}

func (td *TempDisk) Close() error {
	return td.store.Close()
}

func (td *TempDisk) BucketExists(bucket string) (bool, error) {
	if bucket == "" {
		bucket = defaultBucket
	}
	var exists bool
	err := td.store.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b != nil {
			exists = true
		}
		return nil
	})
	return exists, err
}

func (td *TempDisk) Get(key, bucket string) ([]byte, error) {
	if bucket == "" {
		bucket = defaultBucket
	}
	var value []byte
	err := td.store.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return fmt.Errorf("bucket %s not found", bucket)
		}
		value = b.Get([]byte(key))
		if value == nil {
			return fmt.Errorf("%s not found", key)
		}
		return nil
	})
	return value, err
}

func (td *TempDisk) Put(key, value, bucket string) error {
	if bucket == "" {
		bucket = defaultBucket
	}
	return td.store.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		return b.Put([]byte(key), []byte(value))
	})
}

func (td *TempDisk) BatchInsert(keyValues map[string]string, bucket string) error {
	if bucket == "" {
		bucket = defaultBucket
	}
	return td.store.Batch(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		for key, value := range keyValues {
			if err := b.Put([]byte(key), []byte(value)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (td *TempDisk) BatchUpdate(keyValues map[string]string, bucket string) error {
	return td.BatchInsert(keyValues, bucket) // Essentially the same as BatchInsert in bbolt
}

func (td *TempDisk) BatchDelete(keys []string, bucket string) error {
	if bucket == "" {
		bucket = defaultBucket
	}
	return td.store.Batch(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return fmt.Errorf("bucket %s not found", bucket)
		}
		for _, key := range keys {
			if err := b.Delete([]byte(key)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (td *TempDisk) Delete(key, bucket string) error {
	if bucket == "" {
		bucket = defaultBucket
	}
	return td.store.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return fmt.Errorf("bucket %s not found", bucket)
		}
		return b.Delete([]byte(key))
	})
}

func (td *TempDisk) PutMap(key string, value interface{}, bucket string) error {
	if bucket == "" {
		bucket = defaultBucket
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return td.Put(key, string(data), bucket)
}

func (td *TempDisk) PutJson(key, jsonStr, bucket string) error {
	if bucket == "" {
		bucket = defaultBucket
	}
	return td.Put(key, jsonStr, bucket)
}

func (td *TempDisk) DropBucket(bucket string) error {
	if bucket == "" {
		bucket = defaultBucket
	}
	return td.store.Update(func(tx *bbolt.Tx) error {
		return tx.DeleteBucket([]byte(bucket))
	})
}

func (td *TempDisk) BatchInsertMap(keyValues map[string]interface{}, bucket string) error {
	if bucket == "" {
		bucket = defaultBucket
	}
	return td.store.Batch(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		for key, value := range keyValues {
			data, err := json.Marshal(value)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(key), data); err != nil {
				return err
			}
		}
		return nil
	})
}

func (td *TempDisk) BatchInsertJson(keyValues map[string]string, bucket string) error {
	if bucket == "" {
		bucket = defaultBucket
	}
	return td.store.Batch(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		for key, jsonStr := range keyValues {
			if err := b.Put([]byte(key), []byte(jsonStr)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (td *TempDisk) BatchUpdateMap(keyValues map[string]interface{}, bucket string) error {
	return td.BatchInsertMap(keyValues, bucket)
}

func (td *TempDisk) BatchUpdateJson(keyValues map[string]string, bucket string) error {
	return td.BatchInsertJson(keyValues, bucket)
}

func (td *TempDisk) GetJson(key, bucket string) (string, error) {
	value, err := td.Get(key, bucket)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (td *TempDisk) GetMap(key string, result interface{}, bucket string) error {
	value, err := td.Get(key, bucket)
	if err != nil {
		return err
	}
	return json.Unmarshal(value, result)
}

func (td *TempDisk) Find(result interface{}, where string, limit int, bucket string) error {
	if bucket == "" {
		bucket = defaultBucket
	}
	v := reflect.ValueOf(result)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("result must be a non-nil pointer")
	}
	v = v.Elem()

	count := 0

	return td.store.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return fmt.Errorf("bucket %s not found", bucket)
		}

		return b.ForEach(func(k, vBytes []byte) error {
			if where != "" && !strings.HasPrefix(string(k), where) {
				return nil
			}

			if limit > 0 && count >= limit {
				return nil
			}

			switch v.Kind() {
			case reflect.Map:
				keyType := v.Type().Key()
				valueType := v.Type().Elem()
				mapKey := reflect.New(keyType).Elem()
				mapValue := reflect.New(valueType).Elem()
				if err := json.Unmarshal(k, mapKey.Addr().Interface()); err != nil {
					return err
				}
				if err := json.Unmarshal(vBytes, mapValue.Addr().Interface()); err != nil {
					return err
				}
				v.SetMapIndex(mapKey, mapValue)
			case reflect.Slice:
				elemType := v.Type().Elem()
				newElem := reflect.New(elemType).Elem()
				if err := json.Unmarshal(vBytes, newElem.Addr().Interface()); err != nil {
					return err
				}
				v.Set(reflect.Append(v, newElem))
			default:
				return fmt.Errorf("unsupported type: %s", v.Kind())
			}

			count++
			return nil
		})
	})
}
