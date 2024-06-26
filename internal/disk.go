package internal

import (
	"Scout.go/log"
	"Scout.go/util"
	"github.com/peterbourgon/diskv/v3"
	"go.uber.org/zap"
	"os"
	"sync"
)

type TempDisk struct {
	store *diskv.Diskv
	dir   string
}

func NewTempDisk(name string) *TempDisk {
	dir := util.TempKvPath(name)
	flatTransform := func(s string) []string { return []string{} }
	d := diskv.New(diskv.Options{
		BasePath:     dir,
		Transform:    flatTransform,
		CacheSizeMax: 1024 * 1024,
	})
	return &TempDisk{store: d, dir: dir}
}

func (t *TempDisk) Put(key string, value []byte) error {
	if t.store.Has(key) {
		err := t.Delete(key)
		if err != nil {
			log.L.Error(err.Error(), zap.String("key", key), zap.String("path", t.dir), zap.String("func", "Put"))
			return err
		}
	}
	return t.store.Write(key, value)
}

func (t *TempDisk) Get(key string) ([]byte, error) {
	return t.store.Read(key)
}

func (t *TempDisk) Delete(key string) error {
	return t.store.Erase(key)
}

func (t *TempDisk) Drop() {
	err := t.store.EraseAll()
	if err != nil {
		log.L.Error(err.Error(), zap.String("func", "Drop"), zap.String("name", t.dir))
		return
	}
	err = os.RemoveAll(t.dir)
	if err != nil {
		log.L.Error(err.Error(), zap.String("func", "Drop"), zap.String("name", t.dir))
		return
	}
}

func (t *TempDisk) Prefix(prefix string, data chan<- map[string]interface{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for key := range t.store.KeysPrefix(prefix, nil) {
		d, err := t.store.Read(key)
		if err == nil {
			data <- map[string]interface{}{
				"key":   key,
				"value": d,
			}
		}
	}
	close(data)
}
