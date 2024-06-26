package internal

import (
	"Scout.go/errors"
	"Scout.go/log"
	"Scout.go/models"
	"Scout.go/reg"
	"Scout.go/storage"
	"encoding/json"
	"go.uber.org/zap"
)

func BootIndexesToRegistry() {
	var configs []IndexConfig
	err := DB.Find(&configs, "", 0, IndexConfigStore)
	if err != nil {
		log.L.Error("failed to boot indexes", zap.Error(err))
		return
	}

	for _, config := range configs {
		var indexMap models.IndexMapConfig
		err := json.Unmarshal([]byte(config.Searchable), &indexMap.Searchable)
		if err != nil {
			panic(errors.ErrBootIndex)
		}
		indexMap.Index = config.Name
		index, err := storage.NewIndex(&indexMap)
		if err != nil {
			panic(errors.ErrBootIndex)
		}
		reg.RegisterType(indexMap.Index, index)
	}
}
