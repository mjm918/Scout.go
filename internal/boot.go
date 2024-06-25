package internal

import (
	"Scout.go/errors"
	"Scout.go/models"
	"Scout.go/reg"
	"Scout.go/storage"
	"encoding/json"
)

func BootIndexesToRegistry() {
	var configs []IdxConfig
	DB.Find(&configs)

	for _, config := range configs {
		var indexMap models.IndexMapConfig
		err := json.Unmarshal([]byte(config.Searchable), &indexMap.Searchable)
		if err != nil {
			panic(errors.ErrBootIndex)
		}
		indexMap.Index = config.Name
		index, err := storage.NewIndex(indexMap)
		if err != nil {
			panic(errors.ErrBootIndex)
		}
		reg.RegisterType(indexMap.Index, index)
	}
}
