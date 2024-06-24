package repositories

import (
	"Scout.go/internal"
	"Scout.go/log"
	"Scout.go/models"
	"Scout.go/reg"
	"Scout.go/storage"
	"github.com/goccy/go-json"
	"time"
)

func NewIndexConfig(payload models.IndexMapConfig) (models.IndexConfig, error) {
	start := time.Now()

	for _, searchable := range payload.Searchable {
		if err := searchable.Validate(); err != nil {
			return models.IndexConfig{}, err
		}
	}

	var status models.IndexConfig

	searchable, err := json.Marshal(payload.Searchable)
	if err != nil {
		return status, err
	}
	rec := internal.IdxConfig{
		Name:       payload.Index,
		Searchable: string(searchable),
	}
	affected := internal.DB.Model(&rec).Where("name = ?", payload.Index).Updates(&rec).RowsAffected
	if affected == 0 {
		affected = internal.DB.Create(&rec).RowsAffected
	}
	updateIdxErr := UpdateIndex(payload)
	status.Execution = internal.Elapsed(start)
	status.Status = updateIdxErr == nil
	status.Index = payload.Index

	return status, nil
}

func UpdateIndex(mapConfig models.IndexMapConfig) error {
	_, err := reg.IndexByName(mapConfig.Index)
	if err != nil {
		log.L.Error(err.Error())
		index, err := storage.NewIndex(mapConfig)
		if err != nil {
			log.L.Error(err.Error())
			return err
		}
		reg.RegisterType(mapConfig.Index, index)
	}
	return nil
}
