package engine

import (
	"Scout.go/internal"
	"Scout.go/log"
	"Scout.go/models"
	"Scout.go/reg"
	"Scout.go/storage"
	"Scout.go/util"
	"go.uber.org/zap"
	"time"
)

func NewIndexConfig(payload models.IndexMapConfig) (models.IndexConfigResponse, error) {
	start := time.Now()

	for _, searchable := range payload.Searchable {
		if err := searchable.Validate(); err != nil {
			return models.IndexConfigResponse{}, err
		}
	}

	var status models.IndexConfigResponse
	status.Message = "not reindexing as fields are same"

	// Check if previously inserted config
	var prevRec models.IndexMapConfig
	var needReindex bool = false
	err := internal.DB.Find(&prevRec, payload.Index, 1, internal.IndexConfigStore)
	if err != nil {
		log.L.Error("error getting index config", zap.Error(err))
	}
	if prevRec.Index != "" {
		// Compare with new searchable
		var tmpRec models.IndexMapConfig
		tmpRec.Index = prevRec.Index
		needReindex = tmpRec.IsDifferent(&payload)
	} else {
		needReindex = true
	}

	if needReindex {
		updateIdxErr := UpdateIndex(&payload)
		err = internal.DB.PutMap(payload.Index, &payload, internal.IndexConfigStore)
		if err != nil {
			log.L.Error("error putting index config", zap.Error(err))
		}
		status.Status = updateIdxErr == nil
		status.Message = "reindexing based on new fields"
	}

	status.Execution = util.Elapsed(start)
	status.Index = payload.Index

	return status, nil
}

func UpdateIndex(mapConfig *models.IndexMapConfig) error {
	_, err := reg.IndexByName(mapConfig.Index)
	if err != nil {
		log.L.Error(err.Error())
		index, err := storage.NewIndex(mapConfig)
		if err != nil {
			log.L.Error(err.Error())
			return err
		}
		//ReIndex(index)
		reg.RegisterType(mapConfig.Index, index)
	} else {
		//ReIndex(index)
	}
	return nil
}

/*func ReIndex(index *storage.Index) {
	query := bleve.NewMatchAllQuery()
	searchRequest := bleve.NewSearchRequest(query)
	oldDocs, err := index.Search(searchRequest)
	if err != nil {
		log.L.Error(fmt.Sprintf("error searching bleve index: %s", err.Error()))
	} else {
		tmpKv := internal.NewTempDisk(index.Name())
		for _, hit := range oldDocs.Hits {
			doc, err := index.Get(hit.ID)
			if err != nil {
				log.L.Error(fmt.Sprintf("error fetching document %s: %v", hit.ID, err))
				continue
			}
			marshalled, err := json.Marshal(doc)
			if err != nil {
				log.L.Error(fmt.Sprintf("error marshalling document %s: %v", hit.ID, err))
				continue
			}
			putErr := tmpKv.Put(index.Name()+":"+string(hit.ID), marshalled)
			if putErr != nil {
				log.L.Error(fmt.Sprintf("error putting document %s %s: %v", index.Name(), hit.ID, putErr))
			}
		}

		data := make(chan map[string]interface{})
		var wg sync.WaitGroup
		wg.Add(1)
		go tmpKv.Prefix(index.Name()+":", data, &wg)

		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for pair := range data {
					key := pair["key"].(string)
					value := pair["value"]

				}
			}(i)
		}

		wg.Wait()
	}
}*/
