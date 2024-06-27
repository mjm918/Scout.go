package storage

import (
	"Scout.go/errors"
	"Scout.go/internal"
	"Scout.go/log"
	scoutmap "Scout.go/mapping"
	"Scout.go/models"
	"Scout.go/util"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/mapping"
	bleveindex "github.com/blevesearch/bleve_index_api"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Index struct {
	indexMapping *mapping.IndexMappingImpl
	logger       *log.BaseLog

	index     bleve.Index
	indexPath string
}

func NewIndex(config *models.IndexMapConfig) (*Index, error) {
	dir := util.IndexPath(config.Index)
	mapper, err := scoutmap.NewIndexMapping(config)
	if err != nil {
		return nil, err
	}
	idx, err := createIndex(dir, mapper, log.AppLog)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

func createIndex(dir string, indexMapping *mapping.IndexMappingImpl, logger *log.BaseLog) (*Index, error) {
	var index bleve.Index

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// create new index
		index, err = bleve.NewUsing(dir, indexMapping, scorch.Name, scorch.Name, nil)
		if err != nil {
			logger.Error(errors.ErrCreateIndex.Error(), zap.String("dir", dir), zap.Error(err))
			return nil, err
		}
	} else {
		// open existing index
		index, err = bleve.OpenUsing(dir, map[string]interface{}{
			"create_if_missing": true,
			"error_if_exists":   false,
		})
		if err != nil {
			logger.Error(errors.ErrOpenIndex.Error(), zap.String("dir", dir), zap.Error(err))
			return nil, err
		}
	}

	return &Index{
		index:        index,
		indexPath:    dir,
		indexMapping: indexMapping,
		logger:       logger,
	}, nil
}

func (i *Index) Close() error {
	if err := i.index.Close(); err != nil {
		i.logger.Error(errors.ErrCloseIndex.Error(), zap.Error(err))
		return err
	}

	return nil
}

func (i *Index) Get(id string) (map[string]interface{}, error) {
	doc, err := i.index.Document(id)
	if err != nil {
		i.logger.Error(errors.ErrNoDoc.Error(), zap.String("id", id), zap.Error(err))
		return nil, err
	}
	if doc == nil {
		err := errors.ErrNotFound
		i.logger.Debug(errors.ErrFoundDoc.Error(), zap.String("id", id), zap.Error(err))
		return nil, err
	}

	fields := make(map[string]interface{}, 0)
	doc.VisitFields(func(field bleveindex.Field) {
		var v interface{}
		switch field := field.(type) {
		case bleveindex.TextField:
			v = field.Text()
		case bleveindex.NumericField:
			n, err := field.Number()
			if err == nil {
				v = n
			}
		case bleveindex.DateTimeField:
			d, _, err := field.DateTime()
			if err == nil {
				v = d.Format(time.RFC3339Nano)
			}
		}
		existing, existed := fields[field.Name()]
		if existed {
			switch existing := existing.(type) {
			case []interface{}:
				fields[field.Name()] = append(existing, v)
			case interface{}:
				arr := make([]interface{}, 2)
				arr[0] = existing
				arr[1] = v
				fields[field.Name()] = arr
			}
		} else {
			fields[field.Name()] = v
		}
	})

	return fields, nil
}

func (i *Index) Search(searchRequest *bleve.SearchRequest) (*bleve.SearchResult, error) {
	searchResult, err := i.index.Search(searchRequest)
	if err != nil {
		i.logger.Error(errors.ErrSearchDoc.Error(), zap.Any("search_request", searchRequest), zap.Error(err))
		return nil, err
	}

	return searchResult, nil
}

func (i *Index) Index(id string, fields map[string]interface{}) error {
	if err := i.index.Index(id, fields); err != nil {
		i.logger.Error(errors.ErrIndexDoc.Error(), zap.String("id", id), zap.Error(err))
		return err
	}

	return nil
}

func (i *Index) Delete(id string) error {
	if err := i.index.Delete(id); err != nil {
		i.logger.Error(errors.ErrDeleteDoc.Error(), zap.String("id", id), zap.Error(err))
		return err
	}

	return nil
}

func (i *Index) BulkIndex(docs []map[string]interface{}) (int, error) {
	batch := i.index.NewBatch()

	count := 0

	for _, doc := range docs {
		id, ok := doc["id"].(string)
		if !ok {
			err := errors.ErrNil
			i.logger.Error(errors.ErrMissingId.Error(), zap.Error(err))
			continue
		}
		fields, ok := doc["fields"].(map[string]interface{})
		if !ok {
			err := errors.ErrNil
			i.logger.Error(errors.ErrMissingFields.Error(), zap.Error(err))
			continue
		}

		if err := batch.Index(id, fields); err != nil {
			i.logger.Error(errors.ErrIndexBatch.Error(), zap.String("id", id), zap.Error(err))
			continue
		}
		count++
	}

	err := i.index.Batch(batch)
	if err != nil {
		i.logger.Error(errors.ErrIndexBatch.Error(), zap.Int("count", count), zap.Error(err))
		return count, err
	}

	if count <= 0 {
		err := errors.ErrNoUpdate
		i.logger.Error(errors.ErrNoUpdate.Error(), zap.Any("count", count), zap.Error(err))
		return count, err
	}

	return count, nil
}

func (i *Index) BulkDelete(ids []string) (int, error) {
	batch := i.index.NewBatch()

	count := 0

	for _, id := range ids {
		batch.Delete(id)
		count++
	}

	err := i.index.Batch(batch)
	if err != nil {
		i.logger.Error(errors.ErrDeleteDoc.Error(), zap.Int("count", count), zap.Error(err))
		return count, err
	}

	return count, nil
}

func (i *Index) Mapping() *mapping.IndexMappingImpl {
	return i.indexMapping
}

func (i *Index) Stats() map[string]interface{} {
	return i.index.StatsMap()
}

func (i *Index) Name() string {
	return filepath.Base(i.indexPath)
}

func (i *Index) Path() string {
	return i.indexPath
}

func (i *Index) PrepareAndIndex(data []map[string]interface{}) error {
	var indexMapConfig models.IndexMapConfig
	err := internal.DB.Find(&indexMapConfig, i.Name(), 1, internal.IndexConfigStore)
	if err != nil {
		log.AppLog.E(i.Name(), "error getting index config", zap.Error(err))
		return err
	}
	if len(data) == 0 {
		return errors.ErrNoDoc
	}
	var wg sync.WaitGroup
	var mu sync.Mutex

	/**
	data = [{fields...}]
	norm = [{id:string,fields:{fields...}}]
	*/
	norm := make([]map[string]interface{}, 0)
	for _, d := range data {
		wg.Add(1)
		go func(t map[string]interface{}, w *sync.WaitGroup, m *sync.Mutex, r *[]map[string]interface{}, c *models.IndexMapConfig) {
			defer w.Done()
			m.Lock()
			v, ok := t[c.UniqueId]
			if ok {
				vs, er := util.ToString(v)
				if er != nil {
					log.AppLog.E(c.Index, "unique id expected as string", zap.Any("id", v))
					return
				}
				n := map[string]interface{}{
					"id":     vs,
					"fields": t,
				}
				*r = append(*r, n)
			} else {
				log.AppLog.E(c.Index, "unique ID not found in index mapping", zap.String("id", c.UniqueId), zap.Any("data", t))
			}
			m.Unlock()
		}(d, &wg, &mu, &norm, &indexMapConfig)
	}
	wg.Wait()

	count, err := i.BulkIndex(util.MakeUniqueById(norm))
	if err != nil {
		return err
	}
	log.AppLog.I(i.Name(), "bulk indexing completed...", zap.Int("count", count))

	return nil
}
