package storage

import (
	"Scout.go/errors"
	"Scout.go/internal"
	"Scout.go/log"
	"Scout.go/models"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/standard"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/mapping"
	bleveindex "github.com/blevesearch/bleve_index_api"
	"go.uber.org/zap"
	"os"
	"time"
)

type Index struct {
	indexMapping *mapping.IndexMappingImpl
	logger       *zap.Logger

	index bleve.Index
}

func NewIndex(config models.IndexMapConfig) (*Index, error) {
	dir := internal.IndexPath(config.Index)

	docMap := bleve.NewDocumentMapping()

	for _, searchable := range config.Searchable {
		if searchable.Type == models.String {
			textFieldMapping := bleve.NewTextFieldMapping()
			textFieldMapping.Analyzer = standard.Name
			textFieldMapping.Store = false
			docMap.AddFieldMappingsAt(searchable.Field, textFieldMapping)
		}
		if searchable.Type == models.Number {
			numericFieldMapping := bleve.NewNumericFieldMapping()
			numericFieldMapping.Store = false
			numericFieldMapping.DocValues = true
			docMap.AddFieldMappingsAt(searchable.Field, numericFieldMapping)
		}
	}

	mapper := mapping.NewIndexMapping()
	mapper.DefaultMapping = docMap

	if err := mapper.Validate(); err != nil {
		return nil, err
	}

	idx, err := createIndex(dir, mapper, log.L)
	if err != nil {
		return nil, err
	}
	return idx, nil
}

func createIndex(dir string, indexMapping *mapping.IndexMappingImpl, logger *zap.Logger) (*Index, error) {
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
			"create_if_missing": false,
			"error_if_exists":   false,
		})
		if err != nil {
			logger.Error(errors.ErrOpenIndex.Error(), zap.String("dir", dir), zap.Error(err))
			return nil, err
		}
	}

	return &Index{
		index:        index,
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
