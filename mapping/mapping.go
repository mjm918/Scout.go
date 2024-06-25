package mapping

import (
	"Scout.go/models"
	"encoding/json"
	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/standard"
	"github.com/blevesearch/bleve/v2/mapping"
	"io/ioutil"
	"os"
)

func NewIndexMapping(config models.IndexMapConfig) (*mapping.IndexMappingImpl, error) {
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
		if searchable.Type == models.Boolean {
			boolFieldMapping := bleve.NewBooleanFieldMapping()
			boolFieldMapping.Store = false
			boolFieldMapping.DocValues = true
			docMap.AddFieldMappingsAt(searchable.Field, boolFieldMapping)
		}
		if searchable.Type == models.DateTime {
			dateTimeFieldMapping := bleve.NewDateTimeFieldMapping()
			dateTimeFieldMapping.Store = false
			dateTimeFieldMapping.DocValues = true
			docMap.AddFieldMappingsAt(searchable.Field, dateTimeFieldMapping)
		}
	}

	mapper := mapping.NewIndexMapping()
	mapper.DefaultMapping = docMap
	if err := mapper.Validate(); err != nil {
		return nil, err
	}
	return mapper, nil
}

func NewIndexMappingFromBytes(indexMappingBytes []byte) (*mapping.IndexMappingImpl, error) {
	indexMapping := mapping.NewIndexMapping()

	if err := indexMapping.UnmarshalJSON(indexMappingBytes); err != nil {
		return nil, err
	}

	if err := indexMapping.Validate(); err != nil {
		return nil, err
	}

	return indexMapping, nil
}

func NewIndexMappingFromMap(indexMappingMap map[string]interface{}) (*mapping.IndexMappingImpl, error) {
	indexMappingBytes, err := json.Marshal(indexMappingMap)
	if err != nil {
		return nil, err
	}

	indexMapping, err := NewIndexMappingFromBytes(indexMappingBytes)
	if err != nil {
		return nil, err
	}

	return indexMapping, nil
}

func NewIndexMappingFromFile(indexMappingPath string) (*mapping.IndexMappingImpl, error) {
	_, err := os.Stat(indexMappingPath)
	if err != nil {
		if os.IsNotExist(err) {
			// does not exist
			return nil, err
		}
		// other error
		return nil, err
	}

	// read index mapping file
	indexMappingFile, err := os.Open(indexMappingPath)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = indexMappingFile.Close()
	}()

	indexMappingBytes, err := ioutil.ReadAll(indexMappingFile)
	if err != nil {
		return nil, err
	}

	indexMapping, err := NewIndexMappingFromBytes(indexMappingBytes)
	if err != nil {
		return nil, err
	}

	return indexMapping, nil
}
