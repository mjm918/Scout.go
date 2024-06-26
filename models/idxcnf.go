package models

import (
	"errors"
)

type FieldType string

const (
	String   FieldType = "string"
	Number   FieldType = "number"
	DateTime FieldType = "datetime"
	Boolean  FieldType = "boolean"
)

type IndexSearchable struct {
	Field string    `json:"field"`
	Type  FieldType `json:"type"`
}

func (a *IndexSearchable) Validate() error {
	switch a.Type {
	case String, Number, DateTime, Boolean:
		return nil
	default:
		return errors.New("invalid field type")
	}
}

type IndexMapConfig struct {
	Index      string            `json:"index"`
	Searchable []IndexSearchable `json:"searchable"`
}

type IndexConfigResponse struct {
	Index     string `json:"index"`
	Status    bool   `json:"status"`
	Execution string `json:"execution"`
	Message   string `json:"message"`
}

func (a *IndexMapConfig) IsDifferent(other *IndexMapConfig) bool {
	if a.Index != other.Index {
		return true
	}
	if len(a.Searchable) == 0 {
		return true
	}
	i := 0
	for _, searchable := range a.Searchable {
		for _, otherSearchable := range other.Searchable {
			if searchable.Field == otherSearchable.Field && searchable.Type == otherSearchable.Type {
				i = i + 1
			}
		}
	}
	return i != len(other.Searchable)
}
