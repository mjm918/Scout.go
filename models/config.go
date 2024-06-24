package models

import "errors"

type FieldType string

const (
	String FieldType = "string"
	Number FieldType = "number"
)

type IndexSearchable struct {
	Field string    `json:"field"`
	Type  FieldType `json:"type"`
}

func (a *IndexSearchable) Validate() error {
	switch a.Type {
	case String, Number:
		return nil
	default:
		return errors.New("invalid field type")
	}
}

type IndexMapConfig struct {
	Index      string            `json:"index"`
	Searchable []IndexSearchable `json:"searchable"`
}

type IndexConfig struct {
	Index     string `json:"index"`
	Status    bool   `json:"status"`
	Execution string `json:"execution"`
}
