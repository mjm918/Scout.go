package models

type IndexRecordCount struct {
	Index     string `json:"index"`
	Count     uint32 `json:"count"`
	Execution string `json:"execution"`
}
