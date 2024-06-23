package models

type IndexConfig struct {
	Index     string `json:"index"`
	Status    bool   `json:"status"`
	Execution string `json:"execution"`
}
