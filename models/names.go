package models

type IndexNames struct {
	Indexes   []string `json:"indexes"`
	Execution string   `json:"execution"`
}

type IndexStats struct {
	Stats     map[string]map[string]interface{} `json:"stats"`
	Execution string                            `json:"execution"`
}
