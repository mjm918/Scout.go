package models

type IndexInsertion struct {
	Index     string `json:"index"`
	Uid       string `json:"uid"`
	Status    bool   `json:"status"`
	Execution string `json:"execution"`
}

type IndexBatchInsertion struct {
	Index     string `json:"index"`
	Count     uint32 `json:"count"`
	Execution string `json:"execution"`
}
