package models

type IndexDeletion struct {
	Status    bool   `json:"status"`
	Execution string `json:"execution"`
}

type IndexDataDeletion struct {
	Status    bool   `json:"status"`
	Uid       string `json:"uid"`
	Execution string `json:"execution"`
}

type IndexDataBatchDeletion struct {
	Uid       []string `json:"uid"`
	Execution string   `json:"execution"`
}
