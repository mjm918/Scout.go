package internal

type IdxConfig struct {
	ID         uint   `json:"id" gorm:"primary_key"`
	Name       string `json:"name"`
	Searchable string `json:"searchable"`
}
