package internal

type IdxConfig struct {
	Name       string `json:"name" gorm:"unique"`
	Searchable string `json:"searchable"`
}
