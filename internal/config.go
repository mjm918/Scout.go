package internal

type IdxConfig struct {
	Name       string `json:"name" gorm:"unique"`
	Searchable string `json:"searchable"`
}

type DbConfig struct {
	Host     string `json:"host"`
	Port     string `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
}
