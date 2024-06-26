package internal

import (
	"errors"
	"github.com/goccy/go-json"
)

const (
	DbConfigStore    = "_db_config_"
	IndexConfigStore = "_index_config_"
)

type IndexConfig struct {
	Name       string `json:"name" gorm:"unique"`
	Searchable string `json:"searchable"`
}

type DbConfig struct {
	Host         string `json:"host"`
	Port         uint   `json:"port"`
	User         string `json:"user"`
	Password     string `json:"password"`
	Database     string `json:"database"`
	Index        string `json:"index" gorm:"unique"`
	WatchTable   string `json:"watch_table"`
	MakerHook    string `json:"maker_hook"`
	MakerHeaders string `json:"maker_headers"`
}

func (a *DbConfig) Validate() error {
	if a.Host == "" || a.User == "" || a.Password == "" || a.Database == "" || a.Index == "" || a.WatchTable == "" {
		j, _ := json.MarshalIndent(a, "", " ")
		return errors.New("invalid db config - " + string(j))
	}
	return nil
}
