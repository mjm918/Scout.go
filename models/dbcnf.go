package models

import (
	"errors"
	"github.com/goccy/go-json"
)

type DbConfig struct {
	Host         string `json:"host"`
	Port         uint   `json:"port"`
	User         string `json:"user"`
	Password     string `json:"password"`
	Database     string `json:"database"`
	Index        string `json:"index"`
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

func (a *DbConfig) SafePort() uint {
	if a.Port == 0 {
		return 3306
	} else {
		return a.Port
	}
}
