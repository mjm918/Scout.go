package internal

import (
	"Scout.go/errors"
	"Scout.go/util"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
)

var DB *gorm.DB

func ConnectDatabase() {
	database, err := gorm.Open("sqlite3", util.ConfigPath())
	if err != nil {
		panic(errors.ErrCreateConfig)
	}
	database.AutoMigrate(&IdxConfig{})
	database.AutoMigrate(&DbConfig{})
	DB = database
}
