package main

import (
	"Scout.go/binlog"
	"Scout.go/errors"
	"Scout.go/event"
	"Scout.go/internal"
	slog "Scout.go/log"
	"Scout.go/models"
	"Scout.go/reg"
	"Scout.go/server"
	"Scout.go/storage"
	"Scout.go/util"
	"github.com/joho/godotenv"
	"log"
	"os"
	"time"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	_, err = time.LoadLocation(os.Getenv("TIME_LOCATION"))
	if err != nil {
		log.Fatal(err.Error())
		return
	}
	util.BaseLogDir()
	util.BaseConfigDir()
	internal.NewDiskStorage()

	var configs []models.IndexMapConfig
	err = internal.DB.Find(&configs, "", 0, internal.IndexConfigStore)
	if err != nil {
		log.Printf("failed to boot indexes %v", err)
		return
	}

	for _, config := range configs {
		index, err := storage.NewIndex(&config)
		if err != nil {
			panic(errors.ErrBootIndex)
		}
		reg.RegisterType(config.Index, index)
	}

	event.PubSubChannel = event.InitPubSub()
	wt := binlog.WatchDataChanges().Boot()
	ps := event.PubSubChannel.Subscribe("db-cnf")
	if wt != nil {
		go wt.ListenForNewHost(ps)
	}

	server.StartServer(slog.CreateLogger())
}
