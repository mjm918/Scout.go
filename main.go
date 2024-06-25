package main

import (
	"Scout.go/internal"
	logger "Scout.go/log"
	"Scout.go/server"
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
	internal.ConnectDatabase()
	internal.BootIndexesToRegistry()

	server.StartServer(logger.L)
}
