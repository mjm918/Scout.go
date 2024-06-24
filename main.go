package main

import (
	"Scout.go/internal"
	logger "Scout.go/log"
	"Scout.go/server"
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
	internal.BaseLogDir()
	internal.BaseConfigDir()
	internal.ConnectDatabase()

	server.StartServer(logger.L)
}
