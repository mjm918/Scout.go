package server

import (
	"Scout.go/engine"
	"Scout.go/internal"
	"Scout.go/reg"
	"Scout.go/routes"
	"context"
	"errors"
	"fmt"
	ginzap "github.com/gin-contrib/zap"
	"github.com/gin-gonic/autotls"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"golang.org/x/crypto/acme/autocert"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func StartServer(log *zap.Logger) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	gin.SetMode(os.Getenv("MODE"))

	router := gin.Default()
	// middleware setup - start
	router.Use(ginzap.Ginzap(log, time.RFC3339, true))
	router.Use(ginzap.RecoveryWithZap(log, true))
	router.Use(gin.Recovery())
	// middleware setup - end

	// route setup - start
	router.GET("/ping", routes.Ping)
	router.GET("/indexes", routes.GetIndexes)
	router.GET("/stats", routes.GetIndexStats)
	router.PUT("/config", routes.PutConfig)
	router.POST("/binlog", routes.PostDbConfigPerIndex)
	router.GET("/binlog/:index", routes.GetDbConfigPerIndex)
	router.GET("/log/:index", routes.GetIndexLog)
	// route setup - end

	srv := &http.Server{
		Addr:    ":7040",
		Handler: router,
	}

	go func() {
		if gin.IsDebugging() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Info(fmt.Sprintf("listen: %s\n", err))
			}
		} else {
			m := autocert.Manager{
				Prompt:     autocert.AcceptTOS,
				HostPolicy: autocert.HostWhitelist(os.Getenv("DOMAIN")),
				Cache:      autocert.DirCache(os.Getenv("TLS_CACHE")),
			}
			err := autotls.RunWithManager(router, &m)
			if err != nil {
				log.Fatal(err.Error())
				return
			}
		}
	}()

	<-ctx.Done()

	stop()
	defer func() {
		var idx = engine.Indexes()
		for _, indexName := range idx.Indexes {
			index, err := reg.IndexByName(indexName)
			if err != nil {
				log.Fatal("failed to get index", zap.Error(err))
			} else {
				err := index.Close()
				if err != nil {
					log.Fatal("failed to close index", zap.Error(err))
					return
				}
			}
		}
		err := internal.DB.Close()
		if err != nil {
			log.Fatal("failed to close temp disk", zap.Error(err))
		}
	}()
	log.Info("shutting down gracefully, press Ctrl+C again to force")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal(fmt.Sprintf("server forced to shutdown: %s", err))
	}
	log.Info("server exiting")
}
