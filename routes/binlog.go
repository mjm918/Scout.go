package routes

import (
	"Scout.go/event"
	"Scout.go/internal"
	"Scout.go/models"
	"Scout.go/util"
	"github.com/gin-gonic/gin"
	"net/http"
	"time"
)

func PostDbConfigPerIndex(c *gin.Context) {
	var reqBody models.DbConfig
	if err := c.ShouldBindJSON(&reqBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if reqBody.Port < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid port number"})
		return
	}
	v := reqBody.Validate()
	if v != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "required field(s) missing [host, username, password, database, index, watch_table]\n" + v.Error()})
		return
	}

	start := time.Now()

	err := internal.DB.PutMap(reqBody.Index, reqBody, internal.DbConfigStore)
	if err != nil {
		c.JSON(http.StatusCreated, gin.H{"message": "error saving config", "execution": util.Elapsed(start)})
		return
	}
	// Notify watchman to watch a new server
	event.PubSubChannel.Publish("db-cnf", &reqBody)
	c.JSON(http.StatusCreated, gin.H{"message": "database config saved", "execution": util.Elapsed(start)})
}

func GetDbConfigPerIndex(c *gin.Context) {
	var result models.DbConfig
	start := time.Now()
	err := internal.DB.GetMap(c.Param("index"), &result, internal.DbConfigStore)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"message": err.Error(), "execution": util.Elapsed(start), "index": c.Param("index")})
		return
	}
	c.JSON(http.StatusOK, gin.H{"config": result, "execution": util.Elapsed(start), "index": c.Param("index")})
}
