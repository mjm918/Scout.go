package routes

import (
	"Scout.go/engine"
	"Scout.go/models"
	"github.com/gin-gonic/gin"
	"net/http"
)

func PutConfig(c *gin.Context) {
	var reqBody models.IndexMapConfig
	if err := c.ShouldBindJSON(&reqBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	resp, err := engine.NewIndexConfig(reqBody)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	} else {
		c.JSON(http.StatusOK, resp)
	}
}

func GetIndexes(c *gin.Context) {
	c.JSON(http.StatusOK, engine.Indexes())
}

func GetIndexStats(c *gin.Context) {
	c.JSON(http.StatusOK, engine.IndexStats())
}
