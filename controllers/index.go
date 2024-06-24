package controllers

import (
	"Scout.go/models"
	"Scout.go/repositories"
	"github.com/gin-gonic/gin"
	"net/http"
)

func PutConfig(c *gin.Context) {
	var reqBody models.IndexMapConfig
	if err := c.ShouldBindJSON(&reqBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	resp, err := repositories.NewIndexConfig(reqBody)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	} else {
		c.JSON(http.StatusOK, resp)
	}
}

func GetConfig(c *gin.Context) {
	c.JSON(http.StatusOK, repositories.GetIndexes())
}
