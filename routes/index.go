package routes

import (
	"Scout.go/engine"
	"Scout.go/models"
	"Scout.go/reg"
	"fmt"
	"github.com/gin-gonic/gin"
	"net/http"
	"strconv"
)

func PutConfig(c *gin.Context) {
	var reqBody models.IndexMapConfig
	if err := c.ShouldBindJSON(&reqBody); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := reqBody.Validate(); err != nil {
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

func GetSearch(c *gin.Context) {
	idxName := c.Param("index")
	if idxName == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "index is required"})
		return
	}
	query := c.Param("query")
	if query == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "query is required"})
		return
	}
	index, err := reg.IndexByName(idxName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	offsetStr := c.Param("offset")
	limitStr := c.Param("limit")

	offset := 0
	limit := 0

	if offsetStr != "" {
		offset, err = strconv.Atoi(offsetStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid offset: %s", err.Error())})
			return
		}
	}
	if limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid limit: %s", err.Error())})
			return
		}
	}
	c.JSON(http.StatusOK, index.Query(query, offset, limit))
}
