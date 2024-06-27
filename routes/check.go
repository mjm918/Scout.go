package routes

import (
	"Scout.go/internal"
	"Scout.go/util"
	"github.com/gin-gonic/gin"
	"net/http"
	"time"
)

func GetIndexLog(c *gin.Context) {
	start := time.Now()

	index := c.Param("index")
	//var logs []map[string]interface{}
	//err := internal.LogDb.Find(&logs, util.GetUnixTimePrefixForCurrentMonth(), 0, index)
	logs, err := internal.LogDb.GetLogs(index)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error(), "execution": util.Elapsed(start)})
		return
	}
	c.JSON(http.StatusOK, gin.H{"data": logs, "execution": util.Elapsed(start)})
}
