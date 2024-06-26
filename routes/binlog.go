package routes

import (
	"Scout.go/internal"
	"github.com/gin-gonic/gin"
	"net/http"
)

func PostDbConfigPerIndex(c *gin.Context) {
	var reqBody internal.DbConfig
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
	/*var tmpRec []internal.DbConfig
	internal.DB.Where("`index` = ?", reqBody.Index).Find(&tmpRec)
	*/
	if internal.DB.Where("`index` = ?", reqBody.Index).Assign(&reqBody).FirstOrCreate(&internal.DbConfig{}).Error == nil {
		c.JSON(http.StatusCreated, gin.H{"message": "Database config was saved successfully"})
	} else {
		c.JSON(http.StatusCreated, gin.H{"message": "Database config was not saved successfully"})
	}
}

func GetDbConfigPerIndex(c *gin.Context) {
	var result []internal.DbConfig
	internal.DB.Find(&result)
	c.JSON(http.StatusOK, gin.H{"config": result})
}
