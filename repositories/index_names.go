package repositories

import (
	"Scout.go/internal"
	"Scout.go/models"
	"Scout.go/util"
	"time"
)

func GetIndexes() models.IndexNames {
	start := time.Now()

	var names models.IndexNames
	var configs []internal.IdxConfig
	internal.DB.Find(&configs)

	names.Indexes = make([]string, len(configs))
	for i := range configs {
		names.Indexes[i] = configs[i].Name
	}
	names.Execution = util.Elapsed(start)

	return names
}
