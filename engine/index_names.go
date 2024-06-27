package engine

import (
	"Scout.go/internal"
	"Scout.go/log"
	"Scout.go/models"
	"Scout.go/reg"
	"Scout.go/util"
	"go.uber.org/zap"
	"time"
)

func Indexes() models.IndexNames {
	start := time.Now()

	var names models.IndexNames
	var configs []models.IndexMapConfig
	err := internal.DB.Find(&configs, "", 0, internal.IndexConfigStore)
	if err != nil {
		log.AppLog.Error("indexes error - ", zap.Error(err))
		return models.IndexNames{}
	}

	names.Indexes = make([]string, len(configs))
	for i := range configs {
		names.Indexes[i] = configs[i].Index
	}
	names.Execution = util.Elapsed(start)

	return names
}

func IndexStats() models.IndexStats {
	start := time.Now()

	var res models.IndexStats

	stats := make(map[string]map[string]interface{})

	for k, v := range reg.Registry {
		stats[k] = v.Stats()["index"].(map[string]interface{})
	}
	res.Stats = stats
	res.Execution = util.Elapsed(start)
	return res
}
