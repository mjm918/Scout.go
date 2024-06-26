package engine

import (
	"Scout.go/internal"
	"Scout.go/models"
	"Scout.go/reg"
	"Scout.go/util"
	"time"
)

func Indexes() models.IndexNames {
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
