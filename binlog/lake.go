package binlog

import (
	"Scout.go/log"
	"Scout.go/models"
	"Scout.go/util"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/go-resty/resty/v2"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"sync"
)

type ScoutMySqlEventHandler struct {
	canal.DummyEventHandler
	changes   []canal.RowsEvent
	changesMu sync.Mutex
	cnf       *models.DbConfig
}

func (h *ScoutMySqlEventHandler) OnRow(e *canal.RowsEvent) error {
	if e.Action == canal.UpdateAction || e.Action == canal.InsertAction {
		h.changesMu.Lock()
		h.changes = append(h.changes, *e)
		h.changesMu.Unlock()
		h.postDataToMaker()
	}
	return nil
}

func (h *ScoutMySqlEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (h *ScoutMySqlEventHandler) Start() {

}

func (h *ScoutMySqlEventHandler) postDataToMaker() {
	h.changesMu.Lock()
	if h.changes != nil && len(h.changes) > 0 {
		log.L.Info("flush collection", zap.Int("count", len(h.changes)), zap.String("db", h.cnf.Database))
		client := resty.New().R()
		if h.cnf.MakerHeaders != nil && len(h.cnf.MakerHeaders) > 0 {
			for _, header := range h.cnf.MakerHeaders {
				client.SetHeader(header.HeaderKey, header.HeaderVal)
			}
		}
		if h.cnf.MakerHook != "" {
			client.SetBody(util.Map(h.changes, func(e canal.RowsEvent) []map[string]interface{} {
				v := make([]map[string]interface{}, 0)
				columns := util.Map(e.Table.Columns, func(t schema.TableColumn) string {
					return t.Name
				})
				for _, r := range e.Rows {
					row := make(map[string]interface{})
					for i, col := range r {
						row[columns[i]] = col
					}
					v = append(v, row)
				}
				return v
			}))
			response, err := client.Post(h.cnf.MakerHook)
			if err != nil {
				log.L.Error("maker hook error", zap.Error(err))
			}
			log.L.Info("maker hook response", zap.Any("response", response))
		}
		h.changes = nil
	}
	h.changesMu.Unlock()
}
