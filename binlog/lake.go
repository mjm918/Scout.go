package binlog

import (
	"Scout.go/models"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"math/rand"
)

type ScoutMySqlEventHandler struct {
	canal.DummyEventHandler
	maker *Maker
}

func (h *ScoutMySqlEventHandler) OnRow(e *canal.RowsEvent) error {
	if e.Action == canal.UpdateAction || e.Action == canal.InsertAction {
		id := rand.Int31()
		h.maker.EventChannel <- &CanalEvent{
			Status: "start",
			ID:     id,
			Event:  e,
		}
	}
	return nil
}

func (h *ScoutMySqlEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func NewScoutMySqlEventHandler(cnf *models.DbConfig) *ScoutMySqlEventHandler {
	m := NewMaker(cnf)
	return &ScoutMySqlEventHandler{
		maker: m,
	}
}

func (h *ScoutMySqlEventHandler) Stop() {
	h.maker.EventChannel <- &CanalEvent{
		Status: "stop",
		ID:     0,
		Event:  nil,
	}
	h.maker.Done <- struct{}{}
	h.maker = nil
}
