package binlog

import (
	"Scout.go/internal"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/exp/slices"
	"math/rand"
	"strings"
)

type ScoutMySqlEventHandler struct {
	canal.DummyEventHandler
	maker *Maker
}

func (h *ScoutMySqlEventHandler) OnRow(e *canal.RowsEvent) error {
	if slices.Contains(strings.Split(h.maker.DbCnf.WatchTable, ","), e.Table.Name) {
		id := rand.Int31()
		h.maker.EventChannel <- &CanalEvent{
			Status: "start",
			ID:     id,
			Event:  e,
		}
		go internal.DB.LogIt(fmt.Sprintf("Binlog: Table - %s Count - %d", e.Table.Name, len(e.Rows)), h.maker.DbCnf.Index)
	}
	return nil
}

func (h *ScoutMySqlEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func NewScoutMySqlEventHandler(m *Maker) *ScoutMySqlEventHandler {
	go m.Start()
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
