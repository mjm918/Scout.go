package binlog

import (
	"Scout.go/log"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
)

type ScoutMySqlEventHandler struct {
	canal.DummyEventHandler
}

func (h *ScoutMySqlEventHandler) OnRow(e *canal.RowsEvent) error {
	fmt.Printf("Action: %s, Database: %s, Table: %s\n", e.Action, e.Table.Schema, e.Table.Name)
	return nil
}

func (h *ScoutMySqlEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	log.CL.Info("mysql binlog pos", zap.Any("pos", pos))
	return nil
}
