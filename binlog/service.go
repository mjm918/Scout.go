package binlog

import (
	"Scout.go/errors"
	"Scout.go/internal"
	"Scout.go/log"
	"Scout.go/util"
	"database/sql"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"golang.org/x/exp/slices"
	"strconv"
	"strings"
	"time"

	"Scout.go/models"
	"go.uber.org/zap"
)

type Watchman struct {
	Canal   *canal.Canal
	Handler *ScoutMySqlEventHandler
}

type Service struct {
	Warehouse map[string]*Watchman

	servers []string
}

func WatchDataChanges() *Service {
	return &Service{
		Warehouse: make(map[string]*Watchman),
		servers:   make([]string, 0),
	}
}

func (a *Service) Boot() *Service {
	var result []models.DbConfig
	err := internal.DB.Find(&result, "", 0, internal.DbConfigStore)
	if err != nil {
		log.AppLog.Error("error booting WatchDataChanges", zap.Error(err))
		return nil
	}
	for _, config := range result {
		a.AssignNewWatchman(&config)
	}
	return a
}

func (a *Service) AssignNewWatchman(config *models.DbConfig) {
	c, er := a.GetWatchman(config)
	if er == nil {
		a.Warehouse[config.Index] = c
	}
}

func (a *Service) ListenForNewHost(ch chan interface{}) {
	for msg := range ch {
		switch v := msg.(type) {
		case *models.DbConfig:
			log.AppLog.Info("requesting a new watchman", zap.String("index", v.Index))
			w, ok := a.Warehouse[v.Index]
			if ok {
				log.AppLog.Warn("existing watchman found. closing connection", zap.String("index", v.Index))
				w.Canal.Close()
				w.Handler.Stop()
				time.Sleep(5 * time.Second)
			}
			a.AssignNewWatchman(v)
		default:
			log.AppLog.Error("unexpected message type", zap.Any("msg", msg))
		}
	}
}

func getMasterStatus(dbCfg *models.DbConfig) (string, uint32, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/", dbCfg.User, dbCfg.Password, dbCfg.Host, dbCfg.SafePort())
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", 0, err
	}
	defer db.Close()

	var file string
	var position uint32
	var binlogDoDB, binlogIgnoreDB, executedGTIDSet sql.NullString

	err = db.QueryRow("SHOW MASTER STATUS").Scan(&file, &position, &binlogDoDB, &binlogIgnoreDB, &executedGTIDSet)
	if err != nil {
		return "", 0, err
	}

	return file, position, nil
}

// GetWatchman initializes and returns a Canal instance for the specified database configuration.
func (a *Service) GetWatchman(dbCfg *models.DbConfig) (*Watchman, error) {
	if dbCfg.WatchTable == "" {
		log.AppLog.E(dbCfg.Index, "GetWatchman", zap.Error(errors.ErrNoWatchTable))
		return nil, nil
	}
	if dbCfg.Database == "" {
		log.AppLog.E(dbCfg.Index, "GetWatchman", zap.Error(errors.ErrNoWatchDb))
		return nil, nil
	}
	if !slices.Contains(a.servers, dbCfg.Host) {
		tables := util.Map(strings.Split(dbCfg.WatchTable, ","), func(t string) string {
			return strings.Trim(dbCfg.Database, " ") + "." + strings.Trim(t, " ")
		})

		cfg := canal.NewDefaultConfig()
		cfg.User = dbCfg.User
		cfg.Password = dbCfg.Password
		cfg.ServerID = 2001
		cfg.Addr = dbCfg.Host + ":" + strconv.Itoa(int(dbCfg.SafePort()))
		cfg.Charset = "utf8"
		cfg.Flavor = "mysql"
		cfg.IncludeTableRegex = tables // it does not work all the time, we have another filtering in OnRow
		cfg.Dump.TableDB = dbCfg.Database
		cfg.Dump.Tables = strings.Split(dbCfg.WatchTable, ",")
		cfg.Dump.ExecutionPath = ""
		cfg.Logger = log.CanalLog

		a.servers = append(a.servers, dbCfg.Host)

		c, err := canal.NewCanal(cfg)
		if err != nil {
			log.AppLog.E(dbCfg.Index, "error creating canal instance", zap.Error(err), zap.String("host", dbCfg.Host))
			return nil, nil
		}

		m := NewMaker(dbCfg)
		go m.DoFirstTimeIndex()

		w := Watchman{
			Canal:   c,
			Handler: NewScoutMySqlEventHandler(m),
		}
		c.SetEventHandler(w.Handler)

		log.AppLog.Info("starting watchman", zap.String("host", dbCfg.Host))
		go c.Run()

		file, pos, err := getMasterStatus(dbCfg)
		if err != nil {
			log.AppLog.Fatal("error getting master status", zap.Error(err), zap.String("host", dbCfg.Host))
			return nil, nil
		}
		startPos := mysql.Position{Name: file, Pos: pos}
		go a.monitorBinlogChanges(c, dbCfg, startPos, cfg)

		return &w, nil
	}
	return nil, nil
}

// monitorBinlogChanges monitors changes in the binlog file and position, and restarts the Canal instance if necessary.
func (a *Service) monitorBinlogChanges(c *canal.Canal, dbCfg *models.DbConfig, startPos mysql.Position, cfg *canal.Config) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		currentFile, currentPos, err := getMasterStatus(dbCfg)
		if err != nil {
			log.CanalLog.Error("error getting master status on rotate", zap.Error(err))
			continue
		}

		if currentFile != startPos.Name || currentPos != startPos.Pos {
			startPos.Name = currentFile
			startPos.Pos = currentPos

			if c != nil {
				c.Close()
			}
			c, err = canal.NewCanal(cfg) // Recreate the canal instance
			if err != nil {
				log.CanalLog.Error("error creating canal on rotate", zap.Error(err))
				continue
			}

			c.SetEventHandler(NewScoutMySqlEventHandler(NewMaker(dbCfg)))
			go c.RunFrom(startPos)
		}
	}
}
