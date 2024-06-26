package binlog

import (
	"Scout.go/errors"
	"Scout.go/internal"
	"Scout.go/util"
	"database/sql"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"golang.org/x/exp/slices"
	"strconv"
	"strings"
	"time"

	//"Scout.go/internal"
	"Scout.go/log"
	"Scout.go/models"
	"go.uber.org/zap"
)

type Watchman struct {
	Canal   *canal.Canal
	Handler ScoutMySqlEventHandler
}

type Service struct {
	Warehouse map[string]Watchman

	servers []string
}

func WatchDataChanges() *Service {
	return &Service{
		Warehouse: make(map[string]Watchman),
		servers:   make([]string, 0),
	}
}

func (a *Service) Boot() *Service {
	var result []models.DbConfig
	err := internal.DB.Find(&result, "", 0, internal.DbConfigStore)
	if err != nil {
		log.L.Error("error booting WatchDataChanges", zap.Error(err))
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
			log.L.Info("requesting a new watchman", zap.String("index", v.Index))
			if a.Warehouse[v.Index].Canal != nil {
				log.L.Warn("existing watchman found. closing connection and running GC", zap.String("index", v.Index))
				a.Warehouse[v.Index].Canal.Close()
				time.Sleep(10 * time.Second)
			}
			a.AssignNewWatchman(v)
		default:
			log.L.Error("unexpected message type", zap.Any("msg", msg))
		}
	}
}

func getMasterStatus(dbCfg *models.DbConfig) (string, uint32, error) {
	dsn := dbCfg.User + ":" + dbCfg.Password + "@tcp(" + dbCfg.Host + ":" + strconv.Itoa(int(dbCfg.SafePort())) + ")/"
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
func (a *Service) GetWatchman(dbCfg *models.DbConfig) (Watchman, error) {
	if dbCfg.WatchTable == "" {
		log.CL.Error("GetWatchman", zap.Error(errors.ErrNoWatchTable))
		return Watchman{}, nil
	}
	if dbCfg.Database == "" {
		log.CL.Error("GetWatchman", zap.Error(errors.ErrNoWatchDb))
		return Watchman{}, nil
	}

	if !slices.Contains(a.servers, dbCfg.Host) {
		tables := util.Map(strings.Split(dbCfg.WatchTable, ","), func(t string) string {
			return dbCfg.Database + "." + t
		})

		cfg := canal.NewDefaultConfig()
		cfg.User = dbCfg.User
		cfg.Password = dbCfg.Password
		cfg.ServerID = 2001
		cfg.Addr = dbCfg.Host + ":" + strconv.Itoa(int(dbCfg.SafePort()))
		cfg.Charset = "utf8"
		cfg.Flavor = "mysql"
		cfg.IncludeTableRegex = []string{fmt.Sprintf("^%s$", strings.Join(tables, "|"))}
		cfg.Dump.TableDB = dbCfg.Database
		cfg.Dump.Tables = strings.Split(dbCfg.WatchTable, ",")
		cfg.Dump.ExecutionPath = ""
		cfg.Logger = log.CL

		a.servers = append(a.servers, dbCfg.Host)

		c, err := canal.NewCanal(cfg)
		if err != nil {
			log.CL.Error("error creating canal instance", zap.Error(err), zap.String("host", dbCfg.Host))
			return Watchman{}, nil
		}
		w := Watchman{
			Canal:   c,
			Handler: ScoutMySqlEventHandler{},
		}
		c.SetEventHandler(&w.Handler)
		c.Run()

		/*c.GetMasterPos()

		file, pos, err := getMasterStatus(dbCfg)
		if err != nil {
			log.CL.Fatal("error getting master status", zap.Error(err), zap.String("host", dbCfg.Host))
			return Watchman{}, nil
		}

		startPos := mysql.Position{Name: file, Pos: pos}

		go a.monitorBinlogChanges(c, dbCfg, startPos, cfg)*/

		return w, nil
	}
	return Watchman{}, nil
}

// monitorBinlogChanges monitors changes in the binlog file and position, and restarts the Canal instance if necessary.
func (a *Service) monitorBinlogChanges(c *canal.Canal, dbCfg *models.DbConfig, startPos mysql.Position, cfg *canal.Config) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		currentFile, currentPos, err := getMasterStatus(dbCfg)
		if err != nil {
			log.CL.Error("error getting master status on rotate", zap.Error(err))
			continue
		}

		if currentFile != startPos.Name || currentPos != startPos.Pos {
			startPos.Name = currentFile
			startPos.Pos = currentPos

			c.Close()
			c, err = canal.NewCanal(cfg) // Recreate the canal instance
			if err != nil {
				log.CL.Error("error creating canal on rotate", zap.Error(err))
				continue
			}

			c.SetEventHandler(&ScoutMySqlEventHandler{})
			go c.RunFrom(startPos)
		}
	}
}
