package binlog

import (
	"Scout.go/internal"
	"Scout.go/log"
	"Scout.go/models"
	"Scout.go/reg"
	"Scout.go/storage"
	"Scout.go/util"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Maker struct {
	MakerInterface

	changes          []*canal.RowsEvent
	debouncedChannel chan *CanalEvent
	changesMu        sync.Mutex
	index            *storage.Index
}

type CanalEvent struct {
	Status string
	ID     int32
	Event  *canal.RowsEvent
}

func NewMaker(cnf *models.DbConfig) *Maker {
	searchIndex, err := reg.IndexByName(cnf.Index)
	if err != nil {
		log.AppLog.E(cnf.Index, "watching data changes but no index found", zap.Error(err))
	}
	i := Maker{
		changes:          make([]*canal.RowsEvent, 0),
		debouncedChannel: nil,
		changesMu:        sync.Mutex{},
		index:            searchIndex,
	}
	i.DbCnf = cnf
	i.EventChannel = nil
	i.Done = nil

	return &i
}

func (b *Maker) DoFirstTimeIndex() {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local", b.DbCnf.User, b.DbCnf.Password, b.DbCnf.Host, b.DbCnf.SafePort(), b.DbCnf.Database)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.AppLog.E(b.DbCnf.Index, "failed to connect to database", zap.Error(err))
		return
	}

	const batchSize = 1000

	tables := strings.Split(b.DbCnf.WatchTable, ",")
	for _, table := range tables {
		if !b.isFirstTimeFetchNeeded(table) {
			log.AppLog.I(b.DbCnf.Index, "isFirstTimeFetchNeeded: skipping ... ", zap.String("table", table))
			continue
		}
		var totalRecords int
		db.Table(table).Select("COUNT(*) AS totalCount").Scan(&totalRecords)
		if totalRecords > 0 {
			numBatches := (totalRecords + batchSize - 1) / batchSize
			for batch := 0; batch < numBatches; batch++ {
				offset := batch * batchSize

				var dataToPost []map[string]interface{}
				db.Table(table).Select("*").Offset(offset).Limit(batchSize).Scan(&dataToPost)

				if len(dataToPost) > 0 {
					b.followUserProtocol(dataToPost)
				}
			}
			_ = internal.DB.Put(fmt.Sprintf("completed:%s:%s", b.DbCnf.Database, table), time.Now().Format(time.DateTime), "")
		}
	}
}

func (b *Maker) Start() {
	b.EventChannel = make(chan *CanalEvent, 1000)
	defer close(b.EventChannel)

	b.Done = make(chan struct{})
	defer close(b.Done)

	b.debouncedChannel = b.debounce(100*time.Millisecond, 1*time.Second, b.EventChannel)
	defer close(b.debouncedChannel)

OUTER:
	for {
		select {
		case <-b.Done:
			break OUTER
		case event := <-b.debouncedChannel:
			if event == nil {
				break
			}
			if event.Status == "start" || event.Status == "stop" {
				b.processData()
			}
		}
	}
	time.Sleep(100 * time.Millisecond)
}

func (b *Maker) processData() {
	b.changesMu.Lock()
	if b.changes != nil && len(b.changes) > 0 {
		changes := util.Map(b.changes, func(e *canal.RowsEvent) []map[string]interface{} {
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
		})
		dataToPost := make([]map[string]interface{}, 0)
		for _, rows := range changes {
			for _, row := range rows {
				dataToPost = append(dataToPost, row)
			}
		}
		b.followUserProtocol(dataToPost)
		b.changes = nil
	}
	b.changesMu.Unlock()
}

func (b *Maker) followUserProtocol(dataToPost []map[string]interface{}) {
	client := resty.New().R()
	if b.DbCnf.MakerHeaders != nil && len(b.DbCnf.MakerHeaders) > 0 {
		for _, header := range b.DbCnf.MakerHeaders {
			client.SetHeader(header.HeaderKey, header.HeaderVal)
		}
	}
	if b.DbCnf.MakerHook != "" {
		client.SetBody(dataToPost)
		log.AppLog.Info("data to post", zap.Any("data", dataToPost))
		response, err := client.Post(b.DbCnf.MakerHook)
		if err != nil {
			log.AppLog.E(b.DbCnf.Index, "maker hook error", zap.Error(err))
		}
		log.AppLog.Info("maker hook response", zap.Any("response", response))
	} else {
		if b.index != nil {
			err := b.index.PrepareAndIndex(dataToPost)
			if err != nil {
				log.AppLog.E(b.DbCnf.Index, "prepare index error", zap.Error(err))
			}
		} else {
			log.AppLog.E(b.DbCnf.Index, "prepare data to index (nil)")
		}
	}
}

func (b *Maker) debounce(min time.Duration, max time.Duration, input chan *CanalEvent) chan *CanalEvent {
	output := make(chan *CanalEvent)

	go func() {
		var (
			buffer   *CanalEvent
			ok       bool
			minTimer <-chan time.Time
			maxTimer <-chan time.Time
		)

		// Start debouncing
		for {
			select {
			case buffer, ok = <-input:
				if !ok {
					return
				}
				if buffer.Event != nil {
					b.changesMu.Lock()
					b.changes = append(b.changes, buffer.Event)
					b.changesMu.Unlock()
				}
				minTimer = time.After(min)
				if maxTimer == nil {
					maxTimer = time.After(max)
				}
			case <-minTimer:
				minTimer, maxTimer = nil, nil
				output <- buffer
			case <-maxTimer:
				minTimer, maxTimer = nil, nil
				output <- buffer
			}
		}
	}()

	return output
}

func (b *Maker) isFirstTimeFetchNeeded(table string) bool {
	compareWith, err := strconv.ParseFloat(os.Getenv("FULL_SYNC_SINCE"), 64)
	if err != nil {
		panic("FULL_SYNC_SINCE env variable not set / invalid value")
	}
	v, err := internal.DB.Get(fmt.Sprintf("completed:%s:%s", b.DbCnf.Database, table), "")
	if err == nil {
		return true
	}
	t, err := time.Parse(time.DateTime, string(v))
	if err == nil {
		log.AppLog.E(b.DbCnf.Index, "isFirstTimeFetchNeeded date parse error", zap.String("table", table), zap.Any("value", string(v)))
		return true
	}
	duration := time.Since(t).Minutes()
	return !(duration > compareWith)
}
