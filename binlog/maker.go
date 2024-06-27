package binlog

import (
	"Scout.go/log"
	"Scout.go/models"
	"Scout.go/reg"
	"Scout.go/storage"
	"Scout.go/util"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"
	"sync"
	"time"
)

type Maker struct {
	EventChannel chan *CanalEvent
	Done         chan struct{}
	DbCnf        *models.DbConfig

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
		DbCnf:            cnf,
		EventChannel:     nil,
		Done:             nil,
		debouncedChannel: nil,
		changesMu:        sync.Mutex{},
		index:            searchIndex,
	}
	go i.Start()
	return &i
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
				b.makeData()
			}
		}
	}
	time.Sleep(100 * time.Millisecond)
}

func (b *Maker) makeData() {
	b.changesMu.Lock()
	if b.changes != nil && len(b.changes) > 0 {
		client := resty.New().R()
		if b.DbCnf.MakerHeaders != nil && len(b.DbCnf.MakerHeaders) > 0 {
			for _, header := range b.DbCnf.MakerHeaders {
				client.SetHeader(header.HeaderKey, header.HeaderVal)
			}
		}
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
		b.changes = nil
	}
	b.changesMu.Unlock()
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
