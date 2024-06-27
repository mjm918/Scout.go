package binlog

import (
	"Scout.go/log"
	"Scout.go/models"
	"Scout.go/util"
	"fmt"
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

	changes          []*canal.RowsEvent
	cnf              *models.DbConfig
	debouncedChannel chan *CanalEvent
	changesMu        sync.Mutex
}

type CanalEvent struct {
	Status string
	ID     int32
	Event  *canal.RowsEvent
}

var start time.Time

func NewMaker(cnf *models.DbConfig) *Maker {
	i := Maker{
		changes:          make([]*canal.RowsEvent, 0),
		cnf:              cnf,
		EventChannel:     nil,
		Done:             nil,
		debouncedChannel: nil,
		changesMu:        sync.Mutex{},
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
				fmt.Printf("[%s] Received debounced event %s with ID %d\n", time.Since(start), event.Status, event.ID)
				b.makeData()
			}
		}
	}
	fmt.Printf("[%s] Exited debounce loop...\n", time.Since(start))
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("[%s] Done.\n", time.Since(start))
}

func (b *Maker) makeData() {
	log.L.Info("makeData()", zap.Int("count", len(b.changes)), zap.String("db", b.cnf.Database))

	b.changesMu.Lock()
	if b.changes != nil && len(b.changes) > 0 {
		client := resty.New().R()
		if b.cnf.MakerHeaders != nil && len(b.cnf.MakerHeaders) > 0 {
			for _, header := range b.cnf.MakerHeaders {
				client.SetHeader(header.HeaderKey, header.HeaderVal)
			}
		}
		if b.cnf.MakerHook != "" {
			dataToPost := util.Map(b.changes, func(e *canal.RowsEvent) []map[string]interface{} {
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
			client.SetBody(dataToPost)
			log.L.Info("data to post", zap.Any("data", dataToPost))
			response, err := client.Post(b.cnf.MakerHook)
			if err != nil {
				log.L.Error("maker hook error", zap.Error(err))
			}
			log.L.Info("maker hook response", zap.Any("response", response))
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
					fmt.Printf("[%s] Received raw event %s with ID %d\n", time.Since(start), buffer.Status, buffer.ID)
					b.changesMu.Lock()
					b.changes = append(b.changes, buffer.Event)
					b.changesMu.Unlock()
				}
				minTimer = time.After(min)
				if maxTimer == nil {
					maxTimer = time.After(max)
				}
			case <-minTimer:
				fmt.Printf("[%s] Flush Min timer is up!\n", time.Since(start))
				minTimer, maxTimer = nil, nil
				output <- buffer
			case <-maxTimer:
				fmt.Printf("[%s] Flush Max timer is up!\n", time.Since(start))
				minTimer, maxTimer = nil, nil
				output <- buffer
			}
		}
	}()

	return output
}
