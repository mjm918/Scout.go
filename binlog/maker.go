package binlog

import (
	"Scout.go/models"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"sync"
	"time"
)

type Maker struct {
	Changes      []*canal.RowsEvent
	EventChannel chan *CanalEvent
	Done         chan struct{}

	cnf              *models.DbConfig
	debouncedChannel chan *CanalEvent
	changesMu        sync.Mutex
}

type CanalEvent struct {
	Status string
	ID     int
	Event  *canal.RowsEvent
}

var start time.Time

func NewMaker(cnf *models.DbConfig) *Maker {
	return &Maker{
		Changes:          make([]*canal.RowsEvent, 0),
		cnf:              cnf,
		EventChannel:     nil,
		Done:             nil,
		debouncedChannel: nil,
		changesMu:        sync.Mutex{},
	}
}

func (b *Maker) Start() {
	b.EventChannel = make(chan *CanalEvent, 100)
	defer close(b.EventChannel)

	b.Done = make(chan struct{})
	defer close(b.Done)

	b.debouncedChannel = b.debounce(100*time.Millisecond, 500*time.Millisecond, b.EventChannel)
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
				//event.doSomething()
			}
		}
	}
	fmt.Printf("[%s] Exited loop...\n", time.Since(start))
	time.Sleep(100 * time.Millisecond)
	fmt.Printf("[%s] Done.\n", time.Since(start))
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
				fmt.Printf("[%s] Received raw event %s with ID %d\n", time.Since(start), buffer.Status, buffer.ID)
				b.changesMu.Lock()
				b.Changes = append(b.Changes, buffer.Event)
				b.changesMu.Unlock()
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
