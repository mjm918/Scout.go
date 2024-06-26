package event

import "sync"

type PubSub struct {
	mu          sync.RWMutex
	subscribers map[string][]chan interface{}
}

var PubSubChannel *PubSub

func InitPubSub() *PubSub {
	return &PubSub{
		subscribers: make(map[string][]chan interface{}),
	}
}

func (ps *PubSub) Subscribe(topic string) chan interface{} {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	ch := make(chan interface{})
	ps.subscribers[topic] = append(ps.subscribers[topic], ch)

	return ch
}

func (ps *PubSub) Unsubscribe(topic string, ch chan interface{}) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	subscribers := ps.subscribers[topic]
	for i, subscriber := range subscribers {
		if subscriber == ch {
			ps.subscribers[topic] = append(subscribers[:i], subscribers[i+1:]...)
			close(ch)
			break
		}
	}
}

func (ps *PubSub) Publish(topic string, msg interface{}) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for _, subscriber := range ps.subscribers[topic] {
		go func(subscriber chan interface{}) {
			subscriber <- msg
		}(subscriber)
	}
}
