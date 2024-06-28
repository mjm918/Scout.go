package binlog

import "Scout.go/models"

type MakerInterface struct {
	EventChannel chan *CanalEvent
	Done         chan struct{}
	DbCnf        *models.DbConfig
}

func (b *MakerInterface) Start()            {}
func (b *MakerInterface) DoFirstTimeIndex() {}
