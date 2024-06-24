package reg

import (
	yrr "Scout.go/errors"
	"Scout.go/storage"
	"errors"
	"fmt"
)

type IndexRegistry map[string]*storage.Index

var Registry = make(IndexRegistry, 0)

func RegisterType(name string, typ *storage.Index) {
	if _, exists := Registry[name]; exists {
		panic(errors.New(fmt.Sprintf("attempted to register duplicate index: %s", name)))
	}
	Registry[name] = typ
}

func IndexByName(name string) (*storage.Index, error) {
	index, exists := Registry[name]
	if exists {
		return index, nil
	}
	return nil, yrr.ErrNotFound
}
