package server

import (
	"github.com/elxirhealth/courier/pkg/api"
	"context"
)

type courier struct {
	// config
	// libri balancer
	// DataStore client
	// health server
	// metrics server
	// stop chan
	// stopped chan
	// logger
}

func (*courier) Get(ctx context.Context, rq *api.GetRequest) (*api.GetResponse, error) {
	// validate request
	// check DataStore (DS) cache for value
	// if not found, check libri for value
	// if not found, return missing value error
	// otherwise, insert value into cache and update entry in access log
	// if missing from DS cache, insert into cache
	panic("not implemented")
}

func (*courier) Put(ctx context.Context, rq *api.PutRequest) (*api.PutResponse, error) {
	// validate request
	// check DS cache for value
	// if found, return w/ already exists status
	// otherwise, insert value into cache
	// insert entry into access log
	// add entry to toPut chan
	panic("not implemented")
}

