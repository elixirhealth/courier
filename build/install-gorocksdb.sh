#!/usr/bin/env bash

CGO_CFLAGS="-I/usr/local/include/rocksdb" \
CGO_LDFLAGS="-L/usr/local/lib/librocksdb.a -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy" \
go install github.com/elxirhealth/courier/vendor/github.com/tecbot/gorocksdb
