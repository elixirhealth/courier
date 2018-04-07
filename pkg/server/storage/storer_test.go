package storage

import (
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestAccessRecord_MarshalLogObject(t *testing.T) {
	oe := zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig())
	ar := NewCachePutAccessRecord()
	err := ar.MarshalLogObject(oe)
	assert.Nil(t, err)
}

func TestKeyGetTimes(t *testing.T) {
	now := time.Now()
	x := &KeyGetTimes{
		{Key: "b", GetTime: now.Add(2 * time.Second)},
		{Key: "a", GetTime: now.Add(1 * time.Second)},
		{Key: "c", GetTime: now.Add(3 * time.Second)},
	}
	heap.Init(x)
	heap.Push(x, KeyGetTime{Key: "d", GetTime: now.Add(4 * time.Second)})
	assert.Equal(t, 4, x.Len())

	x1 := heap.Pop(x).(KeyGetTime)
	x2 := heap.Pop(x).(KeyGetTime)
	x3 := heap.Pop(x).(KeyGetTime)
	x4 := heap.Pop(x).(KeyGetTime)

	assert.Equal(t, "d", x1.Key)
	assert.Equal(t, "c", x2.Key)
	assert.Equal(t, "b", x3.Key)
	assert.Equal(t, "a", x4.Key)

	assert.True(t, x1.GetTime.After(x2.GetTime))
	assert.True(t, x2.GetTime.After(x3.GetTime))
	assert.True(t, x3.GetTime.After(x4.GetTime))
}
