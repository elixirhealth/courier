package cache

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
	ar := newCachePutAccessRecord()
	err := ar.MarshalLogObject(oe)
	assert.Nil(t, err)
}

func TestKeyGetTimes(t *testing.T) {
	now := time.Now()
	x := &keyGetTimes{
		{key: "b", getTime: now.Add(2 * time.Second)},
		{key: "a", getTime: now.Add(1 * time.Second)},
		{key: "c", getTime: now.Add(3 * time.Second)},
	}
	heap.Init(x)
	heap.Push(x, keyGetTime{key: "d", getTime: now.Add(4 * time.Second)})
	assert.Equal(t, 4, x.Len())

	x1 := heap.Pop(x).(keyGetTime)
	x2 := heap.Pop(x).(keyGetTime)
	x3 := heap.Pop(x).(keyGetTime)
	x4 := heap.Pop(x).(keyGetTime)

	assert.Equal(t, "d", x1.key)
	assert.Equal(t, "c", x2.key)
	assert.Equal(t, "b", x3.key)
	assert.Equal(t, "a", x4.key)

	assert.True(t, x1.getTime.After(x2.getTime))
	assert.True(t, x2.getTime.After(x3.getTime))
	assert.True(t, x3.getTime.After(x4.getTime))
}
