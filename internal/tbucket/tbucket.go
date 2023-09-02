package tbucket

import (
	conf "go-touch-grass/config"
	"time"
)

type TBucket struct {
	ts            int64
	resetDuration int64
	maxTokens     int
	tokens        int
}

func New(config *conf.Config) *TBucket {
	tokens := config.TBucketMaxTokens
	return &TBucket{
		ts:            0,
		resetDuration: config.TBucketResetDuration,
		maxTokens:     tokens,
		tokens:        tokens,
	}
}

func (tb *TBucket) MakeQuery() bool {
	now := time.Now().UnixMilli()
	if now-tb.ts > tb.resetDuration {
		tb.ts = now
		tb.tokens = tb.maxTokens - 1
		return true
	}
	if tb.tokens != 0 {
		tb.tokens -= 1
		return true
	}
	return false
}
