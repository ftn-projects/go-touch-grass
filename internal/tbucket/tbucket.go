package tbucket

import (
	"errors"
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

func (tb *TBucket) MakeQuery() error {
	now := time.Now().UnixMilli()
	if now-tb.ts > tb.resetDuration {
		tb.ts = now
		tb.tokens = tb.maxTokens - 1
		return nil
	}
	if tb.tokens != 0 {
		tb.tokens -= 1
		return nil
	}
	return errors.New("previse zahteva molimo sacekajte")
}
