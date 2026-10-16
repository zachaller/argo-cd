package cache

import (
	"context"
	"errors"
	"time"
)

var (
	ErrCacheMiss      = errors.New("cache: key is missing")
	ErrCacheKeyLocked = errors.New("cache: key is locked")
	CacheLockedValue  = "locked"
)

// ErrPatternSubscriptionUnsupported is returned when the underlying cache client cannot subscribe to key patterns.
var ErrPatternSubscriptionUnsupported = errors.New("cache: pattern subscriptions are not supported by this cache client")

type Item struct {
	Key             string
	Object          any
	CacheActionOpts CacheActionOpts
}

type CacheActionOpts struct {
	// Delete item from cache
	Delete bool
	// Disable writing if key already exists (NX)
	DisableOverwrite bool
	// Expiration is the cache expiration time.
	Expiration time.Duration
}

type CacheClient interface {
	Set(item *Item) error
	Rename(oldKey string, newKey string, expiration time.Duration) error
	Get(key string, obj any) error
	Delete(key string) error
	OnUpdated(ctx context.Context, key string, callback func() error) error
	NotifyUpdated(key string) error
}

// PatternSubscriber is implemented by cache clients which can deliver update notifications for every key matching a
// glob pattern (as understood by Redis PSUBSCRIBE), rather than for a single key.
type PatternSubscriber interface {
	// OnUpdatedPattern blocks until ctx is done, invoking callback with the full key of every notification whose key
	// matches pattern.
	OnUpdatedPattern(ctx context.Context, pattern string, callback func(key string) error) error
}
