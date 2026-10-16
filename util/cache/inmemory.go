package cache

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"path"
	"sync"
	"time"

	gocache "github.com/patrickmn/go-cache"
)

func NewInMemoryCache(expiration time.Duration) *InMemoryCache {
	return &InMemoryCache{
		memCache: gocache.New(expiration, 1*time.Minute),
	}
}

func init() {
	gob.Register([]any{})
}

// compile-time validation of adherence of the CacheClient contract
var (
	_ CacheClient       = &InMemoryCache{}
	_ PatternSubscriber = &InMemoryCache{}
)

type InMemoryCache struct {
	memCache *gocache.Cache

	subscribersLock sync.RWMutex
	subscribers     map[*patternSubscription]struct{}
}

type patternSubscription struct {
	pattern string
	keys    chan string
}

func (i *InMemoryCache) Set(item *Item) error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(item.Object)
	if err != nil {
		return err
	}
	if item.CacheActionOpts.DisableOverwrite {
		// go-redis doesn't throw an error on Set with NX, so absorbing here to keep the interface consistent
		_ = i.memCache.Add(item.Key, buf, item.CacheActionOpts.Expiration)
	} else {
		i.memCache.Set(item.Key, buf, item.CacheActionOpts.Expiration)
	}
	return nil
}

func (i *InMemoryCache) Rename(oldKey string, newKey string, expiration time.Duration) error {
	bufIf, found := i.memCache.Get(oldKey)
	if !found {
		return ErrCacheMiss
	}
	i.memCache.Set(newKey, bufIf, expiration)
	i.memCache.Delete(oldKey)
	return nil
}

// HasSame returns true if key with the same value already present in cache
func (i *InMemoryCache) HasSame(key string, obj any) (bool, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(obj)
	if err != nil {
		return false, err
	}

	bufIf, found := i.memCache.Get(key)
	if !found {
		return false, nil
	}
	existingBuf, ok := bufIf.(bytes.Buffer)
	if !ok {
		panic(fmt.Errorf("InMemoryCache has unexpected entry: %v", existingBuf))
	}
	return bytes.Equal(buf.Bytes(), existingBuf.Bytes()), nil
}

func (i *InMemoryCache) Get(key string, obj any) error {
	bufIf, found := i.memCache.Get(key)
	if !found {
		return ErrCacheMiss
	}
	buf := bufIf.(bytes.Buffer)
	return gob.NewDecoder(&buf).Decode(obj)
}

func (i *InMemoryCache) Delete(key string) error {
	i.memCache.Delete(key)
	return nil
}

func (i *InMemoryCache) Flush() {
	i.memCache.Flush()
}

func (i *InMemoryCache) OnUpdated(_ context.Context, _ string, _ func() error) error {
	return nil
}

// OnUpdatedPattern delivers in-process notifications published with NotifyUpdated for keys matching pattern.
func (i *InMemoryCache) OnUpdatedPattern(ctx context.Context, pattern string, callback func(key string) error) error {
	sub := &patternSubscription{pattern: pattern, keys: make(chan string, 100)}
	i.subscribersLock.Lock()
	if i.subscribers == nil {
		i.subscribers = map[*patternSubscription]struct{}{}
	}
	i.subscribers[sub] = struct{}{}
	i.subscribersLock.Unlock()
	defer func() {
		i.subscribersLock.Lock()
		delete(i.subscribers, sub)
		i.subscribersLock.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case key := <-sub.keys:
			if err := callback(key); err != nil {
				return err
			}
		}
	}
}

func (i *InMemoryCache) NotifyUpdated(key string) error {
	i.subscribersLock.RLock()
	defer i.subscribersLock.RUnlock()
	for sub := range i.subscribers {
		if matched, _ := path.Match(sub.pattern, key); matched {
			select {
			case sub.keys <- key:
			default:
				// Like Redis pub/sub, notifications are best effort: drop when the subscriber is not keeping up.
			}
		}
	}
	return nil
}

// Items return a list of items in the cache; requires passing a constructor function
// so that the items can be decoded from gob format.
func (i *InMemoryCache) Items(createNewObject func() any) (map[string]any, error) {
	result := map[string]any{}

	for key, value := range i.memCache.Items() {
		buf := value.Object.(bytes.Buffer)
		obj := createNewObject()
		err := gob.NewDecoder(&buf).Decode(obj)
		if err != nil {
			return nil, err
		}

		result[key] = obj
	}

	return result, nil
}
