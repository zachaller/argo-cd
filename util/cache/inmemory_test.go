package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type foo struct {
	Bar string
}

func TestInMemoryCache(t *testing.T) {
	t.Parallel()
	cache := NewInMemoryCache(1 * time.Hour)
	// https://stackoverflow.com/questions/46671636/gob-decode-giving-decodevalue-of-unassignable-value-error
	obj := &foo{}
	// cache miss
	err := cache.Get("my-key", obj)
	assert.Equal(t, ErrCacheMiss, err)
	// cache hit
	err = cache.Set(&Item{Key: "my-key", Object: &foo{Bar: "bar"}})
	require.NoError(t, err)
	err = cache.Get("my-key", obj)
	require.NoError(t, err)
	assert.Equal(t, &foo{Bar: "bar"}, obj)
}

func TestInMemoryCache_OnUpdatedPattern(t *testing.T) {
	cache := NewInMemoryCache(time.Hour)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	received := make(chan string, 10)
	done := make(chan error, 1)
	go func() {
		done <- cache.OnUpdatedPattern(ctx, "app|*|v1", func(key string) error {
			received <- key
			return nil
		})
	}()
	require.Eventually(t, func() bool {
		cache.subscribersLock.RLock()
		defer cache.subscribersLock.RUnlock()
		return len(cache.subscribers) == 1
	}, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, cache.NotifyUpdated("cluster|x|v1"))
	require.NoError(t, cache.NotifyUpdated("app|guestbook|v1"))
	select {
	case key := <-received:
		assert.Equal(t, "app|guestbook|v1", key)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for notification")
	}

	cancel()
	require.NoError(t, <-done)
	cache.subscribersLock.RLock()
	assert.Empty(t, cache.subscribers)
	cache.subscribersLock.RUnlock()
}
