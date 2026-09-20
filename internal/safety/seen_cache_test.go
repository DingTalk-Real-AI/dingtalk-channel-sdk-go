package safety

import (
	"container/list"
	"sync"
	"testing"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// mockRedis 简单的 mock Redis 客户端
type mockRedis struct {
	mu   sync.Mutex
	data map[string]mockRedisEntry
}

type mockRedisEntry struct {
	value     string
	expiresAt time.Time
}

func newMockRedis() *mockRedis {
	return &mockRedis{
		data: make(map[string]mockRedisEntry),
	}
}

func (m *mockRedis) Exists(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	entry, ok := m.data[key]
	if !ok {
		return false
	}
	if time.Now().After(entry.expiresAt) {
		delete(m.data, key)
		return false
	}
	return true
}

func (m *mockRedis) SetEx(key string, value string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.data[key] = mockRedisEntry{
		value:     value,
		expiresAt: time.Now().Add(ttl),
	}
	return nil
}

func (m *mockRedis) Close() error {
	return nil
}

func TestSeenCache_Basic(t *testing.T) {
	cfg := types.DedupConfig{
		TTL:           5 * time.Minute,
		MaxEntries:    10,
		SweepInterval: 1 * time.Second,
	}
	cache := NewSeenCache(cfg, nil, "")
	defer cache.Dispose()

	// 首次添加
	if cache.Has("key1") {
		t.Error("expected key1 not found initially")
	}

	cache.Add("key1")

	// 再次检查应命中
	if !cache.Has("key1") {
		t.Error("expected key1 to be found after Add")
	}

	// 不同的键
	if cache.Has("key2") {
		t.Error("expected key2 not found")
	}
}

func TestSeenCache_CheckAndMark(t *testing.T) {
	cfg := types.DedupConfig{
		TTL:        5 * time.Minute,
		MaxEntries: 10,
	}
	cache := NewSeenCache(cfg, nil, "")
	defer cache.Dispose()

	// 首次检查并标记 - 返回 false（首次出现）
	if cache.CheckAndMark("msg1") {
		t.Error("expected CheckAndMark to return false for first occurrence")
	}

	// 再次检查并标记 - 返回 true（重复）
	if !cache.CheckAndMark("msg1") {
		t.Error("expected CheckAndMark to return true for duplicate")
	}
}

func TestSeenCache_MultipleKeys(t *testing.T) {
	cfg := types.DedupConfig{
		TTL:        5 * time.Minute,
		MaxEntries: 10,
	}
	cache := NewSeenCache(cfg, nil, "")
	defer cache.Dispose()

	// 添加多个键
	cache.Add("proto1", "msg1", "fp:abc123")

	// 任一键命中即返回 true
	if !cache.Has("proto1") {
		t.Error("expected proto1 to be found")
	}
	if !cache.Has("msg1") {
		t.Error("expected msg1 to be found")
	}
	if !cache.Has("fp:abc123") {
		t.Error("expected fingerprint to be found")
	}
	if !cache.Has("proto1", "msg1", "fp:abc123") {
		t.Error("expected any of the keys to be found")
	}

	// 不存在的键
	if cache.Has("other") {
		t.Error("expected other key not found")
	}
}

func TestSeenCache_TTL(t *testing.T) {
	cfg := types.DedupConfig{
		TTL:        100 * time.Millisecond, // 短 TTL 用于测试
		MaxEntries: 10,
	}
	cache := NewSeenCache(cfg, nil, "")
	defer cache.Dispose()

	cache.Add("short-lived")

	// 立即检查应存在
	if !cache.Has("short-lived") {
		t.Error("expected key to exist immediately after Add")
	}

	// 等待超过 TTL
	time.Sleep(150 * time.Millisecond)

	// 应该过期
	if cache.Has("short-lived") {
		t.Error("expected key to be expired after TTL")
	}
}

func TestSeenCache_LRUEviction(t *testing.T) {
	cfg := types.DedupConfig{
		TTL:        10 * time.Minute,
		MaxEntries: 3, // 小容量测试 LRU
	}
	cache := NewSeenCache(cfg, nil, "")
	defer cache.Dispose()

	// 添加 3 个条目（满容量）
	cache.Add("key1")
	cache.Add("key2")
	cache.Add("key3")

	// 全部应存在
	if !cache.Has("key1") || !cache.Has("key2") || !cache.Has("key3") {
		t.Error("expected all 3 keys to exist")
	}

	// 添加第 4 个条目，应驱逐最旧的 key1
	cache.Add("key4")

	if cache.Has("key1") {
		t.Error("expected key1 to be evicted (oldest)")
	}
	if !cache.Has("key2") || !cache.Has("key3") || !cache.Has("key4") {
		t.Error("expected key2, key3, key4 to exist")
	}
}

func TestSeenCache_Redis(t *testing.T) {
	redis := newMockRedis()
	cfg := types.DedupConfig{
		TTL:        5 * time.Minute,
		MaxEntries: 10,
	}
	cache := NewSeenCache(cfg, redis, "test:")
	defer cache.Dispose()

	// 添加到缓存（应同时写入 Redis）
	cache.Add("key1")

	// 验证 Redis 中存在
	if !redis.Exists("test:key1") {
		t.Error("expected key to exist in Redis")
	}

	// 清空内存缓存，模拟重启
	cache.mu.Lock()
	cache.items = make(map[string]*list.Element)
	cache.evictList = list.New()
	cache.mu.Unlock()

	// 内存中不存在
	cache.mu.Lock()
	_, memExists := cache.items["key1"]
	cache.mu.Unlock()
	if memExists {
		t.Error("expected key1 not in memory after clear")
	}

	// Has 应从 Redis 恢复并返回 true
	if !cache.Has("key1") {
		t.Error("expected key1 to be found via Redis")
	}

	// 验证回填到内存
	cache.mu.Lock()
	_, memExists = cache.items["key1"]
	cache.mu.Unlock()
	if !memExists {
		t.Error("expected key1 to be back-filled into memory")
	}
}

func TestSeenCache_Sweep(t *testing.T) {
	cfg := types.DedupConfig{
		TTL:           50 * time.Millisecond,
		MaxEntries:    10,
		SweepInterval: 100 * time.Millisecond,
	}
	cache := NewSeenCache(cfg, nil, "")
	defer cache.Dispose()

	// 添加多个条目
	cache.Add("key1", "key2", "key3")

	// 立即存在
	if !cache.Has("key1") {
		t.Error("expected key1 to exist")
	}

	// 等待过期 + sweep
	time.Sleep(200 * time.Millisecond)

	// sweep 后应清理
	cache.mu.Lock()
	count := len(cache.items)
	cache.mu.Unlock()

	if count > 0 {
		t.Errorf("expected all keys to be swept, but %d remain", count)
	}
}

func TestSeenCache_Disabled(t *testing.T) {
	cfg := types.DedupConfig{
		TTL:        5 * time.Minute,
		MaxEntries: 10,
	}
	cache := NewSeenCache(cfg, nil, "")
	defer cache.Dispose()

	// 添加和检查正常工作（去重总是启用的，通过 cfg 控制）
	cache.Add("key1")
	if !cache.Has("key1") {
		t.Error("expected key1 to be found")
	}
}

func TestContentFingerprint(t *testing.T) {
	fp1 := ContentFingerprint("chat1", 1234567890000, "text", "hello")
	fp2 := ContentFingerprint("chat1", 1234567890000, "text", "hello")
	fp3 := ContentFingerprint("chat1", 1234567890000, "text", "world")

	// 相同输入应产生相同指纹
	if fp1 != fp2 {
		t.Errorf("expected identical fingerprints, got %s != %s", fp1, fp2)
	}

	// 不同内容应产生不同指纹
	if fp1 == fp3 {
		t.Errorf("expected different fingerprints for different content")
	}

	// 指纹应以 "fp:" 前缀
	if len(fp1) < 3 || fp1[:3] != "fp:" {
		t.Errorf("expected fingerprint to start with 'fp:', got %s", fp1)
	}
}

func TestSeenCache_EmptyKeys(t *testing.T) {
	cfg := types.DedupConfig{
		TTL:        5 * time.Minute,
		MaxEntries: 10,
	}
	cache := NewSeenCache(cfg, nil, "")
	defer cache.Dispose()

	// 空键应被忽略
	cache.Add("", "key1", "")
	
	if !cache.Has("key1") {
		t.Error("expected key1 to be found")
	}
	
	// Has 空键应返回 false
	if cache.Has("") {
		t.Error("expected empty key to not match")
	}
	
	// CheckAndMark 空键
	if cache.CheckAndMark("", "", "") {
		t.Error("expected CheckAndMark with only empty keys to return false")
	}
}
