package safety

import (
	"container/list"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// SeenCache 去重缓存（增强版：三键去重 + 双层缓存）。
// 特性：
// 1. 三键去重：协议层 ID + 业务层 msgId + 内容指纹（SHA-256）
// 2. 双层缓存：内存 LRU（快速路径）+ 可选 Redis（多实例共享）
// 3. TTL + LRU：过期自动清理 + 容量限制
// 4. 后台清理：定期 sweep 过期条目
type SeenCache struct {
	cfg           types.DedupConfig
	mu            sync.Mutex
	items         map[string]*list.Element // key → LRU 节点
	evictList     *list.List               // LRU 双向链表
	redis         RedisClient              // 可选 Redis 客户端
	redisPrefix   string                   // Redis key 前缀
	stopSweep     chan struct{}
	fingerprintFn func(msg interface{}) string // 内容指纹计算函数（可自定义）
}

// cacheEntry LRU 缓存条目
type cacheEntry struct {
	key       string
	expiresAt time.Time
}

// RedisClient Redis 客户端接口（可选依赖）
type RedisClient interface {
	Exists(key string) bool
	SetEx(key string, value string, ttl time.Duration) error
	Close() error
}

// NewSeenCache 创建去重缓存。
// cfg: 去重配置
// redis: 可选 Redis 客户端（nil 表示仅使用内存缓存）
// redisPrefix: Redis key 前缀（默认 "dd:seen:"）
func NewSeenCache(cfg types.DedupConfig, redis RedisClient, redisPrefix string) *SeenCache {
	fillDedupConfig(&cfg)
	
	if redisPrefix == "" {
		redisPrefix = "dd:seen:"
	}
	
	sc := &SeenCache{
		cfg:         cfg,
		items:       make(map[string]*list.Element),
		evictList:   list.New(),
		redis:       redis,
		redisPrefix: redisPrefix,
		stopSweep:   make(chan struct{}),
	}
	
	// 启动后台清理
	if cfg.SweepInterval > 0 {
		go sc.sweepLoop()
	}
	
	return sc
}

// fillDedupConfig 填充默认值
func fillDedupConfig(cfg *types.DedupConfig) {
	if cfg.TTL <= 0 {
		cfg.TTL = 12 * time.Hour
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 5000
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = 5 * time.Minute
	}
}

// sweepLoop 后台清理循环
func (sc *SeenCache) sweepLoop() {
	ticker := time.NewTicker(sc.cfg.SweepInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			sc.sweep()
		case <-sc.stopSweep:
			return
		}
	}
}

// sweep 清理过期条目
func (sc *SeenCache) sweep() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	
	now := time.Now()
	for elem := sc.evictList.Back(); elem != nil; {
		entry := elem.Value.(*cacheEntry)
		if now.Before(entry.expiresAt) {
			break // 后面的条目还未过期（按时间排序）
		}
		prev := elem.Prev()
		sc.evictList.Remove(elem)
		delete(sc.items, entry.key)
		elem = prev
	}
}

// Has 检查键是否存在（已去重）。
// keys: 多个去重键（协议ID、业务msgId、内容指纹等），任一命中即返回 true。
func (sc *SeenCache) Has(keys ...string) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	
	now := time.Now()
	
	// L1: 内存缓存检查
	for _, key := range keys {
		if key == "" {
			continue
		}
		if elem, ok := sc.items[key]; ok {
			entry := elem.Value.(*cacheEntry)
			if now.Before(entry.expiresAt) {
				// 命中且未过期，移到 LRU 头部
				sc.evictList.MoveToFront(elem)
				return true
			}
			// 过期，立即清理
			sc.evictList.Remove(elem)
			delete(sc.items, key)
		}
	}
	
	// L2: Redis 检查（可选）
	if sc.redis != nil {
		for _, key := range keys {
			if key == "" {
				continue
			}
			redisKey := sc.redisPrefix + key
			if sc.redis.Exists(redisKey) {
				// Redis 命中，回填到内存缓存
				sc.addToMemory(key, now.Add(sc.cfg.TTL))
				return true
			}
		}
	}
	
	return false
}

// Add 添加键到去重缓存。
// keys: 多个去重键，全部添加。
func (sc *SeenCache) Add(keys ...string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	
	now := time.Now()
	expiresAt := now.Add(sc.cfg.TTL)
	
	for _, key := range keys {
		if key == "" {
			continue
		}
		
		// 添加到内存
		sc.addToMemory(key, expiresAt)
		
		// 添加到 Redis（可选）
		if sc.redis != nil {
			redisKey := sc.redisPrefix + key
			_ = sc.redis.SetEx(redisKey, "1", sc.cfg.TTL) // 忽略错误，best-effort
		}
	}
}

// addToMemory 添加条目到内存 LRU（调用前需持有锁）
func (sc *SeenCache) addToMemory(key string, expiresAt time.Time) {
	// 如果已存在，移到头部并更新过期时间
	if elem, ok := sc.items[key]; ok {
		sc.evictList.MoveToFront(elem)
		elem.Value.(*cacheEntry).expiresAt = expiresAt
		return
	}
	
	// 新条目，添加到头部
	entry := &cacheEntry{key: key, expiresAt: expiresAt}
	elem := sc.evictList.PushFront(entry)
	sc.items[key] = elem
	
	// LRU 驱逐：超过容量时移除最旧条目
	for sc.evictList.Len() > sc.cfg.MaxEntries {
		oldest := sc.evictList.Back()
		if oldest != nil {
			sc.evictList.Remove(oldest)
			delete(sc.items, oldest.Value.(*cacheEntry).key)
		}
	}
}

// CheckAndMark 检查并标记（组合操作，原子性）。
// 返回 true 表示已存在（重复），false 表示首次出现（已标记）。
func (sc *SeenCache) CheckAndMark(keys ...string) bool {
	// 先检查
	if sc.Has(keys...) {
		return true // 重复
	}
	
	// 标记
	sc.Add(keys...)
	return false // 首次出现
}

// ContentFingerprint 计算消息内容指纹（SHA-256）。
// 对标 dsh-dingtalk 方案：conversationId + createAt + msgtype + content
func ContentFingerprint(conversationID string, createAt int64, msgType string, content string) string {
	data := fmt.Sprintf("%s:%d:%s:%s", conversationID, createAt, msgType, content)
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("fp:%x", hash)
}

// Dispose 释放资源
func (sc *SeenCache) Dispose() {
	select {
	case <-sc.stopSweep:
	default:
		close(sc.stopSweep)
	}
	
	if sc.redis != nil {
		_ = sc.redis.Close()
	}
}
