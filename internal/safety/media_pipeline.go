package safety

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// MediaPipelineManager 媒体消息批处理管理器。
// 用于合并连续上传的图片、文件、音视频等媒体消息。
//
// 设计原理：
// - 批次键：(chatID, msgType, replyTo) 三元组
// - 兼容运行：相同键、相同类型、在延迟窗口内
// - 不兼容推送：不同类型或文本消息介入时，先刷新当前批次
// - 容量上限：达到 max_items 时立即刷新
//
// 输出：合并后的 IncomingMessage，batched_sources 字段包含所有原始消息。
type MediaPipelineManager struct {
	mu       sync.Mutex
	cfg      types.MediaBatchConfig
	buckets  map[string]*mediaBucket // key: batchKey
	handler  MediaFlushHandler
}

// MediaFlushHandler 媒体批次刷新回调
type MediaFlushHandler func(ctx context.Context, merged *types.IncomingMessage) error

// mediaBucket 媒体批次桶
type mediaBucket struct {
	sources []*types.IncomingMessage
	timer   *time.Timer
}

// NewMediaPipelineManager 创建媒体批处理管理器
func NewMediaPipelineManager(cfg types.MediaBatchConfig) *MediaPipelineManager {
	fillMediaBatchConfig(&cfg)
	
	return &MediaPipelineManager{
		cfg:     cfg,
		buckets: make(map[string]*mediaBucket),
	}
}

// fillMediaBatchConfig 填充默认配置
func fillMediaBatchConfig(cfg *types.MediaBatchConfig) {
	if cfg.DelayMs <= 0 {
		cfg.DelayMs = 800 // 默认 800ms
	}
	if cfg.MaxItems <= 0 {
		cfg.MaxItems = 9 // 默认 9 个
	}
}

// IsCompatible 检查消息是否为可批处理的媒体类型
func (m *MediaPipelineManager) IsCompatible(msg *types.IncomingMessage) bool {
	if !m.cfg.Enabled {
		return false
	}
	// 钉钉支持的媒体类型：picture, file, audio, video
	switch msg.MsgType {
	case "picture", "file", "audio", "video":
		return true
	default:
		return false
	}
}

// Push 添加消息到批次（如果达到容量上限则立即刷新）
func (m *MediaPipelineManager) Push(ctx context.Context, msg *types.IncomingMessage, handler MediaFlushHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	m.handler = handler
	key := batchKey(msg)
	bucket, exists := m.buckets[key]
	
	if !exists {
		bucket = &mediaBucket{
			sources: make([]*types.IncomingMessage, 0, m.cfg.MaxItems),
		}
		m.buckets[key] = bucket
	}
	
	// 添加到批次
	bucket.sources = append(bucket.sources, msg)
	
	// 达到容量上限，立即刷新
	if len(bucket.sources) >= m.cfg.MaxItems {
		m.cancelTimer(bucket)
		m.flushBucket(ctx, key)
		return
	}
	
	// 重置定时器
	m.cancelTimer(bucket)
	bucket.timer = time.AfterFunc(time.Duration(m.cfg.DelayMs)*time.Millisecond, func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.flushBucket(ctx, key)
	})
}

// FlushIncompatibleFor 刷新与当前消息不兼容的批次
// 当非媒体消息到达时，刷新该会话的所有待处理批次，保证消息顺序
func (m *MediaPipelineManager) FlushIncompatibleFor(ctx context.Context, msg *types.IncomingMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	chatID := msg.ConversationID
	keysToFlush := make([]string, 0)
	
	// 找到所有属于该会话的批次（首段精确匹配，避免 chatID 前缀碰撞）
	for k := range m.buckets {
		if isChatKey(k, chatID) {
			keysToFlush = append(keysToFlush, k)
		}
	}
	
	// 刷新所有找到的批次
	for _, k := range keysToFlush {
		m.flushBucket(ctx, k)
	}
}

// flushBucket 刷新指定批次（调用前需持有锁）
func (m *MediaPipelineManager) flushBucket(ctx context.Context, key string) {
	bucket, exists := m.buckets[key]
	if !exists || len(bucket.sources) == 0 {
		return
	}
	
	// 从 map 中移除
	delete(m.buckets, key)
	
	// 取消定时器
	m.cancelTimer(bucket)
	
	// 合并消息
	merged := mergeBatchSources(bucket.sources)
	
	// 异步调用 handler（释放锁后）
	if m.handler != nil {
		go func() {
			_ = m.handler(ctx, merged) // 忽略错误，best-effort
		}()
	}
}

// cancelTimer 取消定时器
func (m *MediaPipelineManager) cancelTimer(bucket *mediaBucket) {
	if bucket.timer != nil {
		bucket.timer.Stop()
		bucket.timer = nil
	}
}

// Dispose 释放资源，刷新所有待处理批次
func (m *MediaPipelineManager) Dispose(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// 刷新所有批次
	for key := range m.buckets {
		m.flushBucket(ctx, key)
	}
}

// keySep 批次键分隔符：使用不可见字符，避免 chatID 前缀碰撞
// （如 "cid-1" 与 "cid-12" 用 ":" 分隔时前缀匹配会误判同会话）。
const keySep = "\x00"

// batchKey 计算批次键：(chatID, msgType, replyParent) 三元组。
// 相同键的消息合并到一个批次；reply 消息按被引用内容区分，
// 不同引用目标不会互相合并（批次键含引用上下文，钉钉无话题场景故不含 thread）。
func batchKey(msg *types.IncomingMessage) string {
	key := msg.ConversationID + keySep + msg.MsgType
	if msg.MsgType == "reply" && len(msg.Content) > 0 {
		sum := sha256.Sum256(msg.Content)
		key += keySep + hex.EncodeToString(sum[:8])
	}
	return key
}

// isChatKey 判断批次键是否属于指定会话（按完整首段精确匹配）。
func isChatKey(key, chatID string) bool {
	idx := strings.Index(key, keySep)
	if idx < 0 {
		return key == chatID
	}
	return key[:idx] == chatID
}

// mergeBatchSources 合并批次中的消息
// 使用最后一条消息作为载体（最新的元数据），将所有源消息附加到 BatchedSources
func mergeBatchSources(sources []*types.IncomingMessage) *types.IncomingMessage {
	if len(sources) == 1 {
		return sources[0]
	}
	
	// 使用最后一条消息作为基础
	last := sources[len(sources)-1]
	
	// 复制一份，避免修改原消息
	merged := &types.IncomingMessage{}
	*merged = *last
	
	// 合并所有 Resources
	allResources := make([]types.Resource, 0)
	for _, msg := range sources {
		allResources = append(allResources, msg.Resources...)
	}
	merged.Resources = allResources
	
	// 更新文本提示
	count := len(sources)
	kindName := kindDisplayName(merged.MsgType)
	merged.Text = formatBatchText(count, kindName)

	// 保留原始消息列表（用于释放处理锁、逐条标记等后续处理）
	merged.BatchedSources = sources

	return merged
}

// kindDisplayName 获取媒体类型的显示名称
func kindDisplayName(msgType string) string {
	switch msgType {
	case "picture":
		return "图片"
	case "file":
		return "文件"
	case "audio":
		return "语音"
	case "video":
		return "视频"
	default:
		return "媒体"
	}
}

// formatBatchText 格式化批次文本提示
func formatBatchText(count int, kindName string) string {
	if count == 1 {
		return "[" + kindName + "]"
	}
	return "[" + string(rune(count+'0')) + "个" + kindName + "]"
}
