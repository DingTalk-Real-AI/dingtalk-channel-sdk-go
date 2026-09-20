package safety

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// ChatQueue 管理单个 scope（通常是 conversationID）的批处理 + 串行队列。
//
// 两种模式：
// - serialOnly=false: 完整的批处理 + 串行（用于消息）
// - serialOnly=true: 只串行不批处理（用于 cardAction 等事件）
type ChatQueue struct {
	mu             sync.Mutex
	scope          string
	batchCfg       *types.BatchConfig
	mediaBatch     *types.MediaBatchConfig
	serialOnly     bool
	buffer         []*types.IncomingMessage
	bufferChars    int
	timer          *time.Timer
	pendingHandler types.FlushHandler

	// 串行任务队列
	tasks  chan func()
	stopCh chan struct{}
	closed bool
}

// NewChatQueue 创建新的 ChatQueue。
func NewChatQueue(scope string, batchCfg *types.BatchConfig, mediaBatch *types.MediaBatchConfig, serialOnly bool) *ChatQueue {
	cq := &ChatQueue{
		scope:      scope,
		batchCfg:   batchCfg,
		mediaBatch: mediaBatch,
		serialOnly: serialOnly,
		tasks:      make(chan func(), 100),
		stopCh:     make(chan struct{}),
	}
	go cq.worker()
	return cq
}

func (cq *ChatQueue) worker() {
	for {
		select {
		case task, ok := <-cq.tasks:
			if !ok {
				return
			}
			task()
		case <-cq.stopCh:
			return
		}
	}
}

// Push 将消息压入批处理缓冲区（仅用于 serialOnly=false 模式）。
func (cq *ChatQueue) Push(ctx context.Context, msg *types.IncomingMessage, handler types.FlushHandler) {
	cq.mu.Lock()
	defer cq.mu.Unlock()

	cq.buffer = append(cq.buffer, msg)
	cq.bufferChars += len(msg.Text)
	if cq.pendingHandler == nil {
		cq.pendingHandler = handler
	}

	// 达到上限立即刷新
	if len(cq.buffer) >= cq.batchCfg.MaxMessages || cq.bufferChars >= cq.batchCfg.MaxChars {
		cq.clearTimer()
		cq.enqueueFlush(ctx)
		return
	}

	// DelayMs=0 或 serialOnly 模式立即刷新
	if cq.batchCfg.DelayMs <= 0 || cq.serialOnly {
		cq.clearTimer()
		cq.enqueueFlush(ctx)
		return
	}

	// Debounce 定时器：长消息用更长的延迟；媒体批处理启用时媒体用媒体窗口
	cq.clearTimer()
	delay := cq.batchCfg.DelayMs
	if cq.bufferChars >= cq.batchCfg.LongThresholdChars {
		delay = cq.batchCfg.LongDelayMs
	}
	if cq.mediaBatch != nil && cq.mediaBatch.Enabled && len(msg.Resources) > 0 {
		delay = time.Duration(cq.mediaBatch.DelayMs) * time.Millisecond
	}

	cq.timer = time.AfterFunc(delay, func() {
		cq.mu.Lock()
		defer cq.mu.Unlock()
		cq.timer = nil
		cq.enqueueFlush(ctx)
	})
}

// Run 串行执行任务（用于非批处理事件，如 cardAction）。
func (cq *ChatQueue) Run(ctx context.Context, task func() error) error {
	// 先刷新待处理的批处理
	cq.mu.Lock()
	if len(cq.buffer) > 0 {
		cq.clearTimer()
		cq.enqueueFlush(ctx)
	}
	cq.mu.Unlock()

	// 串行执行任务
	errCh := make(chan error, 1)
	cq.tasks <- func() {
		errCh <- task()
	}
	return <-errCh
}

// FlushNow 立即刷新缓冲区并等待队列清空。
func (cq *ChatQueue) FlushNow(ctx context.Context) {
	cq.mu.Lock()
	if len(cq.buffer) > 0 {
		cq.clearTimer()
		cq.enqueueFlush(ctx)
	}
	cq.mu.Unlock()

	// 等待队列清空
	done := make(chan struct{})
	cq.tasks <- func() {
		close(done)
	}
	<-done
}

// Dispose 释放资源。
func (cq *ChatQueue) Dispose() {
	cq.mu.Lock()
	defer cq.mu.Unlock()
	if cq.closed {
		return
	}
	cq.closed = true
	cq.clearTimer()
	close(cq.stopCh)
	close(cq.tasks)
}

func (cq *ChatQueue) clearTimer() {
	if cq.timer != nil {
		cq.timer.Stop()
		cq.timer = nil
	}
}

func (cq *ChatQueue) enqueueFlush(ctx context.Context) {
	if len(cq.buffer) == 0 {
		return
	}

	batch := cq.buffer
	handler := cq.pendingHandler

	cq.buffer = nil
	cq.bufferChars = 0
	cq.pendingHandler = nil

	if handler == nil {
		return
	}

	// serialOnly 模式：逐条分发，不合并
	if cq.serialOnly {
		for _, msg := range batch {
			dispatch := &types.BatchedDispatch{
				Message:   msg,
				SourceIDs: []string{msg.MsgID},
			}
			cq.tasks <- func() {
				_ = handler(ctx, dispatch)
			}
		}
		return
	}

	// 正常批处理模式：合并消息
	dispatch := &types.BatchedDispatch{
		Message:   mergeBatchMessages(batch),
		SourceIDs: extractMessageIDs(batch),
	}

	cq.tasks <- func() {
		_ = handler(ctx, dispatch)
	}
}

func mergeBatchMessages(batch []*types.IncomingMessage) *types.IncomingMessage {
	if len(batch) == 1 {
		return batch[0]
	}

	last := batch[len(batch)-1]

	var contents []string
	for _, m := range batch {
		if m.Text != "" {
			contents = append(contents, m.Text)
		}
	}
	content := strings.Join(contents, "\n\n")

	// 合并 mentions 和 resources
	var mentionAll bool
	var resources []types.Resource
	var mentions []types.Mention

	seenResources := make(map[string]bool)
	seenMentions := make(map[string]bool)

	for _, m := range batch {
		// 检查 @all
		if m.MentionAll {
			mentionAll = true
		}

		// 合并 resources
		for _, r := range m.Resources {
			if !seenResources[r.DownloadCode] {
				seenResources[r.DownloadCode] = true
				resources = append(resources, r)
			}
		}

		// 合并 mentions
		for _, mention := range m.Mentions {
			key := mention.UserID
			if key == "" {
				key = mention.Name
			}
			if !seenMentions[key] {
				seenMentions[key] = true
				mentions = append(mentions, mention)
			}
		}
	}

	merged := *last
	merged.Text = content
	merged.MentionAll = mentionAll
	merged.Resources = resources
	merged.Mentions = mentions

	return &merged
}

func extractMessageIDs(batch []*types.IncomingMessage) []string {
	ids := make([]string, len(batch))
	for i, m := range batch {
		ids[i] = m.MsgID
	}
	return ids
}

// ChatQueueManager 管理多个 ChatQueue，按 scope 索引。
type ChatQueueManager struct {
	mu         sync.RWMutex
	batchCfg   *types.BatchConfig
	mediaBatch *types.MediaBatchConfig
	queueCfg   *types.ChatQueueConfig
	queues     map[string]*ChatQueue
}

// NewChatQueueManager 创建新的 ChatQueueManager。
func NewChatQueueManager(batchCfg *types.BatchConfig, queueCfg *types.ChatQueueConfig, mediaBatch *types.MediaBatchConfig) *ChatQueueManager {
	if queueCfg == nil {
		queueCfg = &types.ChatQueueConfig{Enabled: false}
	}
	return &ChatQueueManager{
		batchCfg:   batchCfg,
		mediaBatch: mediaBatch,
		queueCfg:   queueCfg,
		queues:     make(map[string]*ChatQueue),
	}
}

func (cqm *ChatQueueManager) getOrCreate(scope string, serialOnly bool) *ChatQueue {
	cqm.mu.RLock()
	q, ok := cqm.queues[scope]
	cqm.mu.RUnlock()

	if ok {
		return q
	}

	cqm.mu.Lock()
	defer cqm.mu.Unlock()

	// Double-check
	q, ok = cqm.queues[scope]
	if ok {
		return q
	}

	q = NewChatQueue(scope, cqm.batchCfg, cqm.mediaBatch, serialOnly)
	cqm.queues[scope] = q
	return q
}

// Push 将消息压入指定 scope 的批处理队列。
func (cqm *ChatQueueManager) Push(ctx context.Context, scope string, msg *types.IncomingMessage, handler types.FlushHandler) {
	cqm.getOrCreate(scope, false).Push(ctx, msg, handler)
}

// Run 在指定 scope 串行执行任务。
func (cqm *ChatQueueManager) Run(ctx context.Context, scope string, task func() error) error {
	return cqm.getOrCreate(scope, true).Run(ctx, task)
}

// FlushAll 刷新所有队列。
func (cqm *ChatQueueManager) FlushAll(ctx context.Context) {
	cqm.mu.RLock()
	queues := make([]*ChatQueue, 0, len(cqm.queues))
	for _, q := range cqm.queues {
		queues = append(queues, q)
	}
	cqm.mu.RUnlock()

	var wg sync.WaitGroup
	for _, q := range queues {
		wg.Add(1)
		go func(queue *ChatQueue) {
			defer wg.Done()
			queue.FlushNow(ctx)
		}(q)
	}
	wg.Wait()
}

// Dispose 释放所有队列资源。
func (cqm *ChatQueueManager) Dispose() {
	cqm.FlushAll(context.Background())

	cqm.mu.Lock()
	defer cqm.mu.Unlock()

	for _, q := range cqm.queues {
		q.Dispose()
	}
	cqm.queues = make(map[string]*ChatQueue)
}

// Enabled 返回队列是否启用。
func (cqm *ChatQueueManager) Enabled() bool {
	return cqm.queueCfg.Enabled
}
