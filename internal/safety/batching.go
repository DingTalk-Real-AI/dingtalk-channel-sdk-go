package safety

import (
	"context"
	"sync"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// MessageBatcher 消息批处理器（按会话分组）。
type MessageBatcher struct {
	cfg     types.BatchConfig
	handler types.BatchHandler

	mu        sync.RWMutex
	pipelines map[string]*chatPipeline
}

func NewMessageBatcher(cfg types.BatchConfig, handler types.BatchHandler) *MessageBatcher {
	return &MessageBatcher{
		cfg:       cfg,
		handler:   handler,
		pipelines: make(map[string]*chatPipeline),
	}
}

// Push 添加消息到批处理队列。
func (b *MessageBatcher) Push(ctx context.Context, msg *types.IncomingMessage) {
	scope := msg.ConversationID
	if scope == "" {
		scope = msg.MsgID
	}

	pipeline := b.getOrCreate(scope)
	pipeline.Push(ctx, msg)
}

func (b *MessageBatcher) getOrCreate(scope string) *chatPipeline {
	b.mu.Lock()
	defer b.mu.Unlock()

	if p, ok := b.pipelines[scope]; ok {
		return p
	}

	p := newChatPipeline(b.cfg, scope, b.handler)
	b.pipelines[scope] = p
	return p
}

// FlushAll 立即刷新所有批处理。
func (b *MessageBatcher) FlushAll(ctx context.Context) {
	b.mu.RLock()
	pipelines := make([]*chatPipeline, 0, len(b.pipelines))
	for _, p := range b.pipelines {
		pipelines = append(pipelines, p)
	}
	b.mu.RUnlock()

	var wg sync.WaitGroup
	for _, p := range pipelines {
		wg.Add(1)
		go func(pipeline *chatPipeline) {
			defer wg.Done()
			pipeline.FlushNow(ctx)
		}(p)
	}
	wg.Wait()
}

// Dispose 清理所有批处理。
func (b *MessageBatcher) Dispose() {
	b.FlushAll(context.Background())

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, p := range b.pipelines {
		p.Dispose()
	}
	b.pipelines = make(map[string]*chatPipeline)
}

// chatPipeline 单个会话的批处理管道。
type chatPipeline struct {
	cfg     types.BatchConfig
	scope   string
	handler types.BatchHandler

	mu          sync.Mutex
	buffer      []*types.IncomingMessage
	bufferChars int
	timer       *time.Timer
}

func newChatPipeline(cfg types.BatchConfig, scope string, handler types.BatchHandler) *chatPipeline {
	return &chatPipeline{
		cfg:     cfg,
		scope:   scope,
		handler: handler,
	}
}

// Push 添加消息到管道。
func (p *chatPipeline) Push(ctx context.Context, msg *types.IncomingMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.buffer = append(p.buffer, msg)
	p.bufferChars += len(msg.Text)

	// 检查是否达到阈值
	if len(p.buffer) >= p.cfg.MaxMessages || p.bufferChars >= p.cfg.MaxChars {
		p.clearTimer()
		go p.flush(ctx)
		return
	}

	// 设置延迟刷新
	p.clearTimer()
	delay := p.cfg.DelayMs
	if p.bufferChars >= p.cfg.LongThresholdChars {
		delay = p.cfg.LongDelayMs
	}

	p.timer = time.AfterFunc(delay, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.timer = nil
		go p.flush(ctx)
	})
}

// FlushNow 立即刷新并等待完成。
func (p *chatPipeline) FlushNow(ctx context.Context) {
	p.mu.Lock()
	if len(p.buffer) == 0 {
		p.mu.Unlock()
		return
	}
	p.clearTimer()
	p.mu.Unlock()

	// 同步执行 flush
	p.flush(ctx)
}

func (p *chatPipeline) flush(ctx context.Context) {
	p.mu.Lock()
	if len(p.buffer) == 0 {
		p.mu.Unlock()
		return
	}

	batch := p.buffer
	p.buffer = nil
	p.bufferChars = 0
	p.mu.Unlock()

	// 合并消息
	merged := mergeMessages(batch)
	sourceIDs := make([]string, len(batch))
	for i, m := range batch {
		sourceIDs[i] = m.MsgID
	}

	batched := &types.BatchedMessage{
		Message:   merged,
		SourceIDs: sourceIDs,
	}

	if p.handler != nil {
		_ = p.handler(ctx, batched)
	}
}

func (p *chatPipeline) clearTimer() {
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
}

// Dispose 清理管道。
func (p *chatPipeline) Dispose() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clearTimer()
	p.buffer = nil
}

// mergeMessages 合并多条消息为一条。
func mergeMessages(msgs []*types.IncomingMessage) *types.IncomingMessage {
	if len(msgs) == 0 {
		return nil
	}
	if len(msgs) == 1 {
		return msgs[0]
	}

	// 以最后一条为基础
	last := msgs[len(msgs)-1]
	merged := *last

	// 合并文本内容
	var texts []string
	for _, m := range msgs {
		if m.Text != "" {
			texts = append(texts, m.Text)
		}
	}
	if len(texts) > 0 {
		merged.Text = joinTexts(texts)
	}

	return &merged
}

// joinTexts 连接文本（用换行分隔）。
func joinTexts(texts []string) string {
	if len(texts) == 0 {
		return ""
	}
	if len(texts) == 1 {
		return texts[0]
	}
	result := texts[0]
	for i := 1; i < len(texts); i++ {
		result += "\n\n" + texts[i]
	}
	return result
}
