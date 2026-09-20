package safety

import (
	"context"
	"sync"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// SafetyPipeline 安全管线统一门面，整合所有安全组件到单一入口。
//
// 三层推送接口：
// - PushMessage: 完整管线（过期 → 去重 → 自回复 → 策略 → 锁 → 队列/批处理）
// - PushAction:  简化管线（去重 → 锁 → 串行）用于卡片回调等事件
// - PushLight:   最简管线（仅去重）用于 reaction 等轻量事件
//
// 队列集成：注入 ChatQueueManager 后，消息按会话串行（注册 OnMessage）
// 或延迟合并（注册 OnBatch，媒体资源在窗口内自动合并）；
// 未注入时退化为直接调用，媒体消息走 MediaPipelineManager 合并。
type SafetyPipeline struct {
	mu sync.RWMutex

	// 安全组件
	stale     *StaleDetector
	seen      *SeenCache
	lock      *ProcessingLock
	policy    *PolicyGate
	chatQueue *ChatQueueManager     // 文本批处理 + per-chat 串行队列（可选注入）
	media     *MediaPipelineManager // 媒体批处理（无队列时的兜底路径）

	// 配置
	dropSelfSent     bool
	botRobotCode     string // 机器人 RobotCode，用于自回复过滤
	markAfterHandler bool   // 处理完成后才写入 seen（失败可重投）

	// 回调
	onMessage   MessageHandler
	onBatch     BatchDispatchHandler
	hasOnBatch  func() bool
	onReject    RejectHandler
}

// batchMode 分发时判断是否走批处理路径：OnBatch 已提供且（动态探测）已注册。
func (p *SafetyPipeline) batchMode() bool {
	if p.onBatch == nil {
		return false
	}
	if p.hasOnBatch != nil {
		return p.hasOnBatch()
	}
	return true
}

// MessageHandler 消息处理回调；sources 为合并批次的原始消息（单条时长度为 1）。
type MessageHandler func(ctx context.Context, msg *types.IncomingMessage, sources []*types.IncomingMessage) error

// BatchDispatchHandler 批处理分发回调（OnBatch 路径）。
type BatchDispatchHandler func(ctx context.Context, batch *types.BatchedMessage) error

// RejectHandler 拒绝事件回调。
type RejectHandler func(ctx context.Context, event *types.RejectEvent)

// PipelineOptions SafetyPipeline 选项。
// OnMessage/OnBatch 均为闭包时，Channel 可在构造后动态注册处理器；
// HasOnBatch 用于在分发时动态判断 OnBatch 是否已注册（决定批处理/串行路径）。
type PipelineOptions struct {
	OnMessage    MessageHandler
	OnBatch      BatchDispatchHandler
	OnReject     RejectHandler
	ChatQueue    *ChatQueueManager // 可选：注入后启用 per-chat 串行 + 批处理
	HasOnBatch   func() bool       // 可选：动态探测 OnBatch 是否已注册
	BotRobotCode string            // 机器人 RobotCode（自回复过滤）
}

// NewSafetyPipeline 创建安全管线。
func NewSafetyPipeline(cfg types.SafetyConfig, opts PipelineOptions) *SafetyPipeline {
	if cfg.StaleWindow == 0 {
		cfg = types.DefaultSafetyConfig()
	}

	p := &SafetyPipeline{
		stale:        NewStaleDetector(cfg.StaleWindow),
		seen:         NewSeenCache(cfg.Dedup, nil, ""),
		lock:         NewProcessingLock(cfg.LockTTL, time.Minute),
		policy:       NewPolicyGate(cfg.Policy),
		chatQueue:    opts.ChatQueue,
		media:        NewMediaPipelineManager(cfg.MediaBatch),
		dropSelfSent:     cfg.DropSelfSent,
		botRobotCode:     opts.BotRobotCode,
		markAfterHandler: cfg.MarkAfterHandler,
		onMessage:    opts.OnMessage,
		onBatch:      opts.OnBatch,
		hasOnBatch:   opts.HasOnBatch,
		onReject:     opts.OnReject,
	}
	return p
}

// PushMessage 推送消息到完整安全管线。
// 每条消息按顺序经过：过期检测 → 三键去重 → 自回复过滤 → 策略门控 →
// 处理锁 → 队列（串行/批处理，含媒体合并）或直接/媒体批处理分发。
// 任一环节拒绝即返回并通过 OnReject 上报原因。
func (p *SafetyPipeline) PushMessage(ctx context.Context, protoID string, msg *types.IncomingMessage) {
	// 1. 过期检测
	if p.stale.IsStale(msg.CreateAt) {
		p.emitReject(ctx, msg, types.RejectReasonStale)
		return
	}

	// 2. 去重：协议层投递 ID + 业务 msgId；时间戳与内容齐备时叠加内容指纹，
	//    防止网关换投递 ID 重放同一条消息。createAt 缺失（0）时指纹无区分度，跳过。
	//    MarkAfterHandler 模式下入口只查不写，handler 完成后才标记（失败可重投）。
	keys := []string{protoID, msg.MsgID}
	if msg.Text != "" && msg.CreateAt > 0 {
		keys = append(keys, ContentFingerprint(msg.ConversationID, msg.CreateAt, msg.MsgType, msg.Text))
	}
	dup := false
	if p.markAfterHandler {
		dup = p.seen.Has(keys...)
	} else {
		dup = p.seen.CheckAndMark(keys...)
	}
	if dup {
		p.emitReject(ctx, msg, types.RejectReasonDuplicate)
		return
	}

	// 3. 自回复过滤（仅当机器人身份已知）
	if p.dropSelfSent && p.botRobotCode != "" && msg.SenderID == p.botRobotCode {
		p.emitReject(ctx, msg, types.RejectReasonSelfSent)
		return
	}

	// 4. 策略门控
	decision := p.policy.Evaluate(msg)
	if !decision.Allowed {
		p.emitReject(ctx, msg, decision.Reason)
		return
	}

	// 5. 处理锁：防止同一消息并发处理
	if !p.lock.Acquire(msg.MsgID) {
		p.emitReject(ctx, msg, types.RejectReasonLockContention)
		return
	}

	// 6. 队列路径：per-chat 串行（OnMessage）或批处理（OnBatch）。
	//    ChatQueue 内部在批处理模式下对连续媒体做窗口合并。
	if p.chatQueue != nil && p.chatQueue.Enabled() {
		scope := msg.ConversationID
		if p.batchMode() {
			p.chatQueue.Push(ctx, scope, msg, func(ctx context.Context, d *types.BatchedDispatch) error {
				return p.batchFlush(ctx, d)
			})
			return
		}
		m := msg
		_ = p.chatQueue.Run(ctx, scope, func() error {
			return p.messageFlush(ctx, m, []*types.IncomingMessage{m})
		})
		return
	}

	// 7. 无队列路径：媒体消息优先进入媒体批次
	if p.media.IsCompatible(msg) {
		p.media.Push(ctx, msg, func(ctx context.Context, merged *types.IncomingMessage) error {
			return p.messageFlush(ctx, merged, merged.BatchedSources)
		})
		return
	}
	// 非媒体消息到达时刷新同会话待处理媒体批次，保证消息顺序
	if p.media.cfg.Enabled {
		p.media.FlushIncompatibleFor(ctx, msg)
	}

	// 8. 直接分发：注册了 OnBatch 则以单条批次投递（保持回调契约），否则逐条回调
	if p.batchMode() {
		p.batchFlush(ctx, &types.BatchedDispatch{Message: msg, SourceIDs: []string{msg.MsgID}})
		return
	}
	p.messageFlush(ctx, msg, []*types.IncomingMessage{msg})
}

// PushAction 推送动作事件（卡片回调等）到简化管线：去重 → 锁 → 按 scope 串行执行。
// extraDedupKeys 为附加去重键（如内容指纹）：任一键命中即判重，
// 防止网关换投递 ID 重放同一动作。
func (p *SafetyPipeline) PushAction(ctx context.Context, eventID string, scope string, handler func() error, extraDedupKeys ...string) {
	keys := append([]string{eventID}, extraDedupKeys...)
	var dup bool
	if p.markAfterHandler {
		dup = p.seen.Has(keys...)
	} else {
		dup = p.seen.CheckAndMark(keys...)
	}
	if dup {
		return
	}
	if !p.lock.Acquire(eventID) {
		return
	}

	runner := func() error {
		err := handler()
		p.lock.Release(eventID)
		if p.markAfterHandler && err == nil {
			p.seen.Add(keys...)
		}
		return err
	}

	if p.chatQueue != nil && p.chatQueue.Enabled() {
		_ = p.chatQueue.Run(ctx, scope, runner)
		return
	}
	_ = runner()
}

// PushLight 推送轻量事件（reaction 等）：仅去重后直接执行。
func (p *SafetyPipeline) PushLight(ctx context.Context, eventID string, handler func() error) {
	if p.markAfterHandler {
		if p.seen.Has(eventID) {
			return
		}
	} else if p.seen.CheckAndMark(eventID) {
		return
	}
	if err := handler(); p.markAfterHandler && err == nil {
		p.seen.Add(eventID)
	}
}

// batchFlush 批处理分发回调：投递 OnBatch，释放全部源消息的处理锁；
// MarkAfterHandler 模式下仅当处理成功才写入 seen（失败批次可整体重投）。
func (p *SafetyPipeline) batchFlush(ctx context.Context, d *types.BatchedDispatch) error {
	var err error
	if p.onBatch != nil {
		err = p.onBatch(ctx, &types.BatchedMessage{Message: d.Message, SourceIDs: d.SourceIDs})
	}
	for _, id := range d.SourceIDs {
		p.lock.Release(id)
	}
	if p.markAfterHandler && err == nil {
		for _, id := range d.SourceIDs {
			p.seen.Add(id)
		}
	}
	return err
}

// messageFlush 消息分发回调：调用 OnMessage，释放全部源消息的处理锁；
// MarkAfterHandler 模式下仅当处理成功才写入 seen（失败消息可重投）。
func (p *SafetyPipeline) messageFlush(ctx context.Context, merged *types.IncomingMessage, sources []*types.IncomingMessage) error {
	if len(sources) == 0 {
		sources = []*types.IncomingMessage{merged}
	}
	var err error
	if p.onMessage != nil {
		err = p.onMessage(ctx, merged, sources)
	}
	for _, m := range sources {
		p.lock.Release(m.MsgID)
	}
	if p.markAfterHandler && err == nil {
		for _, m := range sources {
			p.seen.Add(m.MsgID)
		}
	}
	return err
}

// emitReject 触发拒绝事件回调；回调自身异常不外泄，避免影响主流程。
func (p *SafetyPipeline) emitReject(ctx context.Context, msg *types.IncomingMessage, reason types.RejectReason) {
	if p.onReject == nil {
		return
	}
	p.onReject(ctx, &types.RejectEvent{
		MessageID: msg.MsgID,
		ChatID:    msg.ConversationID,
		SenderID:  msg.SenderID,
		Reason:    reason,
	})
}

// SetBotIdentity 设置机器人身份（用于自回复过滤），并同步到策略门控。
func (p *SafetyPipeline) SetBotIdentity(robotCode string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.botRobotCode = robotCode
	if robotCode != "" {
		p.policy.SetBotIdentity(&types.BotIdentity{RobotCode: robotCode})
	}
}

// UpdatePolicy 动态更新策略配置。
func (p *SafetyPipeline) UpdatePolicy(cfg types.PolicyConfig) {
	p.policy.UpdateConfig(cfg)
}

// Dispose 释放资源：先刷新队列中的待处理批次，再关闭各组件。
func (p *SafetyPipeline) Dispose(ctx context.Context) {
	if p.chatQueue != nil {
		p.chatQueue.FlushAll(ctx)
		p.chatQueue.Dispose()
	}
	p.media.Dispose(ctx)
	p.seen.Dispose()
	p.lock.Dispose()
}
