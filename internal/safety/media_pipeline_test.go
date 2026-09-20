package safety

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

func TestMediaPipeline_IsCompatible(t *testing.T) {
	cfg := types.MediaBatchConfig{
		Enabled:  true,
		DelayMs:  800,
		MaxItems: 9,
	}
	mgr := NewMediaPipelineManager(cfg)

	tests := []struct {
		msgType string
		want    bool
	}{
		{"picture", true},
		{"file", true},
		{"audio", true},
		{"video", true},
		{"text", false},
		{"richText", false},
		{"markdown", false},
	}

	for _, tt := range tests {
		t.Run(tt.msgType, func(t *testing.T) {
			msg := &types.IncomingMessage{MsgType: tt.msgType}
			got := mgr.IsCompatible(msg)
			if got != tt.want {
				t.Errorf("IsCompatible(%s) = %v, want %v", tt.msgType, got, tt.want)
			}
		})
	}
}

func TestMediaPipeline_Disabled(t *testing.T) {
	cfg := types.MediaBatchConfig{
		Enabled: false, // 禁用
	}
	mgr := NewMediaPipelineManager(cfg)

	msg := &types.IncomingMessage{MsgType: "picture"}
	if mgr.IsCompatible(msg) {
		t.Error("expected IsCompatible to return false when disabled")
	}
}

func TestMediaPipeline_SingleMessage(t *testing.T) {
	cfg := types.MediaBatchConfig{
		Enabled:  true,
		DelayMs:  100,
		MaxItems: 9,
	}
	mgr := NewMediaPipelineManager(cfg)

	var mu sync.Mutex
	var flushed *types.IncomingMessage

	handler := func(ctx context.Context, merged *types.IncomingMessage) error {
		mu.Lock()
		defer mu.Unlock()
		flushed = merged
		return nil
	}

	msg := &types.IncomingMessage{
		ConversationID: "chat1",
		MsgType:        "picture",
		MsgID:          "msg1",
		Text:           "[图片]",
		Resources: []types.Resource{
			{Type: "image", DownloadCode: "code1"},
		},
	}

	ctx := context.Background()
	mgr.Push(ctx, msg, handler)

	// 等待定时器触发
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if flushed == nil {
		t.Fatal("expected handler to be called")
	}
	if flushed.MsgID != "msg1" {
		t.Errorf("expected MsgID msg1, got %s", flushed.MsgID)
	}
}

func TestMediaPipeline_BatchMultiple(t *testing.T) {
	cfg := types.MediaBatchConfig{
		Enabled:  true,
		DelayMs:  150,
		MaxItems: 9,
	}
	mgr := NewMediaPipelineManager(cfg)

	var mu sync.Mutex
	var flushed *types.IncomingMessage

	handler := func(ctx context.Context, merged *types.IncomingMessage) error {
		mu.Lock()
		defer mu.Unlock()
		flushed = merged
		return nil
	}

	ctx := context.Background()

	// 连续推送 3 张图片
	for i := 1; i <= 3; i++ {
		msg := &types.IncomingMessage{
			ConversationID: "chat1",
			MsgType:        "picture",
			MsgID:          "msg" + string(rune(i+'0')),
			Resources: []types.Resource{
				{Type: "image", DownloadCode: "code" + string(rune(i+'0'))},
			},
		}
		mgr.Push(ctx, msg, handler)
		time.Sleep(20 * time.Millisecond) // 短间隔，在延迟窗口内
	}

	// 等待批次刷新
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if flushed == nil {
		t.Fatal("expected handler to be called")
	}

	// 应该合并了 3 张图片的 Resources
	if len(flushed.Resources) != 3 {
		t.Errorf("expected 3 resources, got %d", len(flushed.Resources))
	}

	// 最后一条消息的 ID
	if flushed.MsgID != "msg3" {
		t.Errorf("expected last message ID msg3, got %s", flushed.MsgID)
	}

	// 检查文本提示
	if flushed.Text != "[3个图片]" {
		t.Errorf("expected batch text '[3个图片]', got '%s'", flushed.Text)
	}
}

func TestMediaPipeline_MaxItemsCap(t *testing.T) {
	cfg := types.MediaBatchConfig{
		Enabled:  true,
		DelayMs:  1000, // 长延迟
		MaxItems: 3,    // 容量上限 3
	}
	mgr := NewMediaPipelineManager(cfg)

	var mu sync.Mutex
	var flushedCount int

	handler := func(ctx context.Context, merged *types.IncomingMessage) error {
		mu.Lock()
		defer mu.Unlock()
		flushedCount++
		return nil
	}

	ctx := context.Background()

	// 推送 3 张图片，应立即触发刷新（不等延迟）
	for i := 1; i <= 3; i++ {
		msg := &types.IncomingMessage{
			ConversationID: "chat1",
			MsgType:        "picture",
			MsgID:          "msg" + string(rune(i+'0')),
			Resources: []types.Resource{
				{Type: "image", DownloadCode: "code" + string(rune(i+'0'))},
			},
		}
		mgr.Push(ctx, msg, handler)
	}

	// 短暂等待异步 handler
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if flushedCount != 1 {
		t.Errorf("expected 1 flush (max items reached), got %d", flushedCount)
	}
}

func TestMediaPipeline_DifferentChats(t *testing.T) {
	cfg := types.MediaBatchConfig{
		Enabled:  true,
		DelayMs:  100,
		MaxItems: 9,
	}
	mgr := NewMediaPipelineManager(cfg)

	var mu sync.Mutex
	var flushedChats []string

	handler := func(ctx context.Context, merged *types.IncomingMessage) error {
		mu.Lock()
		defer mu.Unlock()
		flushedChats = append(flushedChats, merged.ConversationID)
		return nil
	}

	ctx := context.Background()

	// 不同会话的图片，应该分开批次
	msg1 := &types.IncomingMessage{
		ConversationID: "chat1",
		MsgType:        "picture",
		MsgID:          "msg1",
		Resources:      []types.Resource{{Type: "image", DownloadCode: "code1"}},
	}
	msg2 := &types.IncomingMessage{
		ConversationID: "chat2",
		MsgType:        "picture",
		MsgID:          "msg2",
		Resources:      []types.Resource{{Type: "image", DownloadCode: "code2"}},
	}

	mgr.Push(ctx, msg1, handler)
	mgr.Push(ctx, msg2, handler)

	// 等待定时器触发
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// 应该有 2 次刷新（2 个独立批次）
	if len(flushedChats) != 2 {
		t.Errorf("expected 2 flushes, got %d", len(flushedChats))
	}
}

func TestMediaPipeline_DifferentTypes(t *testing.T) {
	cfg := types.MediaBatchConfig{
		Enabled:  true,
		DelayMs:  100,
		MaxItems: 9,
	}
	mgr := NewMediaPipelineManager(cfg)

	var mu sync.Mutex
	var flushed []*types.IncomingMessage

	handler := func(ctx context.Context, merged *types.IncomingMessage) error {
		mu.Lock()
		defer mu.Unlock()
		flushed = append(flushed, merged)
		return nil
	}

	ctx := context.Background()

	// 同一会话，不同媒体类型，应该分开批次
	msg1 := &types.IncomingMessage{
		ConversationID: "chat1",
		MsgType:        "picture",
		MsgID:          "msg1",
	}
	msg2 := &types.IncomingMessage{
		ConversationID: "chat1",
		MsgType:        "file", // 不同类型
		MsgID:          "msg2",
	}

	mgr.Push(ctx, msg1, handler)
	mgr.Push(ctx, msg2, handler)

	// 等待定时器触发
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// 应该有 2 次刷新（不同类型）
	if len(flushed) != 2 {
		t.Errorf("expected 2 flushes for different types, got %d", len(flushed))
	}
}

func TestMediaPipeline_FlushIncompatibleFor(t *testing.T) {
	cfg := types.MediaBatchConfig{
		Enabled:  true,
		DelayMs:  500, // 长延迟
		MaxItems: 9,
	}
	mgr := NewMediaPipelineManager(cfg)

	var mu sync.Mutex
	var flushed *types.IncomingMessage

	handler := func(ctx context.Context, merged *types.IncomingMessage) error {
		mu.Lock()
		defer mu.Unlock()
		flushed = merged
		return nil
	}

	ctx := context.Background()

	// 推送图片
	msg1 := &types.IncomingMessage{
		ConversationID: "chat1",
		MsgType:        "picture",
		MsgID:          "msg1",
		Resources:      []types.Resource{{Type: "image", DownloadCode: "code1"}},
	}
	mgr.Push(ctx, msg1, handler)

	// 立即刷新不兼容的批次（模拟文本消息介入）
	textMsg := &types.IncomingMessage{
		ConversationID: "chat1",
		MsgType:        "text",
		Text:           "hello",
	}
	mgr.FlushIncompatibleFor(ctx, textMsg)

	// 短暂等待异步 handler
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// 应该立即刷新了图片批次（不等延迟）
	if flushed == nil {
		t.Error("expected flush to be triggered by FlushIncompatibleFor")
	}
	if flushed.MsgID != "msg1" {
		t.Errorf("expected flushed message msg1, got %s", flushed.MsgID)
	}
}

func TestMediaPipeline_Dispose(t *testing.T) {
	cfg := types.MediaBatchConfig{
		Enabled:  true,
		DelayMs:  10000, // 很长的延迟
		MaxItems: 9,
	}
	mgr := NewMediaPipelineManager(cfg)

	var mu sync.Mutex
	var flushed *types.IncomingMessage

	handler := func(ctx context.Context, merged *types.IncomingMessage) error {
		mu.Lock()
		defer mu.Unlock()
		flushed = merged
		return nil
	}

	ctx := context.Background()

	// 推送消息但不等待定时器
	msg := &types.IncomingMessage{
		ConversationID: "chat1",
		MsgType:        "picture",
		MsgID:          "msg1",
	}
	mgr.Push(ctx, msg, handler)

	// 立即 Dispose，应该刷新待处理的批次
	mgr.Dispose(ctx)

	// 短暂等待异步 handler
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if flushed == nil {
		t.Error("expected Dispose to flush pending batch")
	}
}

func TestMediaPipeline_FlushIncompatiblePrefixCollision(t *testing.T) {
	// chatID 前缀碰撞回归：cid-1 的文本消息不得误刷 cid-12 的媒体批次
	cfg := types.MediaBatchConfig{Enabled: true, DelayMs: 10000, MaxItems: 9}
	mgr := NewMediaPipelineManager(cfg)

	var mu sync.Mutex
	flushedChats := make([]string, 0)
	handler := func(ctx context.Context, merged *types.IncomingMessage) error {
		mu.Lock()
		defer mu.Unlock()
		flushedChats = append(flushedChats, merged.ConversationID)
		return nil
	}
	ctx := context.Background()

	mgr.Push(ctx, &types.IncomingMessage{ConversationID: "cid-12", MsgType: "picture"}, handler)
	// cid-1 是 cid-12 的前缀：不应触发 cid-12 批次刷新
	mgr.FlushIncompatibleFor(ctx, &types.IncomingMessage{ConversationID: "cid-1", MsgType: "text"})

	mu.Lock()
	n := len(flushedChats)
	mu.Unlock()
	if n != 0 {
		t.Fatalf("prefix chat collision: cid-12 batch flushed %d times by cid-1 message", n)
	}

	// 真正同会话的消息应触发刷新
	mgr.FlushIncompatibleFor(ctx, &types.IncomingMessage{ConversationID: "cid-12", MsgType: "text"})
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(flushedChats) != 1 || flushedChats[0] != "cid-12" {
		t.Fatalf("same-chat flush wrong: %+v", flushedChats)
	}
}

func TestMediaPipeline_ReplyParentKey(t *testing.T) {
	// reply 消息按被引用内容分桶：不同引用目标不合并
	cfg := types.MediaBatchConfig{Enabled: true, DelayMs: 80, MaxItems: 9}
	mgr := NewMediaPipelineManager(cfg)

	var mu sync.Mutex
	var flushes int
	handler := func(ctx context.Context, merged *types.IncomingMessage) error {
		mu.Lock()
		defer mu.Unlock()
		flushes++
		return nil
	}
	ctx := context.Background()

	// 构造 reply 型媒体消息（引用内容不同 → 键不同 → 两个独立批次）
	mgr.Push(ctx, &types.IncomingMessage{ConversationID: "c1", MsgType: "reply",
		Content: []byte(`{"repliedMsg":{"msgId":"r1"}}`), Resources: []types.Resource{{Type: "image", DownloadCode: "a"}}}, handler)
	mgr.Push(ctx, &types.IncomingMessage{ConversationID: "c1", MsgType: "reply",
		Content: []byte(`{"repliedMsg":{"msgId":"r2"}}`), Resources: []types.Resource{{Type: "image", DownloadCode: "b"}}}, handler)
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if flushes != 2 {
		t.Fatalf("distinct reply parents should form 2 buckets, got %d flushes", flushes)
	}
}
