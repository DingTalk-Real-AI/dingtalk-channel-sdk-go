package safety

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

func TestSafetyPipeline_Basic(t *testing.T) {
	cfg := types.SafetyConfig{
		Dedup:       types.DedupConfig{TTL: 5 * time.Minute, MaxEntries: 100},
		Policy:      types.PolicyConfig{},
		StaleWindow: 30 * time.Minute,
		LockTTL:     5 * time.Minute,
	}

	var mu sync.Mutex
	var received *types.IncomingMessage

	opts := PipelineOptions{
		OnMessage: func(ctx context.Context, msg *types.IncomingMessage, sources []*types.IncomingMessage) error {
			mu.Lock()
			defer mu.Unlock()
			received = msg
			return nil
		},
	}

	pipeline := NewSafetyPipeline(cfg, opts)
	defer pipeline.Dispose(context.Background())

	msg := &types.IncomingMessage{
		ConversationID:   "chat1",
		ConversationType: types.ConversationTypeDM,
		SenderID:         "user123",
		SenderStaffID:    "staff123",
		MsgID:            "msg1",
		MsgType:          "text",
		Text:             "hello",
		CreateAt:         time.Now().UnixMilli(),
	}

	ctx := context.Background()
	pipeline.PushMessage(ctx, "proto1", msg)

	// 短暂等待异步处理
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if received == nil {
		t.Fatal("expected message to be processed")
	}
	if received.MsgID != "msg1" {
		t.Errorf("expected MsgID msg1, got %s", received.MsgID)
	}
}

func TestSafetyPipeline_Dedup(t *testing.T) {
	cfg := types.SafetyConfig{
		Dedup:       types.DedupConfig{TTL: 5 * time.Minute, MaxEntries: 100},
		StaleWindow: 30 * time.Minute,
		LockTTL:     5 * time.Minute,
	}

	var mu sync.Mutex
	var count int

	opts := PipelineOptions{
		OnMessage: func(ctx context.Context, msg *types.IncomingMessage, sources []*types.IncomingMessage) error {
			mu.Lock()
			defer mu.Unlock()
			count++
			return nil
		},
	}

	pipeline := NewSafetyPipeline(cfg, opts)
	defer pipeline.Dispose(context.Background())

	msg := &types.IncomingMessage{
		ConversationID:   "chat1",
		ConversationType: types.ConversationTypeDM,
		SenderID:         "user123",
		MsgID:            "msg1",
		MsgType:          "text",
		CreateAt:         time.Now().UnixMilli(),
	}

	ctx := context.Background()

	// 推送两次相同消息
	pipeline.PushMessage(ctx, "proto1", msg)
	pipeline.PushMessage(ctx, "proto1", msg) // 重复

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// 应该只处理一次
	if count != 1 {
		t.Errorf("expected 1 message processed, got %d", count)
	}
}

func TestSafetyPipeline_StaleMessage(t *testing.T) {
	cfg := types.SafetyConfig{
		Dedup:       types.DedupConfig{TTL: 5 * time.Minute, MaxEntries: 100},
		StaleWindow: 10 * time.Minute, // 10分钟窗口
		LockTTL:     5 * time.Minute,
	}

	var mu sync.Mutex
	var processedCount int
	var rejectedCount int

	opts := PipelineOptions{
		OnMessage: func(ctx context.Context, msg *types.IncomingMessage, sources []*types.IncomingMessage) error {
			mu.Lock()
			defer mu.Unlock()
			processedCount++
			return nil
		},
		OnReject: func(ctx context.Context, event *types.RejectEvent) {
			mu.Lock()
			defer mu.Unlock()
			if event.Reason == types.RejectReasonStale {
				rejectedCount++
			}
		},
	}

	pipeline := NewSafetyPipeline(cfg, opts)
	defer pipeline.Dispose(context.Background())

	// 过期消息（15分钟前）
	staleMsg := &types.IncomingMessage{
		ConversationID: "chat1",
		SenderID:       "user123",
		MsgID:          "msg1",
		MsgType:        "text",
		CreateAt:       time.Now().Add(-15 * time.Minute).UnixMilli(),
	}

	// 新鲜消息
	freshMsg := &types.IncomingMessage{
		ConversationID: "chat1",
		SenderID:       "user123",
		MsgID:          "msg2",
		MsgType:        "text",
		CreateAt:       time.Now().UnixMilli(),
	}

	ctx := context.Background()
	pipeline.PushMessage(ctx, "proto1", staleMsg)
	pipeline.PushMessage(ctx, "proto2", freshMsg)

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if processedCount != 1 {
		t.Errorf("expected 1 fresh message processed, got %d", processedCount)
	}
	if rejectedCount != 1 {
		t.Errorf("expected 1 stale message rejected, got %d", rejectedCount)
	}
}

func TestSafetyPipeline_SelfSent(t *testing.T) {
	cfg := types.SafetyConfig{
		Dedup:        types.DedupConfig{TTL: 5 * time.Minute, MaxEntries: 100},
		StaleWindow:  30 * time.Minute,
		LockTTL:      5 * time.Minute,
		DropSelfSent: true,
	}

	var mu sync.Mutex
	var processedCount int
	var rejectedCount int

	opts := PipelineOptions{
		OnMessage: func(ctx context.Context, msg *types.IncomingMessage, sources []*types.IncomingMessage) error {
			mu.Lock()
			defer mu.Unlock()
			processedCount++
			return nil
		},
		OnReject: func(ctx context.Context, event *types.RejectEvent) {
			mu.Lock()
			defer mu.Unlock()
			if event.Reason == types.RejectReasonSelfSent {
				rejectedCount++
			}
		},
		BotRobotCode: "robot123",
	}

	pipeline := NewSafetyPipeline(cfg, opts)
	defer pipeline.Dispose(context.Background())

	// 机器人自己发的消息
	selfMsg := &types.IncomingMessage{
		ConversationID: "chat1",
		SenderID:       "robot123", // 机器人自己
		MsgID:          "msg1",
		MsgType:        "text",
		CreateAt:       time.Now().UnixMilli(),
	}

	// 用户消息
	userMsg := &types.IncomingMessage{
		ConversationID: "chat1",
		SenderID:       "user123",
		MsgID:          "msg2",
		MsgType:        "text",
		CreateAt:       time.Now().UnixMilli(),
	}

	ctx := context.Background()
	pipeline.PushMessage(ctx, "proto1", selfMsg)
	pipeline.PushMessage(ctx, "proto2", userMsg)

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if processedCount != 1 {
		t.Errorf("expected 1 user message processed, got %d", processedCount)
	}
	if rejectedCount != 1 {
		t.Errorf("expected 1 self-sent message rejected, got %d", rejectedCount)
	}
}

func TestSafetyPipeline_PolicyReject(t *testing.T) {
	cfg := types.SafetyConfig{
		Dedup: types.DedupConfig{TTL: 5 * time.Minute, MaxEntries: 100},
		Policy: types.PolicyConfig{
			DMMode: "disabled", // DM 禁用
		},
		StaleWindow: 30 * time.Minute,
		LockTTL:     5 * time.Minute,
	}

	var mu sync.Mutex
	var processedCount int
	var rejectedReason types.RejectReason

	opts := PipelineOptions{
		OnMessage: func(ctx context.Context, msg *types.IncomingMessage, sources []*types.IncomingMessage) error {
			mu.Lock()
			defer mu.Unlock()
			processedCount++
			return nil
		},
		OnReject: func(ctx context.Context, event *types.RejectEvent) {
			mu.Lock()
			defer mu.Unlock()
			rejectedReason = event.Reason
		},
	}

	pipeline := NewSafetyPipeline(cfg, opts)
	defer pipeline.Dispose(context.Background())

	dmMsg := &types.IncomingMessage{
		ConversationID:   "chat1",
		ConversationType: types.ConversationTypeDM,
		SenderID:         "user123",
		SenderStaffID:    "staff123",
		MsgID:            "msg1",
		MsgType:          "text",
		CreateAt:         time.Now().UnixMilli(),
	}

	ctx := context.Background()
	pipeline.PushMessage(ctx, "proto1", dmMsg)

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if processedCount != 0 {
		t.Errorf("expected 0 messages processed (DM disabled), got %d", processedCount)
	}
	if rejectedReason != types.RejectReasonDMDisabled {
		t.Errorf("expected reject reason dm_disabled, got %s", rejectedReason)
	}
}

func TestSafetyPipeline_MediaBatching(t *testing.T) {
	cfg := types.SafetyConfig{
		Dedup: types.DedupConfig{TTL: 5 * time.Minute, MaxEntries: 100},
		MediaBatch: types.MediaBatchConfig{
			Enabled:  true,
			DelayMs:  100,
			MaxItems: 9,
		},
		StaleWindow: 30 * time.Minute,
		LockTTL:     5 * time.Minute,
	}

	var mu sync.Mutex
	var received *types.IncomingMessage

	opts := PipelineOptions{
		OnMessage: func(ctx context.Context, msg *types.IncomingMessage, sources []*types.IncomingMessage) error {
			mu.Lock()
			defer mu.Unlock()
			received = msg
			return nil
		},
	}

	pipeline := NewSafetyPipeline(cfg, opts)
	defer pipeline.Dispose(context.Background())

	ctx := context.Background()

	// 连续推送 2 张图片
	for i := 1; i <= 2; i++ {
		msg := &types.IncomingMessage{
			ConversationID: "chat1",
			SenderID:       "user123",
			MsgID:          "msg" + string(rune(i+'0')),
			MsgType:        "picture",
			CreateAt:       time.Now().UnixMilli(),
			Resources: []types.Resource{
				{Type: "image", DownloadCode: "code" + string(rune(i+'0'))},
			},
		}
		pipeline.PushMessage(ctx, "proto"+string(rune(i+'0')), msg)
		time.Sleep(20 * time.Millisecond)
	}

	// 等待批次刷新
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if received == nil {
		t.Fatal("expected batched media message")
	}

	// 应该合并了 2 张图片
	if len(received.Resources) != 2 {
		t.Errorf("expected 2 resources in batch, got %d", len(received.Resources))
	}
}

func TestSafetyPipeline_SetBotIdentity(t *testing.T) {
	cfg := types.DefaultSafetyConfig()
	opts := PipelineOptions{
		OnMessage: func(ctx context.Context, msg *types.IncomingMessage, sources []*types.IncomingMessage) error {
			return nil
		},
	}

	pipeline := NewSafetyPipeline(cfg, opts)
	defer pipeline.Dispose(context.Background())

	// 设置 bot 身份
	pipeline.SetBotIdentity("robot456")

	// 验证已设置（通过尝试过滤自回复）
	if pipeline.botRobotCode != "robot456" {
		t.Errorf("expected botRobotCode robot456, got %s", pipeline.botRobotCode)
	}
}
