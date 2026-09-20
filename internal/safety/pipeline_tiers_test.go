package safety

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

func tierMsg(id, text string) *types.IncomingMessage {
	return &types.IncomingMessage{
		ConversationID:   "chat-1",
		ConversationType: types.ConversationTypeGroup,
		SenderID:         "user-1",
		SenderStaffID:    "staff-1",
		MsgID:            id,
		MsgType:          "text",
		Text:             text,
		CreateAt:         time.Now().UnixMilli(),
		IsInAtList:       true,
	}
}

// TestPipelineTiers_PushMessage 首条消息应完整走完管线并到达 OnMessage。
func TestPipelineTiers_PushMessage(t *testing.T) {
	var got int32
	p := NewSafetyPipeline(types.DefaultSafetyConfig(), PipelineOptions{
		OnMessage: func(ctx context.Context, m *types.IncomingMessage, s []*types.IncomingMessage) error {
			atomic.AddInt32(&got, 1)
			return nil
		},
	})
	defer p.Dispose(context.Background())

	p.PushMessage(context.Background(), "p-1", tierMsg("m-1", "hello"))
	if atomic.LoadInt32(&got) != 1 {
		t.Fatalf("onMessage calls = %d, want 1", got)
	}
}

// TestPipelineTiers_PushActionDedup 同一事件只执行一次，不同事件都执行。
func TestPipelineTiers_PushActionDedup(t *testing.T) {
	var runs int32
	p := NewSafetyPipeline(types.DefaultSafetyConfig(), PipelineOptions{})
	defer p.Dispose(context.Background())
	run := func() error { atomic.AddInt32(&runs, 1); return nil }

	p.PushAction(context.Background(), "evt-1", "card:c1", run)
	p.PushAction(context.Background(), "evt-1", "card:c1", run) // 重复投递
	p.PushAction(context.Background(), "evt-2", "card:c1", run)

	if n := atomic.LoadInt32(&runs); n != 2 {
		t.Fatalf("action runs = %d, want 2（去重后 evt-1 一次 + evt-2 一次）", n)
	}
}

// TestPipelineTiers_PushActionSerialPerScope 同 scope 的动作必须串行（并发重入为 0）。
func TestPipelineTiers_PushActionSerialPerScope(t *testing.T) {
	qm := NewChatQueueManager(&types.BatchConfig{}, &types.ChatQueueConfig{Enabled: true}, nil)
	var active, maxActive int32
	var mu sync.Mutex
	p := NewSafetyPipeline(types.DefaultSafetyConfig(), PipelineOptions{ChatQueue: qm})
	defer p.Dispose(context.Background())

	slow := func() error {
		cur := atomic.AddInt32(&active, 1)
		mu.Lock()
		if cur > maxActive {
			maxActive = cur
		}
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		atomic.AddInt32(&active, -1)
		return nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p.PushAction(context.Background(), "evt-"+string(rune('a'+i)), "card:same", slow)
		}(i)
	}
	wg.Wait()

	if maxActive != 1 {
		t.Fatalf("max concurrent in scope = %d, want 1（同 scope 串行）", maxActive)
	}
}

// TestPipelineTiers_PushLightDedupAndConcurrency 轻量事件：同 ID 去重、
// 不同 ID 可并发（reaction 场景：无需按会话串行）。
func TestPipelineTiers_PushLightDedupAndConcurrency(t *testing.T) {
	var runs int32
	p := NewSafetyPipeline(types.DefaultSafetyConfig(), PipelineOptions{})
	defer p.Dispose(context.Background())
	inc := func() error { atomic.AddInt32(&runs, 1); return nil }

	p.PushLight(context.Background(), "react-1", inc)
	p.PushLight(context.Background(), "react-1", inc) // 重复 reaction

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ { // 并发不同 reaction 全部执行
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p.PushLight(context.Background(), "react-gen-"+string(rune('a'+i%26))+string(rune('0'+i%10)), inc)
		}(i)
	}
	wg.Wait()

	if n := atomic.LoadInt32(&runs); n != 33 {
		t.Fatalf("light runs = %d, want 33（1 次首投 + 32 个并发不同事件）", n)
	}
}

// TestSeenMarkTiming_DefaultMarksOnEntry 默认模式：入口即标记，
// handler 失败后重投会被判重丢弃（防重复消费优先）。
func TestSeenMarkTiming_DefaultMarksOnEntry(t *testing.T) {
	var calls int32
	p := NewSafetyPipeline(types.DefaultSafetyConfig(), PipelineOptions{
		OnMessage: func(ctx context.Context, m *types.IncomingMessage, s []*types.IncomingMessage) error {
			atomic.AddInt32(&calls, 1)
			return errors.New("boom") // 处理失败
		},
	})
	defer p.Dispose(context.Background())

	msg := tierMsg("m-fail", "payload")
	p.PushMessage(context.Background(), "p-fail", msg)
	p.PushMessage(context.Background(), "p-fail-2", msg) // 重投（换投递 ID）

	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("default mode: handler calls = %d, want 1（失败后重投被去重）", n)
	}
}

// TestSeenMarkTiming_AfterHandlerAllowsRedelivery MarkAfterHandler 模式：
// handler 失败 → 未标记 → 重投会再次处理；成功后 → 标记 → 重投被丢弃。
func TestSeenMarkTiming_AfterHandlerAllowsRedelivery(t *testing.T) {
	var calls int32
	fail := true
	p := NewSafetyPipeline(types.SafetyConfig{
		StaleWindow:       30 * time.Minute,
		LockTTL:           5 * time.Minute,
		MarkAfterHandler:  true,
		Dedup:             types.DedupConfig{TTL: time.Hour, MaxEntries: 100, SweepInterval: time.Hour},
		MediaBatch:        types.DefaultMediaBatchConfig(),
		Policy:            types.PolicyConfig{},
		ChatQueue:         types.ChatQueueConfig{},
		TextBatch:         types.BatchConfig{},
	}, PipelineOptions{
		OnMessage: func(ctx context.Context, m *types.IncomingMessage, s []*types.IncomingMessage) error {
			atomic.AddInt32(&calls, 1)
			if fail {
				return errors.New("transient")
			}
			return nil
		},
	})
	defer p.Dispose(context.Background())

	msg := tierMsg("m-redeliver", "payload")
	p.PushMessage(context.Background(), "p-1", msg) // 失败
	p.PushMessage(context.Background(), "p-2", msg) // 重投 → 应再次处理（未标记）
	fail = false
	p.PushMessage(context.Background(), "p-3", msg) // 成功 → 标记
	p.PushMessage(context.Background(), "p-4", msg) // 再投 → 判重丢弃

	if n := atomic.LoadInt32(&calls); n != 3 {
		t.Fatalf("markAfter mode: handler calls = %d, want 3（失败重投 + 成功 + 判重）", n)
	}
}

// TestSeenMarkTiming_ActionAndLight 同样支持完成后标记语义。
func TestSeenMarkTiming_ActionAndLight(t *testing.T) {
	var runs int32
	fail := true
	p := NewSafetyPipeline(types.SafetyConfig{
		StaleWindow:      30 * time.Minute,
		MarkAfterHandler: true,
		Dedup:            types.DedupConfig{TTL: time.Hour, MaxEntries: 100, SweepInterval: time.Hour},
		MediaBatch:       types.DefaultMediaBatchConfig(),
	}, PipelineOptions{})
	defer p.Dispose(context.Background())

	act := func() error {
		atomic.AddInt32(&runs, 1)
		if fail {
			return errors.New("boom")
		}
		return nil
	}
	p.PushAction(context.Background(), "a-1", "s", act) // 失败，未标记
	p.PushAction(context.Background(), "a-1", "s", act) // 重投 → 再执行
	fail = false
	p.PushAction(context.Background(), "a-1", "s", act) // 成功 → 标记
	p.PushAction(context.Background(), "a-1", "s", act) // 判重

	p.PushLight(context.Background(), "l-1", func() error { atomic.AddInt32(&runs, 1); return nil })
	p.PushLight(context.Background(), "l-1", func() error { atomic.AddInt32(&runs, 1); return nil }) // 判重

	if n := atomic.LoadInt32(&runs); n != 4 {
		t.Fatalf("action/light markAfter runs = %d, want 4", n)
	}
}
