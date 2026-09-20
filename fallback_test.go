package channel

import (
	"context"
	"fmt"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// fallbackTestServer 构造带计数的服务端：token/card/webhook/proactive 各自独立计数。
type fallbackServer struct {
	mu          sync.Mutex
	srv         *httptest.Server
	webhookHits int
	sendHits    int
	webhookCode int // webhook 返回码（200/404）
	cardBodies  []map[string]any
}

func newFallbackServer(t *testing.T, webhookCode int) *fallbackServer {
	t.Helper()
	fs := &fallbackServer{webhookCode: webhookCode}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1.0/oauth2/accessToken", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"accessToken": "tk", "expireIn": 7200})
	})
	mux.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {
		fs.mu.Lock()
		fs.webhookHits++
		code := fs.webhookCode
		fs.mu.Unlock()
		w.WriteHeader(code)
	})
	mux.HandleFunc("/v1.0/robot/groupMessages/send", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		fs.mu.Lock()
		fs.sendHits++
		fs.cardBodies = append(fs.cardBodies, body)
		fs.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	})
	// 卡片创建/更新/状态（流式用）
	mux.HandleFunc("/v1.0/card/instances", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	})
	// 其余端点（卡片创建/更新/收口等）统一回成功，保证流式走真实卡片路径
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	})
	fs.srv = httptest.NewServer(mux)
	t.Cleanup(fs.srv.Close)
	return fs
}

func (fs *fallbackServer) config() Config {
	cfg := testConfig(fs.srv.URL, fs.srv.URL)
	cfg.Outbound = &OutboundConfig{Retry: RetryConfig{MaxAttempts: 3, BaseDelayMs: 1}}
	return cfg
}

// TestReplyExpiredWebhookFallsBackToProactive webhook 已过时效 → 不打 webhook，直接主动发送。
func TestReplyExpiredWebhookFallsBackToProactive(t *testing.T) {
	fs := newFallbackServer(t, http.StatusOK)
	cfg := fs.config()
	ch := New(cfg)
	defer ch.Close()

	past := time.Now().Add(-time.Hour).UnixMilli()
	r := &replier{
		msg: &IncomingMessage{
			ConversationType: ConversationTypeGroup,
			ConversationID:   "cid-1",
			SessionWebhook:   fs.srv.URL + "/webhook",
			WebhookExpiredAt: past,
			MsgID:            "m1",
		},
		cfg: &ch.cfg, tokens: ch.tokens, cards: ch.cards, oapi: ch.oapi, httpc: ch.httpc,
		proactive: ch.proactiveReply,
	}
	if err := r.Text(context.Background(), "hello"); err != nil {
		t.Fatalf("expired webhook should fall back to proactive send, err=%v", err)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.webhookHits != 0 {
		t.Fatalf("expired webhook should be skipped, hits=%d", fs.webhookHits)
	}
	if fs.sendHits != 1 {
		t.Fatalf("proactive send hits = %d, want 1", fs.sendHits)
	}
}

// TestReplyTargetGoneFallsBackToProactive webhook 返回 404（目标撤回）→ 主动发送兜底成功。
func TestReplyTargetGoneFallsBackToProactive(t *testing.T) {
	fs := newFallbackServer(t, http.StatusNotFound)
	cfg := fs.config()
	ch := New(cfg)
	defer ch.Close()

	r := &replier{
		msg: &IncomingMessage{
			ConversationType: ConversationTypeGroup,
			ConversationID:   "cid-1",
			SessionWebhook:   fs.srv.URL + "/webhook",
			MsgID:            "m1",
		},
		cfg: &ch.cfg, tokens: ch.tokens, cards: ch.cards, oapi: ch.oapi, httpc: ch.httpc,
		proactive: ch.proactiveReply,
	}
	if err := r.Text(context.Background(), "hello"); err != nil {
		t.Fatalf("target-gone webhook should fall back, err=%v", err)
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.webhookHits != 1 {
		t.Fatalf("webhook hits = %d, want 1（404 不可重试即停）", fs.webhookHits)
	}
	if fs.sendHits != 1 {
		t.Fatalf("proactive send hits = %d, want 1", fs.sendHits)
	}
}

// TestReplyOtherErrorsDoNotFallBack 非"目标失效"错误（500→unknown 已重试耗尽）不触发兜底。
func TestReplyOtherErrorsDoNotFallBack(t *testing.T) {
	fs := newFallbackServer(t, http.StatusInternalServerError)
	cfg := fs.config()
	ch := New(cfg)
	defer ch.Close()

	r := &replier{
		msg: &IncomingMessage{
			ConversationType: ConversationTypeGroup,
			ConversationID:   "cid-1",
			SessionWebhook:   fs.srv.URL + "/webhook",
			MsgID:            "m1",
		},
		cfg: &ch.cfg, tokens: ch.tokens, cards: ch.cards, oapi: ch.oapi, httpc: ch.httpc,
		proactive: ch.proactiveReply,
	}
	if err := r.Text(context.Background(), "hello"); err == nil {
		t.Fatal("500 should ultimately fail")
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.sendHits != 0 {
		t.Fatalf("non-target-gone error must not fall back, send hits=%d", fs.sendHits)
	}
	if fs.webhookHits != 3 {
		t.Fatalf("500 (unknown, retryable) should exhaust 3 attempts, hits=%d", fs.webhookHits)
	}
}

// TestCardFinishOverflowDeliversRemainder 卡片内容超单帧上限：
// 终帧截断 20000 rune，剩余部分经 webhook 续发（复用文本分片链路）。
func TestCardFinishOverflowDeliversRemainder(t *testing.T) {
	fs := newFallbackServer(t, http.StatusOK)
	cfg := fs.config()
	var replyPayloads sync.Map
	cfg.Outbound.BeforeSend = func(kind, target string, payload any) any {
		if kind == "reply" {
			replyPayloads.Store(target, payload)
		}
		return payload
	}
	ch := New(cfg)
	defer ch.Close()

	msg := &IncomingMessage{
		ConversationType: ConversationTypeGroup,
		ConversationID:   "cid-1",
		SessionWebhook:   fs.srv.URL + "/webhook",
		MsgID:            "m1",
	}
	r := &replier{
		msg: msg, cfg: &ch.cfg, tokens: ch.tokens, cards: ch.cards, oapi: ch.oapi, httpc: ch.httpc,
		proactive: ch.proactiveReply,
	}
	s, err := r.Stream(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	tail := strings.Repeat("尾", 800) // 20800 > 20000，尾部 800 rune 待续发
	content := strings.Repeat("头", 20000) + tail
	if err := s.Finish(content); err != nil {
		t.Fatalf("finish: %v", err)
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.webhookHits != 1 {
		t.Fatalf("overflow remainder should be delivered via 1 webhook chunk, hits=%d", fs.webhookHits)
	}
	// 卡片终帧行为不受影响
	_ = types.ConversationTypeGroup
	found := false
	replyPayloads.Range(func(_, v any) bool {
		if strings.Contains(fmt.Sprintf("%v", v), strings.Repeat("尾", 10)) {
			found = true
		}
		return true
	})
	if !found {
		t.Fatal("remainder chunk should contain overflow tail content")
	}
}
