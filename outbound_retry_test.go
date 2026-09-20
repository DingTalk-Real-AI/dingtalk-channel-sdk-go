package channel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/internal/outbound"
)

// TestRetryOptionsMapping OutboundConfig → RetryOptions 的映射与默认回退。
func TestRetryOptionsMapping(t *testing.T) {
	if got := retryOptions(nil); got.MaxAttempts != outbound.DefaultRetryOptions.MaxAttempts {
		t.Fatalf("nil cfg should use defaults, got %+v", got)
	}
	if got := retryOptions(&Config{}); got.MaxAttempts != outbound.DefaultRetryOptions.MaxAttempts {
		t.Fatalf("zero outbound should use defaults, got %+v", got)
	}
	cfg := &Config{Outbound: &OutboundConfig{Retry: RetryConfig{MaxAttempts: 5, BaseDelayMs: 10}}}
	got := retryOptions(cfg)
	if got.MaxAttempts != 5 || got.BaseDelay != 10*time.Millisecond {
		t.Fatalf("mapped opts = %+v", got)
	}
	// BaseDelay 缺省回退默认值
	cfg = &Config{Outbound: &OutboundConfig{Retry: RetryConfig{MaxAttempts: 2}}}
	got = retryOptions(cfg)
	if got.BaseDelay != outbound.DefaultRetryOptions.BaseDelay {
		t.Fatalf("base fallback = %v", got.BaseDelay)
	}
}

func retryTestChannel(t *testing.T, webhookHits *int32, failFirst int32) (*httptest.Server, *Channel) {
	t.Helper()
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			_ = json.NewEncoder(w).Encode(map[string]any{"accessToken": "tk", "expireIn": 7200})
		case "/webhook":
			mu.Lock()
			*webhookHits++
			n := *webhookHits
			mu.Unlock()
			if int32(n) <= failFirst {
				w.WriteHeader(http.StatusTooManyRequests) // 429 → rate_limited（可重试）
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"success":true}`))
		}
	}))
	t.Cleanup(srv.Close)
	cfg := testConfig(srv.URL, srv.URL)
	cfg.Outbound = &OutboundConfig{Retry: RetryConfig{MaxAttempts: 3, BaseDelayMs: 1}}
	return srv, New(cfg)
}

// TestReplyWebhookRetriesOnRateLimit webhook 回复遇 429 应退避重试并最终成功。
func TestReplyWebhookRetriesOnRateLimit(t *testing.T) {
	var hits int32
	srv, ch := retryTestChannel(t, &hits, 1)
	defer ch.Close()

	// 通过内部 replier 验证（webhookOnce 走 cfg.Outbound.Retry）
	r := &replier{
		msg: &IncomingMessage{ConversationID: "c1", SessionWebhook: srv.URL + "/webhook", MsgID: "m1"},
		cfg: &ch.cfg, tokens: ch.tokens, cards: ch.cards, oapi: ch.oapi, httpc: ch.httpc,
	}
	if err := r.Text(context.Background(), "hello"); err != nil {
		t.Fatalf("text reply should succeed after retry, err=%v", err)
	}
	if hits < 2 {
		t.Fatalf("webhook hits = %d, want >=2（首试 429 + 重试成功）", hits)
	}
}

// TestReplyWebhookNonRetryableFailsFast 格式错误（400）不应重试。
func TestReplyWebhookNonRetryableFailsFast(t *testing.T) {
	var mu sync.Mutex
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1.0/oauth2/accessToken":
			_ = json.NewEncoder(w).Encode(map[string]any{"accessToken": "tk", "expireIn": 7200})
		case "/webhook":
			mu.Lock()
			hits++
			mu.Unlock()
			w.WriteHeader(http.StatusBadRequest) // 400 → format_error（不可重试）
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()
	cfg := testConfig(srv.URL, srv.URL)
	cfg.Outbound = &OutboundConfig{Retry: RetryConfig{MaxAttempts: 3, BaseDelayMs: 1}}
	ch := New(cfg)
	defer ch.Close()

	r := &replier{
		msg: &IncomingMessage{ConversationID: "c1", SessionWebhook: srv.URL + "/webhook", MsgID: "m1"},
		cfg: &ch.cfg, tokens: ch.tokens, cards: ch.cards, oapi: ch.oapi, httpc: ch.httpc,
	}
	if err := r.Text(context.Background(), "hello"); err == nil {
		t.Fatal("format error should fail")
	}
	mu.Lock()
	defer mu.Unlock()
	if hits != 1 {
		t.Fatalf("webhook hits = %d, want 1（不可重试即停）", hits)
	}
}

// TestSendProactiveRetriesOnRateLimit 主动发送遇限流应重试成功。
func TestSendProactiveRetriesOnRateLimit(t *testing.T) {
	var mu sync.Mutex
	batchHits := int32(0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1.0/oauth2/accessToken":
			_ = json.NewEncoder(w).Encode(map[string]any{"accessToken": "tk", "expireIn": 7200})
		case r.URL.Path == "/v1.0/robot/oToMessages/batchSend":
			mu.Lock()
			batchHits++
			n := batchHits
			mu.Unlock()
			if n == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"success":false}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"success":true}`))
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"success":true}`))
		}
	}))
	defer srv.Close()
	cfg := testConfig(srv.URL, srv.URL)
	cfg.Outbound = &OutboundConfig{Retry: RetryConfig{MaxAttempts: 3, BaseDelayMs: 1}}
	ch := New(cfg)
	defer ch.Close()

	if err := ch.SendText(context.Background(), SendTarget{UserID: "u-1"}, "hi"); err != nil {
		t.Fatalf("proactive send should succeed after retry, err=%v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if batchHits < 2 {
		t.Fatalf("batchSend hits = %d, want >=2", batchHits)
	}
}

