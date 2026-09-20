package channel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/internal/safety"
)

// ---- 测试基建：假钉钉 API（token / 卡片五步 / webhook）----

type fakeAPI struct {
	mu           sync.Mutex
	token        int
	create       int
	deliver      int
	businessFail bool
	instances    map[string]string // outTrackId -> 最后 msgContent
	streams      []map[string]any
	webhook      []map[string]any
	t            *testing.T
}

func newFakeAPI(t *testing.T) (*fakeAPI, *httptest.Server) {
	f := &fakeAPI{t: t, instances: map[string]string{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/oauth2/accessToken"):
			f.token++
			writeJSON(w, map[string]any{"accessToken": "tok-1", "expireIn": 7200})
		case r.URL.Path == "/v1.0/card/instances" && r.Method == http.MethodPost:
			f.create++
			writeJSON(w, map[string]any{})
		case r.URL.Path == "/v1.0/card/instances/deliver":
			f.deliver++
			if f.businessFail {
				writeJSON(w, map[string]any{"success": true, "result": []map[string]any{{"success": false, "errorMsg": "spaceId is illegal"}}})
				return
			}
			writeJSON(w, map[string]any{})
		case r.URL.Path == "/v1.0/card/instances" && r.Method == http.MethodPut:
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			otid, _ := body["outTrackId"].(string)
			if pm := cardParamMap(body); pm != nil {
				f.instances[otid], _ = pm["msgContent"].(string)
			}
			writeJSON(w, map[string]any{})
		case r.URL.Path == "/v1.0/card/streaming":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.streams = append(f.streams, body)
			writeJSON(w, map[string]any{})
		case r.URL.Path == "/webhook":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.webhook = append(f.webhook, body)
			writeJSON(w, map[string]any{"errcode": 0})
		case r.URL.Path == "/v1.0/robot/robotInfo":
			// 返回机器人身份信息（用于自回复过滤）
			writeJSON(w, map[string]any{"robotCode": "bot-1", "robotName": "TestBot"})
			return
		default:
			f.t.Errorf("unexpected api call: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(srv.Close)
	return f, srv
}

func cardParamMap(body map[string]any) map[string]any {
	cd, _ := body["cardData"].(map[string]any)
	if cd == nil {
		return nil
	}
	pm, _ := cd["cardParamMap"].(map[string]any)
	return pm
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func testConfig(apiBase, webhookBase string) Config {
	requireMention := false
	return Config{
		ClientID:       "ding-test",
		ClientSecret:   "secret",
		APIBase:        apiBase,
		StreamThrottle: 10 * time.Millisecond,
		CardQPS:        100,
		Policy: PolicyConfig{
			RequireMention: &requireMention,
		},
	}
}

func botFrame(messageID, msgID, text, webhook string) *frame {
	data, _ := json.Marshal(map[string]any{
		"conversationId":   "cid-1",
		"conversationType": "2",
		"msgId":            msgID,
		"senderStaffId":    "staff-1",
		"senderNick":       "John",
		"sessionWebhook":   webhook,
		"text":             map[string]string{"content": text},
		"isInAtList":       true,
		"msgtype":          "text",
	})
	return &frame{
		Type:    "CALLBACK",
		Headers: map[string]string{"topic": topicBotMessage, "messageId": messageID},
		Data:    string(data),
	}
}

// E6：同 messageId 与同 msgId 的重复投递都应被丢弃。
func TestDedupBothLayers(t *testing.T) {
	_, srv := newFakeAPI(t)
	cfg := testConfig(srv.URL, srv.URL)
	cfg.DebugLog = t.Logf  // 启用调试日志
	ch := New(cfg)
	var calls int
	ch.OnMessage(func(ctx context.Context, m *IncomingMessage, r Reply) error {
		t.Logf("Handler called: msgId=%s text=%q", m.MsgID, m.Content)
		calls++
		return nil
	})

	frames := []*frame{
		botFrame("m-1", "b-1", "hi", srv.URL+"/webhook"),
		botFrame("m-1", "b-1", "hi", srv.URL+"/webhook"), // 协议层重复
		botFrame("m-2", "b-1", "hi", srv.URL+"/webhook"), // 业务层重复
		botFrame("m-3", "b-2", "hi", srv.URL+"/webhook"),
	}
	
	for i, f := range frames {
		t.Logf("Dispatching frame %d: messageID=%s msgID from payload", i+1, f.Headers["messageId"])
		ch.dispatchFrame(context.Background(), f)
	}

	if calls != 2 {
		t.Fatalf("expected 2 handler calls, got %d", calls)
	}
}

// E1/E2/E3：stream() 立即建卡，append 节流更新，finish 走终帧+FINISHED。
func TestStreamLifecycle(t *testing.T) {
	f, srv := newFakeAPI(t)
	cfg := testConfig(srv.URL, srv.URL)
	ch := New(cfg)

	var got *IncomingMessage
	ch.OnMessage(func(ctx context.Context, m *IncomingMessage, r Reply) error {
		got = m
		s, err := r.Stream(ctx)
		if err != nil {
			t.Errorf("stream: %v", err)
		}
		_ = s.Append("Hello ")
		_ = s.Append("World") // 节流窗口内被合并，由后续全量补齐
		time.Sleep(30 * time.Millisecond)
		_ = s.Append("!")
		return s.Finish("")
	})
	ch.dispatchFrame(context.Background(), botFrame("m-1", "b-1", "@bot 你好", srv.URL+"/webhook"))

	// 群聊 @ 前缀剥离（E5）
	if got.Text != "你好" {
		t.Fatalf("expected at-stripped text, got %q", got.Text)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.create != 1 || f.deliver != 1 {
		t.Fatalf("card create/deliver = %d/%d, want 1/1", f.create, f.deliver)
	}
	lastFin := false
	for _, st := range f.streams {
		if fin, _ := st["isFinalize"].(bool); fin {
			lastFin = true
			if c, _ := st["content"].(string); !strings.Contains(c, "World") {
				t.Fatalf("finalize content missing accumulated text: %v", c)
			}
		}
	}
	if !lastFin {
		t.Fatalf("no finalize frame sent")
	}
	any := false
	for _, c := range f.instances {
		if strings.Contains(c, "Hello") {
			any = true
		}
	}
	if !any {
		t.Fatalf("no FINISHED status with content, instances=%v", f.instances)
	}
}

// E4：卡片创建失败 → finish 自动降级为 webhook 文本。
func TestStreamFallbackOnCardFailure(t *testing.T) {
	f, _ := newFakeAPI(t)
	// 拆两条路径：API 可用，但卡片创建端点改为 500。
	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1.0/card/instances" && r.Method == http.MethodPost {
			w.WriteHeader(500)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/oauth2/accessToken") {
			writeJSON(w, map[string]any{"accessToken": "tok", "expireIn": 7200})
			return
		}
		if r.URL.Path == "/webhook" {
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.mu.Lock()
			f.webhook = append(f.webhook, body)
			f.mu.Unlock()
			writeJSON(w, map[string]any{})
			return
		}
		w.WriteHeader(200)
	}))
	defer apiSrv.Close()

	ch := New(testConfig(apiSrv.URL, apiSrv.URL))
	ch.OnMessage(func(ctx context.Context, m *IncomingMessage, r Reply) error {
		s, _ := r.Stream(ctx)
		_ = s.Append("final answer")
		return s.Finish("")
	})
	ch.dispatchFrame(context.Background(), botFrame("m-1", "b-1", "hi", apiSrv.URL+"/webhook"))

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.webhook) == 0 {
		t.Fatalf("fallback webhook not called")
	}
	mp, _ := f.webhook[0]["msgParam"].(string)
	if !strings.Contains(mp, "final answer") {
		t.Fatalf("fallback content wrong: %v", f.webhook[0])
	}
}

// E9：text/markdown/image 走 sessionWebhook 的正确 msgKey。
func TestWebhookReplies(t *testing.T) {
	f, srv := newFakeAPI(t)
	ch := New(testConfig(srv.URL, srv.URL))
	ch.OnMessage(func(ctx context.Context, m *IncomingMessage, r Reply) error {
		if err := r.Text(ctx, "plain"); err != nil {
			t.Error(err)
		}
		if err := r.Markdown(ctx, "T", "# md"); err != nil {
			t.Error(err)
		}
		return r.Image(ctx, "https://x/y.png")
	})
	ch.dispatchFrame(context.Background(), botFrame("m-1", "b-1", "hi", srv.URL+"/webhook"))

	f.mu.Lock()
	defer f.mu.Unlock()
	want := []string{"sampleText", "sampleMarkdown", "sampleImageMsg"}
	if len(f.webhook) != 3 {
		t.Fatalf("webhook calls = %d", len(f.webhook))
	}
	for i, w := range want {
		if f.webhook[i]["msgKey"] != w {
			t.Fatalf("call %d msgKey = %v, want %s", i, f.webhook[i]["msgKey"], w)
		}
	}
}

func TestDeduperTTL(t *testing.T) {
	d := safety.NewDeduper(20 * time.Millisecond)
	if d.CheckAndMark("a") {
		t.Fatal("first mark should not be dup")
	}
	if !d.CheckAndMark("a") {
		t.Fatal("second mark should be dup")
	}
	time.Sleep(30 * time.Millisecond)
	if d.CheckAndMark("a") {
		t.Fatal("expired key should not be dup")
	}
}

func TestTokenBucketAndBackoff(t *testing.T) {
	b := newTokenBucket(1000)
	for i := 0; i < 20; i++ {
		if _, err := b.waitFor(nil); err != nil {
			t.Fatalf("waitFor: %v", err)
		}
	}
	b.triggerBackoff()
	b.mu.Lock()
	ended := b.backoffEnd
	b.mu.Unlock()
	if !ended.After(time.Now()) {
		t.Fatal("backoff not armed")
	}
}

// 消息归一化：richText 提取文本 + @提及
func TestNormalizeRichText(t *testing.T) {
	data, _ := json.Marshal(map[string]any{
		"conversationId":   "cid-1",
		"conversationType": "2",
		"msgId":            "rt-1",
		"senderStaffId":    "staff-1",
		"senderNick":       "John",
		"sessionWebhook":   "",
		"isInAtList":       true,
		"msgtype":          "richText",
		"content": map[string]any{
			"richText": []map[string]any{
				{"type": "text", "text": "hello "},
				{"type": "text", "text": "world"},
				{"type": "at", "atUserIds": []string{"staff-2"}},
			},
		},
	})
	msg, err := normalizeIncoming(data)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Text != "hello world" {
		t.Fatalf("richText text = %q, want %q", msg.Text, "hello world")
	}
	if len(msg.Mentions) == 0 || msg.Mentions[0].UserID != "staff-2" {
		t.Fatalf("mentions = %v, want staff-2", msg.Mentions)
	}
}

// 消息归一化：picture 提取资源
func TestNormalizePicture(t *testing.T) {
	data, _ := json.Marshal(map[string]any{
		"conversationId":   "cid-1",
		"conversationType": "2",
		"msgId":            "pic-1",
		"senderStaffId":    "staff-1",
		"senderNick":       "John",
		"sessionWebhook":   "",
		"isInAtList":       true,
		"msgtype":          "picture",
		"content":          map[string]string{"downloadCode": "dc-abc123"},
	})
	msg, err := normalizeIncoming(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(msg.Resources) != 1 {
		t.Fatalf("resources len = %d, want 1", len(msg.Resources))
	}
	if msg.Resources[0].Type != "image" || msg.Resources[0].DownloadCode != "dc-abc123" {
		t.Fatalf("resource = %+v", msg.Resources[0])
	}
}

// 消息归一化：未知类型走 default
func TestNormalizeUnknownType(t *testing.T) {
	data, _ := json.Marshal(map[string]any{
		"conversationId":   "cid-1",
		"conversationType": "2",
		"msgId":            "unk-1",
		"senderStaffId":    "staff-1",
		"senderNick":       "John",
		"sessionWebhook":   "",
		"isInAtList":       true,
		"msgtype":          "someNewType",
		"content":          map[string]string{"content": "fallback text"},
	})
	msg, err := normalizeIncoming(data)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Text != "fallback text" {
		t.Fatalf("unknown type text = %q, want %q", msg.Text, "fallback text")
	}
}

// 消息归一化：file 提取资源 + 文本
func TestNormalizeFile(t *testing.T) {
	data, _ := json.Marshal(map[string]any{
		"conversationId":   "cid-1",
		"conversationType": "2",
		"msgId":            "file-1",
		"senderStaffId":    "staff-1",
		"senderNick":       "John",
		"sessionWebhook":   "",
		"isInAtList":       true,
		"msgtype":          "file",
		"content":          map[string]string{"downloadCode": "dc-file1", "fileName": "report.pdf"},
	})
	msg, err := normalizeIncoming(data)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Text != "[文件: report.pdf]" {
		t.Fatalf("file text = %q, want %q", msg.Text, "[文件: report.pdf]")
	}
	if len(msg.Resources) != 1 || msg.Resources[0].Type != "file" || msg.Resources[0].DownloadCode != "dc-file1" {
		t.Fatalf("resource = %+v", msg.Resources)
	}
}

// 消息归一化：audio 提取语音识别文本
func TestNormalizeAudio(t *testing.T) {
	data, _ := json.Marshal(map[string]any{
		"conversationId":   "cid-1",
		"conversationType": "2",
		"msgId":            "audio-1",
		"senderStaffId":    "staff-1",
		"senderNick":       "John",
		"sessionWebhook":   "",
		"isInAtList":       true,
		"msgtype":          "audio",
		"content":          map[string]string{"downloadCode": "dc-audio1", "recognition": "你好世界"},
	})
	msg, err := normalizeIncoming(data)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Text != "你好世界" {
		t.Fatalf("audio text = %q, want %q", msg.Text, "你好世界")
	}
	if len(msg.Resources) != 1 || msg.Resources[0].Type != "audio" {
		t.Fatalf("resource = %+v", msg.Resources)
	}
}

// 消息归一化：video
func TestNormalizeVideo(t *testing.T) {
	data, _ := json.Marshal(map[string]any{
		"conversationId":   "cid-1",
		"conversationType": "2",
		"msgId":            "video-1",
		"senderStaffId":    "staff-1",
		"senderNick":       "John",
		"sessionWebhook":   "",
		"isInAtList":       true,
		"msgtype":          "video",
		"content":          map[string]string{"downloadCode": "dc-video1", "fileName": "clip.mp4"},
	})
	msg, err := normalizeIncoming(data)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Text != "[视频]" {
		t.Fatalf("video text = %q, want %q", msg.Text, "[视频]")
	}
	if len(msg.Resources) != 1 || msg.Resources[0].Type != "video" {
		t.Fatalf("resource = %+v", msg.Resources)
	}
}

// 消息归一化：actionCard 提取 title + body + actionUrls
func TestNormalizeActionCard(t *testing.T) {
	data, _ := json.Marshal(map[string]any{
		"conversationId":   "cid-1",
		"conversationType": "2",
		"msgId":            "card-1",
		"senderStaffId":    "staff-1",
		"senderNick":       "John",
		"sessionWebhook":   "",
		"isInAtList":       true,
		"msgtype":          "actionCard",
		"content": map[string]any{
			"title": "请假审批",
			"text":  "张三申请年假3天",
			"actionUrlItemList": []map[string]string{
				{"actionUrl": "https:// approve.example.com/1"},
				{"actionUrl": "https://reject.example.com/2"},
			},
		},
	})
	msg, err := normalizeIncoming(data)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(msg.Text, "请假审批") {
		t.Fatalf("actionCard text = %q, want to contain title", msg.Text)
	}
	if !strings.Contains(msg.Text, "张三申请年假3天") {
		t.Fatalf("actionCard text = %q, want to contain body", msg.Text)
	}
	if !strings.Contains(msg.Text, "approve.example.com") {
		t.Fatalf("actionCard text = %q, want to contain action url", msg.Text)
	}
}
