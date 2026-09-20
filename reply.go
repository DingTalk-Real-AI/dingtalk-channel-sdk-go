package channel

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/internal/outbound"
	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// Reply 回复句柄：text/markdown/image 走 sessionWebhook；stream() 走 AI 卡片（SPEC §4）。
type Reply interface {
	Text(ctx context.Context, content string) error
	Markdown(ctx context.Context, title, text string) error
	Image(ctx context.Context, imageURL string) error
	Stream(ctx context.Context) (CardStreamer, error)
	// DownloadURL 换取消息附件下载地址（E9）。
	DownloadURL(ctx context.Context, downloadCode, msgID string) (string, error)
	// UploadMedia 上传媒体文件，返回 mediaId（E9）。mediaType: image|file|video|voice。
	UploadMedia(ctx context.Context, mediaType, filename, contentType string, data []byte) (*MediaUploadResult, error)
}

type replier struct {
	msg    *IncomingMessage
	cfg    *Config
	tokens *tokenProvider
	cards  *cardClient
	oapi   *oapiClient
	httpc  *http.Client
	// proactive 为 webhook 失效（过期/撤回）时的主动发送兜底，由 Channel 注入。
	proactive func(ctx context.Context, msg *IncomingMessage, msgKey string, msgParam any) error
}

// chunkText 超长文本切分：有 code fence 时用 code-fence-aware 切分，
// 否则用 rune-based newline 边界切分（保留完整内容）。
func chunkText(s string, limit int) []string {
	if limit <= 0 || len([]rune(s)) <= limit {
		return []string{s}
	}
	if strings.Contains(s, "```") {
		return outbound.SplitWithCodeFences(s, limit)
	}
	runes := []rune(s)
	var chunks []string
	for len(runes) > 0 {
		if len(runes) <= limit {
			chunks = append(chunks, string(runes))
			break
		}
		cut := -1
		for i := limit; i >= limit/2; i-- {
			if runes[i] == '\n' {
				cut = i + 1
				break
			}
		}
		if cut <= 0 {
			cut = limit
		}
		chunks = append(chunks, string(runes[:cut]))
		runes = runes[cut:]
	}
	return chunks
}

// applyOutbound 出站钩子 + 统一页脚（OutboundConfig）：BeforeSend 可改写 payload，Footer 追加到文本。
func (r *replier) applyOutbound(msgKey string, msgParam any) any {
	out := r.cfg.Outbound
	if out == nil {
		return msgParam
	}
	if out.Footer != "" {
		if m, ok := msgParam.(map[string]string); ok {
			m2 := make(map[string]string, len(m))
			for k, v := range m {
				m2[k] = v
			}
			if msgKey == "sampleText" {
				m2["content"] += "\n\n" + out.Footer
			} else if msgKey == "sampleMarkdown" {
				m2["text"] += "\n\n---\n" + out.Footer
			}
			msgParam = m2
		}
	}
	if out.BeforeSend != nil {
		if replaced := out.BeforeSend("reply", r.msg.ConversationID, msgParam); replaced != nil {
			msgParam = replaced
		}
	}
	return msgParam
}

func (r *replier) afterSend(ok bool, errStr string) {
	if out := r.cfg.Outbound; out != nil && out.AfterSend != nil {
		out.AfterSend("reply", r.msg.ConversationID, ok, errStr)
	}
}

func (r *replier) webhook(ctx context.Context, msgKey string, msgParam any) error {
	msgParam = r.applyOutbound(msgKey, msgParam)

	// webhook 已过期（sessionWebhook 有时效）或缺失：直接走主动发送兜底
	if r.msg.SessionWebhook == "" || webhookExpired(r.msg) {
		if r.msg.SessionWebhook == "" && r.proactive == nil {
			return fmt.Errorf("reply: sessionWebhook missing")
		}
		if r.proactive != nil {
			r.cfg.debugf("reply webhook unavailable (missing/expired), falling back to proactive send")
			return r.proactive(ctx, r.msg, msgKey, msgParam)
		}
	}

	// 超长文本/Markdown：按 limit 分片多次发送，避免单条超限被截断。
	if r.cfg.TextChunkLimit > 0 {
		switch p := msgParam.(type) {
		case map[string]string:
			if content, ok := p["content"]; ok {
				for _, c := range chunkText(content, r.cfg.TextChunkLimit) {
					if err := r.deliverOnce(ctx, msgKey, map[string]string{"content": c}); err != nil {
						return err
					}
				}
				return nil
			}
			if text, ok := p["text"]; ok {
				title := p["title"]
				for _, c := range chunkText(text, r.cfg.TextChunkLimit) {
					if err := r.deliverOnce(ctx, msgKey, map[string]string{"title": title, "text": c}); err != nil {
						return err
					}
				}
				return nil
			}
		}
	}
	return r.deliverOnce(ctx, msgKey, msgParam)
}

// webhookExpired 判断 sessionWebhook 是否已过时效。
func webhookExpired(msg *IncomingMessage) bool {
	return msg.WebhookExpiredAt > 0 && time.Now().UnixMilli() > msg.WebhookExpiredAt
}

// deliverOnce 单次投递：webhook 优先；目标失效（撤回/过期返回 404）时转主动发送兜底。
func (r *replier) deliverOnce(ctx context.Context, msgKey string, msgParam any) error {
	err := r.webhookOnce(ctx, msgKey, msgParam)
	if err == nil {
		return nil
	}
	if r.proactive != nil && types.IsReplyTargetGone(types.ClassifyError(err)) {
		r.cfg.debugf("reply webhook target gone (%v), falling back to proactive send", err)
		return r.proactive(ctx, r.msg, msgKey, msgParam)
	}
	return err
}

func (r *replier) webhookOnce(ctx context.Context, msgKey string, msgParam any) error {
	err := r.webhookOnceInner(ctx, msgKey, msgParam)
	if err != nil {
		r.afterSend(false, err.Error())
		return err
	}
	r.afterSend(true, "")
	return nil
}

func (r *replier) webhookOnceInner(ctx context.Context, msgKey string, msgParam any) error {
	// 出站重试：仅可重试错误（限流/超时/未知）按指数退避重试，格式错误立即失败
	opts := retryOptions(r.cfg)
	return outbound.Retry(ctx, func(attempt int) error {
		return r.webhookDo(ctx, msgKey, msgParam)
	}, opts)
}

// webhookDo 单次 webhook 发送（不含重试）。
func (r *replier) webhookDo(ctx context.Context, msgKey string, msgParam any) error {
	param, _ := json.Marshal(msgParam)
	// 官方文档要求 msgParam 为字符串化 JSON（对象形式会 400）。
	body, _ := json.Marshal(map[string]any{"msgKey": msgKey, "msgParam": string(param)})
	token, err := r.tokens.Get(ctx)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.msg.SessionWebhook, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)
	resp, err := r.httpc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= http.StatusBadRequest {
		return &apiError{Status: resp.StatusCode, Body: string(raw)}
	}
	return nil
}

func (r *replier) Text(ctx context.Context, content string) error {
	return r.webhook(ctx, "sampleText", map[string]string{"content": content})
}

func (r *replier) Markdown(ctx context.Context, title, text string) error {
	if title == "" {
		title = firstLineTitle(text)
	}
	return r.webhook(ctx, "sampleMarkdown", map[string]string{"title": title, "text": text})
}

func (r *replier) Image(ctx context.Context, imageURL string) error {
	return r.webhook(ctx, "sampleImageMsg", map[string]string{"photoURL": imageURL})
}

func (r *replier) UploadMedia(ctx context.Context, mediaType, filename, contentType string, data []byte) (*MediaUploadResult, error) {
	return r.oapi.UploadMedia(ctx, mediaType, filename, contentType, data)
}

func (r *replier) DownloadURL(ctx context.Context, downloadCode, msgID string) (string, error) {
	var out struct {
		DownloadURL string `json:"downloadUrl"`
	}
	path := fmt.Sprintf("/v1.0/robot/messageFiles/download?downloadCode=%s&messageId=%s&robotCode=%s",
		downloadCode, msgID, r.cfg.ClientID)
	if err := r.cards.call(ctx, http.MethodGet, path, nil, &out); err != nil {
		return "", err
	}
	return out.DownloadURL, nil
}

// Stream 立即创建并投递 AI 卡片（E1：先出"输入中"卡，不等首个 token）。
// 创建失败时返回带降级能力的 streamer（append/finish 自动降级为 webhook 文本，E4）。
func (r *replier) Stream(ctx context.Context) (CardStreamer, error) {
	sctx, cancel := context.WithCancel(ctx)
	target := cardTarget{
		IsGroup:        r.msg.ConversationType == ConversationTypeGroup,
		ConversationID: r.msg.ConversationID,
		UserID:         firstNonEmpty(r.msg.SenderStaffID, r.msg.SenderID),
		RobotCode:      r.cfg.ClientID,
	}
	s := &cardStreamer{
		ctx:      sctx,
		cancel:   cancel,
		client:   r.cards,
		target:   target,
		throttle: r.cfg.StreamThrottle,
		fallback: func(ctx context.Context, text string) error {
			if !errorCooldownPass(r.msg.ConversationID, r.cfg.ErrorCooldown) {
				return nil // 冷却期内不再发错误文本（防刷屏）
			}
			return r.Text(ctx, text)
		},
		deliverRest: func(text string) error {
			// 复用文本回复链路：自动按 TextChunkLimit 分片（含代码围栏感知切分）
			if err := r.Text(context.Background(), text); err != nil {
				r.cfg.debugf("card overflow remainder deliver failed: %v", err)
				return err
			}
			return nil
		},
	}
	card, err := r.cards.createAndDeliver(sctx, target)
	if err != nil {
		// 静默降级：streamer 可用，但所有更新走 webhook。
		r.cfg.debugf("card create failed, fallback to webhook text: %v", err)
		return s, nil
	}
	s.card = card
	s.armWatchdog()
	return s, nil
}

// errLimiter 错误兜底文本冷却（connector 同款：同会话 60s 内只发一次，防刷屏）。
var (
	errLimMu   sync.Mutex
	errLimLast = map[string]time.Time{}
)

func errorCooldownPass(key string, cooldown time.Duration) bool {
	if cooldown <= 0 {
		return true
	}
	errLimMu.Lock()
	defer errLimMu.Unlock()
	now := time.Now()
	if t, ok := errLimLast[key]; ok && now.Sub(t) < cooldown {
		return false
	}
	if len(errLimLast) > 4096 { // 防无界增长：超限整体重置（冷却是 best-effort 防刷屏，无需精确）
		errLimLast = make(map[string]time.Time)
	}
	errLimLast[key] = now
	return true
}

// CardStreamer 流式卡片句柄（Append/Finish/Fail），语义见 SPEC §4。
type CardStreamer interface {
	Append(delta string) error
	Finish(finalText string) error
	Fail(errText string) error
	Abort() error
	CardDelivered() bool
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func firstLineTitle(text string) string {
	for _, line := range strings.Split(text, "\n") {
		t := strings.TrimLeft(line, "#*-> \t")
		if t != "" {
			if len(t) > 20 {
				t = t[:20]
			}
			return t
		}
	}
	return "Message"
}
