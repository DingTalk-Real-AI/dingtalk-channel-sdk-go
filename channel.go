package channel

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/internal/normalize"
	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/internal/safety"
)

// MessageHandler 业务处理器：只关心"用户说了什么、机器人回什么"。
type MessageHandler func(ctx context.Context, msg *IncomingMessage, reply Reply) error

// CardActionHandler 卡片交互处理器（E7）。
type CardActionHandler func(ctx context.Context, action *CardAction, reply Reply) error

// RejectHandler 消息被策略拒绝时的回调。
type RejectHandler func(ctx context.Context, event *RejectEvent)

// Channel 钉钉会话接入层。用法：
//
//	ch := channel.New(channel.Config{ClientID: "...", ClientSecret: "..."})
//	ch.OnMessage(func(ctx context.Context, msg *channel.IncomingMessage, reply channel.Reply) error {
//		s, _ := reply.Stream(ctx)
//		for _, tok := range myLLM(msg.Text) {
//			_ = s.Append(tok)
//		}
//		return s.Finish("")
//	})
//	return ch.Start(ctx)
type Channel struct {
	cfg    Config
	tokens *tokenProvider
	cards  *cardClient
	bucket *tokenBucket
	oapi   *oapiClient
	conn   *streamConn
	httpc  *http.Client
	
	// 统一安全管线：过期/去重/策略/锁/队列全在这里
	pipeline *safety.SafetyPipeline
	// chatQueueMgr 与 pipeline 共享同一实例（测试可直接 FlushAll）
	chatQueueMgr *safety.ChatQueueManager

	// 功能组件
	botIdentity *botIdentityProvider
	hooks       *LifecycleHooks

	onMessage    MessageHandler
	onCardAction CardActionHandler
	onBatch      BatchHandler
	onReject     RejectHandler

	convMu    sync.Mutex
	convLocks map[string]*sync.Mutex
}

// New 创建 Channel。Config 会在此时应用默认值。
func New(cfg Config) *Channel {
	cfg.fill()
	httpc := &http.Client{Timeout: 15 * time.Second}
	tokens := newTokenProvider(&cfg, httpc)
	bucket := newTokenBucket(cfg.CardQPS)

	ch := &Channel{
		cfg:         cfg,
		tokens:      tokens,
		cards:       newCardClient(&cfg, tokens, bucket, httpc),
		oapi:        newOapiClient(&cfg, httpc),
		bucket:      bucket,
		httpc:       httpc,
		botIdentity: newBotIdentityProvider(&cfg, httpc, tokens),
		hooks:       newLifecycleHooks(),
		convLocks:   make(map[string]*sync.Mutex),
	}

	// per-chat 串行 + 批处理队列（消息与卡片回调共用）
	batchCfg := DefaultBatchConfig()
	chatQueueMgr := safety.NewChatQueueManager(&batchCfg, cfg.ChatQueue, cfg.MediaBatch)
	ch.chatQueueMgr = chatQueueMgr

	// 安全管线：OnMessage/OnBatch 用闭包读取 ch 上的最新注册，
	// 保证构造后再注册 handler 依然生效
	pipelineOpts := safety.PipelineOptions{
		ChatQueue: chatQueueMgr,
		OnMessage: func(ctx context.Context, msg *IncomingMessage, sources []*IncomingMessage) error {
			if ch.onMessage == nil {
				return nil
			}
			reply := &replier{
				msg:       msg,
				cfg:       &ch.cfg,
				tokens:    ch.tokens,
				cards:     ch.cards,
				oapi:      ch.oapi,
				httpc:     ch.httpc,
				proactive: ch.proactiveReply,
			}
			return ch.onMessage(ctx, msg, reply)
		},
		OnBatch: func(ctx context.Context, batch *BatchedMessage) error {
			if ch.onBatch == nil {
				return nil
			}
			return ch.onBatch(ctx, batch)
		},
		// 分发时动态探测 OnBatch 注册状态：注册了才走批处理路径，否则 per-chat 串行
		HasOnBatch: func() bool { return ch.onBatch != nil },
		OnReject: func(ctx context.Context, event *RejectEvent) {
			if ch.onReject != nil {
				ch.onReject(ctx, event)
			}
		},
		BotRobotCode: "",
	}
	ch.pipeline = safety.NewSafetyPipeline(cfg.Safety, pipelineOpts)

	ch.conn = newStreamConn(&ch.cfg, httpc, ch.dispatchFrame)
	return ch
}

// ── Handler 注册 ──

// OnMessage 注册消息处理器（单聊/群聊统一入口，E5）。
func (c *Channel) OnMessage(h MessageHandler) { c.onMessage = h }

// OnCardAction 注册卡片交互处理器；注册后自动订阅卡片回调 topic。
func (c *Channel) OnCardAction(h CardActionHandler) {
	c.onCardAction = h
	c.conn.cardTopicWanted = true
}

// OnReject 注册消息被策略拒绝时的回调。
func (c *Channel) OnReject(h RejectHandler) { c.onReject = h }

// OnBatchMessage 注册批处理消息处理器。
// 注册后消息将按会话分组、延迟合并后投递，而非逐条处理。
// 注意：批处理由 SafetyPipeline 内部管理，配置在 Config.Safety.TextBatch。
func (c *Channel) OnBatchMessage(h BatchHandler, cfgs ...BatchConfig) {
	c.onBatch = h
	// 批处理配置已在 Config.Safety.TextBatch 中设置
}

// ── Lifecycle hooks ──

// OnReady 注册连接就绪回调。
func (c *Channel) OnReady(fn func()) { c.hooks.OnReady(fn) }

// OnError 注册连接错误回调。
func (c *Channel) OnError(fn func(err error)) { c.hooks.OnError(fn) }

// OnReconnecting 注册重连中回调。
func (c *Channel) OnReconnecting(fn func()) { c.hooks.OnReconnecting(fn) }

// OnReconnected 注册重连成功回调。
func (c *Channel) OnReconnected(fn func()) { c.hooks.OnReconnected(fn) }

// OnDisconnected 注册断开连接回调。
func (c *Channel) OnDisconnected(fn func()) { c.hooks.OnDisconnected(fn) }

// ── Policy ──

// UpdatePolicy 动态更新策略配置。
func (c *Channel) UpdatePolicy(cfg PolicyConfig) { 
	c.pipeline.UpdatePolicy(cfg)
}

// GetPolicy 获取当前策略配置。
func (c *Channel) GetPolicy() PolicyConfig { 
	// 注意：Pipeline 中的 PolicyGate 不暴露 GetConfig，需要保存或从 cfg 读取
	return c.cfg.Safety.Policy
}

// ── Bot Identity ──

// GetBotIdentity 获取机器人身份信息（带缓存）。
func (c *Channel) GetBotIdentity(ctx context.Context) *BotIdentity {
	bot := c.botIdentity.Get(ctx)
	
	// 同步到 SafetyPipeline（用于自回复过滤）
	if bot != nil && c.pipeline != nil {
		c.pipeline.SetBotIdentity(bot.RobotCode)
	}
	
	return bot
}

// ── Lifecycle ──

// Start 阻塞运行 Stream 连接（内部自动重连，E8）。ctx 取消或 Close 后返回。
func (c *Channel) Start(ctx context.Context) error {
	if c.onMessage == nil && c.onBatch == nil {
		return errNoHandler
	}
	if err := c.cfg.validateTransport(); err != nil {
		return err
	}
	if c.cfg.Transport == TransportHTTP {
		return &channelError{"http mode has no long-running connection; serve c.HTTPCallbackHandler() per HTTP request instead of Start()"}
	}
	return c.conn.Run(ctx)
}

// Close 停止连接与重连。
func (c *Channel) Close() {
	c.conn.Close()
	
	// 清理 SafetyPipeline
	if c.pipeline != nil {
		c.pipeline.Dispose(context.Background())
	}
}

// proactiveReply webhook 失效时的主动发送兜底：
// 群聊按 openConversationId 群发，单聊发给消息发送者。
func (c *Channel) proactiveReply(ctx context.Context, msg *IncomingMessage, msgKey string, msgParam any) error {
	var target SendTarget
	if msg.ConversationType == ConversationTypeGroup {
		target = SendTarget{ConversationID: msg.ConversationID}
	} else {
		target = SendTarget{UserID: firstNonEmpty(msg.SenderID, msg.SenderStaffID)}
	}
	return c.proactiveSend(ctx, target, msgKey, msgParam)
}

var errNoHandler = &channelError{"OnMessage handler not registered"}

type channelError struct{ msg string }

func (e *channelError) Error() string { return e.msg }

// ── 内部调度 ──

// dispatchFrame 处理一帧业务数据，返回 ACK data。
func (c *Channel) dispatchFrame(ctx context.Context, f *frame) string {
	switch f.topic() {
	case topicBotMessage:
		c.handleBotMessage(ctx, f)
	case topicCardInstanceCB:
		c.handleCardAction(ctx, f)
	default:
		c.cfg.debugf("unsubscribed topic %q ignored", f.topic())
	}
	return ""
}

func (c *Channel) handleBotMessage(ctx context.Context, f *frame) {
	msg, err := normalize.NormalizeIncoming([]byte(f.Data))
	if err != nil {
		c.cfg.debugf("bad bot message payload: %v", err)
		return
	}
	c.processIncoming(ctx, f.messageID(), msg)
}

// processIncoming 传输无关的消息入口，委托给 SafetyPipeline：
// 过期 → 去重 → 自回复 → 策略 → 锁 → 队列（串行/批处理）。Stream 与 HTTP 模式共用。
func (c *Channel) processIncoming(ctx context.Context, protoID string, msg *IncomingMessage) {
	c.pipeline.PushMessage(ctx, protoID, msg)
}

func (c *Channel) handleCardAction(ctx context.Context, f *frame) {
	if c.onCardAction == nil {
		return
	}
	var d struct {
		OutTrackID  string          `json:"outTrackId"`
		UserID      string          `json:"userId"`
		DataContent json.RawMessage `json:"dataContent"`
	}
	if err := json.Unmarshal([]byte(f.Data), &d); err != nil {
		c.cfg.debugf("bad card action payload: %v", err)
		return
	}
	action := &CardAction{OutTrackID: d.OutTrackID, UserID: d.UserID, DataContent: d.DataContent, Raw: []byte(f.Data)}
	msg := &IncomingMessage{ConversationID: action.OutTrackID, SessionWebhook: ""}

	// 卡片回调走 PushAction：去重（投递 ID + 动作内容指纹，防换 ID 重放）
	// + 处理锁 + 同卡片串行
	actionFP := safety.ContentFingerprint("card:"+action.OutTrackID, 0, action.UserID, string(action.DataContent))
	c.pipeline.PushAction(ctx, f.messageID(), "card:"+action.OutTrackID, func() error {
		reply := &replier{msg: msg, cfg: &c.cfg, tokens: c.tokens, cards: c.cards, oapi: c.oapi, httpc: c.httpc, proactive: c.proactiveReply}
		return c.onCardAction(ctx, action, reply)
	}, actionFP)
}

func (c *Channel) lockConversation(id string) func() {
	c.convMu.Lock()
	lock, ok := c.convLocks[id]
	if !ok {
		lock = &sync.Mutex{}
		c.convLocks[id] = lock
	}
	c.convMu.Unlock()
	lock.Lock()
	return lock.Unlock
}

// ── DownloadFile ──

// DownloadFile 下载媒体文件到内存。
// mediaType: "image" | "file" | "video" | "voice"
func (c *Channel) DownloadFile(ctx context.Context, downloadCode, msgID, mediaType string) ([]byte, error) {
	resp, err := c.openMedia(ctx, downloadCode, msgID, mediaType)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// DownloadFileToFile 流式下载媒体文件到本地路径，不整块占用内存（大附件只有流缓冲开销）。
// destPath 的父目录必须已存在；先写同目录临时文件再原子重命名，失败不落半截文件。
// 返回写入的字节数。mediaType: "image" | "file" | "video" | "voice"
func (c *Channel) DownloadFileToFile(ctx context.Context, downloadCode, msgID, mediaType, destPath string) (int64, error) {
	if destPath == "" {
		return 0, &channelError{"destPath cannot be empty"}
	}
	tmp, err := os.CreateTemp(filepath.Dir(destPath), "."+filepath.Base(destPath)+".tmp-*")
	if err != nil {
		return 0, err
	}
	tmpName := tmp.Name()

	resp, err := c.openMedia(ctx, downloadCode, msgID, mediaType)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return 0, err
	}
	n, err := io.Copy(tmp, resp.Body)
	_ = resp.Body.Close()
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmpName, destPath)
	}
	if err != nil {
		_ = os.Remove(tmpName)
		return n, err
	}
	return n, nil
}

// openMedia 换取媒体下载 URL（含 SSRF 校验）并打开响应体。
func (c *Channel) openMedia(ctx context.Context, downloadCode, msgID, mediaType string) (*http.Response, error) {
	if downloadCode == "" {
		return nil, &channelError{"downloadCode cannot be empty"}
	}

	// 先换取下载 URL
	var out struct {
		DownloadURL string `json:"downloadUrl"`
	}
	path := "/v1.0/robot/messageFiles/download?downloadCode=" + downloadCode + "&messageId=" + msgID + "&robotCode=" + c.cfg.ClientID
	if err := c.cards.call(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	if out.DownloadURL == "" {
		return nil, &channelError{"empty download URL"}
	}

	// SSRF guard
	if err := safety.AssertPublicURLWithAllowlist(ctx, out.DownloadURL, c.cfg.SSRFAllowlist); err != nil {
		return nil, err
	}

	// 下载文件内容
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, out.DownloadURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, &channelError{"download failed: http " + strconv.Itoa(resp.StatusCode)}
	}
	return resp, nil
}
