package channel

import (
	"reflect"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

const (
	DefaultAPIBase        = "https://api.dingtalk.com"
	DefaultCardTemplateID = "02fcf2f4-5e02-4a85-b672-46d1f715543e.schema"
	DefaultStreamThrottle = 800 * time.Millisecond
	DefaultCardWatchdog   = 10 * time.Minute // 孤儿卡强制收口（connector 同款）
	DefaultErrorCooldown  = 60 * time.Second // 错误兜底文本冷却（防刷屏）
	DefaultStaleWindow    = 30 * time.Minute // 过期消息过滤
	defaultTextChunkLimit = 3500             // 超长文本分片
	defaultCardQPS        = 20.0
	defaultQPSBackoff     = 2 * time.Second
	DefaultKeepAliveIdle  = 120 * time.Second
	DefaultPongWait       = 5 * time.Second
	DefaultReconnectBase  = 1 * time.Second
	DefaultReconnectMax   = 30 * time.Second
	topicBotMessage       = "/v1.0/im/bot/messages/get"
	topicCardInstanceCB   = "/v1.0/card/instances/callback"
	UserAgent             = "dingtalk-channel-sdk-go/v0.1.0"
)

// 传输模式。对齐钉钉官方两种接收消息模式：stream 为默认长连接；
// http 为 HTTP 模式（dispatcher 形态：验签与分发由 SDK 负责，HTTP 服务由外部提供，见 http_mode.go）。
const (
	TransportStream = "stream"
	TransportHTTP   = "http"
)

// Config 是 Channel 的全部配置。ClientID/ClientSecret 为必填，
// 其余项为零值默认（见常量）。
type Config struct {
	ClientID     string
	ClientSecret string

	// APIBase 覆盖默认 https://api.dingtalk.com（测试/私有化用）。
	APIBase string
	// OapiBase 覆盖默认 https://oapi.dingtalk.com（媒体上传走旧版 OAPI）。
	OapiBase string
	// CardTemplateID 覆盖默认 AI 卡片模板。
	CardTemplateID string
	// StreamThrottle 单卡片流式更新最小间隔（默认 800ms）。
	StreamThrottle time.Duration
	// CardWatchdog 卡片孤儿保护：超过该时长未收口则强制 finish（默认 10min，<=0 关闭）。
	CardWatchdog time.Duration
	// ErrorCooldown 同会话错误兜底文本的发送冷却（默认 60s，<=0 关闭）。
	ErrorCooldown time.Duration
	// StaleMessageWindow 丢弃早于该窗龄的入站消息（默认 30min，<=0 关闭）。
	StaleMessageWindow time.Duration
	// TextChunkLimit 超长文本/Markdown 回复的分片上限（默认 3500，<=0 关闭；newline 感知切分）。
	TextChunkLimit int
	// CardQPS 卡片 API 全局令牌桶速率（默认 20，官方上限约 40）。
	CardQPS float64
	// AutoReconnect 断线自动重连（默认 true）。
	AutoReconnect *bool
	// KeepAliveIdle 空闲多久发 ws ping（默认 120s）。
	KeepAliveIdle time.Duration
	// Policy 消息策略配置。
	Policy PolicyConfig
	// Batch 旧批处理配置（已废弃，推荐使用 ChatQueue）。
	Batch *BatchConfig
	// ChatQueue per-chat 串行队列配置。
	ChatQueue *ChatQueueConfig
	// MediaBatch 媒体批处理（默认关闭）：启用后同会话连续媒体在窗口内合并投递。
	MediaBatch *MediaBatchConfig
	// Outbound 出站配置：重试参数、BeforeSend/AfterSend 钩子、统一页脚。
	Outbound *OutboundConfig
	// SSRFAllowlist 白名单主机名（精确或 *.suffix），命中跳过公网校验。
	SSRFAllowlist []string
	// Transport 入站传输模式：TransportStream（默认）或 TransportHTTP（HTTP 模式）。
	Transport string
	// HTTPTimestampTolerance HTTP 模式验签时间戳容忍窗口（默认 1h，<=0 关闭窗口检查）。
	HTTPTimestampTolerance time.Duration
	
	// Safety 统一安全配置。
	Safety types.SafetyConfig
	
	// DebugLog 可选调试日志钩子。
	DebugLog func(format string, args ...any)
}

func (c *Config) fill() {
	if c.APIBase == "" {
		c.APIBase = DefaultAPIBase
	}
	if c.OapiBase == "" {
		c.OapiBase = DefaultOapiBase
	}
	if c.CardTemplateID == "" {
		c.CardTemplateID = DefaultCardTemplateID
	}
	if c.StreamThrottle <= 0 {
		c.StreamThrottle = DefaultStreamThrottle
	}
	if c.CardWatchdog == 0 {
		c.CardWatchdog = DefaultCardWatchdog
	}
	if c.ErrorCooldown == 0 {
		c.ErrorCooldown = DefaultErrorCooldown
	}
	if c.TextChunkLimit == 0 {
		c.TextChunkLimit = defaultTextChunkLimit
	}
	if c.CardQPS <= 0 {
		c.CardQPS = defaultCardQPS
	}
	if c.KeepAliveIdle <= 0 {
		c.KeepAliveIdle = DefaultKeepAliveIdle
	}
	if c.Transport == "" {
		c.Transport = TransportStream
	}
	if c.HTTPTimestampTolerance == 0 {
		c.HTTPTimestampTolerance = DefaultHTTPTimestampTolerance
	}
	
	if c.ChatQueue == nil {
		defaultCfg := DefaultChatQueueConfig()
		c.ChatQueue = &defaultCfg
	}
	if c.MediaBatch == nil {
		c.MediaBatch = &MediaBatchConfig{Enabled: false, DelayMs: 800, MaxItems: 9}
	}

	// Safety 统一安全配置：未显式设置时沿用默认值；
	// 旧版分散字段（Policy/StaleMessageWindow）设置过则以旧字段为准。
	if c.Safety.StaleWindow == 0 {
		c.Safety = types.DefaultSafetyConfig()
		c.Safety.MediaBatch = MediaBatchConfig{Enabled: c.MediaBatch.Enabled, DelayMs: c.MediaBatch.DelayMs, MaxItems: c.MediaBatch.MaxItems}
		if c.StaleMessageWindow > 0 {
			c.Safety.StaleWindow = c.StaleMessageWindow
		}
		if !reflect.DeepEqual(c.Policy, PolicyConfig{}) {
			c.Safety.Policy = c.Policy
		}
		if c.Batch != nil {
			c.Safety.TextBatch = *c.Batch
		}
	}
}

// validateTransport 校验传输模式：未知值报错。
func (c *Config) validateTransport() error {
	switch c.Transport {
	case TransportStream, TransportHTTP:
		return nil
	default:
		return &channelError{"Config.Transport: unknown transport " + c.Transport + " (supported: stream, http)"}
	}
}

func (c *Config) reconnectEnabled() bool {
	return c.AutoReconnect == nil || *c.AutoReconnect
}

func (c *Config) debugf(format string, args ...any) {
	if c.DebugLog != nil {
		c.DebugLog(format, args...)
	}
}
