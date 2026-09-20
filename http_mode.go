package channel

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/internal/normalize"
)

// HTTP 模式传输（dispatcher 形态）：SDK 负责验签与消息分发，HTTP 服务由外部提供
// （serverless 函数入口、企业网关等）。回复仍走消息体内的 sessionWebhook，
// 处理管线与 Stream 模式完全共用（见 processIncoming）。

// DefaultHTTPTimestampTolerance 验签时间戳容忍窗口（防重放，默认 1 小时）。
const DefaultHTTPTimestampTolerance = time.Hour

// verifyHTTPSign 校验钉钉 HTTP 模式回调签名：
// sign = Base64(HmacSHA256(key=appSecret, msg=timestamp+"\n"+appSecret))，
// 并校验 timestamp 未超出容忍窗口（<=0 关闭窗口检查）。timestamp 兼容秒/毫秒。
func verifyHTTPSign(secret, timestamp, sign string, tolerance time.Duration, now time.Time) error {
	if sign == "" || timestamp == "" {
		return &channelError{"http mode: missing timestamp/sign headers"}
	}
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return &channelError{"http mode: invalid timestamp header"}
	}
	if ts < 1e11 { // 秒级时间戳统一换算为毫秒
		ts *= 1000
	}
	if tolerance > 0 {
		age := now.UnixMilli() - ts
		if age < 0 {
			age = -age
		}
		if age > tolerance.Milliseconds() {
			return &channelError{"http mode: timestamp outside tolerance window"}
		}
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "\n" + secret))
	want := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(want), []byte(sign)) {
		return &channelError{"http mode: signature mismatch"}
	}
	return nil
}

// HandleHTTPCallback 处理一帧 HTTP 模式回调消息体（企业内部机器人，
// body 与 Stream 的 data 载荷同构）。验签失败或载荷非法返回 error
// （调用方应回 401/400）；业务处理与 Stream 模式一致——吞错并快速返回。
func (c *Channel) HandleHTTPCallback(ctx context.Context, body []byte, timestamp, sign string) error {
	if c.onMessage == nil && c.onBatch == nil {
		return errNoHandler
	}
	if err := verifyHTTPSign(c.cfg.ClientSecret, timestamp, sign, c.cfg.HTTPTimestampTolerance, time.Now()); err != nil {
		return err
	}
	msg, err := normalize.NormalizeIncoming(body)
	if err != nil {
		return &channelError{"http mode: bad bot message payload: " + err.Error()}
	}
	// HTTP 模式回调无协议层投递 ID，两层去重均落 msgId；
	// 重试导致的重复推送由 DedupCache 幂等吸收。
	c.processIncoming(ctx, msg.MsgID, msg)
	return nil
}

// HTTPCallbackHandler 返回可直接挂到 http.ServeMux 的处理器：
// 读取请求头 timestamp/sign 与请求体，验签失败回 401、载荷非法回 400、其余回 200。
// 仅传输适配，不含业务逻辑。
func (c *Channel) HTTPCallbackHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
		if err != nil {
			http.Error(w, "read body failed", http.StatusBadRequest)
			return
		}
		if err := c.HandleHTTPCallback(r.Context(), body, r.Header.Get("timestamp"), r.Header.Get("sign")); err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}
