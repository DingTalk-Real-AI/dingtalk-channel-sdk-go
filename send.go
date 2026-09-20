package channel

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/internal/outbound"
)

// SendTarget 主动发消息目标。
// UserID（单聊）与 ConversationID（群聊 openConversationId）二选一。
type SendTarget struct {
	UserID         string
	ConversationID string

	// 群聊可选：@ 能力（dws 验证的官方参数）。
	AtUserIds     []string
	AtDingtalkIds []string
	AtAll         bool
}

func (t SendTarget) valid() bool {
	return (t.UserID != "") != (t.ConversationID != "")
}

// SendText 主动发文本（不依赖入站消息）。
func (c *Channel) SendText(ctx context.Context, target SendTarget, content string) error {
	return c.proactiveSend(ctx, target, "sampleText", map[string]string{"content": content})
}

// SendMarkdown 主动发 Markdown。
func (c *Channel) SendMarkdown(ctx context.Context, target SendTarget, title, text string) error {
	if title == "" {
		title = firstLineTitle(text)
	}
	return c.proactiveSend(ctx, target, "sampleMarkdown", map[string]string{"title": title, "text": text})
}

// SendFile 主动发文件消息（sampleFile）：把上传的媒体（RawMediaID，带 @ 前缀）作为文件送达，
// 接收方点开即可查看/下载——图片可靠送达群/单聊用这个（流式卡片 content 不支持内嵌图片，协议限制）。
func (c *Channel) SendFile(ctx context.Context, target SendTarget, fileName, rawMediaID string) error {
	return c.proactiveSend(ctx, target, "sampleFile", map[string]string{
		"mediaId":  rawMediaID,
		"fileName": fileName,
		"fileType": "png",
	})
}

// SendVideo 主动发视频消息（sampleVideo，对齐官方 connector sendVideoProactive）。
// rawVideoMediaID/rawPicMediaID 为 uploadMedia 返回的 RawMediaID（带 @）；durationMs 毫秒。
func (c *Channel) SendVideo(ctx context.Context, target SendTarget, rawVideoMediaID, rawPicMediaID string, durationMs int) error {
	if durationMs <= 0 {
		durationMs = 60000
	}
	return c.proactiveSend(ctx, target, "sampleVideo", map[string]string{
		"duration":     strconv.Itoa(durationMs),
		"videoMediaId": rawVideoMediaID,
		"videoType":    "mp4",
		"picMediaId":   rawPicMediaID,
	})
}

// SendAudio 主动发音频消息（sampleAudio，对齐官方 connector sendAudioProactive）。
func (c *Channel) SendAudio(ctx context.Context, target SendTarget, rawMediaID string, durationMs int) error {
	if durationMs <= 0 {
		durationMs = 60000
	}
	return c.proactiveSend(ctx, target, "sampleAudio", map[string]string{
		"mediaId":  rawMediaID,
		"duration": strconv.Itoa(durationMs),
	})
}

// SendImage 主动发图片（photoURL 需公网可访问；OAPI 上传的 mediaId 无公网 URL，本地图请用 SendFile）。
func (c *Channel) SendImage(ctx context.Context, target SendTarget, imageURL string) error {
	return c.proactiveSend(ctx, target, "sampleImageMsg", map[string]string{"photoURL": imageURL})
}

func (c *Channel) proactiveSend(ctx context.Context, target SendTarget, msgKey string, msgParam any) error {
	if !target.valid() {
		return &channelError{"SendTarget: 恰好设置 UserID（单聊）或 ConversationID（群聊）之一"}
	}

	// 出站钩子 + 统一页脚（OutboundConfig）
	targetID := target.UserID
	if targetID == "" {
		targetID = target.ConversationID
	}
	if out := c.cfg.Outbound; out != nil {
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
			if replaced := out.BeforeSend("send", targetID, msgParam); replaced != nil {
				msgParam = replaced
			}
		}
	}
	sendErr := c.proactiveSendInner(ctx, target, msgKey, msgParam)
	if out := c.cfg.Outbound; out != nil && out.AfterSend != nil {
		errStr := ""
		if sendErr != nil {
			errStr = sendErr.Error()
		}
		out.AfterSend("send", targetID, sendErr == nil, errStr)
	}
	return sendErr
}

func (c *Channel) proactiveSendInner(ctx context.Context, target SendTarget, msgKey string, msgParam any) error {
	param, err := json.Marshal(msgParam)
	if err != nil {
		return err
	}
	paramStr := string(param) // msgParam 必须字符串化 JSON（官方文档）

	// 出站重试：仅可重试错误（限流/超时/未知）按指数退避重试
	opts := retryOptions(&c.cfg)
	if target.UserID != "" {
		return outbound.Retry(ctx, func(attempt int) error {
			return c.cards.call(ctx, http.MethodPost, "/v1.0/robot/oToMessages/batchSend", map[string]any{
				"robotCode": c.cfg.ClientID,
				"userIds":   []string{target.UserID},
				"msgKey":    msgKey,
				"msgParam":  paramStr,
			}, nil)
		}, opts)
	}

	body := map[string]any{
		"robotCode":          c.cfg.ClientID,
		"openConversationId": target.ConversationID,
		"msgKey":             msgKey,
		"msgParam":           paramStr,
	}
	if len(target.AtUserIds) > 0 {
		body["atUserIds"] = target.AtUserIds
	}
	if len(target.AtDingtalkIds) > 0 {
		body["atOpendingtalkIds"] = target.AtDingtalkIds
	}
	if target.AtAll {
		body["isAtAll"] = true
	}
	return outbound.Retry(ctx, func(attempt int) error {
		return c.cards.call(ctx, http.MethodPost, "/v1.0/robot/groupMessages/send", body, nil)
	}, opts)
}
