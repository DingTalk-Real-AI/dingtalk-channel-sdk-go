// Package normalize 负责入站消息的归一化：解析钉钉机器人回调载荷，
// 按消息类型提取文本/资源/提及，产出 types.IncomingMessage（SPEC §3.1）。
package normalize

import (
	"encoding/json"
	"strings"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// botCallbackData 与钉钉 /v1.0/im/bot/messages/get 的 data 字段一一对应。
type botCallbackData struct {
	ConversationID            string         `json:"conversationId"`
	AtUsers                   []types.AtUser `json:"atUsers"`
	ChatbotCorpID             string         `json:"chatbotCorpId"`
	ChatbotUserID             string         `json:"chatbotUserId"`
	MsgID                     string         `json:"msgId"`
	SenderNick                string         `json:"senderNick"`
	IsAdmin                   bool           `json:"isAdmin"`
	SenderStaffID             string         `json:"senderStaffId"`
	SessionWebhookExpiredTime int64          `json:"sessionWebhookExpiredTime"`
	CreateAt                  int64          `json:"createAt"`
	SenderCorpID              string         `json:"senderCorpId"`
	ConversationType          string         `json:"conversationType"`
	SenderID                  string         `json:"senderId"`
	ConversationTitle         string         `json:"conversationTitle"`
	IsInAtList                bool           `json:"isInAtList"`
	SessionWebhook            string         `json:"sessionWebhook"`
	Text                      struct {
		Content string `json:"content"`
	} `json:"text"`
	MsgType string          `json:"msgtype"`
	Content json.RawMessage `json:"content"`
}

// parseContent 按消息类型分发到对应 converter，提取文本内容和媒体资源。
// 钉钉机器人回调支持: text / richText / picture / file / audio / video /
// markdown / actionCard / interactiveCard / reply。
func parseContent(msgType string, rawContent json.RawMessage, atUsers []types.AtUser) (text string, resources []types.Resource, mentions []types.Mention) {
	if len(rawContent) == 0 {
		return
	}
	var m map[string]interface{}
	if err := json.Unmarshal(rawContent, &m); err != nil {
		return
	}
	switch msgType {
	case "text":
		text = convertText(m)
	case "richText":
		text, mentions = convertRichText(m, atUsers)
	case "picture":
		text, resources = convertPicture(m)
	case "file":
		text, resources = convertFile(m)
	case "audio":
		text, resources = convertAudio(m)
	case "video":
		text, resources = convertVideo(m)
	case "markdown":
		text = convertMarkdown(m)
	case "actionCard":
		text = convertActionCard(m)
	case "interactiveCard":
		text = convertInteractiveCard(m)
	case "reply":
		text = convertReply(m)
	default:
		if t, ok := m["content"].(string); ok {
			text = t
		}
	}
	return
}

func toString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func normalizeConversationType(t string) string {
	if t == "1" {
		return types.ConversationTypeDM
	}
	return types.ConversationTypeGroup
}

// NormalizeIncoming 将钉钉机器人回调原始 JSON 归一化为 IncomingMessage。
func NormalizeIncoming(raw json.RawMessage) (*types.IncomingMessage, error) {
	var d botCallbackData
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	ctype := normalizeConversationType(d.ConversationType)

	// 按消息类型提取文本和资源
	parsedText, resources, mentions := parseContent(d.MsgType, d.Content, d.AtUsers)

	// 对 text 类型，仍从 text.content 取值以保持兼容
	if d.MsgType == "" || d.MsgType == "text" {
		parsedText = strings.TrimSpace(d.Text.Content)
	}

	text := parsedText
	if ctype == types.ConversationTypeGroup {
		// 群聊中 @机器人 会把前缀带进 text.content，剥掉 @xxx 部分（E5）。
		if i := strings.Index(text, " "); i >= 0 && strings.HasPrefix(text, "@") {
			text = strings.TrimSpace(text[i+1:])
		}
	}

	// 合并 AtUsers 到 mentions
	mentionAll := false
	for _, au := range d.AtUsers {
		if au.StaffID == "all" || au.DingtalkID == "all" {
			mentionAll = true
		}
		mentions = append(mentions, types.Mention{
			UserID: au.StaffID,
			Name:   au.StaffID,
		})
	}

	return &types.IncomingMessage{
		ConversationID:    d.ConversationID,
		ConversationType:  ctype,
		ConversationTitle: d.ConversationTitle,
		SenderID:          d.SenderID,
		SenderStaffID:     d.SenderStaffID,
		SenderNick:        d.SenderNick,
		SenderCorpID:      d.SenderCorpID,
		Text:              text,
		MsgType:           d.MsgType,
		Content:           d.Content,
		Resources:         resources,
		Mentions:          mentions,
		MentionAll:        mentionAll,
		AtUsers:           d.AtUsers,
		SessionWebhook:    d.SessionWebhook,
		WebhookExpiredAt:  d.SessionWebhookExpiredTime,
		MsgID:             d.MsgID,
		CreateAt:          d.CreateAt,
		IsAdmin:           d.IsAdmin,
		IsInAtList:        d.IsInAtList,
		Raw:               raw,
	}, nil
}
