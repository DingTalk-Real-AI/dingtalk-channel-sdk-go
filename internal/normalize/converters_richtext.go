package normalize

import (
	"strings"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// convertRichText 从 richText 数组提取拼接文本与 @提及列表。
func convertRichText(m map[string]interface{}, atUsers []types.AtUser) (string, []types.Mention) {
	return parseRichText(m, atUsers)
}

// parseRichText 遍历富文本段：text 段拼接正文，at 段收集 userId/手机号提及。
func parseRichText(m map[string]interface{}, atUsers []types.AtUser) (string, []types.Mention) {
	var sb strings.Builder
	var mentions []types.Mention
	arr, ok := m["richText"].([]interface{})
	if !ok {
		return "", nil
	}
	for _, item := range arr {
		elem, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		typ, _ := elem["type"].(string)
		switch typ {
		case "text":
			t, _ := elem["text"].(string)
			sb.WriteString(t)
		case "at":
			uids, _ := elem["atUserIds"].([]interface{})
			mobiles, _ := elem["atMobiles"].([]interface{})
			for _, uid := range uids {
				mentions = append(mentions, types.Mention{UserID: toString(uid)})
			}
			for _, mob := range mobiles {
				mentions = append(mentions, types.Mention{UserID: toString(mob), Name: toString(mob)})
			}
		}
	}
	return sb.String(), mentions
}
