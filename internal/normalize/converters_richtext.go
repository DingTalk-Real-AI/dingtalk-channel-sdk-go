package normalize

import (
	"strings"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// convertRichText 从 richText 数组提取拼接文本、@提及列表与内嵌媒体资源。
func convertRichText(m map[string]interface{}, atUsers []types.AtUser) (string, []types.Mention, []types.Resource) {
	return parseRichText(m, atUsers)
}

// parseRichText 遍历富文本段：text 段拼接正文，at 段收集 userId/手机号提及，
// picture/file 段提取下载资源（对齐 lark channel-sdk 的富文本附件区能力）。
// 脏数据防御：段值类型不符或下载码为空时跳过该段，不影响其余段落；
// 同一下载码在单条消息内去重。
func parseRichText(m map[string]interface{}, atUsers []types.AtUser) (string, []types.Mention, []types.Resource) {
	var sb strings.Builder
	var mentions []types.Mention
	var resources []types.Resource
	seen := map[string]bool{}
	arr, ok := m["richText"].([]interface{})
	if !ok {
		return "", nil, nil
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
		case "picture":
			// 段值即钉钉下载码；仅接受非空字符串，避免脏数据进入资源列表。
			code, _ := elem["picture"].(string)
			if code == "" || seen[code] {
				continue
			}
			seen[code] = true
			resources = append(resources, types.Resource{Type: "image", DownloadCode: code})
		case "file":
			code, _ := elem["downloadCode"].(string)
			fn, _ := elem["fileName"].(string)
			if code == "" || seen[code] {
				continue
			}
			seen[code] = true
			resources = append(resources, types.Resource{Type: "file", DownloadCode: code, FileName: fn})
		}
	}
	return sb.String(), mentions, resources
}
