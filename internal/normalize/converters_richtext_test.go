package normalize

import (
	"encoding/json"
	"testing"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

func TestConvertRichText(t *testing.T) {
	payload := `{"richText":[
		{"type":"text","text":"你好 "},
		{"type":"at","atUserIds":["u1","u2"],"atMobiles":["13800000000"]},
		{"type":"text","text":"看这张图"}
	]}`
	text, _, mentions := parseContent("richText", json.RawMessage(payload), nil)
	if text != "你好 看这张图" {
		t.Fatalf("richText text = %q", text)
	}
	if len(mentions) != 3 {
		t.Fatalf("mentions = %d, want 3（2 userId + 1 mobile）", len(mentions))
	}
	if mentions[0].UserID != "u1" || mentions[1].UserID != "u2" {
		t.Fatalf("userId mentions wrong: %+v", mentions[:2])
	}
	if mentions[2].Name != "13800000000" {
		t.Fatalf("mobile mention should carry name, got %+v", mentions[2])
	}
}

func TestConvertRichTextEmpty(t *testing.T) {
	text, _, mentions := parseContent("richText", json.RawMessage(`{}`), nil)
	if text != "" || mentions != nil {
		t.Fatal("no richText array should yield empty")
	}
	// 非对象元素跳过
	text, _, _ = parseContent("richText", json.RawMessage(`{"richText":["str",{"type":"text","text":"ok"}]}`), nil)
	if text != "ok" {
		t.Fatalf("non-map elements skipped, text = %q", text)
	}
}

func TestConvertRichTextAtUsersMerge(t *testing.T) {
	// atUsers 由 NormalizeIncoming 层合并（此处验证 parse 透传）
	_, mentions, _ := parseContent("richText", json.RawMessage(`{"richText":[]}`), []types.AtUser{{StaffID: "s1"}})
	if len(mentions) != 0 {
		t.Fatal("parseContent itself should not merge atUsers")
	}
}
