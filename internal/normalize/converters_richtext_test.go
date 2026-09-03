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

// 对齐 lark channel-sdk 富文本附件区：picture/file 段提取为资源，重复下载码去重。
func TestConvertRichTextResources(t *testing.T) {
	payload := `{"richText":[
		{"type":"text","text":"图1 "},
		{"type":"picture","picture":"dc-1"},
		{"type":"picture","picture":"dc-1"},
		{"type":"picture","picture":"dc-2"},
		{"type":"file","downloadCode":"dc-3","fileName":"report.pdf"},
		{"type":"text","text":" 图2"}
	]}`
	text, resources, _ := parseContent("richText", json.RawMessage(payload), nil)
	if text != "图1  图2" {
		t.Fatalf("richText text = %q", text)
	}
	if len(resources) != 3 {
		t.Fatalf("resources = %d, want 3（重复 dc-1 去重）: %+v", len(resources), resources)
	}
	if resources[0].Type != "image" || resources[0].DownloadCode != "dc-1" {
		t.Fatalf("resource[0] wrong: %+v", resources[0])
	}
	if resources[2].Type != "file" || resources[2].DownloadCode != "dc-3" || resources[2].FileName != "report.pdf" {
		t.Fatalf("file resource wrong: %+v", resources[2])
	}
}

// 脏数据防御：非法段值不影响其余段落，也不会产生空资源。
func TestConvertRichTextResourcesDirtyData(t *testing.T) {
	payload := `{"richText":[
		{"type":"picture","picture":123},
		{"type":"picture","picture":""},
		{"type":"picture"},
		{"type":"file","downloadCode":42},
		{"type":"text","text":"ok"}
	]}`
	text, resources, _ := parseContent("richText", json.RawMessage(payload), nil)
	if text != "ok" {
		t.Fatalf("text = %q, want ok", text)
	}
	if resources != nil {
		t.Fatalf("dirty segments must yield no resources, got %+v", resources)
	}
}
