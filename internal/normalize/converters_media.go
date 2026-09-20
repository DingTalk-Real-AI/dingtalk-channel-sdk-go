package normalize

import "github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"

// convertPicture 提取图片资源；downloadCode 为钉钉侧下载凭证。
func convertPicture(m map[string]interface{}) (string, []types.Resource) {
	dc, _ := m["downloadCode"].(string)
	if dc != "" {
		return "[图片]", []types.Resource{{Type: "image", DownloadCode: dc}}
	}
	return "[图片]", nil
}

// convertFile 提取文件资源，文件名缺失时回退占位文案。
func convertFile(m map[string]interface{}) (string, []types.Resource) {
	dc, _ := m["downloadCode"].(string)
	fn, _ := m["fileName"].(string)
	var resources []types.Resource
	if dc != "" {
		resources = append(resources, types.Resource{Type: "file", DownloadCode: dc, FileName: fn})
	}
	if fn == "" {
		fn = "文件"
	}
	return "[文件: " + fn + "]", resources
}

// convertAudio 提取语音资源；有识别文本时优先透出识别结果。
func convertAudio(m map[string]interface{}) (string, []types.Resource) {
	dc, _ := m["downloadCode"].(string)
	fn, _ := m["fileName"].(string)
	rec, _ := m["recognition"].(string)
	var resources []types.Resource
	if dc != "" {
		resources = append(resources, types.Resource{Type: "audio", DownloadCode: dc, FileName: fn, Recognition: rec})
	}
	if rec != "" {
		return rec, resources
	}
	return "[语音消息]", resources
}

// convertVideo 提取视频资源。
func convertVideo(m map[string]interface{}) (string, []types.Resource) {
	dc, _ := m["downloadCode"].(string)
	fn, _ := m["fileName"].(string)
	var resources []types.Resource
	if dc != "" {
		resources = append(resources, types.Resource{Type: "video", DownloadCode: dc, FileName: fn})
	}
	return "[视频]", resources
}
