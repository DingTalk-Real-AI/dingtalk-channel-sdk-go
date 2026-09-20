# dingtalk-channel-sdk-go

[English](./README.md) | **简体中文**

钉钉 Channel SDK（Go 版）——与 Agent runtime 解耦的会话接入层：Stream 长连接、入站事件归一化、统一安全管线、AI 卡片流式回复，一个高阶 Channel 全部覆盖。

要求 Go 1.21+。

## 安装

```bash
go get github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go
```

## 最小示例

```go
package main

import (
	"context"
	"log"
	"os"

	channel "github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go"
)

func main() {
	ch := channel.New(channel.Config{
		ClientID:     os.Getenv("DD_CLIENT_ID"),
		ClientSecret: os.Getenv("DD_CLIENT_SECRET"),
	})

	ch.OnMessage(func(ctx context.Context, msg *channel.IncomingMessage, reply channel.Reply) error {
		return reply.Text(ctx, "received: "+msg.Text)
	})

	if err := ch.Start(context.Background()); err != nil {
		log.Fatalf("start channel: %v", err)
	}
}
```

`Start(ctx)` 建立 Stream 长连接并阻塞运行（自动重连）；回复走 sessionWebhook，不依赖公网入口。

## 核心特性

- **AI 卡片流式回复**：`reply.Stream()` 立即出"输入中"卡片，token 逐字追加（800ms 节流），finish 定格；孤儿 watchdog 强制收口、卡片失败自动降级文本、超长内容分片续发
- **统一安全管线 SafetyPipeline**：三层推送接口（消息/卡片回调/轻量事件）；三键去重（投递 ID + msgId + 内容指纹）、策略门控（管理员/全局名单/逐群覆盖/@all）、per-chat 串行与批处理、连续媒体窗口合并
- **出站可靠性**：webhook 回复与主动发送指数退避重试（限流/超时可重试、格式错误即停）；webhook 过期或目标撤回自动转主动发送兜底
- **双传输模式**：Stream（默认，无需公网）与 HTTP 模式（官方验签内置，网关/serverless 直挂）
- **livecheck 一键真机验收**：连接 → 收消息 → 流式卡片全周期 → 媒体上传，逐步 PASS/FAIL

流式回复只要三行：

```go
s, _ := reply.Stream(ctx)
for _, tok := range myLLM(msg.Text) { _ = s.Append(tok) }
return s.Finish("")
```

## 文档

| 主题 | 内容 |
|------|------|
| [SPEC.md](./SPEC.md) | 四语言统一契约与 E1–E10 效果验收清单 |
| [GUIDE.md](./GUIDE.md) | 接入指南：让 Agent 接入群聊/单聊 |
| [OVERVIEW.md](./OVERVIEW.md) | 架构分层与模块总览 |
| 高级配置 | 策略门控 / 生命周期钩子 / 批处理 / 出站钩子 / HTTP 模式（见下方「高级配置」） |

## 示例

| 示例 | 说明 |
|------|------|
| `example/echo` | 最小回声机器人 |
| `example/fullflow` | 全功能：主动发送 + 媒体上传内嵌 |
| `example/livecheck` | 真机一键验收 |
| `example/agent` | LLM 流式 agent（DeepSeek 等） |

## 包边界

业务代码通常只需导入根包：

```go
import channel "github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go"
```

`types` 为公开共享类型；`internal/` 属实现细节，不在兼容性承诺范围内。

## 高级配置

| 配置 | 默认 | 说明 |
|------|------|------|
| `Policy` | 全开放 | 准入策略：@要求、群/发送者黑白名单、管理员、逐群覆盖 |
| `Safety.MarkAfterHandler` | `false` | `true` = 处理成功才标记去重，失败消息可重投重试 |
| `ChatQueue` | 启用 | 同会话消息强制串行 |
| `MediaBatch` | 关闭 | 连续图片/文件/音视频窗口内合并投递 |
| `Outbound` | — | 统一页脚、BeforeSend/AfterSend 钩子、重试参数 |
| `SSRFAllowlist` | — | 内网 CDN 等下载 URL 豁免 |
| `Transport` | `stream` | `http` = HTTP 模式（`HandleHTTPCallback` / `HTTPCallbackHandler`） |
| `OnReject` | — | 拒绝事件回调（含原因），可观测所有被丢弃消息 |

主动发送：`ch.SendText(ctx, channel.SendTarget{UserID: "staff-1"}, "...")`（群聊用 `ConversationID`，支持 @）。

## 本地开发

```bash
go test ./...        # 126 个测试
go test -race ./...
```

真实联调：`DD_CLIENT_ID=... DD_CLIENT_SECRET=... go run ./example/livecheck`

## License

MIT
