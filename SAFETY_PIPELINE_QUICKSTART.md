# 安全管线快速入门

SDK 内置统一安全管线(SafetyPipeline):**过期过滤、消息去重、自回复过滤、策略门控、处理锁、文本/媒体批处理**。管线属于 SDK 内部实现(`internal/safety`,Go 的 internal 包对模块外不可见),外部无法也无需直接导入——所有能力通过 `channel.New` 的 **`Config.Safety`** 配置即可使用。

## 5分钟上手

下面是一个可直接编译运行的完整程序:

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	channel "github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go"
	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

func main() {
	cfg := channel.Config{
		ClientID:     os.Getenv("DD_CLIENT_ID"),
		ClientSecret: os.Getenv("DD_CLIENT_SECRET"),
		Safety:       types.DefaultSafetyConfig(), // 过期/去重/策略/锁默认全开
	}

	bot := channel.New(cfg)

	// 1. 消息处理器:完整管线(过期 → 去重 → 自回复 → 策略 → 锁 → 批处理)全部通过后才回调
	bot.OnMessage(func(ctx context.Context, msg *channel.IncomingMessage, reply channel.Reply) error {
		log.Printf("Processing: %s", msg.Text)
		// 业务处理逻辑
		return reply.Text(ctx, "hello")
	})

	// 2. 监听拒绝事件(过期/重复/策略拒绝/锁竞争等)
	bot.OnReject(func(ctx context.Context, event *channel.RejectEvent) {
		log.Printf("Rejected: %s, reason: %s", event.MessageID, event.Reason)
	})

	// 3. 启动 Stream 长连接(阻塞,放 goroutine 中)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := bot.Start(ctx); err != nil {
			log.Printf("channel exited: %v", err)
			cancel()
		}
	}()

	// 4. 优雅退出
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	bot.Close() // 停止连接并清理管线
}
```

---

## 高级配置

### 调整去重与媒体批处理

在默认值基础上覆盖字段(推荐先取 `DefaultSafetyConfig()` 再改,避免遗漏默认值):

```go
cfg := channel.Config{
	ClientID:     os.Getenv("DD_CLIENT_ID"),
	ClientSecret: os.Getenv("DD_CLIENT_SECRET"),
}

cfg.Safety = types.DefaultSafetyConfig()
cfg.Safety.Dedup = types.DedupConfig{
	TTL:           12 * time.Hour, // 去重记录保留时长
	MaxEntries:    5000,           // LRU 容量上限
	SweepInterval: 5 * time.Minute,
}
// 媒体批处理默认关闭，显式配置以启用
cfg.MediaBatch = &channel.MediaBatchConfig{
	Enabled:  true,
	DelayMs:  800, // 800ms 合并窗口
	MaxItems: 9,   // 单批最多合并 9 个媒体
}
cfg.Safety.StaleWindow = 30 * time.Minute // 过期消息窗口
cfg.Safety.DropSelfSent = true            // 丢弃机器人自发的消息

bot := channel.New(cfg)

// 注册批处理消息处理器
bot.OnBatchMessage(func(ctx context.Context, batch *channel.BatchMessage, reply channel.Reply) error {
	log.Printf("Received batch with %d media items", len(batch.Messages))
	return reply.Text(ctx, fmt.Sprintf("Received %d media items", len(batch.Messages)))
})
```

**效果**: 用户连续上传 5 张图片 → 合并为一个批次 → 减少 API 调用。

### 配置访问策略

```go
trueVal, falseVal := true, false

cfg.Safety.Policy = types.PolicyConfig{
	// 管理员绕过所有限制(staffId)
	Admins: []string{"admin_staff_id"},

	// 全局发送者控制
	AllowFrom: []string{"user1", "user2"}, // 仅允许这些用户
	DenyFrom:  []string{"blocked_user"},   // 黑名单(优先于 AllowFrom)

	// 单聊策略
	DMMode: "open", // "open" | "disabled" | "allowlist" | "blocklist"

	// 群组策略
	GroupAllowlist: []string{"group1", "group2"},
	RequireMention:      &trueVal,  // 群聊需要 @机器人
	RespondToMentionAll: &falseVal, // 不响应 @all

	// 单群覆盖配置(零值字段沿用全局)
	GroupOverrides: map[string]types.GroupOverride{
		"special_group": {
			RequireMention:      &falseVal, // 该群不要求 @
			RespondToMentionAll: &trueVal,  // 该群响应 @all
			AllowFrom:           []string{"user3"}, // 该群仅允许 user3
		},
	},
}
```

### Redis 持久化(规划中)

去重缓存当前为进程内 LRU(见 `types.DedupConfig`)。基于 Redis 的持久化已在规划中,公开配置暂未暴露,将在后续版本提供。

---

## 三层处理路径

管线内部按事件类型走三条由严到简的路径,对外的注册入口如下:

| 事件类型 | 内部处理路径 | 注册入口 |
|---|---|---|
| 普通消息(文本/媒体/富文本…) | 完整管线:过期 → 去重 → 自回复 → 策略 → 锁 → 批处理 | `OnMessage` / `OnBatchMessage` |
| 卡片回调(按钮点击/表单提交) | 简化管线:去重 → 锁 → 串行 | `OnCardAction` |
| 轻量事件(如 reaction) | 仅去重 | 管线内部自动处理 |

```go
// 卡片回调:管线按 eventID 自动去重并串行执行,无需业务自行加锁
bot.OnCardAction(func(ctx context.Context, action *channel.CardAction, reply channel.Reply) error {
	log.Printf("card action from user %s (track %s)", action.UserID, action.OutTrackID)
	return reply.Text(ctx, "已收到点击")
})
```

---

## 可观测性

### 监听拒绝事件

```go
bot.OnReject(func(ctx context.Context, event *channel.RejectEvent) {
	switch event.Reason {
	case types.RejectReasonStale:
		log.Printf("过期消息: %s", event.MessageID)
	case types.RejectReasonDuplicate:
		log.Printf("重复消息: %s", event.MessageID)
	case types.RejectReasonSelfSent:
		log.Printf("自回复: %s", event.MessageID)
	case types.RejectReasonLockContention:
		log.Printf("锁竞争: %s", event.MessageID)
	case types.RejectReasonDMDisabled:
		log.Printf("DM禁用: 来自 %s", event.SenderID)
	case types.RejectReasonGroupNotAllowed:
		log.Printf("群组未授权: %s", event.ChatID)
	case types.RejectReasonSenderDenied:
		log.Printf("发送者被拒: %s", event.SenderID)
	}

	// 可选:上报到监控系统
	// metrics.RecordReject(string(event.Reason))
})
```

---

## 动态更新配置

```go
// 运行中热更新策略
bot.UpdatePolicy(types.PolicyConfig{
	GroupAllowlist: []string{"group1", "group2", "group3"},
})

// 机器人身份登录后自动探测,可随时查询
if id := bot.GetBotIdentity(ctx); id != nil {
	log.Printf("robot: %s (%s)", id.RobotName, id.RobotCode)
}
```

---

## 性能建议

### 1. 合理设置去重 TTL

```go
// 短期应用（聊天机器人）
cfg.Safety.Dedup.TTL = 1 * time.Hour

// 长期应用（客服系统）
cfg.Safety.Dedup.TTL = 24 * time.Hour
```

### 2. 调整媒体批处理延迟

```go
// 快速响应（低延迟）
cfg.Safety.MediaBatch.DelayMs = 500

// 更多合并（高吞吐）
cfg.Safety.MediaBatch.DelayMs = 1500
```

### 3. 进阶:处理失败允许重投

```go
// 默认 false = 入口即标记去重(吞吐优先);
// true = handler 成功返回后才标记,处理失败的消息可被重投重试,
// 代价是同消息并发重投会撞处理锁(RejectReasonLockContention)
cfg.Safety.MarkAfterHandler = true
```

---

## 测试

业务 handler 本身就是普通函数,可直接单测(不依赖 SDK 内部):

```go
func TestMyHandler(t *testing.T) {
	h := func(ctx context.Context, msg *channel.IncomingMessage, reply channel.Reply) error {
		log.Printf("got %q from %s", msg.Text, msg.SenderStaffID)
		return nil
	}

	msg := &channel.IncomingMessage{
		ConversationID: "test_chat",
		MsgID:          "test_msg",
		MsgType:        "text",
		Text:           "hello",
		CreateAt:       time.Now().UnixMilli(),
	}

	if err := h(context.Background(), msg, nil); err != nil { // reply 可传测试替身
		t.Fatal(err)
	}
}
```

管线自身的完整测试见仓库内 [channel_test.go](./channel_test.go) 与 `internal/safety/*_test.go`。

---

## 常见问题

### Q: SafetyPipeline 与 Channel 的关系?

A: SafetyPipeline 是 SDK 内部的安全管线(`internal/safety`),由 `Channel` 在构造时装配并驱动,外部代码不可直接导入。所有安全能力通过 `channel.New` + `Config.Safety` 使用:新项目直接用 Channel 即可,无需额外集成。

### Q: 如何调整去重行为?

A: 通过 `cfg.Safety.Dedup`(`TTL` / `MaxEntries` / `SweepInterval`)。默认 TTL 12 小时、容量 5000、5 分钟清扫一次过期项。

### Q: 媒体批处理会影响消息顺序吗?

A: 不会。当文本消息介入时,会自动刷新待处理的媒体批次。

### Q: 性能开销如何?

A: 极小。LRU 缓存查询 O(1),锁操作 O(1),策略检查 O(n),其中 n 是白名单/黑名单大小。

---

## 更多资源

- [实现摘要](./SAFETY_PIPELINE_SUMMARY.md)
- [完整示例](./example/agent/main.go)
- [测试用例](./channel_test.go)

---

**祝你使用愉快！** 🎉
