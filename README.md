# dingtalk-channel-sdk-go

**English** | [简体中文](./README.zh-CN.md)

DingTalk Channel SDK (Go) — a conversation access layer decoupled from any agent runtime: Stream long connection, inbound event normalization, a unified safety pipeline, and streaming AI-card replies, all behind one high-level Channel.

Requires Go 1.21+.

## Install

```bash
go get github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go
```

## Minimal Example

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

`Start(ctx)` establishes the Stream long connection and blocks (auto-reconnect); replies go through the sessionWebhook, no public ingress required.

## Highlights

- **Streaming AI-card replies**: `reply.Stream()` delivers a "typing" card immediately, appends tokens as they are generated (800ms throttle), and freezes on finish; an orphan watchdog force-closes stale streams, card failures fall back to plain text, and over-long content continues via chunked messages
- **Unified SafetyPipeline**: three-tier push interface (message / card callback / light event); three-key dedup (delivery ID + msgId + content fingerprint), policy gate (admins / global lists / per-group overrides / @all), per-chat serialization and batching, consecutive-media window merging
- **Outbound reliability**: webhook replies and proactive sends retry with exponential backoff (rate-limit/timeout retryable, format errors fail fast); expired or revoked webhooks automatically fall back to proactive send
- **Dual transport**: Stream (default, no public ingress) and HTTP mode (official signature verification built in; mount on any gateway/serverless)
- **livecheck**: one-command verification against the real environment — connect → receive → full streaming lifecycle → media upload, step-by-step PASS/FAIL

Streaming in three lines:

```go
s, _ := reply.Stream(ctx)
for _, tok := range myLLM(msg.Text) { _ = s.Append(tok) }
return s.Finish("")
```

## Documentation

| Topic | Content |
|-------|---------|
| [SPEC.md](./SPEC.md) | Shared four-language contract and the E1–E10 acceptance checklist |
| [GUIDE.md](./GUIDE.md) | Integration guide: bring your agent into DMs and group chats |
| [OVERVIEW.md](./OVERVIEW.md) | Architecture layers and module overview |
| Advanced config | Policy gate / lifecycle hooks / batching / outbound hooks / HTTP mode (see below) |

## Examples

| Example | Description |
|---------|-------------|
| `example/echo` | Minimal echo bot |
| `example/fullflow` | Full feature: proactive send + media upload & embedding |
| `example/livecheck` | One-command live verification |
| `example/agent` | Streaming LLM agent (DeepSeek etc.) |

## Package Boundaries

Application code typically imports only the root package:

```go
import channel "github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go"
```

`types` exposes shared public types; packages under `internal/` are implementation details and carry no compatibility promise.

## Advanced Config

| Option | Default | Description |
|--------|---------|-------------|
| `Policy` | allow all | Admission policy: @-mention requirement, group/sender allow-block lists, admins, per-group overrides |
| `Safety.MarkAfterHandler` | `false` | `true` = mark dedup only after successful handling; failed messages can be redelivered and retried |
| `ChatQueue` | enabled | Strict per-conversation serialization |
| `MediaBatch` | disabled | Merge consecutive pictures/files/audio/video within a window |
| `Outbound` | — | Unified footer, BeforeSend/AfterSend hooks, retry options |
| `SSRFAllowlist` | — | Exempt internal CDN download URLs |
| `Transport` | `stream` | `http` = HTTP mode (`HandleHTTPCallback` / `HTTPCallbackHandler`) |
| `OnReject` | — | Reject-event callback (with reason) for full observability of dropped messages |

Proactive send: `ch.SendText(ctx, channel.SendTarget{UserID: "staff-1"}, "...")` (groups use `ConversationID`, @ mentions supported).

## Development

```bash
go test ./...        # 126 tests
go test -race ./...
```

Live check: `DD_CLIENT_ID=... DD_CLIENT_SECRET=... go run ./example/livecheck`

## License

MIT
