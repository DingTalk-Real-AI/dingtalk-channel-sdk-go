# DingTalk Channel SDK Family · Project Overview

> Origin issue: [DingTalk-Real-AI/dingtalk-workspace-cli#796](https://github.com/DingTalk-Real-AI/dingtalk-workspace-cli/issues/796)—"Will DingTalk provide an integrated SDK like Channel?"
> This project provides the answer: **Four languages, aligned behavior, ready to use**.

## 1. Deliverables

| Repository | Language | Dependencies | Tests |
|---|---|---|---|
| [DingTalk-Real-AI/dingtalk-channel-sdk-go](https://github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go) | Go 1.22+ | gorilla/websocket | 19 tests (race-clean) |
| [DingTalk-Real-AI/dingtalk-channel-sdk-nodejs](https://github.com/DingTalk-Real-AI/dingtalk-channel-sdk-nodejs) | Node 18+ | ws | 12 tests |
| [DingTalk-Real-AI/dingtalk-channel-sdk-python](https://github.com/DingTalk-Real-AI/dingtalk-channel-sdk-python) | Python 3.10+ | websockets | 12 tests |
| [DingTalk-Real-AI/dingtalk-channel-sdk-java](https://github.com/DingTalk-Real-AI/dingtalk-channel-sdk-java) | JDK 8+ | Java-WebSocket + Gson | 12 tests |

Each repository contains: complete source code, unit tests, `SPEC.md` (unified contract across four languages), streaming echo example, **livecheck live integration program**, README, and MIT LICENSE.

## 2. Positioning and Boundaries

**Session access layer decoupled from Agent runtime**. The SDK handles all the "channel" dirty work, developers only write "what user said, what bot replies":

- **Responsible for**: Stream long connection (establish/heartbeat/exponential backoff reconnection/server disconnect self-healing), event dual-layer deduplication + expired message filtering, sessionWebhook replies (text/Markdown/image, auto-chunking for long content), AI card streaming output (typewriter effect, frame interval race prevention, watchdog orphan protection), card API global rate limiting and QpsLimit backoff, media upload/download and media messages (file/video/audio), Markdown normalization, proactive messaging (DM/group + @), 🤔Thinking/🥳Done status badges, explicit Abort, error fallback cooldown
- **Not responsible for**: Agent runtime (model/prompt/tool orchestration), session context persistence, credential storage, business operations (documents/tables/calendar—domain of dws CLI and skills)

## 3. Quick Start (Four Languages Isomorphic)

```go
ch := channel.New(channel.Config{ClientID: "ding...", ClientSecret: "..."})
ch.OnMessage(func(ctx context.Context, msg *channel.IncomingMessage, reply channel.Reply) error {
    s, _ := reply.Stream(ctx)              // "Inputing" card appears in seconds
    for _, tok := range myLLM(msg.Text) {
        _ = s.Append(tok)                  // Typewriter append (800ms throttling + trailing flush)
    }
    return s.Finish("")                    // Final frame freeze
})
ch.Start(ctx)
```

```js
ch.on('message', async (msg, reply) => { const s = await reply.stream(); ... await s.finish(); });
```
```python
@ch.on_message
async def handle(msg, reply): s = await reply.stream(); await s.append(tok); await s.finish()
```
```java
ch.onMessage((msg, reply) -> { CardStreamer s = reply.stream(); s.append(tok); s.finish(""); });
```

Non-streaming: `reply.Text/Markdown/Image`; attachment download `reply.DownloadURL`; media upload `reply.UploadMedia`;
Proactive messaging (independent of inbound): `ch.SendText/SendMarkdown/SendImage`; group messages support `AtUserIds/AtAll`;
Card interaction: `ch.OnCardAction` (automatic subscription to card topic upon registration).

## 4. Effect Alignment (Acceptance Checklist E1–E10, see SPEC §0)

| | User-visible Effect | Implementation |
|---|---|---|
| E1 | "Inputing" card appears within seconds after sending message | `stream()` immediately creates card + delivers INPUTING |
| E2 | Typewriter-style smooth appending | streaming interface + 800ms throttling + **trailing flush** (no loss in window) + 300ms batching for long intervals |
| E3 | Loading disappears after completion, Markdown freezes | isFinalize final frame + FINISHED status |
| E4 | Card failure/rate limiting invisible to user | Silent fallback to webhook text; QpsLimit backoff 2s retry |
| E5 | Same experience in group/DM | Same Reply API; delivery target auto-selected; group strips @ prefix |
| E6 | Never duplicate replies | messageId+msgId dual-layer deduplication (TTL 5min) |
| E7 | Card interaction closed loop | OnCardAction + auto-subscription |
| E8 | Never disconnected | Exponential backoff reconnection + disconnect immediate reconnection + 120s/5s heartbeat + ACK first |
| E9 | Media send/receive | uploadMedia (OAPI multipart) / downloadURL / image reply |
| E10 | Markdown rendering quality | normalizeForCard (code blocks/tables/quotes DingTalk rendering rules) |

Each item has corresponding unit tests in all four languages; E8 has dedicated disconnect-reconnect e2e regression.

## 5. Protocol Fidelity (True Source, Not Documentation Speculation)

| Capability | True Source |
|---|---|
| Stream wire protocol (open/wss/frame/ACK/heartbeat/topic constants) | Official dingtalk-stream-sdk-go source code line-by-line comparison |
| AI card five-step protocol + rate limiting + Markdown normalization | Official connector (dingtalk-openclaw-connector) card.ts |
| Token (new version/OAPI dual-track) and sessionWebhook payload | Official connector token.ts / messaging.ts + official documentation validation |
| Proactive messaging API | dws (dingtalk-workspace-cli) source code |
| ticket encoding / localIp / UA headers | Cross-comparison across four official stream SDKs |

Protocol-level issues fixed in review rounds: msgParam stringified JSON (official documentation requirement), Go version disconnect mishandling, ACK-first semantics, ticket URL encoding.

## 6. Key Architectural Decision: Why Self-developed Transport Layer Instead of Referencing Official stream-sdk

1. **Official connector doesn't trust itself**: DingTalk official connector source code has `autoReconnect:false, keepAlive:false` all turned off and rewritten (issues #571/#536/#573)
2. **Four-language consistency is this project's acceptance standard**, while official four SDKs have inconsistent heartbeat/reconnection/encoding behaviors; referencing inherits divergence
3. **Dependency weight**: Official Java version pulls Netty multi-module, Python version carries requests+aiohttp dual HTTP stack; self-developed has only 1–2 small dependencies per language, transport layer ~300 lines/language
4. Evolution seam reserved (`Channel → StreamConn → onFrame` single boundary), can add official adapter backend when upstream SDK matures

## 7. Quality Evidence

- **Tests**: Go 14 / Node 12 / Python 12 / Java 12 (BUILD SUCCESS), all with e2e (fake gateway + fake API) and disconnect-reconnect regression
- **Live integration**: Each language `example/livecheck` one-click verification (connection→receive message→text reply→card streaming full cycle→media upload, step-by-step PASS/FAIL):
  `DD_CLIENT_ID=... DD_CLIENT_SECRET=... go run ./example/livecheck` (Node `npm run live`; Python `python example/livecheck.py`; Java `mvn exec:java`)
- **Code review**: Three rounds (protocol consistency / effect alignment / stream layer vs official SDK), fixed 8 issues, all with regression tests

## 8. Known Boundaries and Roadmap

- Live credential integration pending execution (livecheck ready, one command)
- >20MB file chunked upload (v0.2)
- Card template defaults to connector built-in template, recommend configurable for public release
- Platform difference items (reaction/comment/forward) follow DingTalk Open Platform evolution
- Optional: Official stream-sdk adapter backend (`WithTransport` seam reserved)

## 9. Prerequisites

Create **enterprise internal application** in DingTalk Developer Console and enable bot, obtain ClientID/ClientSecret. Stream mode requires no public IP or domain.

---
License: MIT | Contract: `SPEC.md` in each repository | Version: v0.1.0 (Aug 2026)
