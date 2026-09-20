# DingTalk Channel SDK — Four-Language Unified Contract (SPEC v0.1)

> Positioning: **Session access layer decoupled from Agent runtime**. The SDK handles the "channel" dirty work,
> developers only write "what the user said, what the bot replies".

## 0. Effect Alignment Acceptance Checklist (Effect Parity — All Four Languages Verified by This Standard)

Verified by **end-user perceivable behavior**, demonstrable item by item:

| # | User-visible Effect | This SDK Implementation |
|---|---|---|
| E1 | "Inputing" card appears **within seconds** after sending message | `reply.stream()` immediately creates card + delivers INPUTING (doesn't wait for first token) |
| E2 | Reply content **typewriter-style** smooth appending | streaming interface + 800ms throttling + trailing newline removal for non-final frames (prevent flicker) |
| E3 | Loading disappears after completion, content **freezes as complete Markdown** | isFinalize final frame + flowStatus=3 + cardUpdateOptions |
| E4 | Card failure/QPS rate limiting **invisible to user** | Creation failure→silent fallback to webhook text; QpsLimit→backoff 2s retry; no error popup |
| E5 | **Same experience in group/DM** | Same Reply API; delivery target auto-selected IM_GROUP/IM_ROBOT; group auto-strips @ prefix |
| E6 | **Never duplicate reply** to same message | Dual-layer deduplication (messageId+msgId, TTL 5min), discarded still returns ACK |
| E7 | **Card interaction closed loop**: button clicks reach Agent, can update card | OnCardAction (registration auto-subscribes /v1.0/card/instances/callback, all four languages have unit test coverage for dispatch and subscription) + reply update |
| E8 | Bot **never disconnects** (network outage/server switch invisible) | Exponential backoff reconnection + SYSTEM/disconnect immediate reconnection + heartbeat keep-alive + ACK prevents loss |
| E9 | **Media send/receive**: receive image/file downloadable; can upload media and embed | `reply.image(url)` (sampleImageMsg); `reply.uploadMedia()` (OAPI multipart, mediaId can be `![..](mediaId)` embedded in card); `reply.downloadURL()` |
| E10 | Markdown **rendering quality** (code blocks/tables/lists/quotes) | normalizeForCard normalization (see §7) |

> Effect demo script (attached in each language README): echo + streaming simulation (fake LLM emits token every 100ms) must demonstrate E1→E3 full process.

## 1. Responsibility Boundaries

**SDK is responsible for:**
1. Stream long connection (establish, subscribe, heartbeat, disconnect reconnection, server disconnect handling)
2. Event parsing and deduplication (protocol layer messageId + business layer msgId, dual-layer, TTL 5 minutes)
3. Reply sending (sessionWebhook: text / Markdown)
4. AI card streaming output (create → deliver → INPUTING → streaming → FINISHED, typewriter effect)
5. Card API global rate limiting (token bucket + QpsLimit backoff retry)
6. Markdown normalization (adapt to DingTalk AI card renderer's newline/table rules)

**SDK is not responsible for (left to Agent side):**
- Agent runtime (model / prompt / tool orchestration)
- Multi-user topic isolation and Session/context persistence
- Credential storage (only receives clientId/clientSecret)

## 2. Wire Protocol (Stream Mode)

### 2.1 Establish Connection
```
POST {apiBase}/v1.0/gateway/connections/open
{
  "clientId": "...", "clientSecret": "...",
  "ua": "dingtalk-channel-sdk-{lang}/v0.1.0",
  "localIp": "<first non-loopback IPv4>",
  "subscriptions": [ {"type": "CALLBACK", "topic": "/v1.0/im/bot/messages/get"} ],
  "extras": {}
}
→ {"endpoint": "wss://...", "ticket": "..."}
```
HTTP headers: `Content-Type/Accept: application/json`, `User-Agent: dingtalk-channel-sdk-{lang}/v0.1.0`.
WebSocket connection: `{endpoint}?ticket=<url-encoded ticket>` (**ticket needs URL encoding**, same as official Python SDK; topic declared in open request, not in URL).

Subscription types: `CALLBACK` (callback) / `EVENT` (event) / `SYSTEM` (system, SDK internally uses `ping`, `disconnect`).

Fixed topics:
- Bot messages: `/v1.0/im/bot/messages/get` (CALLBACK)
- Card callbacks: `/v1.0/card/instances/callback` (CALLBACK, subscribed only when OnCardAction is registered)

### 2.2 Data Frames
Inbound frame (WebSocket text):
```json
{"specVersion":"1.0","type":"CALLBACK|EVENT|SYSTEM","time":0,
 "headers":{"topic":"...","messageId":"...","contentType":"application/json","time":"..."},
 "data":"<JSON string>"}
```
ACK outbound frame (**must reply**, otherwise server redelivers; **ACK first**—reply `{"success":true}` upon receipt, business processes asynchronously,
aligned with official connector: prevents server timeout redelivery during Agent long tasks; duplicate delivery handled by dual-layer deduplication):

> **Server perspective empirical evidence** (lippi-open-proxy source code, 2026-07-23 production incident postmortem cross-validation):
> ① Server push is `UnaryRequest`—**synchronously waits for ACK**, upstream timeout ~2s; if not received, **redelivered (at-least-once)** via MetaQ,
> and redelivery generates new messageId (business layer msgId deduplication is necessary, protocol layer alone insufficient)—this SDK's ACK first + dual-layer deduplication
> strictly aligns with this semantics. ② Heartbeat contract is **client ping, server auto pong** (gorilla default behavior); when server read loop blocks,
> pong stops, client should timeout reconnect—this SDK's 120s idle ping + 5s pong death judgment is the client implementation of that contract.
> ③ Server ACK validation only requires headers non-empty + contains messageId; only sends/receives Text frames; ticket is server connectionId (passed via URL query).
> ④ **Risk response**: 0723 incident proved server read loop has a mode of being blocked by late/duplicate ACKs (fix is on
> branch for 20260723 release line, whether it's in production depends on release system; local master is stale snapshot, doesn't represent production version).
> Regardless of whether server fix is in place, four client defenses are necessary production self-healing: exactly one ACK per frame, ACK first (no late ACKs),
> pong timeout death reconnection (only way out when server wedged), exponential backoff+jitter reconnection (prevent storm amplification). Latency-sensitive scenarios can
> lower KeepAliveIdleMs (default 120s) to speed up wedged detection.
```json
{"code":200,"headers":{"contentType":"application/json","messageId":"<same as frame>"},
 "message":"ok","data":"{\"success\":true}"}
```
- `SYSTEM/ping`: Reply pong, echo `data` as-is.
- `SYSTEM/disconnect`: Close connection and immediately reconnect (server LB switch).

### 2.3 Heartbeat and Reconnection
- Send WebSocket protocol layer Ping after 120s idle, declare dead if no Pong received within 5s.
- Reconnection: exponential backoff 1s→2s→4s…capped at 30s (with jitter); reset on successful reconnection.
- Read loop exception/disconnect → auto-reconnect (configurable AutoReconnect=false).

> Comparison with official stream-sdk family: official heartbeat Go=120s idle+5s pong, Java(Netty)=60s idle+pong,
> Python=60s ping (no pong detection), Node=isAlive flag+terminate; this SDK uniformly takes Go tier (120s+5s pong),
> stronger than Python/Node. Reconnection official is fixed 3s/10s, this SDK uses exponential backoff+jitter (aligned with official connector).

## 3. Event Model

### 3.1 IncomingMessage (After Normalization)
| Field | Source | Description |
|---|---|---|
| ConversationID | conversationId | Conversation ID |
| ConversationType | conversationType | "1"=DM "2"=group → normalized to `dm`/`group` |
| ConversationTitle | conversationTitle | Group name (group chat) |
| SenderID / SenderStaffID | senderId / senderStaffId | Encrypted ID / Staff ID |
| SenderNick | senderNick | Nickname |
| SenderCorpID | senderCorpId | |
| Text | text.content | **@bot prefix whitespace removed and trimmed** |
| MsgType / Content | msgtype / content | Rich content (images/files etc. passed through as-is) |
| AtUsers | atUsers[] | [{dingtalkId, staffId}] |
| SessionWebhook | sessionWebhook | Reply webhook (includes expiration SessionWebhookExpiredTime) |
| MsgID / CreateAt | msgId / createAt | Business dedup key / Event time |
| Raw | original data | |

### 3.2a Stale Message Filtering

Inbound messages with `createAt` older than `StaleMessageWindow` (default 30min, <=0 disables) from now are directly discarded (still returns ACK)—
old messages flooding in during reconnection storm/redelivery backlog no longer trigger replies.
1. Protocol layer: `headers.messageId` (duplicate callback of same delivery)
2. Business layer: `data.msgId` (messageId changes on server redelivery, msgId doesn't)
TTL 5 minutes, LRU cleanup. Hit means discard (still returns ACK success).

## 4. Reply API (Four Languages Consistent Semantics)

```
reply.text(content)                     → sessionWebhook, msgKey=sampleText
reply.markdown(title, text)             → sessionWebhook, msgKey=sampleMarkdown
reply.image(url)                        → sessionWebhook, msgKey=sampleImageMsg
reply.downloadURL(downloadCode,msgId)   → GET /v1.0/robot/messageFiles/download
reply.uploadMedia(type,name,data[,ct]) → OAPI upload, returns mediaId (see §9a)
s = reply.stream()                      → Immediately create AI card (E1: "inputing" card first)
s.append(delta) / s.append(fullText)    → Streaming update (accumulation semantics decided by caller)
s.finish() / s.finish(fullText)         → Final frame + FINISHED
s.fail(errText)                         → FINISHED(flowStatus=5) or fallback text
```

**Long content chunking**: `TextChunkLimit` (default 3500, <=0 disables)—
text/Markdown replies exceeding limit are split by **newline boundaries** into multiple sends (hard cut if no suitable newline), content not lost.

sessionWebhook payload:
```json
{"msgKey":"sampleText","msgParam":"{\"content\":\"...\"}"}
{"msgKey":"sampleMarkdown","msgParam":"{\"title\":\"...\",\"text\":\"...\"}"}
```
**Note**: `msgParam` must be **stringified JSON** (official documentation requirement, object form returns 400).
Header: `x-acs-dingtalk-access-token: <token>`.

## 4a. Proactive Send

Independent of inbound messages, Agent can initiate anytime:

```
channel.SendText(target, text)
channel.SendMarkdown(target, title, text)      // target can have @: AtUserIds/AtDingtalkIds/AtAll
channel.SendImage(target, imageURL)            // Requires publicly accessible URL
```

- DM (target.UserID): `POST /v1.0/robot/oToMessages/batchSend` `{robotCode, userIds:[...], msgKey, msgParam}`
- Group (target.ConversationID): `POST /v1.0/robot/groupMessages/send` `{robotCode, openConversationId, msgKey, msgParam, atUserIds?, atOpendingtalkIds?, isAtAll?}`

## 4b. Policy Gating and Group Overrides

Global policy (PolicyConfig): group allowlist/blocklist, `RequireMention` (default true), DM mode
(open/allowlist/blocklist/disabled) with corresponding lists.

**Group overrides (GroupOverrides)**: per conversationId group override—`Enabled` (explicitly disable),
`RequireMention` (@ requirement for this group), `AllowFrom`/`BlockFrom` (sender allowlist/blocklist within group, blocklist takes priority).
Evaluation order (four languages consistent):

1. Global blocklist (highest priority, group override cannot exempt)
2. Allowlist admission: global allowlist hit, **or explicit group entry exists** (explicit entry can allow this group in allowlist mode)
3. `Enabled=false` → reject (`group_disabled`)
4. @bot check (group override takes priority over global)
5. `BlockFrom` → `AllowFrom` (sender filtering within group)

## 5. AI Card Protocol (Five Steps)

Template ID default: `02fcf2f4-5e02-4a85-b672-46d1f715543e.schema` (official AI card, configurable).

1. **Create** `POST /v1.0/card/instances`
   `{cardTemplateId, outTrackId: "card_{ts}_{rand}", cardData:{cardParamMap:{config:"{\"autoLayout\":true}"}}, callbackType:"STREAM", imGroupOpenSpaceModel:{supportForward:true}, imRobotOpenSpaceModel:{supportForward:true}}`
2. **Deliver** `POST /v1.0/card/instances/deliver`
   - Group: `{outTrackId, userIdType:1, openSpaceId:"dtv1.card//IM_GROUP.{conversationId}", imGroupOpenDeliverModel:{robotCode}}`
   - DM: `{outTrackId, userIdType:1, openSpaceId:"dtv1.card//IM_ROBOT.{senderStaffId||senderId}", **imRobotOpenDeliverModel**:{spaceType:"IM_ROBOT", robotCode, extension:{dynamicSummary:"true"}}}`
   - robotCode = clientId
   - ⚠️ DM field must be `imRobotOpenDeliverModel` (Deliver not Space; official connector's `imRobotOpenSpaceModel` variant rejected by production: `400 param.spaceDeliverModelEmpty`—Aug 2026 real device evidence, dws source code as reference)
   - ⚠️ **Business-level validation**: deliver returns `{"result":[{"success":false,...}]}` in HTTP 200 body (dws production evidence "observed live"), SDK must scan body for `"success":false` and treat as failure (this SDK's create/deliver both do callChecked)
3. **First frame set INPUTING** `PUT /v1.0/card/instances`
   `{outTrackId, cardData:{cardParamMap:{flowStatus:"2", msgContent:<norm>, staticMsgContent:"", sys_full_json_obj:"{\"order\":[\"msgContent\"]}", config:"{\"autoLayout\":true}"}}}`
4. **Streaming update** `PUT /v1.0/card/streaming`
   `{outTrackId, guid:"{ts}_{rand}", key:"msgContent", content:<norm>, isFull:true, isFinalize:<bool>, isError:false}`
   Remove trailing consecutive newlines for non-final frames (prevent flicker).
5. **Finalize FINISHED** `PUT /v1.0/card/instances`
   First send isFinalize=true streaming, then set `{outTrackId, cardData:{cardParamMap:{flowStatus:"3", msgContent, ...}}, cardUpdateOptions:{updateCardDataByKey:true}}`

flowStatus: 1=PROCESSING 2=INPUTING 3=FINISHED 4=EXECUTING 5=FAILED.

**Frame rhythm (dws connect_card.go empirical evidence ported)**:
- **Frame interval 500ms**: must leave gap between first content frame and delivery, between final frame and previous frame—back-to-back competes with client card pull,
  intermittently renders "content load failed" (this race condition killed dws #407 card implementation)
- **Single frame content limit 20000** (rune-safe truncation, hermes MAX_MESSAGE_LENGTH same)

**Status badges (aligned with dws/hermes)**: `MarkThinking/MarkDone` puts text emotion on **user message**
(`POST /v1.0/robot/emotion/reply|recall`, emotionType=2, emotionId=2659900):
"🤔Thinking" indicates processing, "🥳Done" indicates completion; only supports user-sent messages (bot messages get 500); best-effort doesn't block reply.

**Throttling**:
- Single card streaming update minimum interval 800ms (DingTalk card has same-card concurrency protection, official connector battle-tested value)
- **Updates in window not discarded**: schedule trailing flush (`delay = throttle - elapsed`), content ultimately delivered, avoid "output ends at window tail→screen stuck until finish"
- **Long interval batching**: after >2s no updates (tool call/thinking pause), first flush delayed 300ms to batch, first screen is meaningful text rather than 1-2 characters
- flush and finish concurrency safe: pending flush auto-voided after closed
**Failure fallback**: create/deliver failure → silent fallback to sessionWebhook text; finish failure → fallback send accumulated text.

> **Template scope (dws A/B empirical evidence)**: card template is app-scoped—hermes own template (c629162a-...) renders "content load failed" for other apps;
> default template (02fcf2f4...) is openclaw connector's public template, can be used across apps.
**Ground truth exposure**: `streamer.CardDelivered()/cardDelivered/card_delivered/cardDelivered()` returns whether card was actually delivered successfully (for diagnostics/livecheck, prevent fallback mode from being misreported as success).

**Three lines of defense**:
- **Watchdog** (`CardWatchdog`, default 10min): timer starts after card established, refreshed on successful frame; timeout without finalization→force finish+seal—
  card won't spin forever when upstream Agent hangs/dispatch doesn't return (connector CARD_WATCHDOG_TIMEOUT same)
- **Explicit abort** `Abort()/abort()`: external interruption scenario, seal stream+card set FAILED,
  mutually exclusive with Finish (normal finalization)/Fail (error text) and idempotent
- **Error fallback cooldown** (`ErrorCooldown`, default 60s): same session error fallback text sent only once within 60s, prevent error spam
  (connector deliveredErrorTypes+ERROR_COOLDOWN same)

## 6. Rate Limiting (Global Token Bucket)

- Capacity/rate: default 20 QPS (official limit ~40, conservative value, configurable).
- Recognition: HTTP 403 and response body code string contains `QpsLimit`.
- Strategy: backoff 2s (clear tokens) → retake token retry once; streaming retry with new guid.

## 7. Markdown Normalization (normalizeForCard)

DingTalk AI card renderer conventions (outside code blocks):
- Single `\n` → `<br>`; `\n\n` paragraph preserved
- Inside code block ```: preserve `\n`
- Markdown block syntax lines (list `- / 1.`, table `|`, heading `#`, separator) preserve `\n` before
- Consecutive quote lines `>`: merge into one line with `<br>` connection, continuation lines remove `>` prefix
- Insert empty line before table separator line if none (otherwise doesn't render)

## 8. Token

`POST /v1.0/oauth2/accessToken` `{appKey, appSecret}` → `{accessToken, expireIn}`;
cached by clientId, refreshed 60s before expiration. Header uniformly `x-acs-dingtalk-access-token`.

## 9a. Media Upload (OAPI, compared with official connector media/common.ts)

1. **OAPI token**: `GET {oapiBase}/gettoken?appkey=&appsecret=` → `{errcode:0, access_token, expires_in}` (cached, refresh 60s early; oapiBase default `https://oapi.dingtalk.com`, mutually independent from new version API token)
2. **Upload**: `POST {oapiBase}/media/upload?access_token=&type={image|file|video|voice}`
   multipart/form-data, field name **`media`** (includes filename), Content-Type image uses `image/jpeg`, others `application/octet-stream`
3. **Response**: `{errcode:0, media_id, type, created_at}`; **remove leading `@` from media_id** before use
4. **Media delivery capability matrix (Aug 2026 real device experiment finalized)**:
   - mediaId produced by OAPI `media/upload` **has no public URL**—`down.dingtalk.com/media/<id>` (with @/extension variants) **all 404** (fresh upload immediately verified),
     therefore **card embedding OAPI uploaded images not feasible**; card embedding only works for **already publicly accessible URLs** (like images already in DingTalk media library)
   - **Reliable delivery = independent media message** (aligned with official connector sendVideo/sendAudio/sendFileProactive and dws current behavior—dws has deprecated old upload command and clarified in migration notes "file message delivery, no inline image rendering"):
     `SendFile` (sampleFile: mediaId+fileName+fileType), `SendVideo` (sampleVideo: videoMediaId+picMediaId+duration),
     `SendAudio` (sampleAudio: mediaId+duration)—all three require uploadMedia returned **RawMediaID (with @)**
   - `SendImage` (sampleImageMsg) only accepts public photoURL
   - >20MB files use chunked upload (v0.2 roadmap)

**Live integration (livecheck)**: each language provides `example/livecheck` (Go: `go run ./example/livecheck`;
Node: `npm run live`; Python: `python example/livecheck.py`;
Java: `mvn -q compile exec:java -Dexec.mainClass=...LiveCheck`).
After setting `DD_CLIENT_ID/DD_CLIENT_SECRET` (optional `DD_UPLOAD_FILE`), send a message to the bot,
step-by-step PASS/FAIL: connection → receive message → text reply (token) → card creation (E1) → streaming full cycle (E2/E3) → media upload.

## 9. Four Language API Comparison

| | Go | Node.js | Python | Java |
|---|---|---|---|---|
| Create | `channel.New(cfg)` | `new DingTalkChannel(cfg)` | `DingTalkChannel(cfg)` | `DingTalkChannel.create(cfg)` |
| Receive message | `ch.OnMessage(func(ctx, msg, reply))` | `ch.on('message', async (msg, reply) => {})` | `@ch.on_message` / `ch.on_message(fn)` | `ch.onMessage((msg, reply) -> {})` |
| Card callback | `ch.OnCardAction(...)` | `ch.on('cardAction', ...)` | `ch.on_card_action(fn)` | `ch.onCardAction(...)` |
| Start | `ch.Start(ctx)` | `await ch.start()` | `await ch.start()` | `ch.start()` / `startAsync()` |
| Streaming reply | `st, _ := reply.Stream(); st.Append/Finish` | `const s = reply.stream(); await s.append/finish` | `s = await reply.stream(); await s.append/finish` | `CardStreamer s = reply.stream(); s.append/finish` |

## Directory Structure (Four-layer Packaging)

```
/                      Root package channel: public API (Channel/Config) + connection & assembly
│                      channel.go config.go stream.go frame.go token.go card.go
│                      reply.go send.go lifecycle.go bot_identity.go emotion.go
│                      ratelimit.go http_mode.go oapi.go aliases.go (type aliases, maintain root package bare name compatibility)
├── types/             Shared types and error classification (types.go errors.go)
└── internal/          Implementation layer (invisible to importers)
    ├── normalize/     Inbound normalization: message.go (NormalizeIncoming/parseContent)
    ├── safety/        Admission and stability: policy_gate dedup processing_lock chat_queue batching ssrf_guard
    └── outbound/      Outbound processing: retry splitter markdown (card rendering preprocessing)
```
Dependency direction strictly unidirectional: root → internal layers → types; internal layers don't depend on each other.

## 10. Version and Naming

- Repository: `dingtalk-channel-sdk-{go,nodejs,python,java}` (aligned with dingtalk-stream-sdk-* family)
- UA: `dingtalk-channel-sdk-{lang}/v0.1.0`
- License: MIT
