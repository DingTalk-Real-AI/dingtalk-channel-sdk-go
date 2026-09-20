// Agent 接入示例：钉钉消息 → LLM 流式回复（AI 卡片打字机）。
//
// 用法：
//
//	DD_CLIENT_ID=ding... DD_CLIENT_SECRET=... \
//	DEEPSEEK_API_KEY=sk-... [DEEPSEEK_BASE_URL=...] [AGENT_MODEL=deepseek-chat] \
//	[AGENT_SYS="你是..."] go run ./example/agent
//
// 也支持 Anthropic 兼容网关（ANTHROPIC_BASE_URL + ANTHROPIC_AUTH_TOKEN）；
// 两者都配置时优先 DeepSeek；都未配置时降级 echo 模式。
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	channel "github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go"
)

// ── 会话历史（内存版，按 conversationId 分组，群聊共享上下文）──

type turn struct {
	Role string `json:"role"` // user | assistant
	Text string `json:"content"`
}

var (
	histMu  sync.Mutex
	histMap = map[string][]turn{}
	histCap = 20 // 每会话保留最近 N 轮
)

func historyAppend(cid string, t turn) []turn {
	histMu.Lock()
	defer histMu.Unlock()
	h := append(histMap[cid], t)
	if len(h) > histCap {
		h = h[len(h)-histCap:]
	}
	histMap[cid] = h
	return append([]turn(nil), h...)
}

// ── LLM Provider：DeepSeek(OpenAI 兼容) 优先，其次 Anthropic 兼容网关 ──

type provider int

const (
	provNone provider = iota
	provOpenAI
	provAnthropic
)

type llmConfig struct {
	kind  provider
	base  string
	token string
	model string
}

func llmFromEnv() llmConfig {
	if k := os.Getenv("DEEPSEEK_API_KEY"); k != "" {
		base := os.Getenv("DEEPSEEK_BASE_URL")
		if base == "" {
			base = "https://api.deepseek.com"
		}
		model := os.Getenv("AGENT_MODEL")
		if model == "" {
			model = "deepseek-chat"
		}
		return llmConfig{provOpenAI, strings.TrimRight(base, "/"), k, model}
	}
	if t := os.Getenv("ANTHROPIC_AUTH_TOKEN"); t != "" && os.Getenv("ANTHROPIC_BASE_URL") != "" {
		model := os.Getenv("AGENT_MODEL")
		if model == "" {
			model = "glm-4.7"
		}
		return llmConfig{provAnthropic, strings.TrimRight(os.Getenv("ANTHROPIC_BASE_URL"), "/"), t, model}
	}
	return llmConfig{}
}

type llmErr struct {
	status int
	body   string
}

func (e *llmErr) Error() string { return fmt.Sprintf("llm http %d: %s", e.status, e.body) }

// callLLMStream 流式调用；onDelta 收到增量文本；返回完整回复。
func callLLMStream(ctx context.Context, c llmConfig, system string, turns []turn, onDelta func(string)) (string, error) {
	var url string
	var body []byte
	switch c.kind {
	case provOpenAI:
		url = c.base + "/v1/chat/completions"
		msgs := make([]turn, 0, len(turns)+1)
		msgs = append(msgs, turn{Role: "system", Text: system})
		msgs = append(msgs, turns...)
		body, _ = json.Marshal(map[string]any{
			"model":      c.model,
			"stream":     true,
			"max_tokens": 1024,
			"messages":   msgs,
		})
	case provAnthropic:
		url = c.base + "/v1/messages"
		body, _ = json.Marshal(map[string]any{
			"model":      c.model,
			"max_tokens": 1024,
			"stream":     true,
			"system":     system,
			"messages":   turns,
		})
	default:
		return "", fmt.Errorf("no llm provider configured")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	if c.kind == provAnthropic {
		req.Header.Set("x-api-key", c.token)
		req.Header.Set("anthropic-version", "2023-06-01")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		return "", &llmErr{resp.StatusCode, truncate(buf.String(), 300)}
	}

	var full strings.Builder
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			continue
		}
		deltaText, errMsg := extractDelta(c.kind, payload)
		if errMsg != "" {
			return full.String(), fmt.Errorf("llm stream error: %s", errMsg)
		}
		if deltaText != "" {
			full.WriteString(deltaText)
			if onDelta != nil {
				onDelta(deltaText)
			}
		}
	}
	return full.String(), sc.Err()
}

// extractDelta 从 SSE payload 提取增量文本 / 错误信息。
func extractDelta(kind provider, payload string) (text, errMsg string) {
	switch kind {
	case provOpenAI:
		var ev struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
			Error *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal([]byte(payload), &ev) != nil {
			return "", ""
		}
		if ev.Error != nil {
			return "", ev.Error.Message
		}
		if len(ev.Choices) > 0 {
			return ev.Choices[0].Delta.Content, ""
		}
		return "", ""
	default: // Anthropic
		var ev struct {
			Type  string `json:"type"`
			Delta struct {
				Text string `json:"text"`
			} `json:"delta"`
			Error *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal([]byte(payload), &ev) != nil {
			return "", ""
		}
		if ev.Error != nil {
			return "", ev.Error.Message
		}
		if ev.Type == "content_block_delta" {
			return ev.Delta.Text, ""
		}
		return "", ""
	}
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

// ── 主流程：钉钉消息 → 历史拼接 → LLM 流式 → 卡片打字机 ──

func main() {
	cfg := channel.Config{
		ClientID:     os.Getenv("DD_CLIENT_ID"),
		ClientSecret: os.Getenv("DD_CLIENT_SECRET"),
	}
	if cfg.ClientID == "" || cfg.ClientSecret == "" {
		fmt.Println("需要环境变量 DD_CLIENT_ID / DD_CLIENT_SECRET")
		os.Exit(2)
	}
	sys := os.Getenv("AGENT_SYS")
	if sys == "" {
		sys = "你是钉钉群里的 AI 助手。回答简洁、用 Markdown；代码用代码块。"
	}
	llm := llmFromEnv()
	mode := "echo（未配置 LLM 凭据）"
	if llm.kind == provOpenAI {
		mode = "DeepSeek/OpenAI 流式 model=" + llm.model
	} else if llm.kind == provAnthropic {
		mode = "Anthropic 兼容流式 model=" + llm.model
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	fmt.Printf("== dingtalk agent（Go）== 模式: %s\n", mode)
	ch := channel.New(cfg)

	ch.OnMessage(func(ctx context.Context, msg *channel.IncomingMessage, reply channel.Reply) error {
		fmt.Printf("[%s] %s: %s\n", time.Now().Format("15:04:05"), msg.SenderNick, msg.Text)

		// 贴"🤔思考中"表情（openclaw connector 同款；失败不影响主流程）
		_ = ch.MarkThinking(ctx, msg.ConversationID, msg.MsgID)
		defer func() { _ = ch.RecallThinking(ctx, msg.ConversationID, msg.MsgID) }() // 回复落地后撤回

		s, err := reply.Stream(ctx) // E1：立即出"输入中"卡片
		if err != nil {
			fmt.Println("card create failed:", err)
			return nil
		}

		if llm.kind == provNone {
			_ = s.Finish(fmt.Sprintf("**echo**: %s", msg.Text))
			return nil
		}

		turns := historyAppend(msg.ConversationID, turn{Role: "user", Text: msg.Text})
		var card strings.Builder
		card.WriteString(fmt.Sprintf("> **%s**: %s\n\n", msg.SenderNick, msg.Text))
		full, err := callLLMStream(ctx, llm, sys, turns, func(d string) {
			card.WriteString(d)
			_ = s.Append(card.String()) // 全量追加，SDK 内部做节流
		})
		if err != nil {
			_ = s.Finish(card.String() + "\n\n---\n⚠️ LLM 调用失败: `" + err.Error() + "`")
			fmt.Println("llm error:", err)
			return nil
		}
		historyAppend(msg.ConversationID, turn{Role: "assistant", Text: full})
		if err := s.Finish(card.String()); err != nil {
			fmt.Println("finish failed:", err)
		}
		return nil
	})

	fmt.Println("agent 已连接，在钉钉里 @机器人 或私聊提问...")
	if err := ch.Start(ctx); err != nil && ctx.Err() == nil {
		fmt.Println("stream error:", err)
	}
}
