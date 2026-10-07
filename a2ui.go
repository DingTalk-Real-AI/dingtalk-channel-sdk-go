package channel

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// A2UITarget 使用个人 openDingTalkId 或群 openConversationId，与机器人 staffId 区分。
type A2UITarget struct {
	OpenDingTalkID string
	ConversationID string
}

// A2UICardResult 保留完整回执；BizID 只取服务端 bizId，不使用请求侧 bizCardId。
type A2UICardResult struct {
	BizID         string          `json:"bizId,omitempty"`
	Receipt       json.RawMessage `json:"receipt"`
	UpdateWarning string          `json:"updateWarning,omitempty"`
}

// A2UIClient 可注入发送通道；messages 接受消息对象或 JSON 字符串的非空数组。
type A2UIClient interface {
	SendCard(context.Context, A2UITarget, any) (*A2UICardResult, error)
	UpdateCard(context.Context, string, any, string) (json.RawMessage, error)
}

// DWSA2UIConfig 显式选择 DWS 命令、发送身份和超时；DWS 需单独安装和登录。
type DWSA2UIConfig struct {
	Command []string
	Profile string
	Timeout time.Duration
}

// DWSA2UIClient 按 dingtalk-aicard 的公开 DWS 接入方式发送；始终使用 argv，不调用 shell。
type DWSA2UIClient struct {
	command []string
	profile string
	timeout time.Duration
}

var a2uiFlowStatuses = [...]string{
	"PROCESSING", "INPUTTING", "FINISH", "EXECUTING", "ERROR",
	"ABORTED", "TIMEOUT", "CONFIRMING", "CONFIRMED",
}

// NewDWSA2UIClient 创建可选 DWS 通道，发送身份与机器人应用 Token 独立。
func NewDWSA2UIClient(cfg DWSA2UIConfig) (*DWSA2UIClient, error) {
	command := append([]string(nil), cfg.Command...)
	if len(command) == 0 {
		command = []string{"dws"}
	}
	for _, part := range command {
		if part == "" || strings.ContainsRune(part, 0) {
			return nil, errors.New("command 必须是非空字符串数组")
		}
	}
	profile := ""
	if cfg.Profile != "" {
		var err error
		profile, err = a2uiIdentifier(cfg.Profile, "profile")
		if err != nil {
			return nil, err
		}
		if strings.Contains(profile, ",") {
			return nil, errors.New("A2UI 发送只允许一个 DWS Profile")
		}
	}
	if cfg.Timeout < 0 {
		return nil, errors.New("timeout 必须大于 0")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	return &DWSA2UIClient{command, profile, cfg.Timeout}, nil
}

func a2uiIdentifier(value, name string) (string, error) {
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s 必须是非空标识", name)
	}
	for _, char := range value {
		if char < 32 || char == 127 {
			return "", fmt.Errorf("%s 必须是非空标识", name)
		}
	}
	return strings.TrimSpace(value), nil
}

// SerializeA2UIMessages 只检查消息信封；组件和完整创建状态使用 dingtalk-aicard 校验。
func SerializeA2UIMessages(messages any) (string, error) {
	raw, err := json.Marshal(messages)
	if err != nil {
		return "", errors.New("A2UI 消息不是有效 JSON")
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil || len(items) == 0 {
		return "", errors.New("A2UI 消息必须是非空数组")
	}
	stringsOut := make([]string, 0, len(items))
	for index, item := range items {
		var encoded string
		if err := json.Unmarshal(item, &encoded); err != nil {
			encoded = string(item)
		}
		var message map[string]json.RawMessage
		if json.Unmarshal([]byte(encoded), &message) != nil || message == nil {
			return "", fmt.Errorf("A2UI 消息 %d 不是 JSON 对象", index)
		}
		var version string
		if json.Unmarshal(message["version"], &version) != nil || version != "v1.0" {
			return "", fmt.Errorf("A2UI 消息 %d 必须是 version=v1.0 的对象", index)
		}
		count := 0
		for _, operation := range []string{"createSurface", "updateComponents", "updateDataModel", "deleteSurface"} {
			if data, ok := message[operation]; ok {
				count++
				var value struct {
					SurfaceID string `json:"surfaceId"`
				}
				if json.Unmarshal(data, &value) != nil {
					return "", errors.New("A2UI 操作必须是 JSON 对象")
				}
				if _, err := a2uiIdentifier(value.SurfaceID, "surfaceId"); err != nil {
					return "", err
				}
			}
		}
		if count != 1 {
			return "", fmt.Errorf("A2UI 消息 %d 必须包含一个操作", index)
		}
		stringsOut = append(stringsOut, encoded)
	}
	content, _ := json.Marshal(stringsOut)
	if len(content) > 65536 {
		return "", errors.New("DWS A2UI content 超过 64 KiB")
	}
	return string(content), nil
}

func a2uiEnvelopeChain(raw []byte) ([]map[string]any, error) {
	var value map[string]any
	if json.Unmarshal(raw, &value) != nil || value == nil {
		return nil, errors.New("DWS 输出不是 JSON 对象；发送结果可能未知，请保留现场核实")
	}
	chain := []map[string]any{}
	for i := 0; i < 5 && value != nil; i++ {
		chain = append(chain, value)
		if value["success"] == false || value["ok"] == false || value["isError"] == true || value["error"] != nil || value["dry_run"] == true || value["dryRun"] == true {
			return nil, errors.New("DWS 返回失败回执")
		}
		if outcome, exists := value["outcome"]; exists && outcome != "success" && outcome != "pending" {
			return nil, errors.New("DWS 返回失败回执")
		}
		next := value["data"]
		if next == nil {
			next = value["result"]
		}
		value, _ = next.(map[string]any)
	}
	accepted := false
	for _, item := range chain {
		accepted = accepted || item["success"] == true || item["ok"] == true
	}
	if !accepted {
		return nil, errors.New("DWS 回执未明确确认接受请求；发送结果可能未知，请核实后再重试")
	}
	return chain, nil
}

type a2uiOutput struct{ bytes.Buffer }

func (out *a2uiOutput) Write(data []byte) (int, error) {
	if out.Len()+len(data) > 8*1024*1024 {
		return 0, errors.New("DWS 回执超过 8 MiB")
	}
	return out.Buffer.Write(data)
}

func (c *DWSA2UIClient) invoke(ctx context.Context, args ...string) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	argv := append([]string(nil), c.command[1:]...)
	argv = append(argv, "chat", "message")
	argv = append(argv, args...)
	argv = append(argv, "--format=json", "--yes")
	if c.profile != "" {
		argv = append(argv, "--profile="+c.profile)
	}
	cmd := exec.CommandContext(ctx, c.command[0], argv...)
	var out a2uiOutput
	cmd.Stdout = &out
	cmd.Stderr = io.Discard
	cmd.WaitDelay = time.Second
	if err := cmd.Run(); err != nil {
		// 不包含原始命令行或 stderr；失败、超时后的写入结果可能未知，不自动重试。
		return nil, errors.New("DWS 执行失败或超时；发送结果可能未知，请核实后再重试")
	}
	if _, err := a2uiEnvelopeChain(out.Bytes()); err != nil {
		return nil, err
	}
	return json.RawMessage(append([]byte(nil), out.Bytes()...)), nil
}

// SendCard 创建 PROCESSING 卡片；返回完整回执和可用的服务端 BizID。
func (c *DWSA2UIClient) SendCard(ctx context.Context, target A2UITarget, messages any) (*A2UICardResult, error) {
	dm, group := target.OpenDingTalkID != "", target.ConversationID != ""
	if dm == group {
		return nil, errors.New("A2UI target 必须恰好选择一个接收目标")
	}
	flag, id := "conversation-id", target.ConversationID
	if dm {
		flag, id = "open-dingtalk-id", target.OpenDingTalkID
	}
	id, err := a2uiIdentifier(id, flag)
	if err != nil {
		return nil, err
	}
	content, err := SerializeA2UIMessages(messages)
	if err != nil {
		return nil, err
	}
	receipt, err := c.invoke(ctx, "send-a2ui-card", "--"+flag+"="+id, "--content="+content)
	if err != nil {
		return nil, err
	}
	result := &A2UICardResult{Receipt: receipt}
	chain, _ := a2uiEnvelopeChain(receipt)
	for i := len(chain) - 1; i >= 0; i-- {
		if id, ok := chain[i]["bizId"].(string); ok {
			if normalized, err := a2uiIdentifier(id, "bizId"); err == nil {
				result.BizID = normalized
				break
			}
		}
	}
	if result.BizID == "" {
		result.UpdateWarning = "回执未包含可用的 bizId；请保留回执并核实服务端标识，不要自动重发创建请求。"
	}
	return result, nil
}

// UpdateCard 更新原卡片；FINISH 用于完成静态卡片，messages 应是同一 Surface 的增量。
func (c *DWSA2UIClient) UpdateCard(ctx context.Context, bizID string, messages any, flowStatus string) (json.RawMessage, error) {
	id, err := a2uiIdentifier(bizID, "bizId")
	if err != nil {
		return nil, err
	}
	status := strings.ToUpper(strings.TrimSpace(flowStatus))
	if number, err := strconv.Atoi(status); err == nil && number >= 1 && number <= 9 && len(status) == 1 {
		status = a2uiFlowStatuses[number-1]
	}
	found := false
	for _, allowed := range a2uiFlowStatuses {
		found = found || allowed == status
	}
	if !found {
		return nil, errors.New("不支持的 A2UI flowStatus")
	}
	content, err := SerializeA2UIMessages(messages)
	if err != nil {
		return nil, err
	}
	return c.invoke(ctx, "update-a2ui-card", "--biz-id="+id, "--content="+content, "--flow-status="+status)
}

// SendA2UICard 通过显式配置的 A2UIClient 发送卡片。
func (c *Channel) SendA2UICard(ctx context.Context, target A2UITarget, messages any) (*A2UICardResult, error) {
	if c.cfg.A2UIClient == nil {
		return nil, errors.New("请先配置 A2UIClient（例如 DWSA2UIClient）")
	}
	return c.cfg.A2UIClient.SendCard(ctx, target, messages)
}

// UpdateA2UICard 用服务端 BizID 更新同一卡片，flowStatus 必填。
func (c *Channel) UpdateA2UICard(ctx context.Context, bizID string, messages any, flowStatus string) (json.RawMessage, error) {
	if c.cfg.A2UIClient == nil {
		return nil, errors.New("请先配置 A2UIClient（例如 DWSA2UIClient）")
	}
	return c.cfg.A2UIClient.UpdateCard(ctx, bizID, messages, flowStatus)
}
