package channel

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const a2uiTestMessage = `{"version":"v1.0","updateDataModel":{"surfaceId":"sdk-card","path":"/text","value":"中文、引号\"与 $(echo test) ` + "`test`" + `"}}`

// 模拟 DWS 可执行进程；只写本地 argv 记录，不调用真实服务。
func TestA2UIHelperProcess(t *testing.T) {
	index := -1
	for i, arg := range os.Args {
		if arg == "--a2ui-helper" {
			index = i
			break
		}
	}
	if index < 0 {
		return
	}
	trace, response, mode := os.Args[index+1], os.Args[index+2], os.Args[index+3]
	args := os.Args[index+4:]
	raw, _ := json.Marshal(args)
	file, err := os.OpenFile(trace, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		os.Exit(4)
	}
	_, _ = file.Write(append(raw, '\n'))
	_ = file.Close()
	if mode == "hang" {
		time.Sleep(10 * time.Second)
	}
	if mode == "fail" {
		_, _ = fmt.Fprint(os.Stderr, "不应暴露的凭据占位符")
		os.Exit(2)
	}
	_, _ = fmt.Fprint(os.Stdout, response)
	os.Exit(0)
}

func a2uiFixture(t *testing.T, response, mode string, timeout time.Duration) (*DWSA2UIClient, func() [][]string) {
	t.Helper()
	dir, err := os.MkdirTemp(".", ".a2ui-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	trace := filepath.Join(dir, "trace.jsonl")
	client, err := NewDWSA2UIClient(DWSA2UIConfig{
		Command: []string{os.Args[0], "-test.run=TestA2UIHelperProcess", "--", "--a2ui-helper", trace, response, mode},
		Profile: "corp:user", Timeout: timeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client, func() [][]string {
		raw, err := os.ReadFile(trace)
		if err != nil {
			t.Fatal(err)
		}
		var calls [][]string
		for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			var args []string
			if err := json.Unmarshal([]byte(line), &args); err != nil {
				t.Fatal(err)
			}
			calls = append(calls, args)
		}
		return calls
	}
}

func TestA2UISerializeMessages(t *testing.T) {
	var object map[string]any
	_ = json.Unmarshal([]byte(a2uiTestMessage), &object)
	content, err := SerializeA2UIMessages([]any{object, a2uiTestMessage})
	if err != nil {
		t.Fatal(err)
	}
	var stringsOut []string
	_ = json.Unmarshal([]byte(content), &stringsOut)
	for _, value := range stringsOut {
		var got map[string]any
		if json.Unmarshal([]byte(value), &got) != nil || !reflect.DeepEqual(got, object) {
			t.Fatal("消息语义没有保留")
		}
	}
	if stringsOut[1] != a2uiTestMessage {
		t.Fatal("已有 JSON 字符串被重复编码")
	}
	for _, invalid := range []any{nil, []any{}, map[string]any{}, []any{"not-json"}, []any{nil}, []any{`{"version":"v0.8"}`}, []any{`{"version":"v1.0"}`}, []any{`{"version":"v1.0","deleteSurface":{"surfaceId":""}}`}, []any{`{"version":"v1.0","deleteSurface":{"surfaceId":"x"},"createSurface":{"surfaceId":"x"}}`}} {
		if _, err := SerializeA2UIMessages(invalid); err == nil {
			t.Fatalf("无效信封被接受: %T", invalid)
		}
	}
	object["updateDataModel"].(map[string]any)["value"] = strings.Repeat("中", 24000)
	if _, err := SerializeA2UIMessages([]any{object}); err == nil || !strings.Contains(err.Error(), "64 KiB") {
		t.Fatal("超大 argv 未在执行前拒绝")
	}
}

func TestA2UIChannelSendAndFinish(t *testing.T) {
	client, calls := a2uiFixture(t, `{"ok":true,"outcome":"success","data":{"success":true,"result":{"bizId":"server-biz"}}}`, "ok", 5*time.Second)
	ch := New(Config{ClientID: "unused", ClientSecret: "unused", A2UIClient: client})
	result, err := ch.SendA2UICard(context.Background(), A2UITarget{OpenDingTalkID: "D-user"}, []string{a2uiTestMessage})
	if err != nil || result.BizID != "server-biz" || result.UpdateWarning != "" {
		t.Fatalf("单聊发送失败: %v", err)
	}
	if _, err := ch.SendA2UICard(context.Background(), A2UITarget{ConversationID: "--group-value"}, []string{a2uiTestMessage}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.UpdateA2UICard(context.Background(), result.BizID, []string{a2uiTestMessage}, "3"); err != nil {
		t.Fatal(err)
	}
	argv := calls()
	if len(argv) != 3 || !hasA2UIArg(argv[0], "--open-dingtalk-id=D-user") || !hasA2UIArg(argv[1], "--conversation-id=--group-value") || !hasA2UIArg(argv[2], "--flow-status=FINISH") || !hasA2UIArg(argv[2], "--biz-id=server-biz") {
		t.Fatal("发送与更新参数错误")
	}
	for _, args := range argv {
		if !hasA2UIArg(args, "--profile=corp:user") || !hasA2UIArg(args, "--format=json") || !hasA2UIArg(args, "--yes") {
			t.Fatal("缺少固定身份或 JSON 输出参数")
		}
		for _, arg := range args {
			if strings.Contains(arg, "client-secret") {
				t.Fatal("错误使用机器人凭据")
			}
		}
	}
}

func hasA2UIArg(args []string, value string) bool {
	for _, arg := range args {
		if arg == value {
			return true
		}
	}
	return false
}

func TestA2UIValidationAndOptIn(t *testing.T) {
	if _, err := NewDWSA2UIClient(DWSA2UIConfig{Profile: "corp:user,other:user"}); err == nil {
		t.Fatal("多 Profile 发送没有被拒绝")
	}
	ch := New(Config{})
	if _, err := ch.SendA2UICard(context.Background(), A2UITarget{ConversationID: "cid"}, []string{a2uiTestMessage}); err == nil {
		t.Fatal("未启用 A2UI 时未拒绝")
	}
	if _, err := ch.UpdateA2UICard(context.Background(), "biz", []string{a2uiTestMessage}, "FINISH"); err == nil {
		t.Fatal("未启用 A2UI 时更新未拒绝")
	}
	client, _ := NewDWSA2UIClient(DWSA2UIConfig{Command: []string{"不存在的 dws"}})
	for _, target := range []A2UITarget{{}, {OpenDingTalkID: "user", ConversationID: "group"}, {OpenDingTalkID: " "}} {
		if _, err := client.SendCard(context.Background(), target, []string{a2uiTestMessage}); err == nil || strings.Contains(err.Error(), "执行失败") {
			t.Fatal("错误目标未在执行前拒绝")
		}
	}
	for _, item := range [][2]string{{"", "FINISH"}, {"biz", "unknown"}} {
		if _, err := client.UpdateCard(context.Background(), item[0], []string{a2uiTestMessage}, item[1]); err == nil || strings.Contains(err.Error(), "执行失败") {
			t.Fatal("错误更新参数未在执行前拒绝")
		}
	}
}

func TestA2UIMissingServerID(t *testing.T) {
	client, calls := a2uiFixture(t, `{"success":true,"result":{"bizCardId":"request-only","openTaskId":"task"}}`, "ok", 5*time.Second)
	result, err := client.SendCard(context.Background(), A2UITarget{ConversationID: "cid"}, []string{a2uiTestMessage})
	if err != nil || result.BizID != "" || !strings.Contains(result.UpdateWarning, "不要自动重发") || len(calls()) != 1 {
		t.Fatal("缺少服务端标识时没有保留回执与警告")
	}
}

func TestA2UIFailuresAndTimeout(t *testing.T) {
	for _, item := range [][2]string{{`{"success":false}`, "ok"}, {`{"success":true,"result":{"success":false}}`, "ok"}, {"not-json", "ok"}, {"{}", "ok"}, {`{"result":{"bizId":"unconfirmed"}}`, "ok"}, {`{"ok":true,"outcome":"success","dry_run":true}`, "ok"}, {"{}", "fail"}} {
		client, calls := a2uiFixture(t, item[0], item[1], 5*time.Second)
		if _, err := client.SendCard(context.Background(), A2UITarget{ConversationID: "cid"}, []string{a2uiTestMessage}); err == nil || strings.Contains(err.Error(), "不应暴露") || len(calls()) != 1 {
			t.Fatal("失败被接受、泄露 stderr 或自动重试")
		}
	}
	client, _ := a2uiFixture(t, "{}", "hang", 200*time.Millisecond)
	if _, err := client.SendCard(context.Background(), A2UITarget{ConversationID: "cid"}, []string{a2uiTestMessage}); err == nil || !strings.Contains(err.Error(), "结果可能未知") {
		t.Fatal("超时没有明确报告未知结果")
	}
}
