package channel

import (
	"context"
	"testing"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/internal/safety"
)

func boolPtr(b bool) *bool { return &b }

func overrideTestGate(cfg PolicyConfig) *safety.PolicyGate { return safety.NewPolicyGate(cfg) }

func overrideTestMsg(cid string, inAtList bool) *IncomingMessage {
	return &IncomingMessage{
		ConversationID:   cid,
		ConversationType: ConversationTypeGroup,
		SenderID:         "staff-1",
		IsInAtList:       inAtList,
	}
}

// 显式群条目可在白名单模式下放行该群。
func TestGroupOverrideAdmitsGroupInAllowlistMode(t *testing.T) {
	gate := overrideTestGate(PolicyConfig{
		GroupAllowlist: []string{"cid-allowed"},
		GroupOverrides: map[string]GroupOverride{
			"cid-other": {}, // 零值覆盖：仅用于放行
		},
	})
	if d := gate.Evaluate(overrideTestMsg("cid-other", true)); !d.Allowed {
		t.Fatalf("explicit override should admit group in allowlist mode, got %v", d.Reason)
	}
	if d := gate.Evaluate(overrideTestMsg("cid-unknown", true)); d.Allowed || d.Reason != RejectReasonGroupNotAllowed {
		t.Fatalf("unknown group should be rejected, got %v", d)
	}
}

// 黑名单永不例外：即使有显式群条目也拒绝。
func TestGroupOverrideNeverOverridesBlocklist(t *testing.T) {
	gate := overrideTestGate(PolicyConfig{
		GroupBlocklist: []string{"cid-bad"},
		GroupOverrides: map[string]GroupOverride{
			"cid-bad": {Enabled: boolPtr(true)},
		},
	})
	if d := gate.Evaluate(overrideTestMsg("cid-bad", true)); d.Allowed || d.Reason != RejectReasonGroupBlocked {
		t.Fatalf("blocklist must win over override, got %v", d)
	}
}

// Enabled=false 显式禁用该群。
func TestGroupOverrideDisable(t *testing.T) {
	gate := overrideTestGate(PolicyConfig{
		GroupOverrides: map[string]GroupOverride{
			"cid-1": {Enabled: boolPtr(false)},
		},
	})
	if d := gate.Evaluate(overrideTestMsg("cid-1", true)); d.Allowed || d.Reason != RejectReasonGroupDisabled {
		t.Fatalf("disabled group should be rejected, got %v", d)
	}
}

// RequireMention 覆盖：全局要求 @，覆盖后该群免 @；反之亦然。
func TestGroupOverrideRequireMention(t *testing.T) {
	gate := overrideTestGate(PolicyConfig{
		RequireMention: boolPtr(true),
		GroupOverrides: map[string]GroupOverride{
			"cid-1": {RequireMention: boolPtr(false)},
			"cid-2": {RequireMention: boolPtr(true)},
		},
	})
	if d := gate.Evaluate(overrideTestMsg("cid-1", false)); !d.Allowed {
		t.Fatalf("override requireMention=false should admit without mention, got %v", d.Reason)
	}
	if d := gate.Evaluate(overrideTestMsg("cid-2", false)); d.Allowed || d.Reason != RejectReasonNoMention {
		t.Fatalf("override requireMention=true should reject without mention, got %v", d)
	}
	// 未覆盖的群沿用全局
	if d := gate.Evaluate(overrideTestMsg("cid-3", false)); d.Allowed || d.Reason != RejectReasonNoMention {
		t.Fatalf("global requireMention should apply to non-overridden group, got %v", d)
	}
}

// 群内发送者白名单。
func TestGroupOverrideAllowFrom(t *testing.T) {
	gate := overrideTestGate(PolicyConfig{
		GroupOverrides: map[string]GroupOverride{
			"cid-1": {AllowFrom: []string{"staff-1"}},
		},
	})
	if d := gate.Evaluate(overrideTestMsg("cid-1", true)); !d.Allowed {
		t.Fatalf("allowlisted sender should pass, got %v", d.Reason)
	}
	msg := overrideTestMsg("cid-1", true)
	msg.SenderID = "staff-2"
	if d := gate.Evaluate(msg); d.Allowed || d.Reason != RejectReasonSenderNotAllowed {
		t.Fatalf("non-allowlisted sender should be rejected, got %v", d)
	}
}

// 群内发送者黑名单优先于白名单。
func TestGroupOverrideBlockFrom(t *testing.T) {
	gate := overrideTestGate(PolicyConfig{
		GroupOverrides: map[string]GroupOverride{
			"cid-1": {AllowFrom: []string{"staff-1", "staff-2"}, BlockFrom: []string{"staff-2"}},
		},
	})
	if d := gate.Evaluate(overrideTestMsg("cid-1", true)); !d.Allowed {
		t.Fatalf("staff-1 should pass, got %v", d.Reason)
	}
	msg := overrideTestMsg("cid-1", true)
	msg.SenderID = "staff-2" // 同时在白名单和黑名单 → 黑名单赢
	if d := gate.Evaluate(msg); d.Allowed || d.Reason != RejectReasonSenderBlocked {
		t.Fatalf("blocklist should win, got %v", d)
	}
}

// 端到端：被覆盖禁用的群消息走完整管线 → onReject 收到 group_disabled，处理器不触发。
func TestGroupOverrideE2EThroughPipeline(t *testing.T) {
	_, srv := newFakeAPI(t)
	cfg := testConfig(srv.URL, srv.URL)
	cfg.Policy = PolicyConfig{
		GroupOverrides: map[string]GroupOverride{
			"cid-1": {Enabled: boolPtr(false)},
		},
	}
	ch := New(cfg)
	var got *RejectEvent
	done := make(chan struct{}, 1)
	ch.OnReject(func(ctx context.Context, ev *RejectEvent) {
		got = ev
		done <- struct{}{}
	})
	called := false
	ch.OnMessage(func(ctx context.Context, msg *IncomingMessage, reply Reply) error {
		called = true
		return nil
	})
	ch.dispatchFrame(context.Background(), botFrame("m-ov", "b-ov", "hi", srv.URL+"/webhook"))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("onReject not fired")
	}
	if got == nil || got.Reason != RejectReasonGroupDisabled {
		t.Fatalf("reason = %v, want group_disabled", got)
	}
	if called {
		t.Fatal("handler must not be called for disabled group")
	}
}
