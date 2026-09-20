package safety

import (
	"testing"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

func TestPolicyGate_AdminBypass(t *testing.T) {
	cfg := types.PolicyConfig{
		DMMode:         "disabled", // DM 禁用
		GroupAllowlist: []string{"group1"}, // 只允许 group1
		Admins:         []string{"admin123"}, // 管理员
	}
	pg := NewPolicyGate(cfg)

	// 管理员在 DM 禁用模式下仍可通过
	msg := &types.IncomingMessage{
		ConversationType: types.ConversationTypeDM,
		SenderStaffID:    "admin123",
		SenderID:         "admin123",
	}
	decision := pg.Evaluate(msg)
	if !decision.Allowed {
		t.Error("expected admin to bypass DM disabled policy")
	}

	// 管理员在群组白名单外仍可通过
	msg2 := &types.IncomingMessage{
		ConversationType: types.ConversationTypeGroup,
		ConversationID:   "group2", // 不在白名单
		SenderStaffID:    "admin123",
		SenderID:         "admin123",
		IsInAtList:       true,
	}
	decision2 := pg.Evaluate(msg2)
	if !decision2.Allowed {
		t.Error("expected admin to bypass group allowlist")
	}

	// 非管理员被拒绝
	msg3 := &types.IncomingMessage{
		ConversationType: types.ConversationTypeGroup,
		ConversationID:   "group2",
		SenderStaffID:    "user456",
		SenderID:         "user456",
		IsInAtList:       true,
	}
	decision3 := pg.Evaluate(msg3)
	if decision3.Allowed {
		t.Error("expected non-admin to be rejected by group allowlist")
	}
	if decision3.Reason != types.RejectReasonGroupNotAllowed {
		t.Errorf("expected reason group_not_allowed, got %s", decision3.Reason)
	}
}

func TestPolicyGate_GlobalSenderControl(t *testing.T) {
	cfg := types.PolicyConfig{
		DenyFrom:  []string{"blocked_user"},
		AllowFrom: []string{"allowed_user1", "allowed_user2"},
	}
	pg := NewPolicyGate(cfg)

	// 全局黑名单用户被拒绝（DM）
	msg1 := &types.IncomingMessage{
		ConversationType: types.ConversationTypeDM,
		SenderStaffID:    "blocked_user",
		SenderID:         "blocked_user",
	}
	decision1 := pg.Evaluate(msg1)
	if decision1.Allowed {
		t.Error("expected blocked user to be denied")
	}
	if decision1.Reason != types.RejectReasonSenderDenied {
		t.Errorf("expected reason sender_denied, got %s", decision1.Reason)
	}

	// 全局黑名单用户被拒绝（群组）
	msg2 := &types.IncomingMessage{
		ConversationType: types.ConversationTypeGroup,
		ConversationID:   "group1",
		SenderStaffID:    "blocked_user",
		SenderID:         "blocked_user",
		IsInAtList:       true,
	}
	decision2 := pg.Evaluate(msg2)
	if decision2.Allowed {
		t.Error("expected blocked user to be denied in group")
	}

	// 白名单用户通过
	msg3 := &types.IncomingMessage{
		ConversationType: types.ConversationTypeDM,
		SenderStaffID:    "allowed_user1",
		SenderID:         "allowed_user1",
	}
	decision3 := pg.Evaluate(msg3)
	if !decision3.Allowed {
		t.Error("expected allowed user to pass")
	}

	// 不在白名单的用户被拒绝
	msg4 := &types.IncomingMessage{
		ConversationType: types.ConversationTypeDM,
		SenderStaffID:    "random_user",
		SenderID:         "random_user",
	}
	decision4 := pg.Evaluate(msg4)
	if decision4.Allowed {
		t.Error("expected non-allowlist user to be rejected")
	}
	if decision4.Reason != types.RejectReasonSenderNotAllowed {
		t.Errorf("expected reason sender_not_allowed, got %s", decision4.Reason)
	}
}

func TestPolicyGate_AdminBypassesSenderControl(t *testing.T) {
	cfg := types.PolicyConfig{
		DenyFrom: []string{"admin123"}, // 管理员也在黑名单
		Admins:   []string{"admin123"}, // 但是管理员
	}
	pg := NewPolicyGate(cfg)

	// 管理员绕过黑名单（管理员优先级高于黑名单）
	msg := &types.IncomingMessage{
		ConversationType: types.ConversationTypeDM,
		SenderStaffID:    "admin123",
		SenderID:         "admin123",
	}
	decision := pg.Evaluate(msg)
	if !decision.Allowed {
		t.Error("expected admin to bypass deny list")
	}
}

func TestPolicyGate_MentionAllControl(t *testing.T) {
	respondFalse := false
	respondTrue := true
	requireFalse := false

	cfg := types.PolicyConfig{
		RequireMention:      &requireFalse, // 不要求 @机器人
		RespondToMentionAll: &respondFalse, // 全局不响应 @all
	}
	pg := NewPolicyGate(cfg)

	// @all 消息被拒绝
	msg1 := &types.IncomingMessage{
		ConversationType: types.ConversationTypeGroup,
		ConversationID:   "group1",
		SenderStaffID:    "user1",
		SenderID:         "user1",
		MentionAll:       true,
		IsInAtList:       false, // 只有 @all，没有 @机器人
	}
	decision1 := pg.Evaluate(msg1)
	if decision1.Allowed {
		t.Error("expected @all message to be rejected")
	}
	if decision1.Reason != types.RejectReasonMentionAll {
		t.Errorf("expected reason mention_all_blocked, got %s", decision1.Reason)
	}

	// 群组覆盖允许 @all
	cfg2 := types.PolicyConfig{
		RequireMention:      &requireFalse, // 不要求 @机器人
		RespondToMentionAll: &respondFalse,
		GroupOverrides: map[string]types.GroupOverride{
			"group2": {
				RequireMention:      &requireFalse,
				RespondToMentionAll: &respondTrue,
			},
		},
	}
	pg2 := NewPolicyGate(cfg2)

	msg2 := &types.IncomingMessage{
		ConversationType: types.ConversationTypeGroup,
		ConversationID:   "group2",
		SenderStaffID:    "user1",
		SenderID:         "user1",
		MentionAll:       true,
		IsInAtList:       false,
	}
	decision2 := pg2.Evaluate(msg2)
	if !decision2.Allowed {
		t.Errorf("expected @all to be allowed by group override, got reason: %s", decision2.Reason)
	}
}

func TestPolicyGate_BotIdentity(t *testing.T) {
	cfg := types.PolicyConfig{}
	pg := NewPolicyGate(cfg)

	// 初始没有 bot 身份
	if pg.GetBotIdentity() != nil {
		t.Error("expected no bot identity initially")
	}

	// 设置 bot 身份
	bot := &types.BotIdentity{
		RobotCode: "robot123",
		RobotName: "TestBot",
	}
	pg.SetBotIdentity(bot)

	// 获取 bot 身份
	retrieved := pg.GetBotIdentity()
	if retrieved == nil {
		t.Fatal("expected bot identity to be set")
	}
	if retrieved.RobotCode != "robot123" {
		t.Errorf("expected RobotCode robot123, got %s", retrieved.RobotCode)
	}
	if retrieved.RobotName != "TestBot" {
		t.Errorf("expected RobotName TestBot, got %s", retrieved.RobotName)
	}
}

func TestPolicyGate_GroupOverrideWithMentionAll(t *testing.T) {
	requireTrue := true
	respondTrue := true

	cfg := types.PolicyConfig{
		GroupOverrides: map[string]types.GroupOverride{
			"group1": {
				RequireMention:      &requireTrue,
				RespondToMentionAll: &respondTrue,
			},
		},
	}
	pg := NewPolicyGate(cfg)

	// 只有 @all，没有 @机器人，但群组覆盖允许 @all
	msg := &types.IncomingMessage{
		ConversationType: types.ConversationTypeGroup,
		ConversationID:   "group1",
		SenderStaffID:    "user1",
		SenderID:         "user1",
		MentionAll:       true,
		IsInAtList:       false,
	}
	decision := pg.Evaluate(msg)
	if decision.Allowed {
		t.Error("expected message to be rejected (require @bot but only has @all)")
	}
	if decision.Reason != types.RejectReasonNoMention {
		t.Errorf("expected reason no_mention, got %s", decision.Reason)
	}

	// 既有 @all 又有 @机器人
	msg2 := &types.IncomingMessage{
		ConversationType: types.ConversationTypeGroup,
		ConversationID:   "group1",
		SenderStaffID:    "user1",
		SenderID:         "user1",
		MentionAll:       true,
		IsInAtList:       true,
	}
	decision2 := pg.Evaluate(msg2)
	if !decision2.Allowed {
		t.Errorf("expected message to be allowed, got reason %s", decision2.Reason)
	}
}

func TestPolicyGate_EmptyStaffID(t *testing.T) {
	cfg := types.PolicyConfig{
		Admins:    []string{"admin123"},
		DenyFrom:  []string{"blocked"},
		AllowFrom: []string{"allowed"},
	}
	pg := NewPolicyGate(cfg)

	// 空 staffID 不匹配任何策略（不是管理员、不在黑名单、不在白名单）
	msg := &types.IncomingMessage{
		ConversationType: types.ConversationTypeDM,
		SenderStaffID:    "", // 空
		SenderID:         "user123",
	}
	decision := pg.Evaluate(msg)
	// 因为设置了 AllowFrom，空 staffID 不在名单内，应被拒绝
	if decision.Allowed {
		t.Error("expected empty staffID to be rejected when AllowFrom is set")
	}
	if decision.Reason != types.RejectReasonSenderNotAllowed {
		t.Errorf("expected reason sender_not_allowed, got %s", decision.Reason)
	}
}
