// Package safety 提供入站安全与稳态组件：策略门控、双层去重、处理锁、
// per-chat 串行队列、消息批处理与 SSRF 防护。
package safety

import (
	"sync"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// PolicyGate 消息策略门控（增强版）
type PolicyGate struct {
	mu  sync.RWMutex
	cfg types.PolicyConfig
	bot *types.BotIdentity // 机器人身份（用于自回复过滤等）
}

func NewPolicyGate(cfg types.PolicyConfig) *PolicyGate {
	return &PolicyGate{cfg: cfg}
}

// NewPolicyGateWithBot 创建带机器人身份的策略门控
func NewPolicyGateWithBot(cfg types.PolicyConfig, bot *types.BotIdentity) *PolicyGate {
	return &PolicyGate{cfg: cfg, bot: bot}
}

// Evaluate 评估消息是否允许通过
func (pg *PolicyGate) Evaluate(msg *types.IncomingMessage) types.PolicyDecision {
	pg.mu.RLock()
	defer pg.mu.RUnlock()

	// 1. 管理员绕过（最高优先级）
	if pg.isAdmin(msg.SenderStaffID) {
		return types.PolicyDecision{Allowed: true}
	}

	// 2. 全局发送者黑名单（DenyFrom）
	if pg.isDenied(msg.SenderStaffID) {
		return types.PolicyDecision{Allowed: false, Reason: types.RejectReasonSenderDenied}
	}

	// 3. 全局发送者白名单（AllowFrom）- 如果设置了白名单，必须在名单内
	if len(pg.cfg.AllowFrom) > 0 && !pg.isInAllowFrom(msg.SenderStaffID) {
		return types.PolicyDecision{Allowed: false, Reason: types.RejectReasonSenderNotAllowed}
	}

	// 4. 按会话类型评估
	if msg.ConversationType == types.ConversationTypeGroup {
		return pg.evaluateGroup(msg)
	}
	return pg.evaluateDM(msg)
}

// isAdmin 检查是否为管理员
func (pg *PolicyGate) isAdmin(staffID string) bool {
	if staffID == "" {
		return false
	}
	for _, admin := range pg.cfg.Admins {
		if admin == staffID {
			return true
		}
	}
	return false
}

// isDenied 检查是否在全局黑名单
func (pg *PolicyGate) isDenied(staffID string) bool {
	if staffID == "" {
		return false
	}
	for _, id := range pg.cfg.DenyFrom {
		if id == staffID {
			return true
		}
	}
	return false
}

// isInAllowFrom 检查是否在全局白名单
func (pg *PolicyGate) isInAllowFrom(staffID string) bool {
	if staffID == "" {
		return false
	}
	for _, id := range pg.cfg.AllowFrom {
		if id == staffID {
			return true
		}
	}
	return false
}

func (pg *PolicyGate) evaluateGroup(msg *types.IncomingMessage) types.PolicyDecision {
	// 黑名单检查（最高优先级，群覆盖不可豁免）
	for _, id := range pg.cfg.GroupBlocklist {
		if id == msg.ConversationID {
			return types.PolicyDecision{Allowed: false, Reason: types.RejectReasonGroupBlocked}
		}
	}

	// 群覆盖（显式条目可在白名单模式下放行该群）
	ov, hasOverride := pg.cfg.GroupOverrides[msg.ConversationID]

	// 白名单检查：全局白名单命中，或存在显式群条目
	if len(pg.cfg.GroupAllowlist) > 0 {
		found := false
		for _, id := range pg.cfg.GroupAllowlist {
			if id == msg.ConversationID {
				found = true
				break
			}
		}
		if !found && !hasOverride {
			return types.PolicyDecision{Allowed: false, Reason: types.RejectReasonGroupNotAllowed}
		}
	}

	if hasOverride {
		if ov.Enabled != nil && !*ov.Enabled {
			return types.PolicyDecision{Allowed: false, Reason: types.RejectReasonGroupDisabled}
		}
	}

	// @机器人检查（群覆盖优先）
	requireMention := true
	if hasOverride && ov.RequireMention != nil {
		requireMention = *ov.RequireMention
	} else if pg.cfg.RequireMention != nil {
		requireMention = *pg.cfg.RequireMention
	}
	if requireMention && !msg.IsInAtList {
		return types.PolicyDecision{Allowed: false, Reason: types.RejectReasonNoMention}
	}

	// 群内发送者黑名单（先于白名单）
	if hasOverride {
		for _, id := range ov.BlockFrom {
			if id == msg.SenderID {
				return types.PolicyDecision{Allowed: false, Reason: types.RejectReasonSenderBlocked}
			}
		}
	}

	// 群内发送者白名单
	if hasOverride && len(ov.AllowFrom) > 0 {
		found := false
		for _, id := range ov.AllowFrom {
			if id == msg.SenderID {
				found = true
				break
			}
		}
		if !found {
			return types.PolicyDecision{Allowed: false, Reason: types.RejectReasonSenderNotAllowed}
		}
	}

	// @所有人检查
	respondToMentionAll := false
	if hasOverride && ov.RespondToMentionAll != nil {
		respondToMentionAll = *ov.RespondToMentionAll
	} else if pg.cfg.RespondToMentionAll != nil {
		respondToMentionAll = *pg.cfg.RespondToMentionAll
	}
	// 如果消息包含 @all 且配置不响应 @all，拒绝
	if msg.MentionAll && !respondToMentionAll {
		return types.PolicyDecision{Allowed: false, Reason: types.RejectReasonMentionAll}
	}

	return types.PolicyDecision{Allowed: true}
}

func (pg *PolicyGate) evaluateDM(msg *types.IncomingMessage) types.PolicyDecision {
	mode := pg.cfg.DMMode
	if mode == "" {
		mode = "open"
	}

	switch mode {
	case "disabled":
		return types.PolicyDecision{Allowed: false, Reason: types.RejectReasonDMDisabled}
	case "allowlist":
		found := false
		for _, id := range pg.cfg.DMAllowlist {
			if id == msg.SenderID {
				found = true
				break
			}
		}
		if !found {
			return types.PolicyDecision{Allowed: false, Reason: types.RejectReasonDMNotAllowed}
		}
	case "blocklist":
		for _, id := range pg.cfg.DMBlocklist {
			if id == msg.SenderID {
				return types.PolicyDecision{Allowed: false, Reason: types.RejectReasonDMBlocked}
			}
		}
	}

	return types.PolicyDecision{Allowed: true}
}

// UpdateConfig 更新策略配置
func (pg *PolicyGate) UpdateConfig(cfg types.PolicyConfig) {
	pg.mu.Lock()
	defer pg.mu.Unlock()
	pg.cfg = cfg
}

// GetConfig 获取当前策略配置
func (pg *PolicyGate) GetConfig() types.PolicyConfig {
	pg.mu.RLock()
	defer pg.mu.RUnlock()
	return pg.cfg
}

// SetBotIdentity 设置机器人身份
func (pg *PolicyGate) SetBotIdentity(bot *types.BotIdentity) {
	pg.mu.Lock()
	defer pg.mu.Unlock()
	pg.bot = bot
}

// GetBotIdentity 获取机器人身份
func (pg *PolicyGate) GetBotIdentity() *types.BotIdentity {
	pg.mu.RLock()
	defer pg.mu.RUnlock()
	return pg.bot
}
