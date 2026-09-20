// Package channel 提供与 Agent runtime 解耦的钉钉会话接入层：
// Stream 长连接、事件去重、回复发送、AI 卡片流式输出与限流。
//
// 实现按四层拆分：types（共享类型）、internal/normalize（入站归一化）、
// internal/safety（策略/去重/队列等稳态组件）、internal/outbound（重试/分片）。
// 本文件将拆出的符号以别名形式透出，根包代码与既有调用方无需改动。
package channel

import (
	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/internal/normalize"
	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/internal/outbound"
	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/internal/safety"
	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// ── 类型别名（types 包）──

type (
	AtUser          = types.AtUser
	Resource        = types.Resource
	Mention         = types.Mention
	IncomingMessage = types.IncomingMessage
	CardAction      = types.CardAction

	PolicyConfig   = types.PolicyConfig
	GroupOverride  = types.GroupOverride
	PolicyDecision = types.PolicyDecision
	RejectReason   = types.RejectReason
	RejectEvent    = types.RejectEvent

	BatchConfig    = types.BatchConfig
	BatchedMessage = types.BatchedMessage
	BatchHandler   = types.BatchHandler

	ChatQueueConfig = types.ChatQueueConfig
	FlushHandler    = types.FlushHandler
	BatchedDispatch = types.BatchedDispatch

	MediaBatchConfig = types.MediaBatchConfig
	RetryConfig      = types.RetryConfig
	BeforeSendHook   = types.BeforeSendHook
	AfterSendHook    = types.AfterSendHook
	OutboundConfig   = types.OutboundConfig

	BotIdentity = types.BotIdentity
	DedupConfig = types.DedupConfig

	ErrorCode    = types.ErrorCode
	ChannelError = types.ChannelError

	RetryOptions = outbound.RetryOptions
)

// ── 常量别名 ──

const (
	ConversationTypeDM    = types.ConversationTypeDM
	ConversationTypeGroup = types.ConversationTypeGroup

	DefaultDedupTTL = types.DefaultDedupTTL

	RejectReasonGroupNotAllowed  = types.RejectReasonGroupNotAllowed
	RejectReasonGroupBlocked     = types.RejectReasonGroupBlocked
	RejectReasonGroupDisabled    = types.RejectReasonGroupDisabled
	RejectReasonNoMention        = types.RejectReasonNoMention
	RejectReasonMentionAll       = types.RejectReasonMentionAll
	RejectReasonDMDisabled       = types.RejectReasonDMDisabled
	RejectReasonDMNotAllowed     = types.RejectReasonDMNotAllowed
	RejectReasonDMBlocked        = types.RejectReasonDMBlocked
	RejectReasonSenderNotAllowed = types.RejectReasonSenderNotAllowed
	RejectReasonSenderBlocked    = types.RejectReasonSenderBlocked

	ErrCodeTargetRevoked    = types.ErrCodeTargetRevoked
	ErrCodePermissionDenied = types.ErrCodePermissionDenied
	ErrCodeFormatError      = types.ErrCodeFormatError
	ErrCodeRateLimited      = types.ErrCodeRateLimited
	ErrCodeSendTimeout      = types.ErrCodeSendTimeout
	ErrCodeQpsLimited       = types.ErrCodeQpsLimited
	ErrCodeSSRFBlocked      = types.ErrCodeSSRFBlocked
	ErrCodeUnknown          = types.ErrCodeUnknown
)

// ── 函数别名（types / safety / outbound / normalize）──

var (
	DefaultBatchConfig     = types.DefaultBatchConfig
	DefaultChatQueueConfig = types.DefaultChatQueueConfig

	ClassifyError     = types.ClassifyError
	IsRetryable       = types.IsRetryable
	IsReplyTargetGone = types.IsReplyTargetGone
	IsFormatError     = types.IsFormatError

	AssertPublicURL              = safety.AssertPublicURL
	AssertPublicURLWithAllowlist = safety.AssertPublicURLWithAllowlist

	DefaultRetryOptions = outbound.DefaultRetryOptions
	Retry               = outbound.Retry
	SplitWithCodeFences = outbound.SplitWithCodeFences

	// 内部函数别名：根包与测试继续使用原裸名。
	normalizeIncoming = normalize.NormalizeIncoming
	normalizeForCard  = outbound.NormalizeForCard
)
