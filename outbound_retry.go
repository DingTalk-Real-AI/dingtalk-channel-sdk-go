package channel

import (
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/internal/outbound"
)

// retryOptions 由 OutboundConfig 推导重试参数；未配置时使用默认（3 次 / 500ms 起步指数退避）。
func retryOptions(cfg *Config) *outbound.RetryOptions {
	if cfg != nil && cfg.Outbound != nil && cfg.Outbound.Retry.MaxAttempts > 0 {
		base := time.Duration(cfg.Outbound.Retry.BaseDelayMs) * time.Millisecond
		if base <= 0 {
			base = outbound.DefaultRetryOptions.BaseDelay
		}
		return &outbound.RetryOptions{MaxAttempts: cfg.Outbound.Retry.MaxAttempts, BaseDelay: base}
	}
	return &outbound.DefaultRetryOptions
}
