package outbound

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

func fastOpts() *RetryOptions {
	return &RetryOptions{MaxAttempts: 3, BaseDelay: time.Millisecond}
}

func retryableErr() error {
	return &types.ChannelError{Code: types.ErrCodeRateLimited, Message: "rate limited"}
}

func fatalErr() error {
	return &types.ChannelError{Code: types.ErrCodeFormatError, Message: "bad payload"}
}

// TestRetrySucceedsFirstAttempt 无需重试时应只执行一次。
func TestRetrySucceedsFirstAttempt(t *testing.T) {
	attempts := 0
	err := Retry(context.Background(), func(attempt int) error {
		attempts++
		return nil
	}, fastOpts())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

// TestRetryRetriesUntilSuccess 可重试错误应指数退避后重试直至成功。
func TestRetryRetriesUntilSuccess(t *testing.T) {
	attempts := 0
	err := Retry(context.Background(), func(attempt int) error {
		attempts++
		if attempts < 3 {
			return retryableErr()
		}
		return nil
	}, fastOpts())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

// TestRetryStopsOnNonRetryable 不可重试错误应立即返回，不再尝试。
func TestRetryStopsOnNonRetryable(t *testing.T) {
	attempts := 0
	err := Retry(context.Background(), func(attempt int) error {
		attempts++
		return fatalErr()
	}, fastOpts())
	if err == nil {
		t.Fatal("want error")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1（format_error 不重试）", attempts)
	}
	if !errors.Is(err, fatalErr()) && err.Error() != fatalErr().Error() {
		t.Fatalf("want classified ChannelError, got %v", err)
	}
}

// TestRetryExhaustsAttempts 持续可重试错误应在 MaxAttempts 次后放弃并返回最后错误。
func TestRetryExhaustsAttempts(t *testing.T) {
	attempts := 0
	err := Retry(context.Background(), func(attempt int) error {
		attempts++
		return retryableErr()
	}, fastOpts())
	if err == nil {
		t.Fatal("want error after exhaustion")
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3（MaxAttempts）", attempts)
	}
	ce := types.ClassifyError(err)
	if ce.Code != types.ErrCodeRateLimited {
		t.Fatalf("last error code = %s, want rate_limited", ce.Code)
	}
}

// TestRetryContextCancel 退避等待期间 ctx 取消应立即返回 ctx 错误。
func TestRetryContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	err := Retry(ctx, func(attempt int) error {
		attempts++
		cancel() // 在第一次失败后的退避等待中取消
		return retryableErr()
	}, &RetryOptions{MaxAttempts: 3, BaseDelay: 200 * time.Millisecond})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}

// TestRetryDefaults nil 选项应回退默认值（3 次尝试）。
func TestRetryDefaults(t *testing.T) {
	attempts := 0
	_ = Retry(context.Background(), func(attempt int) error {
		attempts++
		return fatalErr() // 非 retryable 立即停止
	}, nil)
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}
