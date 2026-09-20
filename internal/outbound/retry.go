package outbound

import (
	"context"
	"math"
	"time"

	"github.com/DingTalk-Real-AI/dingtalk-channel-sdk-go/types"
)

// RetryOptions configures retry behavior。
type RetryOptions struct {
	MaxAttempts int
	BaseDelay   time.Duration
}

// DefaultRetryOptions provides sensible defaults.
var DefaultRetryOptions = RetryOptions{
	MaxAttempts: 3,
	BaseDelay:   500 * time.Millisecond,
}

// Retry executes an operation with exponential backoff.
// Only retries errors classified as retryable 。
func Retry(ctx context.Context, op func(attempt int) error, opts *RetryOptions) error {
	if opts == nil {
		opts = &DefaultRetryOptions
	}
	max := opts.MaxAttempts
	if max <= 0 {
		max = 3
	}
	base := opts.BaseDelay
	if base <= 0 {
		base = 500 * time.Millisecond
	}
	var lastErr error
	for attempt := 1; attempt <= max; attempt++ {
		err := op(attempt)
		if err == nil {
			return nil
		}
		classified := types.ClassifyError(err)
		lastErr = classified
		if attempt >= max || !types.IsRetryable(classified) {
			return lastErr
		}
		delay := time.Duration(float64(base) * math.Pow(3, float64(attempt-1)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return lastErr
}
