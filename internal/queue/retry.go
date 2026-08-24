package queue

import (
	"math"
	"time"

	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/models"
)

// NextRetryDelay computes how long to wait before the next attempt.
// attemptNumber is the attempt that JUST failed (1-indexed).
func NextRetryDelay(strategy models.RetryStrategy, baseDelaySeconds int, attemptNumber int) time.Duration {
	const maxDelay = 1 * time.Hour // cap so exponential can't run away forever

	var delay time.Duration
	switch strategy {
	case models.RetryFixed:
		delay = time.Duration(baseDelaySeconds) * time.Second

	case models.RetryLinear:
		// delay grows by one base unit each attempt: base, 2*base, 3*base...
		delay = time.Duration(baseDelaySeconds*attemptNumber) * time.Second

	case models.RetryExponential:
		// classic doubling: base * 2^(attempt-1) -> base, 2*base, 4*base, 8*base...
		multiplier := math.Pow(2, float64(attemptNumber-1))
		delay = time.Duration(float64(baseDelaySeconds)*multiplier) * time.Second

	default: // "none" or unknown -> no retry, caller should treat as immediate DLQ
		return 0
	}

	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}

// ShouldRetry decides whether another attempt is allowed.
func ShouldRetry(strategy models.RetryStrategy, attemptCount, maxRetries int) bool {
	if strategy == models.RetryNone {
		return false
	}
	return attemptCount < maxRetries
}
