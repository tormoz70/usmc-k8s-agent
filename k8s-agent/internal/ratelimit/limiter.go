package ratelimit

import (
	"context"
	"time"

	"golang.org/x/time/rate"
)

type Limiter struct {
	limiter *rate.Limiter
}

func New(requestsPerSecond float64, burst int) *Limiter {
	if requestsPerSecond <= 0 {
		requestsPerSecond = 50
	}
	if burst <= 0 {
		burst = 100
	}
	return &Limiter{limiter: rate.NewLimiter(rate.Limit(requestsPerSecond), burst)}
}

func (l *Limiter) Wait(ctx context.Context) error {
	if l == nil || l.limiter == nil {
		return nil
	}
	return l.limiter.Wait(ctx)
}

func (l *Limiter) Reserve() *rate.Reservation {
	if l == nil || l.limiter == nil {
		return rate.NewLimiter(rate.Inf, 1).Reserve()
	}
	return l.limiter.Reserve()
}

func (l *Limiter) Delay() time.Duration {
	if l == nil || l.limiter == nil {
		return 0
	}
	r := l.limiter.Reserve()
	return r.Delay()
}
