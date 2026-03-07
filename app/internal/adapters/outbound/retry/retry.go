package retry

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

type Backoff struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
}

func DefaultBackoff() Backoff {
	return Backoff{
		InitialDelay: 1 * time.Second,
		MaxDelay:     30 * time.Second,
		Multiplier:   2,
	}
}

func (b Backoff) Duration(attempt int) time.Duration {
	if b.InitialDelay <= 0 {
		b.InitialDelay = time.Second
	}
	if b.MaxDelay <= 0 {
		b.MaxDelay = 30 * time.Second
	}
	if b.Multiplier <= 1 {
		b.Multiplier = 2
	}

	delay := b.InitialDelay
	for i := 1; i < attempt; i++ {
		next := time.Duration(float64(delay) * b.Multiplier)
		if next > b.MaxDelay {
			return b.MaxDelay
		}
		delay = next
	}

	if delay > b.MaxDelay {
		return b.MaxDelay
	}
	return delay
}

func Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func DialWebSocketWithRetry(ctx context.Context, dialer *websocket.Dialer, wsURL, logPrefix string, backoff Backoff) (*websocket.Conn, error) {
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}

	attempt := 1
	for {
		conn, resp, err := dialer.DialContext(ctx, wsURL, nil)
		if err == nil {
			if attempt > 1 {
				log.Printf("%s websocket connected after %d attempts", logPrefix, attempt)
			}
			return conn, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		status := 0
		if resp != nil {
			status = resp.StatusCode
		}

		delay := backoff.Duration(attempt)
		log.Printf("%s websocket dial attempt=%d status=%d err=%v retry_in=%s", logPrefix, attempt, status, err, delay)

		if err := Wait(ctx, delay); err != nil {
			return nil, err
		}

		attempt++
	}
}

func RunWebSocketSessionWithReconnect(
	ctx context.Context,
	wsURL string,
	dialLogPrefix string,
	dialErrPrefix string,
	retryLogPrefix string,
	backoff Backoff,
	runSession func(conn *websocket.Conn) error,
) error {
	reconnectAttempt := 1

	for {
		if ctx.Err() != nil {
			return nil
		}

		conn, err := DialWebSocketWithRetry(ctx, websocket.DefaultDialer, wsURL, dialLogPrefix, backoff)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("%s: %w", dialErrPrefix, err)
		}

		reconnectAttempt = 1
		err = runSession(conn)
		_ = conn.Close()
		if err == nil || ctx.Err() != nil {
			return nil
		}

		delay := backoff.Duration(reconnectAttempt)
		log.Printf("%s: %v; reconnecting in %s", retryLogPrefix, err, delay)
		if err := Wait(ctx, delay); err != nil {
			return nil
		}
		reconnectAttempt++
	}
}

func DoHTTPRequestWithRetry(
	ctx context.Context,
	httpClient *http.Client,
	httpBackoff Backoff,
	httpMaxAttempts int,
	method string,
	url string,
) (*http.Response, error) {
	var lastErr error

	for attempt := 1; attempt <= httpMaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		req, err := http.NewRequestWithContext(ctx, method, url, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		resp, err := httpClient.Do(req)
		if err == nil {
			if IsRetriableHTTPStatus(resp.StatusCode) {
				lastErr = fmt.Errorf("unexpected retriable status: %d", resp.StatusCode)
				_ = resp.Body.Close()
			} else {
				return resp, nil
			}
		} else {
			lastErr = fmt.Errorf("request failed: %w", err)
		}

		if attempt == httpMaxAttempts {
			break
		}

		delay := httpBackoff.Duration(attempt)
		log.Printf("http request attempt=%d failed err=%v retry_in=%s", attempt, lastErr, delay)
		if err := Wait(ctx, delay); err != nil {
			return nil, err
		}
	}

	return nil, ExhaustedError("request", httpMaxAttempts, lastErr)
}

func IsRetriableHTTPStatus(statusCode int) bool {
	if statusCode >= http.StatusInternalServerError {
		return true
	}

	switch statusCode {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func ExhaustedError(prefix string, attempts int, lastErr error) error {
	if lastErr == nil {
		return fmt.Errorf("%s failed after %d attempts", prefix, attempts)
	}
	return fmt.Errorf("%s failed after %d attempts: %w", prefix, attempts, lastErr)
}
