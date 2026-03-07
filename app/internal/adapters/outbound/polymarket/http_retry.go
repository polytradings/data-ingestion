package polymarket

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/polytradings/data-ingestion/internal/adapters/outbound/retry"
)

func doRequestWithRetry(
	ctx context.Context,
	httpClient *http.Client,
	httpBackoff retry.Backoff,
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
			if retry.IsRetriableHTTPStatus(resp.StatusCode) {
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
		if err := retry.Wait(ctx, delay); err != nil {
			return nil, err
		}
	}

	return nil, retry.ExhaustedError("request", httpMaxAttempts, lastErr)
}
