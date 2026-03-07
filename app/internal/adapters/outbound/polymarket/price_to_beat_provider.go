package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/polytradings/data-ingestion/internal/adapters/outbound/retry"
)

type PriceToBeatProvider struct {
	httpClient      *http.Client
	bootstrapAPIURL string
	httpBackoff     retry.Backoff
	httpMaxAttempts int
}

func NewPriceToBeatProvider(bootstrapAPIURL string, httpBackoff retry.Backoff, httpMaxAttempts int) *PriceToBeatProvider {
	lookupURL := strings.TrimSpace(bootstrapAPIURL)
	if lookupURL == "" {
		lookupURL = "https://gamma-api.polymarket.com/markets"
	}
	if httpBackoff.InitialDelay <= 0 || httpBackoff.MaxDelay <= 0 || httpBackoff.Multiplier <= 1 {
		httpBackoff = retry.Backoff{InitialDelay: 500 * time.Millisecond, MaxDelay: 5 * time.Second, Multiplier: 2}
	}
	if httpMaxAttempts <= 0 {
		httpMaxAttempts = 5
	}

	return &PriceToBeatProvider{
		httpClient:      &http.Client{Timeout: 8 * time.Second},
		bootstrapAPIURL: strings.TrimSuffix(lookupURL, "/"),
		httpBackoff:     httpBackoff,
		httpMaxAttempts: httpMaxAttempts,
	}
}

func (p *PriceToBeatProvider) LookupReferencePrice(ctx context.Context, marketID string) (float64, bool, error) {
	url := fmt.Sprintf("%s?slug=%s", p.bootstrapAPIURL, marketID)
	resp, err := retry.DoHTTPRequestWithRetry(ctx, p.httpClient, p.httpBackoff, p.httpMaxAttempts, http.MethodGet, url)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, false, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var rows []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return 0, false, fmt.Errorf("decode response: %w", err)
	}
	if len(rows) == 0 {
		return 0, false, nil
	}

	first := rows[0]
	for _, key := range []string{
		"priceToBeat",
		"price_to_beat",
		"resolutionValue",
		"resolution_value",
		"final_value",
		"finalValue",
		"strike",
		"strikePrice",
		"strike_price",
	} {
		if value, ok := valueAsFloat64(first, key); ok && value > 0 {
			return value, true, nil
		}
	}

	return 0, false, nil
}
