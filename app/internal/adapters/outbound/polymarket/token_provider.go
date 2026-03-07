package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/polytradings/data-ingestion/internal/adapters/outbound/retry"
	"github.com/polytradings/data-ingestion/internal/domain"
)

type PolymarketTokenProvider struct {
	httpClient         *http.Client
	marketLookupAPIURL string
	httpBackoff        retry.Backoff
	httpMaxAttempts    int
}

func NewPolymarketTokenProvider(marketLookupAPIURL string, httpBackoff retry.Backoff, httpMaxAttempts int) *PolymarketTokenProvider {
	lookupURL := strings.TrimSpace(marketLookupAPIURL)
	if lookupURL == "" {
		lookupURL = "https://gamma-api.polymarket.com/markets"
	}
	if httpBackoff.InitialDelay <= 0 || httpBackoff.MaxDelay <= 0 || httpBackoff.Multiplier <= 1 {
		httpBackoff = retry.Backoff{InitialDelay: 500 * time.Millisecond, MaxDelay: 5 * time.Second, Multiplier: 2}
	}
	if httpMaxAttempts <= 0 {
		httpMaxAttempts = 5
	}

	return &PolymarketTokenProvider{
		httpClient:         &http.Client{Timeout: 8 * time.Second},
		marketLookupAPIURL: strings.TrimSuffix(lookupURL, "/"),
		httpBackoff:        httpBackoff,
		httpMaxAttempts:    httpMaxAttempts,
	}
}

func (p *PolymarketTokenProvider) LookupMarketBySlug(ctx context.Context, slug string) (domain.ActiveMarket, bool, error) {
	url := fmt.Sprintf("%s?slug=%s", p.marketLookupAPIURL, slug)

	resp, err := doRequestWithRetry(ctx, p.httpClient, p.httpBackoff, p.httpMaxAttempts, http.MethodGet, url)
	if err != nil {
		return domain.ActiveMarket{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return domain.ActiveMarket{}, false, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	var rows []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return domain.ActiveMarket{}, false, fmt.Errorf("decode response: %w", err)
	}
	if len(rows) == 0 {
		return domain.ActiveMarket{}, false, nil
	}

	first := rows[0]
	marketID := readString(first, "slug", "market_slug")
	if marketID == "" {
		marketID = slug
	}
	conditionID := readString(first, "conditionId", "condition_id")
	closed := readBool(first, "closed")
	tokenIDs := readTokenIDs(first, "clobTokenIds", "clob_token_ids")

	return domain.ActiveMarket{
		MarketID:    marketID,
		ConditionID: conditionID,
		UpTokenID:   tokenAt(tokenIDs, 0),
		DownTokenID: tokenAt(tokenIDs, 1),
		Closed:      closed,
	}, true, nil
}

func readString(body map[string]any, keys ...string) string {
	for _, key := range keys {
		if v, ok := body[key]; ok {
			s := strings.TrimSpace(fmt.Sprintf("%v", v))
			if s != "" {
				return s
			}
		}
	}
	return ""
}

func readBool(body map[string]any, keys ...string) bool {
	for _, key := range keys {
		value, ok := body[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case bool:
			return typed
		default:
			s := strings.ToLower(strings.TrimSpace(fmt.Sprintf("%v", typed)))
			return s == "true" || s == "1"
		}
	}
	return false
}

func readTokenIDs(body map[string]any, keys ...string) []string {
	for _, key := range keys {
		value, ok := body[key]
		if !ok {
			continue
		}
		switch typed := value.(type) {
		case []any:
			out := make([]string, 0, len(typed))
			for _, raw := range typed {
				s := strings.TrimSpace(fmt.Sprintf("%v", raw))
				if s != "" {
					out = append(out, s)
				}
			}
			return out
		case []string:
			out := make([]string, 0, len(typed))
			for _, raw := range typed {
				s := strings.TrimSpace(raw)
				if s != "" {
					out = append(out, s)
				}
			}
			return out
		case string:
			payload := strings.TrimSpace(typed)
			if payload == "" {
				return nil
			}
			var parsed []string
			if err := json.Unmarshal([]byte(payload), &parsed); err == nil {
				return parsed
			}
		}
	}
	return nil
}

func tokenAt(values []string, idx int) string {
	if idx < 0 || idx >= len(values) {
		return ""
	}
	return strings.TrimSpace(values[idx])
}
