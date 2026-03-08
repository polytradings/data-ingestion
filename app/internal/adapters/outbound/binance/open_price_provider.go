package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/polytradings/data-ingestion/internal/adapters/outbound/retry"
)

type OpenPriceProvider struct {
	baseURL         string
	quoteSymbol     string
	httpClient      *http.Client
	httpBackoff     retry.Backoff
	httpMaxAttempts int
}

func NewOpenPriceProvider(baseURL, quoteSymbol string, httpBackoff retry.Backoff, httpMaxAttempts int) *OpenPriceProvider {
	return &OpenPriceProvider{
		baseURL:         strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		quoteSymbol:     strings.ToUpper(strings.TrimSpace(quoteSymbol)),
		httpClient:      &http.Client{Timeout: 10 * time.Second},
		httpBackoff:     httpBackoff,
		httpMaxAttempts: httpMaxAttempts,
	}
}

func (p *OpenPriceProvider) LookupOpenPrice(ctx context.Context, symbol string, slotStart time.Time) (float64, bool, error) {
	baseSymbol := strings.ToUpper(strings.TrimSpace(symbol))
	if baseSymbol == "" {
		return 0, false, fmt.Errorf("symbol is required")
	}
	if p.quoteSymbol == "" {
		p.quoteSymbol = "USDT"
	}

	start := slotStart.UTC().Truncate(time.Minute)
	end := start.Add(time.Minute)

	query := url.Values{}
	query.Set("symbol", baseSymbol+p.quoteSymbol)
	query.Set("interval", "1m")
	query.Set("startTime", strconv.FormatInt(start.UnixMilli(), 10))
	query.Set("endTime", strconv.FormatInt(end.UnixMilli(), 10))
	query.Set("limit", "1")

	endpoint := p.baseURL + "/api/v3/klines?" + query.Encode()
	resp, err := retry.DoHTTPRequestWithRetry(ctx, p.httpClient, p.httpBackoff, p.httpMaxAttempts, http.MethodGet, endpoint)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()

	var payload [][]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, false, fmt.Errorf("decode binance klines: %w", err)
	}
	if len(payload) == 0 || len(payload[0]) < 2 {
		return 0, false, nil
	}

	rawOpen, ok := payload[0][1].(string)
	if !ok {
		return 0, false, fmt.Errorf("unexpected open price type %T", payload[0][1])
	}
	openPrice, err := strconv.ParseFloat(rawOpen, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse open price: %w", err)
	}
	if openPrice <= 0 {
		return 0, false, nil
	}
	return openPrice, true, nil
}
