package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/polytradings/data-ingestion/internal/adapters/outbound/retry"
	"github.com/polytradings/data-ingestion/internal/domain"
)

type Feed struct {
	wsURL   string
	backoff retry.Backoff
}

func NewFeed(wsURL string, backoff retry.Backoff) *Feed {
	if wsURL == "" {
		wsURL = "wss://ws-live-data.polymarket.com"
	}
	if backoff.InitialDelay <= 0 || backoff.MaxDelay <= 0 || backoff.Multiplier <= 1 {
		backoff = retry.DefaultBackoff()
	}
	return &Feed{
		wsURL:   wsURL,
		backoff: backoff,
	}
}

func (f *Feed) Stream(ctx context.Context, symbols []string) (<-chan domain.PriceTick, <-chan error) {
	prices := make(chan domain.PriceTick, 1024)
	errs := make(chan error, 1)
	allowedSymbols := buildAllowedSymbols(symbols)

	go func() {
		defer close(prices)
		defer close(errs)

		err := retry.RunWebSocketSessionWithReconnect(
			ctx,
			f.wsURL,
			"polymarket-crypto",
			"polymarket dial failed",
			"polymarket read/ping failed",
			f.backoff,
			func(conn *websocket.Conn) error {
				subscriptionMsg := buildCryptoPricesChainlinkSubscription()
				if err := conn.WriteMessage(websocket.TextMessage, []byte(subscriptionMsg)); err != nil {
					return fmt.Errorf("polymarket subscribe failed: %w", err)
				}
				return streamPolymarketMessages(ctx, conn, prices, allowedSymbols)
			},
		)
		if err != nil && ctx.Err() == nil {
			errs <- err
		}
	}()

	return prices, errs
}

func streamPolymarketMessages(ctx context.Context, conn *websocket.Conn, prices chan<- domain.PriceTick, allowedSymbols map[string]struct{}) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
				return fmt.Errorf("polymarket ping failed: %w", err)
			}
		default:
		}

		_, payload, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("polymarket read failed: %w", err)
		}

		if string(payload) == "pong" || string(payload) == "PING" || string(payload) == "ping" {
			continue
		}

		tick, ok := parsePriceMessage(payload)
		if !ok {
			continue
		}
		if len(allowedSymbols) > 0 {
			if _, found := allowedSymbols[strings.ToUpper(strings.TrimSpace(tick.Symbol))]; !found {
				continue
			}
		}
		prices <- tick
	}
}

func buildCryptoPricesChainlinkSubscription() string {
	subscriptions := []map[string]any{
		{
			"topic":   "crypto_prices_chainlink",
			"type":    "*",
			"filters": "",
		},
	}

	msg := map[string]any{
		"action":        "subscribe",
		"subscriptions": subscriptions,
	}

	data, _ := json.Marshal(msg)
	return string(data)
}

func buildAllowedSymbols(symbols []string) map[string]struct{} {
	if len(symbols) == 0 {
		return nil
	}

	allowed := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		normalized := normalizeBaseSymbol(symbol)
		if normalized == "" {
			continue
		}
		allowed[normalized] = struct{}{}
	}

	return allowed
}

func parsePriceMessage(payload []byte) (domain.PriceTick, bool) {
	if len(strings.TrimSpace(string(payload))) == 0 {
		return domain.PriceTick{}, false
	}

	var message map[string]any

	if err := json.Unmarshal(payload, &message); err != nil {
		log.Printf("payload = %s, error = %v", string(payload), err)
		return domain.PriceTick{}, false
	}

	// Check if this is a valid chainlink crypto price message
	topic, ok := message["topic"].(string)
	if !ok || topic != "crypto_prices_chainlink" {
		return domain.PriceTick{}, false
	}

	// Extract payload object
	payloadObj, ok := message["payload"].(map[string]any)
	if !ok {
		return domain.PriceTick{}, false
	}

	// Extract symbol
	symbol, ok := valueAsString(payloadObj, "symbol")
	if !ok || symbol == "" {
		return domain.PriceTick{}, false
	}

	// Extract price value
	price, ok := valueAsFloat64(payloadObj, "value")
	if !ok {
		price, ok = extractPriceFromDataSnapshot(payloadObj["data"])
		if !ok {
			return domain.PriceTick{}, false
		}
	}

	// Extract timestamp from payload or use current time
	ts, hasTimestamp := valueAsInt64(payloadObj, "timestamp")
	timestamp := time.Now()
	if hasTimestamp && ts > 0 {
		timestamp = time.UnixMilli(ts)
	}

	symbolOutput := normalizeBaseSymbol(symbol)
	if symbolOutput == "" {
		return domain.PriceTick{}, false
	}

	return domain.PriceTick{
		Source:         "polymarket",
		InstrumentType: domain.InstrumentTypeCrypto,
		Symbol:         symbolOutput,
		Price:          price,
		Timestamp:      timestamp,
	}, true
}

func normalizeBaseSymbol(symbol string) string {
	value := strings.ToUpper(strings.TrimSpace(symbol))
	if value == "" {
		return ""
	}

	if idx := strings.Index(value, "/"); idx > 0 {
		return strings.TrimSpace(value[:idx])
	}

	for _, suffix := range []string{"USDT", "USDC", "USD", "PERP"} {
		if strings.HasSuffix(value, suffix) && len(value) > len(suffix) {
			return strings.TrimSpace(value[:len(value)-len(suffix)])
		}
	}

	return value
}

func extractPriceFromDataSnapshot(data any) (float64, bool) {
	frames, ok := data.([]any)
	if !ok || len(frames) == 0 {
		return 0, false
	}

	for i := len(frames) - 1; i >= 0; i-- {
		frame, ok := frames[i].(map[string]any)
		if !ok {
			continue
		}
		value, ok := valueAsFloat64(frame, "value")
		if ok {
			return value, true
		}
	}

	return 0, false
}
