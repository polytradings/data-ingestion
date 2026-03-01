package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/polytradings/data-ingestion/internal/adapters/outbound/retry"
	"github.com/polytradings/data-ingestion/internal/domain"
)

type MarketFeed struct {
	wsURL   string
	backoff retry.Backoff
}

func NewMarketFeed(wsURL string, backoff retry.Backoff) *MarketFeed {
	if backoff.InitialDelay <= 0 || backoff.MaxDelay <= 0 || backoff.Multiplier <= 1 {
		backoff = retry.DefaultBackoff()
	}
	return &MarketFeed{wsURL: strings.TrimSpace(wsURL), backoff: backoff}
}

func (f *MarketFeed) Stream(ctx context.Context, tokenIDs []string) (<-chan domain.TokenPriceUpdate, <-chan error) {
	updates := make(chan domain.TokenPriceUpdate, 1024)
	errs := make(chan error, 1)

	go func() {
		defer close(updates)
		defer close(errs)

		if len(tokenIDs) == 0 {
			errs <- nil
			return
		}

		payload := map[string]any{
			"type":       "market",
			"assets_ids": tokenIDs,
		}
		rawPayload, _ := json.Marshal(payload)

		backoff := f.backoff
		reconnectAttempt := 1

		for {
			if ctx.Err() != nil {
				return
			}

			conn, err := retry.DialWebSocketWithRetry(ctx, websocket.DefaultDialer, f.wsURL, "polymarket-market", backoff)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				errs <- fmt.Errorf("polymarket market ws dial failed: %w", err)
				return
			}

			if err := conn.WriteMessage(websocket.TextMessage, rawPayload); err != nil {
				_ = conn.Close()
				delay := backoff.Duration(reconnectAttempt)
				log.Printf("polymarket market ws subscribe failed: %v; reconnecting in %s", err, delay)
				if err := retry.Wait(ctx, delay); err != nil {
					return
				}
				reconnectAttempt++
				continue
			}

			reconnectAttempt = 1
			err = streamMarketFrames(ctx, conn, updates)
			_ = conn.Close()
			if err == nil || ctx.Err() != nil {
				return
			}

			delay := backoff.Duration(reconnectAttempt)
			log.Printf("polymarket market ws read failed: %v; reconnecting in %s", err, delay)
			if err := retry.Wait(ctx, delay); err != nil {
				return
			}
			reconnectAttempt++
		}
	}()

	return updates, errs
}

func streamMarketFrames(ctx context.Context, conn *websocket.Conn, updates chan<- domain.TokenPriceUpdate) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_, frame, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("polymarket market ws read failed: %w", err)
		}

		for _, upd := range parseTokenUpdates(frame) {
			updates <- upd
		}
	}
}

func parseTokenUpdates(payload []byte) []domain.TokenPriceUpdate {
	var body any
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil
	}

	out := make([]domain.TokenPriceUpdate, 0, 4)
	walkFrames(body, payload, &out)
	return out
}

func walkFrames(node any, rawPayload []byte, out *[]domain.TokenPriceUpdate) {
	switch typed := node.(type) {
	case []any:
		for _, item := range typed {
			walkFrames(item, rawPayload, out)
		}
	case map[string]any:
		tokenID := findString(typed, "asset_id", "assetId", "token_id", "tokenId")
		price, hasPrice := findFloat(typed, "price", "p", "mid", "last_price")
		if tokenID != "" && hasPrice {
			ts := time.Now()
			if unixMs, ok := findInt64(typed, "timestamp", "ts", "time"); ok && unixMs > 0 {
				ts = time.UnixMilli(unixMs)
			}
			*out = append(*out, domain.TokenPriceUpdate{
				TokenID:    tokenID,
				Price:      price,
				Timestamp:  ts,
				RawPayload: string(rawPayload),
			})
		}
		for _, v := range typed {
			walkFrames(v, rawPayload, out)
		}
	}
}

func findString(body map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := body[key]; ok {
			s := strings.TrimSpace(fmt.Sprintf("%v", value))
			if s != "" {
				return s
			}
		}
	}
	return ""
}

func findFloat(body map[string]any, keys ...string) (float64, bool) {
	for _, key := range keys {
		value, ok := body[key]
		if !ok {
			continue
		}
		s := strings.TrimSpace(fmt.Sprintf("%v", value))
		if s == "" {
			continue
		}
		f, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return f, true
		}
	}
	return 0, false
}

func findInt64(body map[string]any, keys ...string) (int64, bool) {
	for _, key := range keys {
		value, ok := body[key]
		if !ok {
			continue
		}
		s := strings.TrimSpace(fmt.Sprintf("%v", value))
		if s == "" {
			continue
		}
		f, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return int64(f), true
		}
	}
	return 0, false
}
