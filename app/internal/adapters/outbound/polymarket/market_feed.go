package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
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

		err := retry.RunWebSocketSessionWithReconnect(
			ctx,
			f.wsURL,
			"polymarket-market",
			"polymarket market ws dial failed",
			"polymarket market ws read failed",
			f.backoff,
			func(conn *websocket.Conn) error {
				if err := conn.WriteMessage(websocket.TextMessage, rawPayload); err != nil {
					return fmt.Errorf("polymarket market ws subscribe failed: %w", err)
				}
				return streamMarketFrames(ctx, conn, updates)
			},
		)
		if err != nil && ctx.Err() == nil {
			errs <- err
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
		tokenID, _ := valueAsString(typed, "asset_id", "assetId", "token_id", "tokenId")
		price, hasPrice := valueAsFloat64(typed, "price", "p", "mid", "last_price")
		if tokenID != "" && hasPrice {
			ts := time.Now()
			if unixMs, ok := valueAsInt64(typed, "timestamp", "ts", "time"); ok && unixMs > 0 {
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
