package binance

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

type Feed struct {
	baseURL string
	backoff retry.Backoff
}

func NewFeed(baseURL string, backoff retry.Backoff) *Feed {
	if backoff.InitialDelay <= 0 || backoff.MaxDelay <= 0 || backoff.Multiplier <= 1 {
		backoff = retry.DefaultBackoff()
	}
	return &Feed{baseURL: strings.TrimSuffix(baseURL, "/"), backoff: backoff}
}

type combinedStreamMessage struct {
	Stream string           `json:"stream"`
	Data   binanceTradeData `json:"data"`
}

type binanceTradeData struct {
	EventType string      `json:"e"`
	Symbol    string      `json:"s"`
	Price     string      `json:"p"`
	EventTime json.Number `json:"E"`
}

func (f *Feed) Stream(ctx context.Context, symbols []string) (<-chan domain.PriceTick, <-chan error) {
	prices := make(chan domain.PriceTick, 1024)
	errs := make(chan error, 1)

	go func() {
		defer close(prices)
		defer close(errs)

		streams := make([]string, 0, len(symbols))
		for _, s := range symbols {
			streams = append(streams, strings.ToLower(s)+"@trade")
		}

		// Build WebSocket URL with streams parameter
		wsURL := f.baseURL
		if strings.Contains(wsURL, "?") {
			wsURL += "&streams=" + strings.Join(streams, "/")
		} else {
			wsURL += "?streams=" + strings.Join(streams, "/")
		}

		log.Printf("Link do Websocket=%s", wsURL)

		reconnectAttempt := 1
		backoff := f.backoff

		for {
			if ctx.Err() != nil {
				return
			}

			conn, err := retry.DialWebSocketWithRetry(ctx, websocket.DefaultDialer, wsURL, "binance", backoff)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				errs <- fmt.Errorf("binance dial failed: %w", err)
				return
			}

			err = streamBinanceMessages(ctx, conn, prices)
			_ = conn.Close()
			if err == nil || ctx.Err() != nil {
				return
			}

			delay := backoff.Duration(reconnectAttempt)
			log.Printf("binance websocket stream error=%v; reconnecting in %s", err, delay)
			if err := retry.Wait(ctx, delay); err != nil {
				return
			}
			reconnectAttempt++
		}
	}()

	return prices, errs
}

func streamBinanceMessages(ctx context.Context, conn *websocket.Conn, prices chan<- domain.PriceTick) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		_, payload, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("binance read failed: %w", err)
		}

		var msg combinedStreamMessage
		if err := json.Unmarshal(payload, &msg); err != nil {
			log.Printf("error ao decerializar: %s", err)
			continue
		}

		price, err := strconv.ParseFloat(msg.Data.Price, 64)
		if err != nil {
			continue
		}

		timestamp, err := msg.Data.EventTime.Int64()
		if err != nil {
			timestamp = time.Now().UnixMilli()
		}

		datetime := time.UnixMilli(timestamp)

		prices <- domain.PriceTick{
			Source:         "binance",
			InstrumentType: domain.InstrumentTypeCrypto,
			Symbol:         strings.ToUpper(msg.Data.Symbol),
			Price:          price,
			Timestamp:      datetime,
		}
	}
}
