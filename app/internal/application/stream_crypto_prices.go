package application

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/polytradings/data-ingestion/internal/domain"
	"github.com/polytradings/data-ingestion/internal/ports"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type StreamCryptoPricesUseCase struct {
	feed           ports.CryptoPriceFeed
	publisher      ports.MessagePublisher
	subjectPattern string
}

func NewStreamCryptoPricesUseCase(feed ports.CryptoPriceFeed, publisher ports.MessagePublisher, subjectPattern string) *StreamCryptoPricesUseCase {
	return &StreamCryptoPricesUseCase{
		feed:           feed,
		publisher:      publisher,
		subjectPattern: subjectPattern,
	}
}

func (u *StreamCryptoPricesUseCase) Execute(ctx context.Context, symbols []string) error {
	priceCh, errCh := u.feed.Stream(ctx, symbols)

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errCh:
			log.Printf("error")
			if err != nil {
				return fmt.Errorf("stream closed with error: %w", err)
			}
		case tick, ok := <-priceCh:
			if !ok {
				return fmt.Errorf("price stream ended unexpectedly")
			}
			if !shouldPublishCryptoTick(tick) {
				continue
			}
			normalizedSymbol := normalizeCryptoSymbol(tick.Symbol)
			if normalizedSymbol == "" {
				continue
			}
			tick.Symbol = normalizedSymbol

			topicAsset := topicAssetFromSymbol(tick.Symbol)
			subject := fmt.Sprintf(u.subjectPattern, topicAsset)
			if err := u.publisher.PublishCryptoPriceTick(ctx, subject, toProtoTick(tick)); err != nil {
				return fmt.Errorf("publish price tick: %w", err)
			}
			log.Printf("published %s %s %.8f subject=%s", tick.Source, tick.Symbol, tick.Price, subject)
		}
	}
}

func toProtoTick(tick domain.PriceTick) *proto.CryptoPriceTick {
	return &proto.CryptoPriceTick{
		Source:          tick.Source,
		Symbol:          tick.Symbol,
		Price:           tick.Price,
		TimestampUnixMs: tick.Timestamp.UnixMilli(),
	}
}

func topicAssetFromSymbol(symbol string) string {
	value := strings.ToLower(normalizeCryptoSymbol(symbol))
	return value
}

func normalizeCryptoSymbol(symbol string) string {
	value := strings.ToUpper(strings.TrimSpace(symbol))
	if value == "" {
		return ""
	}

	if idx := strings.Index(value, "/"); idx > 0 {
		return strings.TrimSpace(value[:idx])
	}

	for _, suffix := range []string{"USDT", "USDC", "USD", "PERP"} {
		if strings.HasSuffix(value, suffix) && len(value) > len(suffix) {
			return value[:len(value)-len(suffix)]
		}
	}
	return value
}

func shouldPublishCryptoTick(tick domain.PriceTick) bool {
	return tick.Price != 0
}
