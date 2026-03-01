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
	feed            ports.CryptoPriceFeed
	publisher       ports.MessagePublisher
	subjectTemplate string
}

func NewStreamCryptoPricesUseCase(feed ports.CryptoPriceFeed, publisher ports.MessagePublisher, subjectTemplate string) *StreamCryptoPricesUseCase {
	return &StreamCryptoPricesUseCase{
		feed:            feed,
		publisher:       publisher,
		subjectTemplate: subjectTemplate,
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
			topicAsset := topicAssetFromSymbol(tick.Symbol)
			subject := fmt.Sprintf(u.subjectTemplate, topicAsset)
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
	value := strings.ToLower(strings.TrimSpace(symbol))
	for _, suffix := range []string{"usdt", "usdc", "usd", "perp"} {
		if strings.HasSuffix(value, suffix) && len(value) > len(suffix) {
			return value[:len(value)-len(suffix)]
		}
	}
	return value
}
