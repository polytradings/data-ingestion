package ports

import (
	"context"

	"github.com/polytradings/data-ingestion/internal/domain"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type CryptoPriceFeed interface {
	Stream(ctx context.Context, symbols []string) (<-chan domain.PriceTick, <-chan error)
}

type MarketCatalogProvider interface {
	LookupMarketBySlug(ctx context.Context, slug string) (domain.ActiveMarket, bool, error)
}

type TokenMarketFeed interface {
	Stream(ctx context.Context, tokenIDs []string) (<-chan domain.TokenPriceUpdate, <-chan error)
}

type MessagePublisher interface {
	PublishCryptoPriceTick(ctx context.Context, subject string, tick *proto.CryptoPriceTick) error
	PublishTokenPriceTick(ctx context.Context, subject string, tick *proto.TokenPriceTick) error
	PublishMarketCreated(ctx context.Context, subject string, market *proto.MarketCreated) error
}

type MarketRegistry interface {
	Upsert(market domain.MarketTokens)
	Delete(marketID string)
	Snapshot() []domain.MarketTokens
}
