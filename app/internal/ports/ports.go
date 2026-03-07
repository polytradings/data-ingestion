package ports

import (
	"context"
	"time"

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
	PublishMarketInfo(ctx context.Context, subject string, market *proto.MarketInfo) error
	PublishPriceToBeat(ctx context.Context, subject string, payload *proto.PriceToBeat) error
}

type MarketEventConsumer interface {
	SubscribeMarketInfo(ctx context.Context, subject string) (<-chan *proto.MarketInfo, error)
}

type CryptoPriceConsumer interface {
	SubscribeCryptoPriceTick(ctx context.Context, subject string) (<-chan *proto.CryptoPriceTick, error)
}

type PriceToBeatExternalProvider interface {
	LookupReferencePrice(ctx context.Context, marketID string) (price float64, found bool, err error)
}

type PriceToBeatStateStore interface {
	Load(ctx context.Context, marketID string) (*proto.PriceToBeat, bool, error)
	Save(ctx context.Context, marketID string, state *proto.PriceToBeat, ttl time.Duration) error
	Delete(ctx context.Context, marketID string) error
}
