package ports

import (
	"context"
	"time"

	"github.com/polytradings/data-ingestion/internal/proto"
)

type PriceToBeatExternalProvider interface {
	LookupReferencePrice(ctx context.Context, marketID string) (price float64, found bool, err error)
}

type PriceToBeatStateStore interface {
	Load(ctx context.Context, marketID string) (*proto.PriceToBeat, bool, error)
	Save(ctx context.Context, marketID string, state *proto.PriceToBeat, ttl time.Duration) error
	Delete(ctx context.Context, marketID string) error
}
