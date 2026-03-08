package natsadapter

import (
	"context"
	"time"

	"github.com/polytradings/data-ingestion/internal/proto"
)

type NoopCryptoHistoryProvider struct{}

func NewNoopCryptoHistoryProvider() *NoopCryptoHistoryProvider {
	return &NoopCryptoHistoryProvider{}
}

func (n *NoopCryptoHistoryProvider) LoadTicks(context.Context, string, time.Time, time.Time) ([]*proto.CryptoPriceTick, error) {
	return nil, nil
}
