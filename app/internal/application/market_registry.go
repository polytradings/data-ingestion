package application

import (
	"sync"

	"github.com/polytradings/data-ingestion/internal/domain"
)

type InMemoryMarketRegistry struct {
	mu      sync.RWMutex
	markets map[string]domain.MarketTokens
}

func NewInMemoryMarketRegistry() *InMemoryMarketRegistry {
	return &InMemoryMarketRegistry{markets: make(map[string]domain.MarketTokens)}
}

func (r *InMemoryMarketRegistry) Upsert(market domain.MarketTokens) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.markets[market.MarketID] = market
}

func (r *InMemoryMarketRegistry) Delete(marketID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.markets, marketID)
}

func (r *InMemoryMarketRegistry) Snapshot() []domain.MarketTokens {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]domain.MarketTokens, 0, len(r.markets))
	for _, market := range r.markets {
		out = append(out, market)
	}
	return out
}
