package application

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/polytradings/data-ingestion/internal/domain"
	"github.com/polytradings/data-ingestion/internal/ports"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type DiscoverMarketsUseCase struct {
	marketProvider       ports.MarketCatalogProvider
	publisher            ports.MessagePublisher
	cryptos              []domain.Crypto
	marketTypes          []domain.MarketType
	marketCreatedSubject string
	marketExpiredSubject string
	discoverInterval     time.Duration
}

func NewDiscoverMarketsUseCase(
	marketProvider ports.MarketCatalogProvider,
	publisher ports.MessagePublisher,
	cryptos []domain.Crypto,
	marketTypes []domain.MarketType,
	marketCreatedSubject string,
	marketExpiredSubject string,
	discoverInterval time.Duration,
) *DiscoverMarketsUseCase {
	return &DiscoverMarketsUseCase{
		marketProvider:       marketProvider,
		publisher:            publisher,
		cryptos:              cryptos,
		marketTypes:          marketTypes,
		marketCreatedSubject: marketCreatedSubject,
		marketExpiredSubject: marketExpiredSubject,
		discoverInterval:     discoverInterval,
	}
}

func (u *DiscoverMarketsUseCase) Execute(ctx context.Context) error {
	discoverTicker := time.NewTicker(u.discoverInterval)
	defer discoverTicker.Stop()
	expiryTicker := time.NewTicker(1 * time.Second)
	defer expiryTicker.Stop()

	activeMarkets := map[string]domain.ActiveMarket{}

	discoverAndPrune := func(now time.Time) {
		expired := pruneExpiredMarketsWithResult(activeMarkets, now)
		for _, market := range expired {
			log.Printf("market expired market=%s", market.MarketID)
			if err := u.publishMarketExpired(ctx, market); err != nil {
				log.Printf("market expired publish failed market=%s: %v", market.MarketID, err)
			}
		}

		for _, crypto := range u.cryptos {
			for _, marketType := range u.marketTypes {
				startTime, endTime := marketWindow(now, marketType)
				if !now.Before(endTime) {
					continue
				}
				slug := buildMarketSlug(crypto, marketType, startTime)
				if _, ok := activeMarkets[slug]; ok {
					continue
				}

				market, found, err := u.marketProvider.LookupMarketBySlug(ctx, slug)
				if err != nil {
					log.Printf("market lookup failed slug=%s: %v", slug, err)
					continue
				}
				if !found || market.Closed {
					continue
				}

				market.MarketID = slug
				market.Crypto = crypto
				market.TimeframeMinutes = marketType.Minutes()
				market.StartTime = startTime
				market.EndTime = endTime

				if strings.TrimSpace(market.UpTokenID) == "" || strings.TrimSpace(market.DownTokenID) == "" {
					log.Printf("market without both token ids ignored market=%s", slug)
					continue
				}

				activeMarkets[slug] = market
				if err := u.publishMarketCreated(ctx, market); err != nil {
					log.Printf("market created publish failed market=%s: %v", slug, err)
				}
				log.Printf("market discovered market=%s condition=%s timeframe=%dm", slug, market.ConditionID, marketType.Minutes())
			}
		}
	}

	discoverAndPrune(time.Now().UTC())

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-discoverTicker.C:
			discoverAndPrune(time.Now().UTC())
		case <-expiryTicker.C:
			now := time.Now().UTC()
			expired := pruneExpiredMarketsWithResult(activeMarkets, now)
			for _, market := range expired {
				log.Printf("market expired market=%s", market.MarketID)
				if err := u.publishMarketExpired(ctx, market); err != nil {
					log.Printf("market expired publish failed market=%s: %v", market.MarketID, err)
				}
			}
		}
	}
}

func (u *DiscoverMarketsUseCase) publishMarketCreated(ctx context.Context, market domain.ActiveMarket) error {
	payload := marketToProto(market, false)
	return u.publisher.PublishMarketInfo(ctx, u.marketCreatedSubject, payload)
}

func (u *DiscoverMarketsUseCase) publishMarketExpired(ctx context.Context, market domain.ActiveMarket) error {
	payload := marketToProto(market, true)
	return u.publisher.PublishMarketInfo(ctx, u.marketExpiredSubject, payload)
}

func marketToProto(market domain.ActiveMarket, closed bool) *proto.MarketInfo {
	return &proto.MarketInfo{
		Source:             "polymarket",
		MarketId:           market.MarketID,
		ConditionId:        market.ConditionID,
		CryptoSymbol:       market.Crypto.MinName,
		TimeframeMinutes:   int32(market.TimeframeMinutes),
		UpTokenId:          market.UpTokenID,
		DownTokenId:        market.DownTokenID,
		StartUnixMs:        market.StartTime.UnixMilli(),
		EndUnixMs:          market.EndTime.UnixMilli(),
		DiscoveredAtUnixMs: time.Now().UnixMilli(),
		Closed:             closed,
	}
}

// pruneExpiredMarketsWithResult removes expired markets and returns them.
func pruneExpiredMarketsWithResult(active map[string]domain.ActiveMarket, now time.Time) []domain.ActiveMarket {
	var removed []domain.ActiveMarket
	for key, market := range active {
		if market.Closed || !now.Before(market.EndTime) {
			removed = append(removed, market)
			delete(active, key)
		}
	}
	return removed
}
