package application

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/polytradings/data-ingestion/internal/domain"
	"github.com/polytradings/data-ingestion/internal/ports"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type WatchTokenPricesUseCase struct {
	marketConsumer       ports.MarketEventConsumer
	marketFeed           ports.TokenMarketFeed
	publisher            ports.MessagePublisher
	tokenSubjectPattern  string
	marketCreatedSubject string
	marketExpiredSubject string
}

func NewWatchTokenPricesUseCase(
	marketConsumer ports.MarketEventConsumer,
	marketFeed ports.TokenMarketFeed,
	publisher ports.MessagePublisher,
	tokenSubjectPattern string,
	marketCreatedSubject string,
	marketExpiredSubject string,
) *WatchTokenPricesUseCase {
	return &WatchTokenPricesUseCase{
		marketConsumer:       marketConsumer,
		marketFeed:           marketFeed,
		publisher:            publisher,
		tokenSubjectPattern:  tokenSubjectPattern,
		marketCreatedSubject: marketCreatedSubject,
		marketExpiredSubject: marketExpiredSubject,
	}
}

func (u *WatchTokenPricesUseCase) Execute(ctx context.Context) error {
	activeMarkets := map[string]domain.ActiveMarket{}
	tokenToBinding := map[string]tokenBinding{}
	var streamCancel context.CancelFunc
	var updates <-chan domain.TokenPriceUpdate
	var errs <-chan error
	lastTokenSetKey := ""

	refreshFeed := func(force bool) {
		tokens := collectSortedTokenIDs(activeMarkets)
		nextSetKey := strings.Join(tokens, ",")
		if !force && nextSetKey == lastTokenSetKey {
			return
		}

		if streamCancel != nil {
			streamCancel()
			streamCancel = nil
		}

		lastTokenSetKey = nextSetKey
		tokenToBinding = bindTokens(activeMarkets)

		if len(tokens) == 0 {
			updates = nil
			errs = nil
			log.Printf("market feed paused (no active token IDs)")
			return
		}

		streamCtx, cancel := context.WithCancel(ctx)
		streamCancel = cancel
		updates, errs = u.marketFeed.Stream(streamCtx, tokens)
		log.Printf("market feed subscribed token_count=%d", len(tokens))
	}

	createdCh, err := u.marketConsumer.SubscribeMarketInfo(ctx, u.marketCreatedSubject)
	if err != nil {
		return fmt.Errorf("subscribe market created: %w", err)
	}
	expiredCh, err := u.marketConsumer.SubscribeMarketInfo(ctx, u.marketExpiredSubject)
	if err != nil {
		return fmt.Errorf("subscribe market expired: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			if streamCancel != nil {
				streamCancel()
			}
			return nil
		case marketInfo, ok := <-createdCh:
			if !ok {
				createdCh = nil
				if expiredCh == nil {
					log.Printf("all market event channels closed, token-ingestion will no longer receive market updates")
				}
				continue
			}
			market := protoToActiveMarket(marketInfo)
			activeMarkets[market.MarketID] = market
			refreshFeed(false)
			log.Printf("market registered market=%s", market.MarketID)
		case marketInfo, ok := <-expiredCh:
			if !ok {
				expiredCh = nil
				if createdCh == nil {
					log.Printf("all market event channels closed, token-ingestion will no longer receive market updates")
				}
				continue
			}
			delete(activeMarkets, marketInfo.MarketId)
			refreshFeed(false)
			log.Printf("market removed market=%s", marketInfo.MarketId)
		case err, ok := <-errs:
			if !ok {
				errs = nil
				updates = nil
				refreshFeed(true)
				continue
			}
			if err != nil {
				log.Printf("market feed stream error: %v", err)
			}
			refreshFeed(true)
		case update, ok := <-updates:
			if !ok {
				refreshFeed(true)
				continue
			}
			publishTokenUpdate(ctx, u.publisher, u.tokenSubjectPattern, activeMarkets, tokenToBinding, update)
		}
	}
}

func protoToActiveMarket(m *proto.MarketInfo) domain.ActiveMarket {
	return domain.ActiveMarket{
		MarketID:         m.MarketId,
		ConditionID:      m.ConditionId,
		Crypto:           domain.Crypto{MinName: m.CryptoSymbol},
		TimeframeMinutes: int(m.TimeframeMinutes),
		UpTokenID:        m.UpTokenId,
		DownTokenID:      m.DownTokenId,
		StartTime:        time.UnixMilli(m.StartUnixMs).UTC(),
		EndTime:          time.UnixMilli(m.EndUnixMs).UTC(),
		Closed:           m.Closed,
	}
}

func publishTokenUpdate(
	ctx context.Context,
	publisher ports.MessagePublisher,
	tokenSubjectPattern string,
	activeMarkets map[string]domain.ActiveMarket,
	tokenToBinding map[string]tokenBinding,
	update domain.TokenPriceUpdate,
) {
	binding, ok := tokenToBinding[update.TokenID]
	if !ok {
		return
	}
	market, ok := activeMarkets[binding.marketID]
	if !ok {
		return
	}
	if !time.Now().UTC().Before(market.EndTime) {
		return
	}

	subject := fmt.Sprintf(tokenSubjectPattern, sanitizeSubjectToken(market.MarketID))
	payload := &proto.TokenPriceTick{
		Source:          "polymarket",
		MarketId:        market.MarketID,
		ConditionId:     market.ConditionID,
		TokenId:         update.TokenID,
		Side:            binding.side,
		Price:           update.Price,
		TimestampUnixMs: update.Timestamp.UnixMilli(),
	}
	if err := publisher.PublishTokenPriceTick(ctx, subject, payload); err != nil {
		log.Printf("token tick publish failed subject=%s market=%s token=%s: %v", subject, market.MarketID, update.TokenID, err)
		return
	}
	log.Printf(
		"published token market=%s token=%s side=%s price=%.8f subject=%s",
		market.MarketID,
		update.TokenID,
		binding.side,
		update.Price,
		subject,
	)
}

