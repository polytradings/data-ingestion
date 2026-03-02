package application

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/polytradings/data-ingestion/internal/domain"
	"github.com/polytradings/data-ingestion/internal/ports"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type WatchMarketTokensUseCase struct {
	marketProvider       ports.MarketCatalogProvider
	marketFeed           ports.TokenMarketFeed
	publisher            ports.MessagePublisher
	cryptos              []domain.Crypto
	marketTypes          []domain.MarketType
	tokenSubjectTemplate string
	marketCreatedSubject string
	discoverInterval     time.Duration
}

func NewWatchMarketTokensUseCase(
	marketProvider ports.MarketCatalogProvider,
	marketFeed ports.TokenMarketFeed,
	publisher ports.MessagePublisher,
	cryptos []domain.Crypto,
	marketTypes []domain.MarketType,
	tokenSubjectTemplate string,
	marketCreatedSubject string,
	discoverInterval time.Duration,
) *WatchMarketTokensUseCase {
	return &WatchMarketTokensUseCase{
		marketProvider:       marketProvider,
		marketFeed:           marketFeed,
		publisher:            publisher,
		cryptos:              cryptos,
		marketTypes:          marketTypes,
		tokenSubjectTemplate: tokenSubjectTemplate,
		marketCreatedSubject: marketCreatedSubject,
		discoverInterval:     discoverInterval,
	}
}

type tokenBinding struct {
	marketID string
	side     string
}

func (u *WatchMarketTokensUseCase) Execute(ctx context.Context) error {
	discoverTicker := time.NewTicker(u.discoverInterval)
	defer discoverTicker.Stop()
	expiryTicker := time.NewTicker(1 * time.Second)
	defer expiryTicker.Stop()

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

	discoverAndPrune := func(now time.Time) {
		pruned := pruneExpiredMarkets(activeMarkets, now)
		if pruned > 0 {
			log.Printf("pruned expired markets count=%d", pruned)
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

		refreshFeed(false)
	}

	discoverAndPrune(time.Now().UTC())

	for {
		select {
		case <-ctx.Done():
			if streamCancel != nil {
				streamCancel()
			}
			return nil
		case <-discoverTicker.C:
			discoverAndPrune(time.Now().UTC())
		case <-expiryTicker.C:
			if pruneExpiredMarkets(activeMarkets, time.Now().UTC()) > 0 {
				refreshFeed(false)
			}
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
			u.publishTokenUpdate(ctx, activeMarkets, tokenToBinding, update)
		}
	}
}

func (u *WatchMarketTokensUseCase) publishMarketCreated(ctx context.Context, market domain.ActiveMarket) error {
	payload := &proto.MarketCreated{
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
		Closed:             market.Closed,
	}
	if err := u.publisher.PublishMarketCreated(ctx, u.marketCreatedSubject, payload); err != nil {
		return err
	}

	// Also publish the market discovered event for the aggregator
	discoveredPayload := &proto.MarketDiscovered{
		MarketId:           market.MarketID,
		CryptoSymbol:       market.Crypto.MinName,
		UpTokenId:          market.UpTokenID,
		DownTokenId:        market.DownTokenID,
		DiscoveredAtUnixMs: time.Now().UnixMilli(),
		StartUnixMs:        market.StartTime.UnixMilli(),
		EndUnixMs:          market.EndTime.UnixMilli(),
	}
	return u.publisher.PublishMarketDiscovered(ctx, "market.discovered", discoveredPayload)
}

func (u *WatchMarketTokensUseCase) publishTokenUpdate(
	ctx context.Context,
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

	subject := fmt.Sprintf(u.tokenSubjectTemplate, sanitizeSubjectToken(market.MarketID))
	payload := &proto.TokenPriceTick{
		Source:          "polymarket",
		MarketId:        market.MarketID,
		ConditionId:     market.ConditionID,
		TokenId:         update.TokenID,
		Side:            binding.side,
		Price:           update.Price,
		TimestampUnixMs: update.Timestamp.UnixMilli(),
	}
	if err := u.publisher.PublishTokenPriceTick(ctx, subject, payload); err != nil {
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

func pruneExpiredMarkets(active map[string]domain.ActiveMarket, now time.Time) int {
	removed := 0
	for key, market := range active {
		if market.Closed || !now.Before(market.EndTime) {
			delete(active, key)
			removed++
		}
	}
	return removed
}

func collectSortedTokenIDs(markets map[string]domain.ActiveMarket) []string {
	unique := map[string]struct{}{}
	for _, market := range markets {
		if token := strings.TrimSpace(market.UpTokenID); token != "" {
			unique[token] = struct{}{}
		}
		if token := strings.TrimSpace(market.DownTokenID); token != "" {
			unique[token] = struct{}{}
		}
	}

	out := make([]string, 0, len(unique))
	for tokenID := range unique {
		out = append(out, tokenID)
	}
	sort.Strings(out)
	return out
}

func bindTokens(markets map[string]domain.ActiveMarket) map[string]tokenBinding {
	out := make(map[string]tokenBinding, len(markets)*2)
	for _, market := range markets {
		if token := strings.TrimSpace(market.UpTokenID); token != "" {
			out[token] = tokenBinding{marketID: market.MarketID, side: "UP"}
		}
		if token := strings.TrimSpace(market.DownTokenID); token != "" {
			out[token] = tokenBinding{marketID: market.MarketID, side: "DOWN"}
		}
	}
	return out
}

func marketWindow(now time.Time, marketType domain.MarketType) (time.Time, time.Time) {
	if marketType == domain.MarketTypeSixtyMinutes {
		ny := mustLoadLocationOrUTC("America/New_York")
		inNY := now.In(ny).Truncate(time.Hour)
		start := time.Date(inNY.Year(), inNY.Month(), inNY.Day(), inNY.Hour(), 0, 0, 0, ny)
		end := start.Add(time.Hour)
		return start.UTC(), end.UTC()
	}

	minutes := marketType.Minutes()
	truncated := now.UTC().Truncate(time.Duration(minutes) * time.Minute)
	return truncated, truncated.Add(time.Duration(minutes) * time.Minute)
}

func buildMarketSlug(crypto domain.Crypto, marketType domain.MarketType, start time.Time) string {
	if marketType == domain.MarketTypeSixtyMinutes {
		ny := mustLoadLocationOrUTC("America/New_York")
		inNY := start.In(ny)
		month := strings.ToLower(inNY.Month().String())
		day := inNY.Day()
		hour12 := inNY.Format("3")
		ampm := strings.ToLower(inNY.Format("PM"))

		return fmt.Sprintf(
			"%s-up-or-down-%s-%d-%s%s-et",
			strings.ToLower(crypto.FullName),
			month,
			day,
			hour12,
			ampm,
		)
	}

	return fmt.Sprintf(
		"%s-updown-%dm-%d",
		strings.ToLower(crypto.MinName),
		marketType.Minutes(),
		start.UTC().Unix(),
	)
}

func sanitizeSubjectToken(input string) string {
	value := strings.ToLower(strings.TrimSpace(input))
	value = strings.ReplaceAll(value, ".", "_")
	value = strings.ReplaceAll(value, "*", "_")
	value = strings.ReplaceAll(value, ">", "_")
	value = strings.ReplaceAll(value, " ", "_")
	return value
}

func mustLoadLocationOrUTC(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil || loc == nil {
		return time.UTC
	}
	return loc
}
