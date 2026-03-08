package application

import (
	"context"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/polytradings/data-ingestion/internal/ports"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type TrackPriceToBeatUseCase struct {
	marketConsumer       ports.MarketEventConsumer
	cryptoConsumer       ports.CryptoPriceConsumer
	externalProvider     ports.PriceToBeatExternalProvider
	openPriceProvider    ports.OpenPriceProvider
	historyProvider      ports.CryptoPriceHistoryProvider
	stateStore           ports.PriceToBeatStateStore
	publisher            ports.MessagePublisher
	priceToBeatSubject   string
	marketCreatedSubject string
	marketExpiredSubject string
	cryptoSubjectPattern string
	reconcileDelay       time.Duration
	publishThresholdBps  float64
	openGracePeriod      time.Duration
	window               time.Duration
	updateCooldown       time.Duration
	polymarketWeight     float64
}

type recentTick struct {
	timestamp time.Time
	source    string
	price     float64
}

type trackedPriceToBeatMarket struct {
	market               *proto.MarketInfo
	openPrice            float64
	lastPrice            float64
	lastSource           string
	lastMethod           string
	lastConfidence       float64
	revision             int32
	finalized            bool
	lastDynamicPublishAt time.Time
	recentTicks          []recentTick
	window               time.Duration
}

func NewTrackPriceToBeatUseCase(
	marketConsumer ports.MarketEventConsumer,
	cryptoConsumer ports.CryptoPriceConsumer,
	externalProvider ports.PriceToBeatExternalProvider,
	openPriceProvider ports.OpenPriceProvider,
	historyProvider ports.CryptoPriceHistoryProvider,
	stateStore ports.PriceToBeatStateStore,
	publisher ports.MessagePublisher,
	priceToBeatSubject string,
	marketCreatedSubject string,
	marketExpiredSubject string,
	cryptoSubjectPattern string,
	reconcileDelay time.Duration,
	publishThresholdBps float64,
	openGracePeriod time.Duration,
	window time.Duration,
	updateCooldown time.Duration,
	polymarketWeight float64,
) *TrackPriceToBeatUseCase {
	return &TrackPriceToBeatUseCase{
		marketConsumer:       marketConsumer,
		cryptoConsumer:       cryptoConsumer,
		externalProvider:     externalProvider,
		openPriceProvider:    openPriceProvider,
		historyProvider:      historyProvider,
		stateStore:           stateStore,
		publisher:            publisher,
		priceToBeatSubject:   priceToBeatSubject,
		marketCreatedSubject: marketCreatedSubject,
		marketExpiredSubject: marketExpiredSubject,
		cryptoSubjectPattern: cryptoSubjectPattern,
		reconcileDelay:       reconcileDelay,
		publishThresholdBps:  publishThresholdBps,
		openGracePeriod:      openGracePeriod,
		window:               window,
		updateCooldown:       updateCooldown,
		polymarketWeight:     polymarketWeight,
	}
}

func (u *TrackPriceToBeatUseCase) Execute(ctx context.Context) error {
	createdCh, err := u.marketConsumer.SubscribeMarketInfo(ctx, u.marketCreatedSubject)
	if err != nil {
		return fmt.Errorf("subscribe market created: %w", err)
	}
	expiredCh, err := u.marketConsumer.SubscribeMarketInfo(ctx, u.marketExpiredSubject)
	if err != nil {
		return fmt.Errorf("subscribe market expired: %w", err)
	}
	cryptoCh, err := u.cryptoConsumer.SubscribeCryptoPriceTick(ctx, subjectPatternToWildcard(u.cryptoSubjectPattern))
	if err != nil {
		return fmt.Errorf("subscribe crypto prices: %w", err)
	}

	markets := make(map[string]*trackedPriceToBeatMarket)
	marketsBySymbol := make(map[string]map[string]struct{})
	reconcileCh := make(chan string, 256)

	scheduleReconcile := func(marketID string) {
		go func() {
			timer := time.NewTimer(u.reconcileDelay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
			case <-timer.C:
				select {
				case reconcileCh <- marketID:
				case <-ctx.Done():
				}
			}
		}()
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case m, ok := <-createdCh:
			if !ok {
				createdCh = nil
				continue
			}
			state := &trackedPriceToBeatMarket{market: m, window: u.window}
			if restored, found, err := u.stateStore.Load(ctx, m.MarketId); err == nil && found {
				state.lastPrice = restored.PriceToBeat
				state.lastConfidence = restored.Confidence
				state.lastMethod = restored.Method
				state.lastSource = restored.Source
				state.revision = restored.Revision
				state.finalized = restored.Finalized
			}
			markets[m.MarketId] = state
			symbol := strings.ToLower(strings.TrimSpace(m.CryptoSymbol))
			if marketsBySymbol[symbol] == nil {
				marketsBySymbol[symbol] = map[string]struct{}{}
			}
			marketsBySymbol[symbol][m.MarketId] = struct{}{}

			u.bootstrapOpenPrice(ctx, state)
		case m, ok := <-expiredCh:
			if !ok {
				expiredCh = nil
				continue
			}
			if state, found := markets[m.MarketId]; found {
				state.market.Closed = true
				scheduleReconcile(m.MarketId)
			}
		case marketID := <-reconcileCh:
			state, found := markets[marketID]
			if !found || state.finalized {
				continue
			}
			price, foundPrice, err := u.externalProvider.LookupReferencePrice(ctx, marketID)
			if err != nil {
				log.Printf("price-to-beat final reconcile failed market=%s: %v", marketID, err)
				continue
			}
			if foundPrice {
				u.publish(ctx, state, price, "finalized_gamma", "gamma_final", 1, true)
			}
			_ = u.stateStore.Delete(ctx, marketID)
			delete(markets, marketID)
			symbol := strings.ToLower(strings.TrimSpace(state.market.CryptoSymbol))
			delete(marketsBySymbol[symbol], marketID)
		case tick, ok := <-cryptoCh:
			if !ok {
				cryptoCh = nil
				continue
			}
			symbol := strings.ToLower(strings.TrimSpace(tick.Symbol))
			ids := marketsBySymbol[symbol]
			for marketID := range ids {
				state := markets[marketID]
				if state == nil || state.finalized {
					continue
				}
				if time.Now().UTC().After(time.UnixMilli(state.market.EndUnixMs).UTC().Add(u.reconcileDelay)) {
					continue
				}

				tickTs := time.UnixMilli(tick.TimestampUnixMs).UTC()
				startTs := time.UnixMilli(state.market.StartUnixMs).UTC()
				if tickTs.Before(startTs.Add(-u.openGracePeriod)) {
					continue
				}

				state.addRecentTick(tick)
				u.publishWeightedEstimate(ctx, state)
			}
		}
		if createdCh == nil && expiredCh == nil && cryptoCh == nil {
			return nil
		}
	}
}

func (u *TrackPriceToBeatUseCase) bootstrapOpenPrice(ctx context.Context, state *trackedPriceToBeatMarket) {
	startTs := time.UnixMilli(state.market.StartUnixMs).UTC().Truncate(time.Minute)
	symbol := strings.ToLower(strings.TrimSpace(state.market.CryptoSymbol))

	if u.historyProvider != nil {
		ticks, err := u.historyProvider.LoadTicks(ctx, symbol, startTs.Add(-u.openGracePeriod), startTs.Add(u.openGracePeriod))
		if err != nil {
			log.Printf("history load failed market=%s: %v", state.market.MarketId, err)
		} else if price, found := selectClosestBinanceTick(ticks, startTs, u.openGracePeriod); found {
			state.openPrice = price
			u.publish(ctx, state, price, "binance_open", "jetstream_binance_open_tick", 0.9, false)
			return
		}
	}

	if u.openPriceProvider != nil {
		if price, found, err := u.openPriceProvider.LookupOpenPrice(ctx, symbol, startTs); err != nil {
			log.Printf("binance open price lookup failed market=%s: %v", state.market.MarketId, err)
		} else if found {
			state.openPrice = price
			u.publish(ctx, state, price, "binance_open", "binance_historical_kline", 0.85, false)
			return
		}
	}

	if price, found, err := u.externalProvider.LookupReferencePrice(ctx, state.market.MarketId); err == nil && found {
		state.openPrice = price
		u.publish(ctx, state, price, "external_bootstrap", "gamma_reference", 0.55, false)
	} else if err != nil {
		log.Printf("price-to-beat bootstrap lookup failed market=%s: %v", state.market.MarketId, err)
	}
}

func (u *TrackPriceToBeatUseCase) publishWeightedEstimate(ctx context.Context, state *trackedPriceToBeatMarket) {
	if state.openPrice <= 0 {
		return
	}
	now := time.Now().UTC()
	if !state.lastDynamicPublishAt.IsZero() && now.Sub(state.lastDynamicPublishAt) < u.updateCooldown {
		return
	}
	price, confidence, ok := u.weightedPriceFromWindow(state)
	if !ok {
		return
	}
	u.publish(ctx, state, price, "stream_estimated", "weighted_window_delta", confidence, false)
	state.lastDynamicPublishAt = now
}

func (u *TrackPriceToBeatUseCase) weightedPriceFromWindow(state *trackedPriceToBeatMarket) (float64, float64, bool) {
	var polySum, binanceSum float64
	var polyCount, binanceCount int
	for _, item := range state.recentTicks {
		switch strings.ToLower(item.source) {
		case "polymarket":
			polySum += item.price
			polyCount++
		case "binance":
			binanceSum += item.price
			binanceCount++
		}
	}
	if binanceCount == 0 {
		return 0, 0, false
	}
	binanceMean := binanceSum / float64(binanceCount)
	if binanceMean <= 0 {
		return 0, 0, false
	}
	if polyCount == 0 {
		return state.openPrice, 0.4, true
	}
	polyMean := polySum / float64(polyCount)
	deltaRatio := (polyMean - binanceMean) / binanceMean
	weightedDelta := deltaRatio * u.polymarketWeight
	candidate := state.openPrice * (1 + weightedDelta)
	if candidate <= 0 {
		return 0, 0, false
	}
	confidence := 0.45 + math.Min(0.4, float64(polyCount+binanceCount)/200)
	return candidate, confidence, true
}

func (s *trackedPriceToBeatMarket) addRecentTick(tick *proto.CryptoPriceTick) {
	if tick == nil || tick.Price <= 0 {
		return
	}
	now := time.Now().UTC()
	tickTs := time.UnixMilli(tick.TimestampUnixMs).UTC()
	if tickTs.After(now.Add(5 * time.Second)) {
		return
	}
	s.recentTicks = append(s.recentTicks, recentTick{timestamp: tickTs, source: tick.Source, price: tick.Price})
	window := s.window
	if window <= 0 {
		window = 10 * time.Minute
	}
	cutoff := now.Add(-window)
	idx := sort.Search(len(s.recentTicks), func(i int) bool {
		return !s.recentTicks[i].timestamp.Before(cutoff)
	})
	if idx > 0 {
		s.recentTicks = append([]recentTick(nil), s.recentTicks[idx:]...)
	}
}

func selectClosestBinanceTick(ticks []*proto.CryptoPriceTick, target time.Time, maxDistance time.Duration) (float64, bool) {
	var (
		bestPrice float64
		bestDelta time.Duration
		found     bool
	)
	for _, tick := range ticks {
		if tick == nil || tick.Price <= 0 || !strings.EqualFold(tick.Source, "binance") {
			continue
		}
		delta := time.UnixMilli(tick.TimestampUnixMs).UTC().Sub(target)
		if delta < 0 {
			delta = -delta
		}
		if delta > maxDistance {
			continue
		}
		if !found || delta < bestDelta {
			found = true
			bestDelta = delta
			bestPrice = tick.Price
		}
	}
	return bestPrice, found
}

func (u *TrackPriceToBeatUseCase) publish(
	ctx context.Context,
	state *trackedPriceToBeatMarket,
	price float64,
	source string,
	method string,
	confidence float64,
	finalized bool,
) {
	if price <= 0 {
		return
	}
	if state.lastPrice > 0 {
		deltaBps := math.Abs((price-state.lastPrice)/state.lastPrice) * 10000
		sameMetadata := state.lastSource == source && state.lastMethod == method && state.finalized == finalized
		if deltaBps < u.publishThresholdBps && sameMetadata {
			return
		}
	}
	state.lastPrice = price
	state.lastSource = source
	state.lastMethod = method
	state.lastConfidence = confidence
	state.finalized = finalized
	state.revision++

	subject := fmt.Sprintf(u.priceToBeatSubject, sanitizeSubjectToken(state.market.MarketId))
	payload := &proto.PriceToBeat{
		Source:           source,
		MarketId:         state.market.MarketId,
		ConditionId:      state.market.ConditionId,
		CryptoSymbol:     state.market.CryptoSymbol,
		TimeframeMinutes: state.market.TimeframeMinutes,
		PriceToBeat:      price,
		Method:           method,
		Confidence:       confidence,
		ComputedAtUnixMs: time.Now().UTC().UnixMilli(),
		Revision:         state.revision,
		Finalized:        finalized,
	}
	if err := u.publisher.PublishPriceToBeat(ctx, subject, payload); err != nil {
		log.Printf("price-to-beat publish failed market=%s err=%v", state.market.MarketId, err)
		return
	}
	storeTTL := time.Until(time.UnixMilli(state.market.EndUnixMs).UTC().Add(24 * time.Hour))
	if storeTTL < time.Hour {
		storeTTL = time.Hour
	}
	if err := u.stateStore.Save(ctx, state.market.MarketId, payload, storeTTL); err != nil {
		log.Printf("price-to-beat state save failed market=%s err=%v", state.market.MarketId, err)
	}
	log.Printf("published price-to-beat market=%s price=%f method=%s source=%s revision=%d", state.market.MarketId, price, method, source, state.revision)
}

func subjectPatternToWildcard(pattern string) string {
	value := strings.TrimSpace(pattern)
	if value == "" {
		return "crypto.prices.*.v1"
	}
	return strings.ReplaceAll(value, "%s", "*")
}
