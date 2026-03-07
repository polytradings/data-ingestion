package application

import (
	"context"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"github.com/polytradings/data-ingestion/internal/ports"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type TrackPriceToBeatUseCase struct {
	marketConsumer       ports.MarketEventConsumer
	cryptoConsumer       ports.CryptoPriceConsumer
	externalProvider     ports.PriceToBeatExternalProvider
	stateStore           ports.PriceToBeatStateStore
	publisher            ports.MessagePublisher
	priceToBeatSubject   string
	marketCreatedSubject string
	marketExpiredSubject string
	cryptoSubjectPattern string
	reconcileDelay       time.Duration
	publishThresholdBps  float64
	openGracePeriod      time.Duration
}

type trackedPriceToBeatMarket struct {
	market         *proto.MarketInfo
	lastPrice      float64
	lastSource     string
	lastMethod     string
	lastConfidence float64
	revision       int32
	finalized      bool
}

func NewTrackPriceToBeatUseCase(
	marketConsumer ports.MarketEventConsumer,
	cryptoConsumer ports.CryptoPriceConsumer,
	externalProvider ports.PriceToBeatExternalProvider,
	stateStore ports.PriceToBeatStateStore,
	publisher ports.MessagePublisher,
	priceToBeatSubject string,
	marketCreatedSubject string,
	marketExpiredSubject string,
	cryptoSubjectPattern string,
	reconcileDelay time.Duration,
	publishThresholdBps float64,
	openGracePeriod time.Duration,
) *TrackPriceToBeatUseCase {
	return &TrackPriceToBeatUseCase{
		marketConsumer:       marketConsumer,
		cryptoConsumer:       cryptoConsumer,
		externalProvider:     externalProvider,
		stateStore:           stateStore,
		publisher:            publisher,
		priceToBeatSubject:   priceToBeatSubject,
		marketCreatedSubject: marketCreatedSubject,
		marketExpiredSubject: marketExpiredSubject,
		cryptoSubjectPattern: cryptoSubjectPattern,
		reconcileDelay:       reconcileDelay,
		publishThresholdBps:  publishThresholdBps,
		openGracePeriod:      openGracePeriod,
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
			state := &trackedPriceToBeatMarket{market: m}
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

			if price, found, err := u.externalProvider.LookupReferencePrice(ctx, m.MarketId); err != nil {
				log.Printf("price-to-beat bootstrap lookup failed market=%s: %v", m.MarketId, err)
			} else if found {
				u.publish(ctx, state, price, "external_bootstrap", "gamma_reference", 0.55, false)
			}
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

				confidence := 0.45
				method := "stream_first_tick"
				if tickTs.After(startTs.Add(u.openGracePeriod)) {
					confidence = 0.35
					method = "stream_delayed_open"
				}
				u.publish(ctx, state, tick.Price, "stream_estimated", method, confidence, false)
			}
		}
		if createdCh == nil && expiredCh == nil && cryptoCh == nil {
			return nil
		}
	}
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
