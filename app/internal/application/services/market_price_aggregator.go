package services

import (
	"context"
	"log"
	"time"

	"github.com/polytradings/data-ingestion/internal/adapters/outbound/redis"
	"github.com/polytradings/data-ingestion/internal/proto"
)

type MarketPriceAggregator struct {
	redisClient *redis.Client
	publishChan chan *proto.MarketAggregatedPrice
}

func NewMarketPriceAggregator(redisClient *redis.Client, publishChan chan *proto.MarketAggregatedPrice) *MarketPriceAggregator {
	return &MarketPriceAggregator{
		redisClient: redisClient,
		publishChan: publishChan,
	}
}

// HandleMarketDiscovered registers a new market and its token associations
func (a *MarketPriceAggregator) HandleMarketDiscovered(ctx context.Context, market *proto.MarketDiscovered) error {
	log.Printf("market discovered: market_id=%s crypto_symbol=%s up_token=%s down_token=%s",
		market.MarketId, market.CryptoSymbol, market.UpTokenId, market.DownTokenId)

	return a.redisClient.RegisterMarketTokens(ctx, market.MarketId, market.CryptoSymbol, market.UpTokenId, market.DownTokenId, market.StartUnixMs, market.EndUnixMs)
}

// HandleCryptoPrice updates crypto price and publishes aggregated prices for all related markets
func (a *MarketPriceAggregator) HandleCryptoPrice(ctx context.Context, cryptoPrice *proto.CryptoPriceTick) error {
	if cryptoPrice.Price == 0 {
		return nil // Ignore zero prices
	}

	// Find all markets with this crypto
	marketIds, err := a.redisClient.GetMarketsByCryptoSymbol(ctx, cryptoPrice.Symbol)
	if err != nil {
		log.Printf("error getting markets for crypto %s: %v", cryptoPrice.Symbol, err)
		return nil
	}

	// Update and publish each market's aggregated price
	for _, marketId := range marketIds {
		state, err := a.redisClient.GetMarketState(ctx, marketId)
		if err != nil {
			log.Printf("error getting market state %s: %v", marketId, err)
			continue
		}

		if state == nil {
			continue
		}

		// Update crypto price
		state.CryptoPrice = cryptoPrice.Price
		state.LastUpdatedBy = "crypto"

		err = a.redisClient.SetMarketState(ctx, state)
		if err != nil {
			log.Printf("error saving market state %s: %v", marketId, err)
			continue
		}

		// Publish aggregated price
		a.publishAggregatedPrice(state)
	}

	return nil
}

// HandleTokenPrice updates token price and publishes aggregated prices
func (a *MarketPriceAggregator) HandleTokenPrice(ctx context.Context, tokenPrice *proto.TokenPriceTick) error {
	if tokenPrice.Price == 0 {
		return nil // Ignore zero prices
	}

	// Find all markets with this token
	marketIds, err := a.redisClient.GetMarketsByToken(ctx, tokenPrice.TokenId)
	if err != nil {
		log.Printf("error getting markets for token %s: %v", tokenPrice.TokenId, err)
		return nil
	}

	// Update and publish each market's aggregated price
	for _, marketId := range marketIds {
		state, err := a.redisClient.GetMarketState(ctx, marketId)
		if err != nil {
			log.Printf("error getting market state %s: %v", marketId, err)
			continue
		}

		if state == nil {
			continue
		}

		// Update token price based on side
		if tokenPrice.Side == "UP" && state.UpTokenId == tokenPrice.TokenId {
			state.UpTokenPrice = tokenPrice.Price
			state.LastUpdatedBy = "token"
		} else if tokenPrice.Side == "DOWN" && state.DownTokenId == tokenPrice.TokenId {
			state.DownTokenPrice = tokenPrice.Price
			state.LastUpdatedBy = "token"
		}

		err = a.redisClient.SetMarketState(ctx, state)
		if err != nil {
			log.Printf("error saving market state %s: %v", marketId, err)
			continue
		}

		// Publish aggregated price
		a.publishAggregatedPrice(state)
	}

	return nil
}

func (a *MarketPriceAggregator) publishAggregatedPrice(state *redis.MarketState) {
	msg := &proto.MarketAggregatedPrice{
		MarketId:        state.MarketId,
		CryptoSymbol:    state.CryptoSymbol,
		CryptoPrice:     state.CryptoPrice,
		UpTokenId:       state.UpTokenId,
		UpTokenPrice:    state.UpTokenPrice,
		DownTokenId:     state.DownTokenId,
		DownTokenPrice:  state.DownTokenPrice,
		TimestampUnixMs: time.Now().UnixMilli(),
		LastUpdatedBy:   state.LastUpdatedBy,
		StartUnixMs:     state.StartUnixMs,
		EndUnixMs:       state.EndUnixMs,
	}

	select {
	case a.publishChan <- msg:
		log.Printf("published aggregated price: market_id=%s crypto=%.8f up=%.8f down=%.8f",
			msg.MarketId, msg.CryptoPrice, msg.UpTokenPrice, msg.DownTokenPrice)
	default:
		log.Printf("publish channel full for market %s, skipping", state.MarketId)
	}
}
