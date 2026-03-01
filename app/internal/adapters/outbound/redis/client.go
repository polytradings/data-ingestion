package redis

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	rdb *redis.Client
}

type MarketState struct {
	MarketId       string
	CryptoSymbol   string
	CryptoPrice    float64
	UpTokenId      string
	UpTokenPrice   float64
	DownTokenId    string
	DownTokenPrice float64
	LastUpdatedBy  string
}

func NewClient(redisURL string) (*Client, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}

	// Increase connection timeout
	opts.ConnMaxLifetime = 0
	opts.PoolSize = 10

	rdb := redis.NewClient(opts)

	// Test connection with longer timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = rdb.Ping(ctx).Err()
	if err != nil {
		return nil, err
	}

	log.Printf("connected to redis: %s", redisURL)
	return &Client{rdb: rdb}, nil
}

func (c *Client) GetMarketState(ctx context.Context, marketId string) (*MarketState, error) {
	data, err := c.rdb.Get(ctx, "market:"+marketId).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var state MarketState
	err = json.Unmarshal([]byte(data), &state)
	if err != nil {
		return nil, err
	}

	return &state, nil
}

func (c *Client) SetMarketState(ctx context.Context, state *MarketState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return c.rdb.Set(ctx, "market:"+state.MarketId, data, 0).Err()
}

func (c *Client) RegisterMarketTokens(ctx context.Context, marketId, cryptoSymbol, upTokenId, downTokenId string) error {
	state := &MarketState{
		MarketId:     marketId,
		CryptoSymbol: cryptoSymbol,
		UpTokenId:    upTokenId,
		DownTokenId:  downTokenId,
	}
	return c.SetMarketState(ctx, state)
}

func (c *Client) GetMarketsByToken(ctx context.Context, tokenId string) ([]string, error) {
	pattern := "market:*"
	iter := c.rdb.Scan(ctx, 0, pattern, 0).Iterator()

	var marketIds []string
	for iter.Next(ctx) {
		key := iter.Val()
		marketId := key[7:] // remove "market:" prefix
		state, err := c.GetMarketState(ctx, marketId)
		if err != nil {
			continue
		}
		if state != nil && (state.UpTokenId == tokenId || state.DownTokenId == tokenId) {
			marketIds = append(marketIds, marketId)
		}
	}

	return marketIds, iter.Err()
}

func (c *Client) GetMarketsByCryptoSymbol(ctx context.Context, cryptoSymbol string) ([]string, error) {
	symbol := strings.ToLower(cryptoSymbol) // Normalize to lowercase
	pattern := "market:*"
	iter := c.rdb.Scan(ctx, 0, pattern, 0).Iterator()

	var marketIds []string
	for iter.Next(ctx) {
		key := iter.Val()
		marketId := key[7:] // remove "market:" prefix
		state, err := c.GetMarketState(ctx, marketId)
		if err != nil {
			continue
		}
		if state != nil && strings.ToLower(state.CryptoSymbol) == symbol {
			marketIds = append(marketIds, marketId)
		}
	}

	return marketIds, iter.Err()
}

func (c *Client) Close() error {
	return c.rdb.Close()
}
